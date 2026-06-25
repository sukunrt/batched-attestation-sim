package attprop

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/libp2p/go-msgio"
	"github.com/marcopolo/simnet"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// The simnet harness and deterministic-key helpers below are local copies of
// node_test.go's newSimTestNetwork and network.go's NodePrivateKey /
// PeerIDFromNodeNum. They are replicated (not imported) so attprop stays
// self-contained — the package must never import node, even in tests.

// testNodePrivateKey returns a deterministic Ed25519 key for a node number.
func testNodePrivateKey(nodeNum int) crypto.PrivKey {
	var seed [32]byte
	binary.BigEndian.PutUint64(seed[:], uint64(nodeNum))
	r := rand.NewChaCha8(seed)
	key, _, err := crypto.GenerateEd25519Key(r)
	if err != nil {
		log.Fatalf("generate key: %v", err)
	}
	return key
}

func testPeerID(nodeNum int) peer.ID {
	id, err := peer.IDFromPrivateKey(testNodePrivateKey(nodeNum))
	if err != nil {
		log.Fatalf("peer id: %v", err)
	}
	return id
}

type simNet struct{ hosts []host.Host }

func newSimNet(t *testing.T, count int) *simNet {
	t.Helper()
	sim := &simnet.Simnet{LatencyFunc: simnet.StaticLatency(5 * time.Millisecond)}
	link := simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: 1024 * simlibp2p.OneMbps},
		Uplink:   simnet.LinkSettings{BitsPerSecond: 1024 * simlibp2p.OneMbps},
	}
	hosts := make([]host.Host, count)
	for i := range count {
		addr := fmt.Sprintf("/ip4/%s/udp/8000/quic-v1", simnet.IntToPublicIPv4(i))
		h, err := libp2p.New(
			libp2p.Identity(testNodePrivateKey(i)),
			libp2p.ListenAddrStrings(addr),
			simlibp2p.QUICSimnet(sim, link),
			libp2p.DisableIdentifyAddressDiscovery(),
			libp2p.ResourceManager(&libp2pnet.NullResourceManager{}),
		)
		if err != nil {
			t.Fatalf("libp2p.New[%d]: %v", i, err)
		}
		hosts[i] = h
	}
	sim.Start()
	t.Cleanup(func() {
		for _, h := range hosts {
			_ = h.Close()
		}
		sim.Close()
	})
	return &simNet{hosts: hosts}
}

func (sn *simNet) addr(nodeNum int) ma.Multiaddr { return sn.hosts[nodeNum].Addrs()[0] }

// newTestManager builds a Manager wired to a host with a single topic. The
// verifier/tracer are nil — the substrate test exercises only wire.go (handler
// registration, dial, framing, decode), whose code paths never touch them.
func newTestManager(h host.Host) *Manager {
	return New(h, nil, nil, Config{
		Logger:     slog.Default(),
		Topic:      "/eth2/topic0",
		TopicIndex: 0,
	})
}

// waitEvent drains the next event of the wanted concrete type off a manager's
// events channel within synctest, failing on timeout or a different type.
func waitEvent[T event](t *testing.T, m *Manager) T {
	t.Helper()
	select {
	case ev := <-m.events:
		got, ok := ev.(T)
		if !ok {
			t.Fatalf("got event %T, want %T", ev, *new(T))
		}
		return got
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %T", *new(T))
	}
	panic("unreachable")
}

// TestWireSubstrate is the Phase-1 substrate proof for att_propagation: open one
// bidirectional stream per message type between two hosts, send one
// msgio-framed protobuf in each direction on each stream, assert both readers
// post intact inbound events, and assert CloseWrite drives readers to a clean
// exit (peerDownEvent, no error). The eventloop is not running; the test reads
// each manager's events channel directly.
func TestWireSubstrate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw := newSimNet(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// Pick opener (lower peer ID) and receiver per weOpen.
		id0, id1 := testPeerID(0), testPeerID(1)
		opener, receiver := 0, 1
		if !weOpen(id0, id1) {
			opener, receiver = 1, 0
		}
		require.True(t, weOpen(testPeerID(opener), testPeerID(receiver)))

		om := newTestManager(nw.hosts[opener])
		rm := newTestManager(nw.hosts[receiver])

		// Receiver registers its three inbound stream handlers. The opener starts
		// read loops on the streams it dials, so it does not need handlers here.
		rm.Start(ctx)

		// Dial the underlying connection, then drive the opener's ConnectPeer,
		// which opens the three bidirectional streams and posts a peerUpEvent
		// carrying them.
		recvPeerID := testPeerID(receiver)
		require.NoError(t, nw.hosts[opener].Connect(ctx, peer.AddrInfo{
			ID:    recvPeerID,
			Addrs: []ma.Multiaddr{nw.addr(receiver)},
		}))
		om.ConnectPeer(recvPeerID)

		up := waitEvent[peerUpEvent](t, om)
		openerPeerID := testPeerID(opener)
		require.Equal(t, recvPeerID, up.peer)
		require.NotNil(t, up.push)
		require.NotNil(t, up.bitmap)
		require.NotNil(t, up.control)
		rup := waitEvent[peerUpEvent](t, rm)
		require.Equal(t, openerPeerID, rup.peer)
		require.NotNil(t, rup.push)
		require.NotNil(t, rup.bitmap)
		require.NotNil(t, rup.control)
		// PUSH stream: one BatchedAttestationEnvelope in each direction.
		dataEnv := &pb.BatchedAttestationEnvelope{
			Batches: []*pb.BatchedAttestation{{
				AttestationData: []byte{0xaa},
				AttestorIndices: []uint32{7},
				Signatures:      [][]byte{{0xbb}},
			}},
		}
		writeProto(t, up.push, dataEnv)
		gotData := waitEvent[inboundDataEvent](t, rm)
		require.Equal(t, openerPeerID, gotData.from)
		require.Len(t, gotData.env.Batches, 1)
		require.Equal(t, []uint32{7}, gotData.env.Batches[0].AttestorIndices)
		require.Equal(t, []byte{0xaa}, gotData.env.Batches[0].AttestationData)
		writeProto(t, rup.push, dataEnv)
		gotData = waitEvent[inboundDataEvent](t, om)
		require.Equal(t, recvPeerID, gotData.from)
		require.Len(t, gotData.env.Batches, 1)
		require.Equal(t, []uint32{7}, gotData.env.Batches[0].AttestorIndices)
		require.Equal(t, []byte{0xaa}, gotData.env.Batches[0].AttestationData)

		// BITMAP stream: one ControlEnvelope (available-only metadata) each way.
		bmEnv := &pb.ControlEnvelope{
			Metadatas: []*pb.CommitteeAttestationPartsMetadata{{
				Slot:            3,
				AttestationData: []byte{0xaa},
				Available:       []byte{0x80},
			}},
		}
		writeProto(t, up.bitmap, bmEnv)
		gotBM := waitEvent[inboundBitmapEvent](t, rm)
		require.Equal(t, openerPeerID, gotBM.from)
		require.Len(t, gotBM.ctrl.Metadatas, 1)
		require.EqualValues(t, 3, gotBM.ctrl.Metadatas[0].Slot)
		require.Equal(t, []byte{0x80}, gotBM.ctrl.Metadatas[0].Available)
		writeProto(t, rup.bitmap, bmEnv)
		gotBM = waitEvent[inboundBitmapEvent](t, om)
		require.Equal(t, recvPeerID, gotBM.from)
		require.Len(t, gotBM.ctrl.Metadatas, 1)
		require.EqualValues(t, 3, gotBM.ctrl.Metadatas[0].Slot)
		require.Equal(t, []byte{0x80}, gotBM.ctrl.Metadatas[0].Available)

		// CONTROL stream: one AttPropControl (graft/prune) each way.
		ctrlMsg := &pb.AttPropControl{
			Items: []*pb.AttPropControlItem{
				{Op: pb.AttPropMeshOp_GRAFT, Mesh: pb.AttPropMesh_PUSH},
				{Op: pb.AttPropMeshOp_PRUNE, Mesh: pb.AttPropMesh_BITMAP},
			},
		}
		writeProto(t, up.control, ctrlMsg)
		gotCtrl := waitEvent[inboundControlEvent](t, rm)
		require.Equal(t, openerPeerID, gotCtrl.from)
		require.Len(t, gotCtrl.ctrl.Items, 2)
		require.Equal(t, pb.AttPropMeshOp_GRAFT, gotCtrl.ctrl.Items[0].Op)
		require.Equal(t, pb.AttPropMesh_PUSH, gotCtrl.ctrl.Items[0].Mesh)
		require.Equal(t, pb.AttPropMesh_BITMAP, gotCtrl.ctrl.Items[1].Mesh)
		writeProto(t, rup.control, ctrlMsg)
		gotCtrl = waitEvent[inboundControlEvent](t, om)
		require.Equal(t, recvPeerID, gotCtrl.from)
		require.Len(t, gotCtrl.ctrl.Items, 2)
		require.Equal(t, pb.AttPropMeshOp_GRAFT, gotCtrl.ctrl.Items[0].Op)
		require.Equal(t, pb.AttPropMesh_PUSH, gotCtrl.ctrl.Items[0].Mesh)
		require.Equal(t, pb.AttPropMesh_BITMAP, gotCtrl.ctrl.Items[1].Mesh)

		// Half-close every opener stream; each receiver reader should hit EOF and
		// post exactly one peerDownEvent — a clean exit, no Reset/error. The reader
		// closes its local write side, so the opener readers exit too.
		require.NoError(t, up.push.CloseWrite())
		require.NoError(t, up.bitmap.CloseWrite())
		require.NoError(t, up.control.CloseWrite())

		for range 3 {
			down := waitEvent[peerDownEvent](t, rm)
			require.Equal(t, openerPeerID, down.peer)
		}
		for range 3 {
			down := waitEvent[peerDownEvent](t, om)
			require.Equal(t, recvPeerID, down.peer)
		}

		synctest.Wait()
	})
}

func writeProto(t *testing.T, s libp2pnet.Stream, msg proto.Message) {
	t.Helper()
	b, err := proto.Marshal(msg)
	require.NoError(t, err)
	w := msgio.NewVarintWriter(s)
	require.NoError(t, w.WriteMsg(b))
}
