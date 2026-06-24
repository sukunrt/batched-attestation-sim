package node

import (
	"context"
	"encoding/binary"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// attPropSmokeProto is a placeholder for the real per-topic data protocol ID
// (attestation_push_<topic_id>). The exact naming is a Phase-1 detail.
const attPropSmokeProto = protocol.ID("attestation_push_0")

// writeFrame writes a 4-byte big-endian length prefix followed by payload.
func writeFrame(w io.Writer, payload []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readFrame reads one length-prefixed frame. Returns io.EOF cleanly when the
// stream is closed at a frame boundary (no partial header).
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	buf := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// TestAttPropRawStreamSmoke is the Phase-0 de-risk for the att_propagation
// protocol: prove a custom libp2p protocol with a raw, persistent,
// length-prefixed protobuf stream works over the simnet/QUIC harness under
// synctest — the substrate the whole protocol is built on. Backpressure is
// trusted (simnet runs real libp2p/QUIC, so Write blocks under a full
// flow-control window); what this proves is the custom-protocol + framed
// multi-message streaming path the existing gossipsub tests never exercise.
func TestAttPropRawStreamSmoke(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const numFrames = 3

		nw := newSimTestNetwork(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		got := make(chan []*pb.BatchedAttestationEnvelope, 1)

		// Receiver (host 0): register the protocol, drain every framed message
		// off the persistent stream until the sender half-closes.
		nw.hosts[0].SetStreamHandler(attPropSmokeProto, func(s network.Stream) {
			defer s.Close()
			var envs []*pb.BatchedAttestationEnvelope
			for {
				payload, err := readFrame(s)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Errorf("readFrame: %v", err)
					break
				}
				var env pb.BatchedAttestationEnvelope
				if err := proto.Unmarshal(payload, &env); err != nil {
					t.Errorf("unmarshal: %v", err)
					break
				}
				envs = append(envs, &env)
			}
			got <- envs
		})

		// Sender (host 1): dial host 0, open one persistent stream, write N
		// framed protobuf messages, then half-close.
		peerID, err := PeerIDFromNodeNum(0)
		if err != nil {
			t.Fatalf("peer id: %v", err)
		}
		if err := nw.hosts[1].Connect(ctx, peer.AddrInfo{
			ID:    peerID,
			Addrs: []ma.Multiaddr{nw.PeerAddr(0)},
		}); err != nil {
			t.Fatalf("connect: %v", err)
		}
		s, err := nw.hosts[1].NewStream(ctx, peerID, attPropSmokeProto)
		if err != nil {
			t.Fatalf("new stream: %v", err)
		}
		for i := range numFrames {
			env := &pb.BatchedAttestationEnvelope{
				Batches: []*pb.BatchedAttestation{{
					AttestationData: []byte{byte(i)},
					AttestorIndices: []uint32{uint32(i)},
					Signatures:      [][]byte{{byte(i)}},
				}},
			}
			payload, err := proto.Marshal(env)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if err := writeFrame(s, payload); err != nil {
				t.Fatalf("writeFrame[%d]: %v", i, err)
			}
		}
		if err := s.CloseWrite(); err != nil {
			t.Fatalf("close write: %v", err)
		}

		synctest.Wait()
		select {
		case envs := <-got:
			if len(envs) != numFrames {
				t.Fatalf("got %d framed messages, want %d", len(envs), numFrames)
			}
			for i, env := range envs {
				if len(env.Batches) != 1 || len(env.Batches[0].AttestorIndices) != 1 ||
					env.Batches[0].AttestorIndices[0] != uint32(i) {
					t.Fatalf("frame %d corrupted: %+v", i, env)
				}
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for framed messages")
		}
	})
}
