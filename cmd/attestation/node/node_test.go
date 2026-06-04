package node

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// testNetwork is a simnet-backed network shim for Node. Hosts are pre-allocated
// in newSimTestNetwork so Node.Start can look them up by node number without
// touching real sockets. Identities come from nodePrivateKey so connectToPeers'
// peerIDFromNodeNum still resolves correctly.
type testNetwork struct {
	hosts []host.Host
}

func newSimTestNetwork(t *testing.T, count int) *testNetwork {
	t.Helper()
	sim := &simnet.Simnet{LatencyFunc: simnet.StaticLatency(5 * time.Millisecond)}
	linkSettings := simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: 1024 * simlibp2p.OneMbps},
		Uplink:   simnet.LinkSettings{BitsPerSecond: 1024 * simlibp2p.OneMbps},
	}
	hosts := make([]host.Host, count)
	for i := range count {
		addr := fmt.Sprintf("/ip4/%s/udp/8000/quic-v1", simnet.IntToPublicIPv4(i))
		h, err := libp2p.New(
			libp2p.Identity(NodePrivateKey(i)),
			libp2p.ListenAddrStrings(addr),
			simlibp2p.QUICSimnet(sim, linkSettings),
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
	return &testNetwork{hosts: hosts}
}

func (tn *testNetwork) PeerAddr(nodeNum int) ma.Multiaddr {
	return tn.hosts[nodeNum].Addrs()[0]
}

type receivedAtt struct {
	att        *pb.Attestation
	topicIndex int
}

// partialEvent captures a partial-mode publish or receive for inspection.
type partialEvent struct {
	slot       int
	topicIndex int
	position   int
	latencyMs  int64
}

type testTracer struct {
	mu               sync.Mutex
	received         map[int][]receivedAtt // nodeNum -> classic-mode receives
	partialPublishes []partialEvent        // partial-mode self-publishes
	partialReceives  []partialEvent        // partial-mode receives
}

func newTestTracer() *testTracer {
	return &testTracer{received: make(map[int][]receivedAtt)}
}

func (t *testTracer) OnPublish(att *pb.Attestation, topicIndex int) {}

func (t *testTracer) OnReceive(nodeNum int, att *pb.Attestation, topicIndex int, latencyMs int64) {
	clone := proto.Clone(att).(*pb.Attestation)
	t.mu.Lock()
	t.received[nodeNum] = append(t.received[nodeNum], receivedAtt{att: clone, topicIndex: topicIndex})
	t.mu.Unlock()
}

func (t *testTracer) OnPartialPublish(slot, topicIndex, position int, attData []byte) {
	t.mu.Lock()
	t.partialPublishes = append(t.partialPublishes, partialEvent{
		slot: slot, topicIndex: topicIndex, position: position,
	})
	t.mu.Unlock()
}

func (t *testTracer) OnPartialReceive(slot, topicIndex, position int, attData []byte, latencyMs int64) {
	t.mu.Lock()
	t.partialReceives = append(t.partialReceives, partialEvent{
		slot: slot, topicIndex: topicIndex, position: position, latencyMs: latencyMs,
	})
	t.mu.Unlock()
}

var testGossipsubParams = GossipsubParams{D: 8, Dlow: 6, Dhigh: 12}

var testVerificationDelay = func() time.Duration { return 5 * time.Millisecond }

func TestNodeMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numSlots     = 3
			slotDuration = 500 * time.Millisecond
		)

		nw := newSimTestNetwork(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()

		// node 0 publishes in slot 1, node 1 publishes in slot 2
		nodes := []*Node{
			{
				Num:                 0,
				PublishSlots:        map[int]struct{}{1: {}},
				NumTopics:           1,
				AttestationDataSize: 64,
				SignatureSize:       36,
				GossipsubParams:     testGossipsubParams,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
			},
			{
				Num:                 1,
				PublishSlots:        map[int]struct{}{2: {}},
				NumTopics:           1,
				AttestationDataSize: 64,
				SignatureSize:       36,
				GossipsubParams:     testGossipsubParams,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
			},
		}
		for _, n := range nodes {
			n.Host = nw.hosts[n.Num]
			n.Start(ctx)
		}
		time.Sleep(1 * time.Second)

		nodes[0].ConnectToPeers([]int{1})

		for _, n := range nodes {
			n.JoinTopics()
		}

		time.Sleep(1 * time.Second)

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() {
				n.Run(numSlots, slotDuration)
			})
		}
		wg.Wait()

		tr.mu.Lock()
		got0 := tr.received[0]
		got1 := tr.received[1]
		tr.mu.Unlock()

		// node 0 should have received node 1's publish (slot 2)
		if len(got0) < 1 {
			t.Fatalf("node 0: expected at least 1 message, got %d", len(got0))
		}
		if !hasAttestation(got0, 1, 2) {
			t.Fatalf("node 0: missing attestation from=1 slot=2")
		}

		// node 1 should have received node 0's publish (slot 1)
		if len(got1) < 1 {
			t.Fatalf("node 1: expected at least 1 message, got %d", len(got1))
		}
		if !hasAttestation(got1, 0, 1) {
			t.Fatalf("node 1: missing attestation from=0 slot=1")
		}
	})
}

func TestMultipleMessagesPerAttestor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numSlots               = 2
			slotDuration           = 500 * time.Millisecond
			numMessagesPerAttestor = 3
		)

		nw := newSimTestNetwork(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()

		// node 0 publishes in slot 1, node 1 publishes in slot 2
		nodes := []*Node{
			{
				Num:                    0,
				PublishSlots:           map[int]struct{}{1: {}},
				NumTopics:              1,
				AttestationDataSize:    64,
				SignatureSize:          36,
				NumMessagesPerAttestor: numMessagesPerAttestor,
				GossipsubParams:        testGossipsubParams,
				VerificationDelay:      testVerificationDelay,
				Network:                nw,
				Tracer:                 tr,
			},
			{
				Num:                    1,
				PublishSlots:           map[int]struct{}{2: {}},
				NumTopics:              1,
				AttestationDataSize:    64,
				SignatureSize:          36,
				NumMessagesPerAttestor: numMessagesPerAttestor,
				GossipsubParams:        testGossipsubParams,
				VerificationDelay:      testVerificationDelay,
				Network:                nw,
				Tracer:                 tr,
			},
		}
		for _, n := range nodes {
			n.Host = nw.hosts[n.Num]
			n.Start(ctx)
		}
		time.Sleep(1 * time.Second)

		nodes[0].ConnectToPeers([]int{1})
		for _, n := range nodes {
			n.JoinTopics()
		}
		time.Sleep(1 * time.Second)

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() {
				n.Run(numSlots, slotDuration)
			})
		}
		wg.Wait()

		tr.mu.Lock()
		got0 := tr.received[0]
		got1 := tr.received[1]
		tr.mu.Unlock()

		// node 0 should receive numMessagesPerAttestor messages from node 1 (slot 2)
		for msgIdx := range int32(numMessagesPerAttestor) {
			if !hasAttestationMsg(got0, 1, 2, msgIdx) {
				t.Errorf("node 0: missing attestation from=1 slot=2 msg_index=%d", msgIdx)
			}
		}

		// node 1 should receive numMessagesPerAttestor messages from node 0 (slot 1)
		for msgIdx := range int32(numMessagesPerAttestor) {
			if !hasAttestationMsg(got1, 0, 1, msgIdx) {
				t.Errorf("node 1: missing attestation from=0 slot=1 msg_index=%d", msgIdx)
			}
		}
	})
}

func hasAttestationMsg(atts []receivedAtt, nodeNum, slotNum, msgIndex int32) bool {
	for _, r := range atts {
		if r.att.NodeNum == nodeNum && r.att.SlotNum == slotNum && r.att.MsgIndex == msgIndex {
			return true
		}
	}
	return false
}

func hasAttestation(atts []receivedAtt, nodeNum int32, slotNum int32) bool {
	for _, r := range atts {
		if r.att.NodeNum == nodeNum && r.att.SlotNum == slotNum {
			return true
		}
	}
	return false
}

func TestFanoutPublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numSlots     = 3
			slotDuration = 500 * time.Millisecond
		)

		nw := newSimTestNetwork(t, 3)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()

		// node 0 is fanout: publishes in slot 1 but does NOT subscribe
		// nodes 1 and 2 are mesh: subscribe and receive
		nodes := []*Node{
			{
				Num:                  0,
				PublishSlots:         map[int]struct{}{1: {}},
				NumTopics:            1,
				CommitteeMemberships: []TopicMembership{{TopicIndex: 0}},
				AttestationDataSize:  64,
				SignatureSize:        36,
				Fanout:               true,
				GossipsubParams:      testGossipsubParams,
				VerificationDelay:    testVerificationDelay,
				Network:              nw,
				Tracer:               tr,
			},
			{
				Num:                 1,
				PublishSlots:        map[int]struct{}{2: {}},
				NumTopics:           1,
				AttestationDataSize: 64,
				SignatureSize:       36,
				GossipsubParams:     testGossipsubParams,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
			},
			{
				Num:                 2,
				PublishSlots:        map[int]struct{}{3: {}},
				NumTopics:           1,
				AttestationDataSize: 64,
				SignatureSize:       36,
				GossipsubParams:     testGossipsubParams,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
			},
		}
		for _, n := range nodes {
			n.Host = nw.hosts[n.Num]
			n.Start(ctx)
		}
		time.Sleep(1 * time.Second)

		// Connect all nodes
		nodes[0].ConnectToPeers([]int{1, 2})
		nodes[1].ConnectToPeers([]int{2})

		for _, n := range nodes {
			n.JoinTopics()
		}

		time.Sleep(1 * time.Second)

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() {
				n.Run(numSlots, slotDuration)
			})
		}
		wg.Wait()

		tr.mu.Lock()
		got0 := tr.received[0]
		got1 := tr.received[1]
		got2 := tr.received[2]
		tr.mu.Unlock()

		// node 0 is fanout: should NOT receive any messages (no subscription)
		if len(got0) != 0 {
			t.Errorf("fanout node 0: expected 0 messages, got %d", len(got0))
		}

		// node 1 (mesh) should receive node 0's fanout publish (slot 1) and node 2's publish (slot 3)
		if !hasAttestation(got1, 0, 1) {
			t.Errorf("node 1: missing attestation from fanout node 0, slot 1")
		}
		if !hasAttestation(got1, 2, 3) {
			t.Errorf("node 1: missing attestation from node 2, slot 3")
		}

		// node 2 (mesh) should receive node 0's fanout publish (slot 1) and node 1's publish (slot 2)
		if !hasAttestation(got2, 0, 1) {
			t.Errorf("node 2: missing attestation from fanout node 0, slot 1")
		}
		if !hasAttestation(got2, 1, 2) {
			t.Errorf("node 2: missing attestation from node 1, slot 2")
		}
	})
}

func TestTenNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes          = 10
			numSlots          = 5
			peersPerNode      = 4
			publishersPerSlot = 3
			slotDuration      = 300 * time.Millisecond
		)

		rng := rand.New(rand.NewChaCha8([32]byte{42}))

		// assign 3 random publishers per slot
		publishSlots := make([]map[int]struct{}, numNodes)
		for i := range publishSlots {
			publishSlots[i] = make(map[int]struct{})
		}
		for slot := 1; slot <= numSlots; slot++ {
			perm := rng.Perm(numNodes)
			for _, n := range perm[:publishersPerSlot] {
				publishSlots[n][slot] = struct{}{}
			}
		}

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			nodes[i] = &Node{
				Num:                 i,
				PublishSlots:        publishSlots[i],
				NumTopics:           1,
				AttestationDataSize: 64,
				SignatureSize:       36,
				GossipsubParams:     testGossipsubParams,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
			}
			nodes[i].Host = nw.hosts[i]
			nodes[i].Start(ctx)
		}

		// connect each node to 4 random peers
		for i := range numNodes {
			candidates := make([]int, 0, numNodes-1)
			for j := range numNodes {
				if j != i {
					candidates = append(candidates, j)
				}
			}
			rng.Shuffle(len(candidates), func(a, b int) {
				candidates[a], candidates[b] = candidates[b], candidates[a]
			})
			nodes[i].ConnectToPeers(candidates[:peersPerNode])
		}

		for _, n := range nodes {
			n.JoinTopics()
		}

		time.Sleep(2 * time.Second)

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() {
				n.Run(numSlots, slotDuration)
			})
		}
		wg.Wait()

		totalPublished := numSlots * publishersPerSlot
		for i, n := range nodes {
			tr.mu.Lock()
			got := len(tr.received[i])
			tr.mu.Unlock()
			pubCount := len(n.PublishSlots)
			expected := totalPublished - pubCount
			if got < expected {
				t.Errorf("node %d: received %d attestations, expected at least %d", i, got, expected)
			}
		}
	})
}

func TestMultiTopic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numTopics    = 2
			numSlots     = 3
			slotDuration = 500 * time.Millisecond
		)

		nw := newSimTestNetwork(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()

		// node 0 publishes in slot 1, node 1 publishes in slot 2
		// Both are mesh nodes that join all topics
		nodes := []*Node{
			{
				Num:                 0,
				PublishSlots:        map[int]struct{}{1: {}},
				NumTopics:           numTopics,
				AttestationDataSize: 64,
				SignatureSize:       36,
				GossipsubParams:     testGossipsubParams,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
			},
			{
				Num:                 1,
				PublishSlots:        map[int]struct{}{2: {}},
				NumTopics:           numTopics,
				AttestationDataSize: 64,
				SignatureSize:       36,
				GossipsubParams:     testGossipsubParams,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
			},
		}
		for _, n := range nodes {
			n.Host = nw.hosts[n.Num]
			n.Start(ctx)
		}
		time.Sleep(1 * time.Second)

		nodes[0].ConnectToPeers([]int{1})
		for _, n := range nodes {
			n.JoinTopics()
		}
		time.Sleep(1 * time.Second)

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() {
				n.Run(numSlots, slotDuration)
			})
		}
		wg.Wait()

		// Each node should receive the other's messages on BOTH topics
		for _, receiverNode := range []int{0, 1} {
			tr.mu.Lock()
			got := tr.received[receiverNode]
			tr.mu.Unlock()

			senderNode := 1 - receiverNode
			senderSlot := int32(senderNode + 1) // node 0 publishes slot 1, node 1 publishes slot 2

			for topic := range numTopics {
				found := false
				for _, r := range got {
					if r.att.NodeNum == int32(senderNode) && r.att.SlotNum == senderSlot && r.topicIndex == topic {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("node %d: missing attestation from=%d slot=%d topic=%d",
						receiverNode, senderNode, senderSlot, topic)
				}
			}
		}
	})
}
