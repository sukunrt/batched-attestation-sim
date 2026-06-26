package node

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------------
// E2E helpers — drive the real gossipsub stack over simnet.
// -----------------------------------------------------------------------------

// e2eOpts is the per-node config knobs that vary across E2E scenarios. Defaults
// match the rest of the partial-message simulation: small attestations, tight
// publish loop, partial messages enabled, no fanout.
type e2eOpts struct {
	num                       int
	publishSlot               int                  // 0 → never publishes
	numTopics                 int                  // default 1
	fanout                    bool                 // default false (mesh)
	fanoutTopicIndex          int                  // only used if fanout=true
	gossipsubParams           GossipsubParams      // default tight mesh
	maxPerAttestation         int                  // default 16
	publishInterval           time.Duration        // default 20ms
	disableIHave              bool                 // default false
	numAttestors              int                  // total network size for context
	publishStart              time.Time            // shared across all nodes in a run
	slotDuration              time.Duration        // shared across all nodes in a run
	rpcTracer                 pubsub.RPCTracer     // optional, observe wire-level RPCs
	verifyDelay               func() time.Duration // default 5ms
	divergentAttestorFraction float64              // default 0 (no forks)
	committeeSize             int                  // default 128
	memberships               []TopicMembership    // default: one per topic at position == num
}

func defaultE2EOpts(num, publishSlot int, publishStart time.Time, slotDuration time.Duration, numAttestors int) e2eOpts {
	return e2eOpts{
		num:               num,
		publishSlot:       publishSlot,
		numTopics:         1,
		fanout:            false,
		gossipsubParams:   testGossipsubParams,
		maxPerAttestation: 16,
		publishInterval:   20 * time.Millisecond,
		numAttestors:      numAttestors,
		publishStart:      publishStart,
		slotDuration:      slotDuration,
		verifyDelay:       func() time.Duration { return 5 * time.Millisecond },
		committeeSize:     128,
	}
}

// newE2EPartialNode builds a Node configured for partial-message propagation.
//
// If opts.memberships is nil, defaults to one membership per topic at
// position == opts.num (so node_num doubles as position). Tests using fanout
// must set opts.fanoutTopicIndex; the single membership is placed on that
// topic at position == opts.num.
func newE2EPartialNode(opts e2eOpts, nw *testNetwork, tr *testTracer) *Node {
	publishSlots := map[int]struct{}{}
	if opts.publishSlot > 0 {
		publishSlots[opts.publishSlot] = struct{}{}
	}
	memberships := opts.memberships
	if memberships == nil {
		if opts.fanout {
			memberships = []TopicMembership{{TopicIndex: opts.fanoutTopicIndex, Position: opts.num}}
		} else {
			for i := range opts.numTopics {
				memberships = append(memberships, TopicMembership{TopicIndex: i, Position: opts.num})
			}
		}
	}
	n := &Node{
		Num:                       opts.num,
		PublishSlots:              publishSlots,
		NumTopics:                 opts.numTopics,
		CommitteeMemberships:      memberships,
		Fanout:                    opts.fanout,
		GossipsubParams:           opts.gossipsubParams,
		VerificationDelay:         opts.verifyDelay,
		Host:                      nw.hosts[opts.num],
		Network:                   nw,
		Tracer:                    tr,
		UsePartialMessages:        true,
		AttestationDataSize:       32,
		SignatureSize:             16,
		MaxPeersPerAttestation:    opts.maxPerAttestation,
		DivergentAttestorFraction: opts.divergentAttestorFraction,
		PublishInterval:           opts.publishInterval,
		VerificationBatchWindow:   2 * time.Millisecond,
		DisableIHaveGossip:        opts.disableIHave,
		CommitteeSize:             opts.committeeSize,
		PublishStart:              opts.publishStart,
		SlotDuration:              opts.slotDuration,
	}
	if opts.rpcTracer != nil {
		n.RPCTracer = opts.rpcTracer
	}
	return n
}

// -----------------------------------------------------------------------------
// countingRPCTracer — in-memory wrapper that counts wire RPCs by type.
//
// Implements pubsub.RPCTracer. Tests use it to assert that partial-message
// parts-metadata (the IHAVE/IWANT control envelope) actually flows on the
// wire, without parsing log files.
// -----------------------------------------------------------------------------

type countingRPCTracer struct {
	mu              sync.Mutex
	partialMDSent   int // RPCs sent with non-empty PartsMetadata
	partialMDRecv   int // RPCs received with non-empty PartsMetadata
	partialDataSent int // RPCs sent with non-empty PartialMessage
	partialDataRecv int // RPCs received with non-empty PartialMessage
}

func (c *countingRPCTracer) OnRPCSent(_ peer.ID, _ time.Duration, rpc *pubsub_pb.RPC) {
	p := rpc.GetPartial()
	if p == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(p.GetPartsMetadata()) > 0 {
		c.partialMDSent++
	}
	if len(p.GetPartialMessage()) > 0 {
		c.partialDataSent++
	}
}

func (c *countingRPCTracer) OnRPCReceived(_ peer.ID, _ time.Duration, rpc *pubsub_pb.RPC) {
	p := rpc.GetPartial()
	if p == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(p.GetPartsMetadata()) > 0 {
		c.partialMDRecv++
	}
	if len(p.GetPartialMessage()) > 0 {
		c.partialDataRecv++
	}
}

func (c *countingRPCTracer) OnPeerRTT(peer.ID, string, time.Duration, string, string) {}
func (c *countingRPCTracer) OnMeshSize(string, int)                                   {}
func (c *countingRPCTracer) Close() error                                             { return nil }

func (c *countingRPCTracer) snapshot() (mdSent, mdRecv, dataSent, dataRecv int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.partialMDSent, c.partialMDRecv, c.partialDataSent, c.partialDataRecv
}

// runE2E starts all nodes, wires the supplied peer graph (entries: src→[dsts]),
// joins topics, sleeps until publishStart, then runs each node for numSlots and
// waits for completion. Returns once everything has stopped.
func runE2E(t *testing.T, ctx context.Context, nodes []*Node, connect map[int][]int, publishStart time.Time, numSlots int, slotDuration time.Duration) {
	t.Helper()

	for _, n := range nodes {
		n.Start(ctx)
	}
	time.Sleep(1 * time.Second)

	for src, dsts := range connect {
		nodes[src].ConnectToPeers(dsts)
	}

	for _, n := range nodes {
		n.JoinTopics()
	}

	// Let the mesh settle before publish.
	time.Sleep(2 * time.Second)
	time.Sleep(time.Until(publishStart))

	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			n.Run(numSlots, slotDuration)
		}(n)
	}
	wg.Wait()
}

// expectAttestation asserts node `n` holds an attestation at committee
// position `from` in slot `slot` on the named topic, either pending validation
// or already validated. Searches every bucket for the slot since the
// publisher's attestation_data may have ended up in any of them under forks.
func expectAttestation(t *testing.T, n *Node, topic string, slot, from int) bool {
	t.Helper()
	ss := n.partial.getSlotState(topic, slot)
	if ss == nil {
		t.Errorf("node %d: missing slot state for slot %d", n.Num, slot)
		return false
	}
	n.partial.mu.Lock()
	defer n.partial.mu.Unlock()
	for _, b := range ss.attestationsMap {
		if _, ok := b.validated[from]; ok {
			return true
		}
		if _, ok := b.validating[from]; ok {
			return true
		}
	}
	t.Errorf("node %d: missing attestation position=%d slot=%d (no bucket has it)", n.Num, from, slot)
	return false
}

// expectAttestationInBucket is the fork-aware variant: it asserts the position
// landed in the bucket keyed by the given attestation_data bytes.
func expectAttestationInBucket(t *testing.T, n *Node, topic string, slot, from int, data []byte) bool {
	t.Helper()
	ss := n.partial.getSlotState(topic, slot)
	if ss == nil {
		t.Errorf("node %d: missing slot state for slot %d", n.Num, slot)
		return false
	}
	n.partial.mu.Lock()
	defer n.partial.mu.Unlock()
	b, ok := ss.attestationsMap[string(hashAttestationData(data))]
	if !ok {
		t.Errorf("node %d: missing bucket for slot %d", n.Num, slot)
		return false
	}
	_, validated := b.validated[from]
	_, validating := b.validating[from]
	if !validated && !validating {
		t.Errorf("node %d: missing attestation position=%d slot=%d in bucket", n.Num, from, slot)
		return false
	}
	return true
}

func expectPartialReceive(t *testing.T, tr *testTracer, slot, topicIndex, position int) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, ev := range tr.partialReceives {
		if ev.slot == slot && ev.topicIndex == topicIndex && ev.position == position {
			return
		}
	}
	t.Errorf("missing partial receive slot=%d topic=%d position=%d", slot, topicIndex, position)
}

func partialReceivePositions(tr *testTracer, slot, topicIndex int) map[int]struct{} {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	positions := make(map[int]struct{})
	for _, ev := range tr.partialReceives {
		if ev.slot == slot && ev.topicIndex == topicIndex {
			positions[ev.position] = struct{}{}
		}
	}
	return positions
}

func partialReceiveDigests(tr *testTracer, slot, topicIndex int) map[string]struct{} {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	digests := make(map[string]struct{})
	for _, ev := range tr.partialReceives {
		if ev.slot == slot && ev.topicIndex == topicIndex {
			digests[ev.attDigest] = struct{}{}
		}
	}
	return digests
}

// -----------------------------------------------------------------------------
// Two nodes
// -----------------------------------------------------------------------------

func TestE2ETwoNodesBidirectionalPropagation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numSlots     = 2
			slotDuration = 1 * time.Second
			numAttestors = 8
		)

		nw := newSimTestNetwork(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)

		nodes := []*Node{
			newE2EPartialNode(defaultE2EOpts(0, 1, publishStart, slotDuration, numAttestors), nw, tr),
			newE2EPartialNode(defaultE2EOpts(1, 2, publishStart, slotDuration, numAttestors), nw, tr),
		}

		runE2E(t, ctx, nodes, map[int][]int{0: {1}}, publishStart, numSlots, slotDuration)

		expectPartialReceive(t, tr, 1, 0, 0)
		expectPartialReceive(t, tr, 2, 0, 1)
	})
}

// -----------------------------------------------------------------------------
// Multi-hop chain — forwarding via mesh peers across 3 hops.
// -----------------------------------------------------------------------------

func TestE2EChainPropagationAllToAll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes     = 4
			numSlots     = 2
			slotDuration = 3 * time.Second
		)

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)

		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			nodes[i] = newE2EPartialNode(defaultE2EOpts(i, 1, publishStart, slotDuration, numNodes), nw, tr)
		}

		runE2E(t, ctx, nodes,
			map[int][]int{0: {1}, 1: {2}, 2: {3}},
			publishStart, numSlots, slotDuration)

		positions := partialReceivePositions(tr, 1, 0)
		for j := range numNodes {
			assert.Contains(t, positions, j)
		}
	})
}

// -----------------------------------------------------------------------------
// Sparse mesh + gossip peers — exercises the IHAVE/IWANT path.
// -----------------------------------------------------------------------------

func TestE2EGossipPathDeliversToNonMeshPeer(t *testing.T) {
	// 8 fully-connected nodes with a small mesh (D=2). Each node has ~5
	// gossip peers for which it must use IHAVE/IWANT to deliver. After two
	// slots every node should observe every other node's attestation.
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes     = 8
			numSlots     = 2
			slotDuration = 2 * time.Second
		)

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)
		tightMesh := GossipsubParams{D: 4, Dlow: 4, Dhigh: 5} // Dhigh must be >= Dscore (4)

		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			opts := defaultE2EOpts(i, 1, publishStart, slotDuration, numNodes)
			opts.gossipsubParams = tightMesh
			opts.publishInterval = 50 * time.Millisecond
			nodes[i] = newE2EPartialNode(opts, nw, tr)
		}

		// Full mesh of connections — but gossipsub keeps only D peers in
		// each topic's mesh, so the rest become gossip peers.
		conn := map[int][]int{}
		for i := range numNodes {
			peers := make([]int, 0, numNodes-1)
			for j := range numNodes {
				if j != i {
					peers = append(peers, j)
				}
			}
			conn[i] = peers
		}

		runE2E(t, ctx, nodes, conn, publishStart, numSlots, slotDuration)

		positions := partialReceivePositions(tr, 1, 0)
		for j := range numNodes {
			assert.Contains(t, positions, j)
		}
	})
}

// -----------------------------------------------------------------------------
// DisableIHaveGossip — propagation must still work via the mesh push path.
// -----------------------------------------------------------------------------

func TestE2EPropagationWithoutIHaveGossip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes     = 4
			numSlots     = 2
			slotDuration = 2 * time.Second
		)

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)

		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			opts := defaultE2EOpts(i, 1, publishStart, slotDuration, numNodes)
			opts.disableIHave = true
			nodes[i] = newE2EPartialNode(opts, nw, tr)
		}

		// Full mesh — with the default D=8, every connected peer is a
		// mesh peer, so the push path is sufficient.
		conn := map[int][]int{}
		for i := range numNodes {
			peers := make([]int, 0, numNodes-1)
			for j := range numNodes {
				if j != i {
					peers = append(peers, j)
				}
			}
			conn[i] = peers
		}

		runE2E(t, ctx, nodes, conn, publishStart, numSlots, slotDuration)

		positions := partialReceivePositions(tr, 1, 0)
		for j := range numNodes {
			assert.Contains(t, positions, j)
		}
	})
}

// -----------------------------------------------------------------------------
// Fanout publisher — node 0 publishes without joining the subscription side.
// -----------------------------------------------------------------------------

func TestE2EFanoutPublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numSlots     = 2
			slotDuration = 2 * time.Second
			numAttestors = 4
		)

		nw := newSimTestNetwork(t, 3)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)

		opts0 := defaultE2EOpts(0, 1, publishStart, slotDuration, numAttestors)
		opts0.fanout = true
		opts0.fanoutTopicIndex = 0
		// Fanout publishers don't need a publish loop ticking. Default opts
		// still work; fanout path is taken via Fanout=true.
		nodes := []*Node{
			newE2EPartialNode(opts0, nw, tr),
			newE2EPartialNode(defaultE2EOpts(1, 0, publishStart, slotDuration, numAttestors), nw, tr),
			newE2EPartialNode(defaultE2EOpts(2, 0, publishStart, slotDuration, numAttestors), nw, tr),
		}

		runE2E(t, ctx, nodes,
			map[int][]int{0: {1, 2}, 1: {2}},
			publishStart, numSlots, slotDuration)

		// Nodes 1 and 2 should observe the fanout publisher's slot-1 attestation.
		expectPartialReceive(t, tr, 1, 0, 0)
	})
}

// -----------------------------------------------------------------------------
// Multi-topic — each topic's slot state is independent.
// -----------------------------------------------------------------------------

func TestE2EMultiTopicIndependentState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numTopics    = 2
			numSlots     = 2
			slotDuration = 2 * time.Second
		)

		nw := newSimTestNetwork(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)

		opts0 := defaultE2EOpts(0, 1, publishStart, slotDuration, 2)
		opts0.numTopics = numTopics
		opts1 := defaultE2EOpts(1, 2, publishStart, slotDuration, 2)
		opts1.numTopics = numTopics

		nodes := []*Node{
			newE2EPartialNode(opts0, nw, tr),
			newE2EPartialNode(opts1, nw, tr),
		}

		runE2E(t, ctx, nodes, map[int][]int{0: {1}}, publishStart, numSlots, slotDuration)

		// Each topic should observe both nodes' attestations.
		for i := range numTopics {
			expectPartialReceive(t, tr, 1, i, 0)
			expectPartialReceive(t, tr, 2, i, 1)
		}
	})
}

// -----------------------------------------------------------------------------
// Wire-level: parts-metadata (IHAVE/IWANT) envelopes must flow.
// -----------------------------------------------------------------------------

// TestE2EPartsMetadataObservedOnWire attaches a countingRPCTracer to every
// node and asserts that, after a run with a tight mesh, every node has both
// sent and received at least one RPC carrying a non-empty PartsMetadata
// envelope — i.e. the partial-message gossip path is actually exercised.
func TestE2EPartsMetadataObservedOnWire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes     = 16
			numSlots     = 2
			slotDuration = 2 * time.Second
		)

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)
		tightMesh := GossipsubParams{D: 4, Dlow: 4, Dhigh: 5} // Dhigh must be >= Dscore (4)

		tracers := make([]*countingRPCTracer, numNodes)
		nodes := make([]*Node, numNodes)

		for i := range numNodes {
			tracers[i] = &countingRPCTracer{}
			opts := defaultE2EOpts(i, 1, publishStart, slotDuration, numNodes)
			opts.gossipsubParams = tightMesh
			opts.publishInterval = 50 * time.Millisecond
			opts.rpcTracer = tracers[i]
			nodes[i] = newE2EPartialNode(opts, nw, tr)
		}

		// Full mesh of TCP-level connections; gossipsub keeps D of them as
		// mesh peers and treats the rest as gossip peers per topic.
		conn := map[int][]int{}
		for i := range numNodes {
			peers := make([]int, 0, numNodes-1)
			for j := range numNodes {
				if j != i {
					peers = append(peers, j)
				}
			}
			conn[i] = peers
		}

		runE2E(t, ctx, nodes, conn, publishStart, numSlots, slotDuration)

		for i, tr := range tracers {
			mdSent, mdRecv, _, _ := tr.snapshot()
			assert.Greater(t, mdSent, 0, "node %d: expected >=1 sent RPC with PartsMetadata", i)
			assert.Greater(t, mdRecv, 0, "node %d: expected >=1 received RPC with PartsMetadata", i)
		}
	})
}

// -----------------------------------------------------------------------------
// Fork coexistence — two AttestationData variants must propagate as
// independent buckets at the same slot, with no cross-contamination of
// validated sets.
// -----------------------------------------------------------------------------

func TestE2EForkBucketsCoexist(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes     = 6
			numSlots     = 2
			slotDuration = 2 * time.Second
		)

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)

		// Every node attests in slot 1; with divergentAttestorFraction=0.5
		// each node independently picks data variant 0 or 1.
		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			opts := defaultE2EOpts(i, 1, publishStart, slotDuration, numNodes)
			opts.divergentAttestorFraction = 0.5
			nodes[i] = newE2EPartialNode(opts, nw, tr)
		}

		// Full mesh connectivity so dissemination is fast.
		conn := map[int][]int{}
		for i := range numNodes {
			peers := make([]int, 0, numNodes-1)
			for j := range numNodes {
				if j != i {
					peers = append(peers, j)
				}
			}
			conn[i] = peers
		}

		runE2E(t, ctx, nodes, conn, publishStart, numSlots, slotDuration)

		digests := partialReceiveDigests(tr, 1, 0)
		assert.GreaterOrEqual(t, len(digests), 2, "expected at least two fork buckets to propagate")

		positions := partialReceivePositions(tr, 1, 0)
		assert.Equal(t, numNodes, len(positions), "union of observed positions must include every attestor")
	})
}
