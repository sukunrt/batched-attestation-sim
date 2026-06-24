package node

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// -----------------------------------------------------------------------------
// Unit helpers — priority variants of the partial-message test helpers. The
// shared bucket/peer types (AttestationState, peerState) and `collected` /
// makePeers / peerAcceptsPartial helpers are reused from partial_unit_test.go.
// -----------------------------------------------------------------------------

// newPriorityUnitManager builds a partial-priority manager with a backing
// batchVerifier suitable for unit tests. MaxPeersPerAttestation is set high so
// the lifetime ceiling never interferes unless a test lowers it before seeding.
func newPriorityUnitManager(t *testing.T) *priorityAttestationManager {
	t.Helper()
	node := &Node{
		MaxPeersPerAttestation:     64,
		MaxAttestationsPerMessage:  30,
		CommitteeSize:              testCommitteeSize,
		VerificationDelay:          func() time.Duration { return 5 * time.Millisecond },
		PerAttestationVerification: 0,
		VerificationBatchWindow:    2 * time.Millisecond,
	}
	node.verifier = newBatchVerifier(
		node.VerificationDelay,
		node.PerAttestationVerification,
		node.VerificationBatchWindow,
		slog.Default(),
	)
	go node.verifier.run()
	t.Cleanup(func() { node.verifier.stop() })
	return newPriorityAttestationManager(node, time.Now(), time.Second, nil)
}

// runPriorityPublishActions invokes the manager's publishActions iterator and
// decodes every emitted PublishAction, collecting them per peer IN YIELD ORDER
// (a peer may receive several chunked actions).
func runPriorityPublishActions(
	t *testing.T,
	m *priorityAttestationManager,
	topic string,
	slot int,
	peerStates map[peer.ID]peerState,
	peerRequestsPartial func(peer.ID) bool,
) map[peer.ID][]collected {
	t.Helper()
	fn := m.publishActions(topic, slot)
	if fn == nil {
		return nil
	}
	out := map[peer.ID][]collected{}
	for p, action := range fn(peerStates, peerRequestsPartial) {
		c := collected{rawCtrl: action.EncodedPartsMetadata, rawParts: action.EncodedPartialMessage}
		if len(action.EncodedPartsMetadata) > 0 {
			c.ctrl = &pb.ControlEnvelope{}
			require.NoError(t, proto.Unmarshal(action.EncodedPartsMetadata, c.ctrl))
		}
		if len(action.EncodedPartialMessage) > 0 {
			c.payload = &pb.BatchedAttestationEnvelope{}
			require.NoError(t, proto.Unmarshal(action.EncodedPartialMessage, c.payload))
		}
		out[p] = append(out[p], c)
	}
	return out
}

// seedValidated seeds a bucket with validated positions at the given sendCount
// values and registers each in the priority index — the state a real
// publishLocal/markValidated would leave behind. Positions are inserted in
// ascending order for deterministic within-level ordering.
func seedValidated(m *priorityAttestationManager, topic string, slot int, data []byte, sendCounts map[int]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ss := m.getOrCreateSlotState(topic, slot)
	b := getOrCreateBucket(ss, data)
	for _, pos := range slices.Sorted(maps.Keys(sendCounts)) {
		sc := sendCounts[pos]
		b.attestations[pos] = &PartialAttestationEntry{
			Position:  pos,
			Signature: []byte(fmt.Sprintf("sig%d", pos)),
			Data:      b.data,
		}
		b.validated[pos] = struct{}{}
		b.sendCount[pos] = sc
		ss.indexAddValidated(string(b.data), pos, sc)
	}
}

// dataPositionsInOrder returns the AttestorIndices across all batches of one
// collected data action, in batch+index order.
func dataPositionsInOrder(c collected) []int {
	var out []int
	if c.payload == nil {
		return out
	}
	for _, batch := range c.payload.Batches {
		for _, idx := range batch.AttestorIndices {
			out = append(out, int(idx))
		}
	}
	return out
}

// dataActions / metadataActions split a peer's yielded actions by kind.
func dataActions(actions []collected) []collected {
	var out []collected
	for _, a := range actions {
		if a.payload != nil {
			out = append(out, a)
		}
	}
	return out
}

func metadataActions(actions []collected) []collected {
	var out []collected
	for _, a := range actions {
		if a.ctrl != nil {
			out = append(out, a)
		}
	}
	return out
}

func intRange(start, n int) []int {
	out := make([]int, n)
	for i := range n {
		out[i] = start + i
	}
	return out
}

// -----------------------------------------------------------------------------
// (1) Priority order — least-forwarded positions go out first, chunked at N.
// -----------------------------------------------------------------------------

func TestPriorityOrdersByForwardCount(t *testing.T) {
	m := newPriorityUnitManager(t) // MaxPeers=64, N=30
	sendCounts := map[int]int{}
	for pos := range 50 {
		sendCounts[pos] = pos // sendCount[pos] == pos => pos sits at level pos
	}
	seedValidated(m, "t0", 1, []byte("d"), sendCounts)

	out := runPriorityPublishActions(t, m, "t0", 1, makePeers(1, false), peerAcceptsPartial)
	actions := out[peer.ID("p0")]
	require.Len(t, dataActions(actions), 2, "50 positions at N=30 => two data messages")

	assert.Equal(t, intRange(0, 30), dataPositionsInOrder(actions[0]), "first message = lowest-forwarded 0..29")
	assert.Equal(t, intRange(30, 20), dataPositionsInOrder(actions[1]), "second message = 30..49")
	assert.Nil(t, actions[0].ctrl, "mesh peer gets no metadata")

	m.mu.Lock()
	b := m.getSlotState("t0", 1).attestationsMap["d"]
	for pos := range 50 {
		assert.Equal(t, pos+1, b.sendCount[pos], "sendCount bumped for pos %d", pos)
	}
	checkIndexInvariant(t, m.getSlotState("t0", 1))
	m.mu.Unlock()
}

// -----------------------------------------------------------------------------
// (2) Skip-and-continue — positions the peer already holds are skipped, and the
// next-in-order needed positions fill the message.
// -----------------------------------------------------------------------------

func TestPrioritySkipsAlreadyAvailableAndContinues(t *testing.T) {
	m := newPriorityUnitManager(t)
	sendCounts := map[int]int{}
	for pos := range 40 {
		sendCounts[pos] = 0 // all at level 0, insertion order 0..39
	}
	seedValidated(m, "t0", 1, []byte("d"), sendCounts)

	// Mesh peer already has the lowest-order positions {0..4}.
	m.mu.Lock()
	b := m.getSlotState("t0", 1).attestationsMap["d"]
	bps := initAndGetPeerAttestationState(b, peer.ID("p0"), testCommitteeSize)
	for _, pos := range []int{0, 1, 2, 3, 4} {
		bps.available.Set(pos)
	}
	m.mu.Unlock()

	out := runPriorityPublishActions(t, m, "t0", 1, makePeers(1, false), peerAcceptsPartial)
	actions := dataActions(out[peer.ID("p0")])
	require.Len(t, actions, 2)

	first := dataPositionsInOrder(actions[0])
	require.Len(t, first, 30, "first message still holds a full N of needed positions")
	assert.Equal(t, intRange(5, 30), first, "skips {0..4}, draws 5..34")
	assert.Equal(t, intRange(35, 5), dataPositionsInOrder(actions[1]))
}

// -----------------------------------------------------------------------------
// (3) Cross-bucket chunk — a chunk is drawn from the global lowest-sendCount
// set across both buckets, regrouped into one BatchedAttestation per bucket.
// -----------------------------------------------------------------------------

func TestPriorityCrossBucketChunk(t *testing.T) {
	m := newPriorityUnitManager(t)
	// Interleave: A.pos i at sendCount 2i (even levels), B.pos i at 2i+1 (odd).
	scA := map[int]int{}
	scB := map[int]int{}
	for i := range 20 {
		scA[i] = 2 * i
		scB[i] = 2*i + 1
	}
	seedValidated(m, "t0", 1, []byte("A"), scA) // bucketSeq 0
	seedValidated(m, "t0", 1, []byte("B"), scB) // bucketSeq 1

	out := runPriorityPublishActions(t, m, "t0", 1, makePeers(1, false), peerAcceptsPartial)
	actions := dataActions(out[peer.ID("p0")])
	require.Len(t, actions, 2, "40 positions at N=30 => 30 + 10")

	// Global ascending order is A0,B0,A1,B1,…; first 30 => A0..A14 and B0..B14.
	first := actions[0]
	require.Len(t, first.payload.Batches, 2, "first chunk drawn from both buckets")
	byData := map[string][]int{}
	for _, batch := range first.payload.Batches {
		for _, idx := range batch.AttestorIndices {
			byData[string(batch.AttestationData)] = append(byData[string(batch.AttestationData)], int(idx))
		}
	}
	assert.Equal(t, intRange(0, 15), byData["A"], "A contributes its 15 lowest-forwarded positions, in order")
	assert.Equal(t, intRange(0, 15), byData["B"], "B contributes its 15 lowest-forwarded positions, in order")
	// Batches ordered by stable bucketSeq: A before B.
	assert.Equal(t, []byte("A"), first.payload.Batches[0].AttestationData)
	assert.Equal(t, []byte("B"), first.payload.Batches[1].AttestationData)

	// Union across both messages == every position in both buckets.
	all := map[string][]int{}
	for _, a := range actions {
		for _, batch := range a.payload.Batches {
			for _, idx := range batch.AttestorIndices {
				all[string(batch.AttestationData)] = append(all[string(batch.AttestationData)], int(idx))
			}
		}
	}
	assert.ElementsMatch(t, intRange(0, 20), all["A"])
	assert.ElementsMatch(t, intRange(0, 20), all["B"])
}

// -----------------------------------------------------------------------------
// (4) Gossip want, chunked — a gossip peer's >N requested positions are answered
// in full across several ≤N data messages, and pendingWant is cleared.
// -----------------------------------------------------------------------------

func TestPriorityGossipWantChunked(t *testing.T) {
	m := newPriorityUnitManager(t)
	sendCounts := map[int]int{}
	for pos := range 50 {
		sendCounts[pos] = 0
	}
	seedValidated(m, "t0", 1, []byte("d"), sendCounts)

	// Gossip peer requests positions 0..39 (no Available advertised).
	wanted := intRange(0, 40)
	m.mu.Lock()
	b := m.getSlotState("t0", 1).attestationsMap["d"]
	bps := initAndGetPeerAttestationState(b, peer.ID("p0"), testCommitteeSize)
	for _, pos := range wanted {
		bps.pendingWant.Set(pos)
	}
	m.mu.Unlock()

	peers := map[peer.ID]peerState{peer.ID("p0"): {gossipPeer: true, sendAvailableList: false}}
	out := runPriorityPublishActions(t, m, "t0", 1, peers, peerAcceptsPartial)
	actions := out[peer.ID("p0")]

	data := dataActions(actions)
	require.Len(t, data, 2, "40 wanted positions at N=30 => 30 + 10")
	var got []int
	for _, a := range data {
		got = append(got, dataPositionsInOrder(a)...)
	}
	assert.ElementsMatch(t, wanted, got, "exactly the wanted positions, nothing else")
	assert.Empty(t, metadataActions(actions), "no Available and no Requests for this peer => no metadata message")

	m.mu.Lock()
	assert.Equal(t, 0, b.peers[peer.ID("p0")].pendingWant.OnesCount(), "pendingWant cleared after the tick")
	m.mu.Unlock()
}

// -----------------------------------------------------------------------------
// (5) Under-cap — fewer than N candidates produce a single message.
// -----------------------------------------------------------------------------

func TestPriorityUnderCapSingleMessage(t *testing.T) {
	m := newPriorityUnitManager(t)
	sendCounts := map[int]int{}
	for pos := range 10 {
		sendCounts[pos] = 0
	}
	seedValidated(m, "t0", 1, []byte("d"), sendCounts)

	out := runPriorityPublishActions(t, m, "t0", 1, makePeers(1, false), peerAcceptsPartial)
	actions := out[peer.ID("p0")]
	require.Len(t, dataActions(actions), 1)
	assert.Len(t, dataPositionsInOrder(actions[0]), 10)
	assert.Nil(t, actions[0].ctrl)
}

// -----------------------------------------------------------------------------
// (6) Lifetime ceiling — a position at sendCount == MaxPeersPerAttestation is
// never a candidate and never appears in the index.
// -----------------------------------------------------------------------------

func TestPriorityLifetimeCeilingExcluded(t *testing.T) {
	m := newPriorityUnitManager(t)
	m.node.MaxPeersPerAttestation = 2 // set before seeding so the index is sized to it
	seedValidated(m, "t0", 1, []byte("d"), map[int]int{0: 0, 1: 2, 2: 0})

	out := runPriorityPublishActions(t, m, "t0", 1, makePeers(1, false), peerAcceptsPartial)
	actions := out[peer.ID("p0")]
	require.Len(t, dataActions(actions), 1)

	got := dataPositionsInOrder(actions[0])
	assert.ElementsMatch(t, []int{0, 2}, got)
	assert.NotContains(t, got, 1, "position at the lifetime ceiling is never sent")

	m.mu.Lock()
	ss := m.getSlotState("t0", 1)
	b := ss.attestationsMap["d"]
	assert.Equal(t, 1, b.sendCount[0])
	assert.Equal(t, 1, b.sendCount[2])
	assert.Equal(t, 2, b.sendCount[1])
	assert.False(t, indexContains(ss, "d", 1), "maxed-out position must be absent from the index")
	checkIndexInvariant(t, ss)
	m.mu.Unlock()
}

// -----------------------------------------------------------------------------
// (8) Round-robin spread — with several requesting peers and more positions than
// fit one message, each peer gets one ≤ N chunk per pass (no peer is drained
// before the others start), and the first pass partitions the lowest-forwarded
// positions disjointly across peers. This is the spread the old per-peer-drain
// order lacked; assertions are order-independent of the map's peer iteration.
// -----------------------------------------------------------------------------

func TestPriorityRoundRobinSpreadsAcrossPeers(t *testing.T) {
	m := newPriorityUnitManager(t) // MaxPeers=64, N=30
	const numPeers, numPos = 3, 90
	sendCounts := map[int]int{}
	for pos := range numPos {
		sendCounts[pos] = 0 // all least-forwarded, so every peer needs all of them
	}
	seedValidated(m, "t0", 1, []byte("d"), sendCounts)

	// Capture the (peer, data-positions) sequence across the whole tick in yield
	// order — runPriorityPublishActions groups by peer and would lose it.
	type send struct {
		peer peer.ID
		pos  []int
	}
	var seq []send
	for p, action := range m.publishActions("t0", 1)(makePeers(numPeers, false), peerAcceptsPartial) {
		if len(action.EncodedPartialMessage) == 0 {
			continue
		}
		env := &pb.BatchedAttestationEnvelope{}
		require.NoError(t, proto.Unmarshal(action.EncodedPartialMessage, env))
		seq = append(seq, send{p, dataPositionsInOrder(collected{payload: env})})
	}

	// 3 peers × ceil(90/30)=3 messages each = 9 data messages, emitted in 3
	// passes of 3 (one per distinct peer) — proving round-robin, not per-peer
	// drain (which would emit peer A's three messages back to back).
	require.Len(t, seq, numPeers*3)
	for pass := range 3 {
		peersThisPass := map[peer.ID]bool{}
		for i := range numPeers {
			peersThisPass[seq[pass*numPeers+i].peer] = true
		}
		assert.Len(t, peersThisPass, numPeers, "pass %d sends one message to each distinct peer", pass)
	}

	// First pass partitions the 90 lowest-forwarded positions disjointly: the
	// commit-as-you-go draw hands each peer a different slice.
	seen := map[int]bool{}
	for i := range numPeers {
		for _, pos := range seq[i].pos {
			assert.False(t, seen[pos], "position %d sent to two peers in the first pass", pos)
			seen[pos] = true
		}
	}
	assert.Len(t, seen, numPos, "first pass covers every position exactly once")

	// Across all passes every peer still ends up with every position.
	perPeer := map[peer.ID][]int{}
	for _, s := range seq {
		perPeer[s.peer] = append(perPeer[s.peer], s.pos...)
	}
	require.Len(t, perPeer, numPeers)
	for p, got := range perPeer {
		assert.ElementsMatch(t, intRange(0, numPos), got, "peer %s eventually receives every position", p)
	}

	m.mu.Lock()
	checkIndexInvariant(t, m.getSlotState("t0", 1))
	m.mu.Unlock()
}

// -----------------------------------------------------------------------------
// Index invariant helpers (test-only).
// -----------------------------------------------------------------------------

func indexContains(ss *prioritySlotState, bucketKey string, pos int) bool {
	e := idxEntry{bucketKey, pos}
	for k := range ss.levels {
		if _, ok := ss.levels[k].at[e]; ok {
			return true
		}
	}
	return false
}

// checkIndexInvariant asserts: an entry is in exactly one level k iff its
// position is validated and sendCount < maxPeers, with k == sendCount.
func checkIndexInvariant(t *testing.T, ss *prioritySlotState) {
	t.Helper()
	// Forward: every validated, under-cap position is at the right level.
	for bk, b := range ss.attestationsMap {
		for pos := range b.validated {
			sc := b.sendCount[pos]
			e := idxEntry{bk, pos}
			if sc >= ss.maxPeers {
				assert.False(t, indexContains(ss, bk, pos), "maxed position %s/%d must not be indexed", bk, pos)
				continue
			}
			at, ok := ss.levels[sc].at[e]
			assert.True(t, ok, "validated position %s/%d missing from level %d", bk, pos, sc)
			if ok {
				assert.Equal(t, e, ss.levels[sc].entries[at], "level index map out of sync for %s/%d", bk, pos)
			}
		}
	}
	// Reverse: every indexed entry corresponds to a validated position at the
	// matching sendCount level.
	for k := range ss.levels {
		for _, e := range ss.levels[k].entries {
			b := ss.attestationsMap[e.bucketKey]
			require.NotNil(t, b, "indexed entry references unknown bucket %q", e.bucketKey)
			_, validated := b.validated[e.pos]
			assert.True(t, validated, "indexed entry %s/%d not validated", e.bucketKey, e.pos)
			assert.Equal(t, k, b.sendCount[e.pos], "entry %s/%d at level %d but sendCount %d", e.bucketKey, e.pos, k, b.sendCount[e.pos])
		}
	}
}

// -----------------------------------------------------------------------------
// (7) E2E — a hub node accumulates > N attestations and forwards them; every
// outgoing data batch stays ≤ N and all attestations reach the observer.
// -----------------------------------------------------------------------------

// newE2EPriorityNode builds a Node configured for partial-priority propagation.
// Mirrors newE2EPartialNode but selects the priority strategy.
func newE2EPriorityNode(opts e2eOpts, maxPerMessage int, nw *testNetwork, tr *testTracer) *Node {
	n := newE2EPartialNode(opts, nw, tr)
	n.UsePartialMessages = false
	n.PartialPriorityMode = true
	n.MaxAttestationsPerMessage = maxPerMessage
	return n
}

// expectAttestationPriority is the priority-manager analogue of
// expectAttestation: it asserts node n holds position `from` (validated or
// validating) in any bucket of the slot.
func expectAttestationPriority(t *testing.T, n *Node, topic string, slot, from int) bool {
	t.Helper()
	ss := n.partialPriority.getSlotState(topic, slot)
	if ss == nil {
		t.Errorf("node %d: missing slot state for slot %d", n.Num, slot)
		return false
	}
	n.partialPriority.mu.Lock()
	defer n.partialPriority.mu.Unlock()
	for _, b := range ss.attestationsMap {
		if _, ok := b.validated[from]; ok {
			return true
		}
		if _, ok := b.validating[from]; ok {
			return true
		}
	}
	t.Errorf("node %d: missing attestation position=%d slot=%d", n.Num, from, slot)
	return false
}

// batchSizeRPCTracer records the largest single outgoing data RPC (summed
// attestations across its batches) so a test can assert the per-message cap.
type batchSizeRPCTracer struct {
	mu      sync.Mutex
	maxSeen int
	dataN   int
}

func (c *batchSizeRPCTracer) OnRPCSent(_ peer.ID, _ time.Duration, rpc *pubsub_pb.RPC) {
	p := rpc.GetPartial()
	if p == nil || len(p.GetPartialMessage()) == 0 {
		return
	}
	var env pb.BatchedAttestationEnvelope
	if err := proto.Unmarshal(p.GetPartialMessage(), &env); err != nil {
		return
	}
	total := 0
	for _, b := range env.Batches {
		total += len(b.AttestorIndices)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dataN++
	if total > c.maxSeen {
		c.maxSeen = total
	}
}

func (c *batchSizeRPCTracer) OnRPCReceived(peer.ID, time.Duration, *pubsub_pb.RPC) {}
func (c *batchSizeRPCTracer) OnPeerRTT(peer.ID, string, time.Duration, string, string) {}
func (c *batchSizeRPCTracer) OnMeshSize(string, int)                                   {}
func (c *batchSizeRPCTracer) Close() error                                             { return nil }

func (c *batchSizeRPCTracer) snapshot() (maxSeen, dataN int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxSeen, c.dataN
}

func TestE2EPriorityChunksLargeSendAndDelivers(t *testing.T) {
	// 8 fully-connected nodes with a small mesh, every node attesting in slot 1.
	// Each accumulates all 8 positions — more than the small per-message cap N —
	// so any send to a peer that lacks them must be split into ≤ N chunks. This
	// mirrors the reliable gossip-path delivery topology; the small N is what
	// forces the partial-priority chunking to be exercised.
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes      = 8
			numSlots      = 2
			slotDuration  = 2 * time.Second
			maxPerMessage = 4
		)

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)
		tightMesh := GossipsubParams{D: 4, Dlow: 4, Dhigh: 5} // Dhigh must be >= Dscore (4)

		tracers := make([]*batchSizeRPCTracer, numNodes)
		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			tracers[i] = &batchSizeRPCTracer{}
			opts := defaultE2EOpts(i, 1, publishStart, slotDuration, numNodes)
			opts.gossipsubParams = tightMesh
			opts.publishInterval = 50 * time.Millisecond
			opts.rpcTracer = tracers[i]
			nodes[i] = newE2EPriorityNode(opts, maxPerMessage, nw, tr)
		}

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

		topic := topicName(0)

		// Every node received every other node's slot-1 attestation.
		for i, n := range nodes {
			for j := range numNodes {
				if i == j {
					continue
				}
				expectAttestationPriority(t, n, topic, 1, j)
			}
		}

		// Every outgoing data batch stayed within the cap.
		var totalData int
		for i, c := range tracers {
			maxSeen, dataN := c.snapshot()
			assert.LessOrEqual(t, maxSeen, maxPerMessage, "node %d: no data message may exceed N attestations", i)
			totalData += dataN
		}
		assert.Positive(t, totalData, "data RPCs should have been sent")

		// At least one node ended with > N validated positions: combined with the
		// per-batch cap above, that proves a large send was split into chunks.
		var maxValidated int
		for _, n := range nodes {
			n.partialPriority.mu.Lock()
			if ss := n.partialPriority.getSlotState(topic, 1); ss != nil {
				for _, b := range ss.attestationsMap {
					maxValidated = max(maxValidated, len(b.validated))
				}
			}
			n.partialPriority.mu.Unlock()
		}
		assert.Greater(t, maxValidated, maxPerMessage, "a node should accumulate > N positions, forcing chunking")
	})
}
