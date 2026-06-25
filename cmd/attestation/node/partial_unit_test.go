package node

import (
	"fmt"
	"iter"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
	"github.com/ethp2p/simlab/cmd/attestation/verify"
)

// -----------------------------------------------------------------------------
// Test helpers — mock the gossipsub interface without a real network.
// -----------------------------------------------------------------------------

const testCommitteeSize = 128

// newPartialUnitManager builds a partial-message manager with a backing
// verify.Verifier suitable for unit tests. No real network is involved.
func newPartialUnitManager(t *testing.T) *partialAttestationManager {
	t.Helper()
	node := &Node{
		MaxPeersPerAttestation:     16,
		CommitteeSize:              testCommitteeSize,
		VerificationDelay:          func() time.Duration { return 5 * time.Millisecond },
		PerAttestationVerification: 0,
		VerificationBatchWindow:    2 * time.Millisecond,
	}
	node.verifier = verify.New(
		node.VerificationDelay,
		node.PerAttestationVerification,
		node.VerificationBatchWindow,
		slog.Default(),
	)
	go node.verifier.Run()
	t.Cleanup(func() { node.verifier.Stop() })
	return newPartialAttestationManager(node, time.Now(), time.Second, nil)
}

// collected captures the output of one publishActions iteration so tests can
// inspect what would be sent to each peer.
type collected struct {
	ctrl     *pb.ControlEnvelope
	payload  *pb.BatchedAttestationEnvelope
	rawCtrl  []byte
	rawParts []byte
}

// runPublishActions invokes the manager's publishActions iterator and decodes
// every emitted PublishAction.
func runPublishActions(
	t *testing.T,
	m *partialAttestationManager,
	topic string,
	slot int,
	peerStates map[peer.ID]peerState,
	peerRequestsPartial func(peer.ID) bool,
) map[peer.ID]collected {
	t.Helper()
	fn := m.publishActions(topic, slot)
	if fn == nil {
		return nil
	}
	out := map[peer.ID]collected{}
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
		out[p] = c
	}
	return out
}

// peerRequestsPartial callbacks for runPublishActions: whether a peer has
// requested partial messages (and thus may receive data payloads). The
// extension drops data to a peer that returns false.
func peerAcceptsPartial(peer.ID) bool  { return true }
func peerDeclinesPartial(peer.ID) bool { return false }

// makePeers returns n peer states with IDs "p0".."p<n-1>" and the given
// gossipPeer flag.
func makePeers(n int, gossip bool) map[peer.ID]peerState {
	peers := make(map[peer.ID]peerState, n)
	for i := range n {
		// A gossip peer is modeled as freshly selected by an EmitGossip
		// heartbeat, so it is primed to receive our Available list this tick.
		peers[peer.ID(fmt.Sprintf("p%d", i))] = peerState{gossipPeer: gossip, sendAvailableList: gossip}
	}
	return peers
}

// -----------------------------------------------------------------------------
// slotGroupID / groupIDToSlot
// -----------------------------------------------------------------------------

func TestSlotGroupIDRoundtrip(t *testing.T) {
	for _, slot := range []int{0, 1, 255, 256, 65535, 1 << 24} {
		assert.Equal(t, slot, groupIDToSlot(slotGroupID(slot)), "slot=%d", slot)
	}
}

func TestGroupIDToSlotShortInput(t *testing.T) {
	assert.Equal(t, 1, groupIDToSlot([]byte{1}))
	assert.Equal(t, 0x0102, groupIDToSlot([]byte{1, 2}))
}

// -----------------------------------------------------------------------------
// Bitmap helpers
// -----------------------------------------------------------------------------

func TestNewCommitteeBitmapZero(t *testing.T) {
	b := newCommitteeBitmap(128)
	assert.Len(t, b, 16)
	assert.Equal(t, 0, b.OnesCount())
}

// -----------------------------------------------------------------------------
// Bucket behavior
// -----------------------------------------------------------------------------

func TestPublishLocalCreatesBucketAndMarksValidated(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("topic0", 5, 3, []byte("sig"), []byte("dataA"))
	ss := m.getSlotState("topic0", 5)
	require.NotNil(t, ss)
	require.Len(t, ss.attestationsMap, 1)
	b := ss.attestationsMap[testAttKey("dataA")]
	require.NotNil(t, b)
	assert.Contains(t, b.validated, 3)
	assert.NotContains(t, b.validating, 3)
}

func TestPublishLocalSeparatesBucketsByAttestationData(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("topic0", 5, 3, []byte("sig"), []byte("dataA"))
	m.publishLocal("topic0", 5, 3, []byte("sig"), []byte("dataB"))
	ss := m.getSlotState("topic0", 5)
	require.NotNil(t, ss)
	require.Len(t, ss.attestationsMap, 2, "different attestation_data must produce separate buckets at the same slot")
	assert.Contains(t, ss.attestationsMap[testAttKey("dataA")].validated, 3)
	assert.Contains(t, ss.attestationsMap[testAttKey("dataB")].validated, 3)
}

func TestPublishLocalDuplicateNoop(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("topic0", 1, 3, []byte("sig"), []byte("d"))
	m.publishLocal("topic0", 1, 3, []byte("sig-dup"), []byte("d"))
	ss := m.getSlotState("topic0", 1)
	b := ss.attestationsMap[testAttKey("d")]
	assert.Equal(t, "sig", string(b.attestations[3].Signature), "duplicate add must not overwrite")
}

func TestBucketAddReceivedPendingValidation(t *testing.T) {
	b := newAttestationState([]byte("shared-data"))
	newEntries := b.addReceived([]int{2, 5}, [][]byte{[]byte("s2"), []byte("s5")})
	require.Len(t, newEntries, 2)
	assert.Contains(t, b.validating, 2)
	assert.Contains(t, b.validating, 5)
	assert.NotContains(t, b.validated, 2)

	// Overlap returns only the genuinely-new entries.
	newEntries = b.addReceived([]int{5, 8}, [][]byte{[]byte("dup"), []byte("s8")})
	require.Len(t, newEntries, 1)
	entry := newEntries[0].(*PartialAttestationEntry)
	assert.Equal(t, 8, entry.Position)
}

func TestMarkValidatedPromotes(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("topic0", 1, 0, []byte("s"), []byte("d"))
	ss := m.getSlotState("topic0", 1)
	b := ss.attestationsMap[testAttKey("d")]
	entries := b.addReceived([]int{4}, [][]byte{[]byte("s4")})
	assert.NotContains(t, b.validated, 4)

	m.markValidated("topic0", 1, []byte("d"), entries)

	assert.Contains(t, b.validated, 4)
	assert.NotContains(t, b.validating, 4)
}

func TestMarkValidatedUnknownBucketNoPanic(t *testing.T) {
	m := newPartialUnitManager(t)
	require.NotPanics(t, func() {
		m.markValidated("topic0", 99, []byte("nope"), []any{&PartialAttestationEntry{Position: 1}})
	})
}

// -----------------------------------------------------------------------------
// Wire format
// -----------------------------------------------------------------------------

func TestBatchedAttestationEnvelopeRoundtrip(t *testing.T) {
	env := &pb.BatchedAttestationEnvelope{
		Batches: []*pb.BatchedAttestation{
			{AttestationData: []byte("a"), AttestorIndices: []uint32{0, 5}, Signatures: [][]byte{[]byte("s0"), []byte("s5")}},
			{AttestationData: []byte("b"), AttestorIndices: []uint32{9}, Signatures: [][]byte{[]byte("s9")}},
		},
	}
	encoded, err := proto.Marshal(env)
	require.NoError(t, err)
	decoded := &pb.BatchedAttestationEnvelope{}
	require.NoError(t, proto.Unmarshal(encoded, decoded))
	require.Len(t, decoded.Batches, 2)
	assert.Equal(t, []uint32{0, 5}, decoded.Batches[0].AttestorIndices)
	assert.Equal(t, []uint32{9}, decoded.Batches[1].AttestorIndices)
}

func TestControlEnvelopeRoundtrip(t *testing.T) {
	avail := newCommitteeBitmap(testCommitteeSize)
	avail.Set(1)
	req := newCommitteeBitmap(testCommitteeSize)
	req.Set(2)

	env := &pb.ControlEnvelope{
		Metadatas: []*pb.CommitteeAttestationPartsMetadata{
			{Slot: 7, AttestationData: []byte("a"), Available: []byte(avail), Requests: []byte(req)},
		},
	}
	encoded, err := proto.Marshal(env)
	require.NoError(t, err)
	got := &pb.ControlEnvelope{}
	require.NoError(t, proto.Unmarshal(encoded, got))
	require.Len(t, got.Metadatas, 1)
	assert.Equal(t, int32(7), got.Metadatas[0].Slot)
	gotAvail := bitmap.Bitmap(got.Metadatas[0].Available)
	assert.True(t, gotAvail.Get(1))
}

// -----------------------------------------------------------------------------
// buildBucketMetadata
// -----------------------------------------------------------------------------

func TestBuildBucketMetadataNilWhenNothingToSay(t *testing.T) {
	b := newAttestationState([]byte("d"))
	got := getAttestationMetadata(b, testCommitteeSize, 1, nil, true)
	assert.Nil(t, got)
}

func TestBuildBucketMetadataAvailableIncludesAllValidated(t *testing.T) {
	b := newAttestationState([]byte("d"))
	b.validated[0] = struct{}{}
	b.validated[5] = struct{}{}
	b.validated[2] = struct{}{}

	got := getAttestationMetadata(b, testCommitteeSize, 1, nil, true)
	require.NotNil(t, got)
	gotBm := bitmap.Bitmap(got.Available)
	assert.True(t, gotBm.Get(0))
	assert.True(t, gotBm.Get(2))
	assert.True(t, gotBm.Get(5))
	assert.Empty(t, got.Requests)
}

func TestBuildBucketMetadataRequestsPopulated(t *testing.T) {
	b := newAttestationState([]byte("d"))
	got := getAttestationMetadata(b, testCommitteeSize, 1, []int{1, 4, 8}, false)
	require.NotNil(t, got)
	reqBm := bitmap.Bitmap(got.Requests)
	for _, pos := range []int{1, 4, 8} {
		assert.True(t, reqBm.Get(pos), "position %d should be set in requests", pos)
	}
	assert.Empty(t, got.Available)
}

// -----------------------------------------------------------------------------
// encodeBatch
// -----------------------------------------------------------------------------

func TestEncodeBatchEmitsIndicesAndOrdersSignatures(t *testing.T) {
	m := newPartialUnitManager(t)
	b := newAttestationState([]byte("d"))
	b.attestations[0] = &PartialAttestationEntry{Position: 0, Signature: []byte("sig0"), Data: b.data}
	b.attestations[5] = &PartialAttestationEntry{Position: 5, Signature: []byte("sig5"), Data: b.data}

	batch := m.encodeBatch(b, []int{0, 5})
	require.NotNil(t, batch)
	assert.Equal(t, []uint32{0, 5}, batch.AttestorIndices)
	require.Len(t, batch.Signatures, 2)
	assert.Equal(t, []byte("sig0"), batch.Signatures[0])
	assert.Equal(t, []byte("sig5"), batch.Signatures[1])
}

func TestEncodeBatchSendsFullDataOncePerPeer(t *testing.T) {
	m := newPartialUnitManager(t)
	b := newAttestationState([]byte("d"))
	b.attestations[0] = &PartialAttestationEntry{Position: 0, Signature: []byte("sig0"), Data: b.data}
	p := peer.ID("p0")

	first := m.encodeBatchForPeer(p, b, []int{0})
	require.Equal(t, []byte("d"), first.AttestationData)
	require.Empty(t, first.AttestationDataHash)

	second := m.encodeBatchForPeer(p, b, []int{0})
	require.Empty(t, second.AttestationData)
	require.Equal(t, b.dataHash, second.AttestationDataHash)

	otherPeer := m.encodeBatchForPeer(peer.ID("p1"), b, []int{0})
	require.Equal(t, []byte("d"), otherPeer.AttestationData)
}

// -----------------------------------------------------------------------------
// selectAndCommitSends
// -----------------------------------------------------------------------------

func TestSelectAndCommitMeshPeerSendsAll(t *testing.T) {
	m := newPartialUnitManager(t)
	b := newAttestationState([]byte("d"))
	for _, pos := range []int{0, 1, 2} {
		b.validated[pos] = struct{}{}
		b.attestations[pos] = &PartialAttestationEntry{Position: pos, Signature: []byte("s"), Data: b.data}
	}

	bps := initAndGetPeerAttestationState(b, peer.ID("p0"), testCommitteeSize)
	got := m.claimAttestationsToSend(b, bps, false)
	assert.ElementsMatch(t, []int{0, 1, 2}, got)
	assert.True(t, bps.available.Get(0))
	assert.True(t, bps.available.Get(1))
	assert.True(t, bps.available.Get(2))
}

func TestSelectAndCommitGossipPeerNoWantNothing(t *testing.T) {
	m := newPartialUnitManager(t)
	b := newAttestationState([]byte("d"))
	b.validated[0] = struct{}{}
	b.attestations[0] = &PartialAttestationEntry{Position: 0, Signature: []byte("s"), Data: b.data}

	bps := newPeerAttestationState(testCommitteeSize)
	got := m.claimAttestationsToSend(b, bps, true)
	assert.Empty(t, got)
}

func TestSelectAndCommitGossipPeerHonorsPendingWant(t *testing.T) {
	m := newPartialUnitManager(t)
	b := newAttestationState([]byte("d"))
	for _, pos := range []int{0, 1, 2} {
		b.validated[pos] = struct{}{}
		b.attestations[pos] = &PartialAttestationEntry{Position: pos, Signature: []byte("s"), Data: b.data}
	}
	bps := newPeerAttestationState(testCommitteeSize)
	bps.pendingWant.Set(1)

	got := m.claimAttestationsToSend(b, bps, true)
	assert.Equal(t, []int{1}, got)
}

func TestSelectAndCommitSkipsAlreadyAvailable(t *testing.T) {
	m := newPartialUnitManager(t)
	b := newAttestationState([]byte("d"))
	b.validated[0] = struct{}{}
	b.attestations[0] = &PartialAttestationEntry{Position: 0, Signature: []byte("s"), Data: b.data}
	bps := newPeerAttestationState(testCommitteeSize)
	bps.available.Set(0)

	got := m.claimAttestationsToSend(b, bps, false)
	assert.Empty(t, got)
}

func TestSelectAndCommitBudgetExhausted(t *testing.T) {
	m := newPartialUnitManager(t)
	m.node.MaxPeersPerAttestation = 2
	b := newAttestationState([]byte("d"))
	b.validated[0] = struct{}{}
	b.attestations[0] = &PartialAttestationEntry{Position: 0, Signature: []byte("s"), Data: b.data}
	b.sendCount[0] = 2

	bps := newPeerAttestationState(testCommitteeSize)
	got := m.claimAttestationsToSend(b, bps, false)
	assert.Empty(t, got)
}

// -----------------------------------------------------------------------------
// selectIWantTargets
// -----------------------------------------------------------------------------

func TestSelectIWantTargetsCapsAtMaxPerPosition(t *testing.T) {
	b := newAttestationState([]byte("d"))
	peers := map[peer.ID]peerState{}
	// More peers than the cap, all advertising the same position.
	const numPeers = maxIWantPerPosition + 3
	for i := range numPeers {
		id := peer.ID(fmt.Sprintf("p%d", i))
		bps := newPeerAttestationState(testCommitteeSize)
		bps.available.Set(5)
		b.peers[id] = bps
		peers[id] = peerState{gossipPeer: true}
	}
	wants := selectIWantTargets(b, peers, testCommitteeSize)
	var count int
	for _, idxs := range wants {
		for _, v := range idxs {
			if v == 5 {
				count++
			}
		}
	}
	assert.Equal(t, maxIWantPerPosition, count, "must cap at maxIWantPerPosition")
	assert.Equal(t, maxIWantPerPosition, b.requestCount[5])
}

func TestSelectIWantTargetsSkipsPositionWeAlreadyHave(t *testing.T) {
	b := newAttestationState([]byte("d"))
	b.attestations[5] = &PartialAttestationEntry{Position: 5}
	peers := map[peer.ID]peerState{peer.ID("p0"): {gossipPeer: true}}
	bps := newPeerAttestationState(testCommitteeSize)
	bps.available.Set(5)
	b.peers["p0"] = bps
	wants := selectIWantTargets(b, peers, testCommitteeSize)
	assert.Empty(t, wants)
}

func TestSelectIWantTargetsIgnoresNonGossipPeers(t *testing.T) {
	b := newAttestationState([]byte("d"))
	peers := map[peer.ID]peerState{peer.ID("p0"): {gossipPeer: false}}
	bps := newPeerAttestationState(testCommitteeSize)
	bps.available.Set(5)
	b.peers["p0"] = bps
	wants := selectIWantTargets(b, peers, testCommitteeSize)
	assert.Empty(t, wants)
}

func TestSelectIWantTargetsSortsPerPeerOutput(t *testing.T) {
	b := newAttestationState([]byte("d"))
	peers := map[peer.ID]peerState{peer.ID("p0"): {gossipPeer: true}}
	bps := newPeerAttestationState(testCommitteeSize)
	bps.available.Set(8)
	bps.available.Set(1)
	bps.available.Set(4)
	b.peers["p0"] = bps
	wants := selectIWantTargets(b, peers, testCommitteeSize)
	require.Contains(t, wants, peer.ID("p0"))
	assert.Equal(t, []int{1, 4, 8}, wants[peer.ID("p0")])
}

// -----------------------------------------------------------------------------
// publishActions
// -----------------------------------------------------------------------------

func TestPublishActionsMeshPeerReceivesData(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("d"))

	peers := makePeers(1, false)
	out := runPublishActions(t, m, "t0", 1, peers, peerAcceptsPartial)
	require.Len(t, out, 1)
	got := out[peer.ID("p0")]
	require.NotNil(t, got.payload, "mesh peer should receive a partial-message payload")
	require.Len(t, got.payload.Batches, 1)
	assert.Equal(t, []uint32{0}, got.payload.Batches[0].AttestorIndices)
	assert.Nil(t, got.ctrl, "mesh peers receive no control envelope")
}

func TestPublishActionsMeshPeerNoDataWhenPeerDoesntRequestPartial(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("d"))

	peers := makePeers(1, false)
	out := runPublishActions(t, m, "t0", 1, peers, peerDeclinesPartial)
	assert.Empty(t, out)
}

func TestPublishActionsGossipPeerGetsAvailableOnly(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("d"))

	peers := makePeers(1, true)
	out := runPublishActions(t, m, "t0", 1, peers, peerDeclinesPartial)
	require.Len(t, out, 1)
	got := out[peer.ID("p0")]
	require.NotNil(t, got.ctrl)
	require.Len(t, got.ctrl.Metadatas, 1)
	md := got.ctrl.Metadatas[0]
	assert.Equal(t, []byte("d"), md.AttestationData)
	availBm := bitmap.Bitmap(md.Available)
	assert.True(t, availBm.Get(0))
	assert.Empty(t, md.Requests, "no peer advertises anything we lack ⇒ no Requests")
	assert.Nil(t, got.payload, "no pendingWant ⇒ no data to a gossip peer")
}

func TestPublishActionsGossipPeerWithPendingWantGetsData(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("d"))
	// Seed pendingWant for the peer in this bucket.
	ss := m.getSlotState("t0", 1)
	b := ss.attestationsMap[testAttKey("d")]
	bps := newPeerAttestationState(testCommitteeSize)
	bps.pendingWant.Set(0)
	b.peers[peer.ID("p0")] = bps

	// A gossip peer that requested partial messages (peerRequestsPartial=true)
	// receives the data for the position it wanted.
	peers := map[peer.ID]peerState{peer.ID("p0"): {gossipPeer: true}}

	out := runPublishActions(t, m, "t0", 1, peers, peerAcceptsPartial)
	require.Len(t, out, 1)
	got := out[peer.ID("p0")]
	require.NotNil(t, got.payload)
	require.Len(t, got.payload.Batches, 1)
	assert.Equal(t, []uint32{0}, got.payload.Batches[0].AttestorIndices)
}

func TestPublishActionsTwoBucketsBothEnveloped(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("forkA"))
	m.publishLocal("t0", 1, 1, []byte("s"), []byte("forkB"))

	peers := makePeers(1, false)
	out := runPublishActions(t, m, "t0", 1, peers, peerAcceptsPartial)
	require.Len(t, out, 1)
	got := out[peer.ID("p0")]
	require.NotNil(t, got.payload)
	require.Len(t, got.payload.Batches, 2, "both buckets should produce a batch in the same envelope")

	seen := map[string]bool{}
	for _, b := range got.payload.Batches {
		seen[string(b.AttestationData)] = true
	}
	assert.True(t, seen["forkA"])
	assert.True(t, seen["forkB"])
}

func TestPublishActionsIHaveSentToAllEligibleGossipPeers(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("d"))

	peers := makePeers(10, true)
	out := runPublishActions(t, m, "t0", 1, peers, peerDeclinesPartial)

	var availRecipients int
	for _, c := range out {
		if c.ctrl != nil && len(c.ctrl.Metadatas) > 0 && len(c.ctrl.Metadatas[0].Available) > 0 {
			availRecipients++
		}
	}
	assert.Equal(t, 10, availRecipients, "every eligible gossip peer must receive an Available envelope")
}

func TestPublishActionsDropsGossipPeerAfterServing(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("d"))

	peers := makePeers(1, true)
	runPublishActions(t, m, "t0", 1, peers, peerDeclinesPartial)

	_, stillTracked := peers[peer.ID("p0")]
	assert.False(t, stillTracked, "gossip peer must be dropped from peerStates after being served")
}

func TestPublishActionsGossipPeerServedOncePerHeartbeat(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("d"))

	peers := makePeers(1, true)

	// First tick: gossip peer receives Available, then is dropped.
	out := runPublishActions(t, m, "t0", 1, peers, peerDeclinesPartial)
	require.NotNil(t, out[peer.ID("p0")].ctrl)
	require.NotEmpty(t, out[peer.ID("p0")].ctrl.Metadatas[0].Available)

	// Second tick with the same map: the dropped peer is no longer served.
	out = runPublishActions(t, m, "t0", 1, peers, peerDeclinesPartial)
	assert.Empty(t, out, "dropped gossip peer must not be served again until re-gossiped")

	// A heartbeat re-adds the peer; it is served Available again.
	m.onEmitGossip("t0", slotGroupID(1), []peer.ID{peer.ID("p0")}, peers)
	out = runPublishActions(t, m, "t0", 1, peers, peerDeclinesPartial)
	require.NotNil(t, out[peer.ID("p0")].ctrl)
	assert.NotEmpty(t, out[peer.ID("p0")].ctrl.Metadatas[0].Available)
}

func TestPublishActionsNoStateForSlotYieldsNothing(t *testing.T) {
	m := newPartialUnitManager(t)
	peers := makePeers(1, false)
	out := runPublishActions(t, m, "t0", 99, peers, peerAcceptsPartial)
	assert.Empty(t, out)
}

func TestPublishActionsPendingWantClearedAfterTick(t *testing.T) {
	m := newPartialUnitManager(t)
	m.publishLocal("t0", 1, 0, []byte("s"), []byte("d"))
	ss := m.getSlotState("t0", 1)
	b := ss.attestationsMap[testAttKey("d")]
	bps := newPeerAttestationState(testCommitteeSize)
	bps.pendingWant.Set(99) // unsatisfiable
	b.peers[peer.ID("p0")] = bps

	peers := map[peer.ID]peerState{peer.ID("p0"): {gossipPeer: true}}
	runPublishActions(t, m, "t0", 1, peers, peerDeclinesPartial)

	assert.Equal(t, 0, b.peers[peer.ID("p0")].pendingWant.OnesCount(), "pendingWant must be cleared after one tick")
}

// -----------------------------------------------------------------------------
// onIncomingRPC
// -----------------------------------------------------------------------------

func encodeControl(t *testing.T, mds []*pb.CommitteeAttestationPartsMetadata) []byte {
	t.Helper()
	env := &pb.ControlEnvelope{Metadatas: mds}
	encoded, err := proto.Marshal(env)
	require.NoError(t, err)
	return encoded
}

func encodeData(t *testing.T, batches []*pb.BatchedAttestation) []byte {
	t.Helper()
	env := &pb.BatchedAttestationEnvelope{Batches: batches}
	encoded, err := proto.Marshal(env)
	require.NoError(t, err)
	return encoded
}

// indicesOf returns the positions as a []uint32 — the wire shape of
// BatchedAttestation.AttestorIndices.
func indicesOf(positions ...int) []uint32 {
	out := make([]uint32, len(positions))
	for i, p := range positions {
		out[i] = uint32(p)
	}
	return out
}

func bitmapWith(positions ...int) []byte {
	bm := newCommitteeBitmap(testCommitteeSize)
	for _, p := range positions {
		bm.Set(p)
	}
	return []byte(bm)
}

func TestOnIncomingRPCAvailableMarksPeerGossipAndUpdatesAvailable(t *testing.T) {
	m := newPartialUnitManager(t)
	topic := "t0"
	pid := peer.ID("p1")
	peers := map[peer.ID]peerState{pid: {}}

	md := &pb.CommitteeAttestationPartsMetadata{
		Slot:            1,
		AttestationData: []byte("d"),
		Available:       bitmapWith(1, 2),
	}
	rpc := &pubsub_pb.PartialMessagesExtension{
		TopicID:       &topic,
		GroupID:       slotGroupID(1),
		PartsMetadata: encodeControl(t, []*pb.CommitteeAttestationPartsMetadata{md}),
	}
	require.NoError(t, m.onIncomingRPC(pid, peers, rpc))

	assert.True(t, peers[pid].gossipPeer)
	ss := m.getSlotState(topic, 1)
	require.NotNil(t, ss)
	b := ss.attestationsMap[testAttKey("d")]
	require.NotNil(t, b)
	bps := b.peers[pid]
	require.NotNil(t, bps)
	assert.True(t, bps.available.Get(1))
	assert.True(t, bps.available.Get(2))
}

func TestOnIncomingRPCRequestsUpdatesPendingWant(t *testing.T) {
	m := newPartialUnitManager(t)
	topic := "t0"
	pid := peer.ID("p1")
	peers := map[peer.ID]peerState{pid: {}}

	md := &pb.CommitteeAttestationPartsMetadata{
		Slot:            1,
		AttestationData: []byte("d"),
		Requests:        bitmapWith(3),
	}
	rpc := &pubsub_pb.PartialMessagesExtension{
		TopicID:       &topic,
		GroupID:       slotGroupID(1),
		PartsMetadata: encodeControl(t, []*pb.CommitteeAttestationPartsMetadata{md}),
	}
	require.NoError(t, m.onIncomingRPC(pid, peers, rpc))

	assert.True(t, peers[pid].gossipPeer)
	ss := m.getSlotState(topic, 1)
	b := ss.attestationsMap[testAttKey("d")]
	bps := b.peers[pid]
	assert.True(t, bps.pendingWant.Get(3))
}

func TestOnIncomingRPCPartialMessageInfersAvailable(t *testing.T) {
	m := newPartialUnitManager(t)
	topic := "t0"
	pid := peer.ID("p1")
	peers := map[peer.ID]peerState{pid: {}}

	batch := &pb.BatchedAttestation{
		AttestationData: []byte("d"),
		AttestorIndices: indicesOf(4, 7),
		Signatures:      [][]byte{[]byte("s4"), []byte("s7")},
	}
	rpc := &pubsub_pb.PartialMessagesExtension{
		TopicID:        &topic,
		GroupID:        slotGroupID(1),
		PartialMessage: encodeData(t, []*pb.BatchedAttestation{batch}),
	}
	require.NoError(t, m.onIncomingRPC(pid, peers, rpc))

	ss := m.getSlotState(topic, 1)
	require.NotNil(t, ss)
	b := ss.attestationsMap[testAttKey("d")]
	require.NotNil(t, b)
	m.mu.Lock()
	_, has4 := b.attestations[4]
	_, has7 := b.attestations[7]
	bps := b.peers[pid]
	m.mu.Unlock()
	assert.True(t, has4)
	assert.True(t, has7)
	require.NotNil(t, bps)
	assert.True(t, bps.available.Get(4))
	assert.True(t, bps.available.Get(7))
}

func TestOnIncomingRPCSubmitsToVerifier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newPartialUnitManager(t)
		topic := "t0"
		pid := peer.ID("p1")
		peers := map[peer.ID]peerState{pid: {}}

		batch := &pb.BatchedAttestation{
			AttestationData: []byte("d"),
			AttestorIndices: indicesOf(9),
			Signatures:      [][]byte{[]byte("s9")},
		}
		rpc := &pubsub_pb.PartialMessagesExtension{
			TopicID:        &topic,
			GroupID:        slotGroupID(1),
			PartialMessage: encodeData(t, []*pb.BatchedAttestation{batch}),
		}
		require.NoError(t, m.onIncomingRPC(pid, peers, rpc))

		time.Sleep(100 * time.Millisecond)

		ss := m.getSlotState(topic, 1)
		b := ss.attestationsMap[testAttKey("d")]
		m.mu.Lock()
		_, validated := b.validated[9]
		_, validating := b.validating[9]
		m.mu.Unlock()
		assert.True(t, validated, "verifier callback should mark received positions validated")
		assert.False(t, validating)
	})
}

func TestOnIncomingRPCSeparatesBucketsAcrossForks(t *testing.T) {
	m := newPartialUnitManager(t)
	topic := "t0"
	pid := peer.ID("p1")
	peers := map[peer.ID]peerState{pid: {}}

	// Two batches with the same position 5 but different attestation_data.
	batches := []*pb.BatchedAttestation{
		{AttestationData: []byte("forkA"), AttestorIndices: indicesOf(5), Signatures: [][]byte{[]byte("sA")}},
		{AttestationData: []byte("forkB"), AttestorIndices: indicesOf(5), Signatures: [][]byte{[]byte("sB")}},
	}
	rpc := &pubsub_pb.PartialMessagesExtension{
		TopicID:        &topic,
		GroupID:        slotGroupID(1),
		PartialMessage: encodeData(t, batches),
	}
	require.NoError(t, m.onIncomingRPC(pid, peers, rpc))

	ss := m.getSlotState(topic, 1)
	require.Len(t, ss.attestationsMap, 2, "forks must produce independent buckets")
	bA := ss.attestationsMap[testAttKey("forkA")]
	bB := ss.attestationsMap[testAttKey("forkB")]
	assert.Equal(t, "sA", string(bA.attestations[5].Signature))
	assert.Equal(t, "sB", string(bB.attestations[5].Signature))
}

func TestOnIncomingRPCBadProtoReturnsError(t *testing.T) {
	m := newPartialUnitManager(t)
	topic := "t0"
	pid := peer.ID("p1")
	peers := map[peer.ID]peerState{pid: {}}

	rpc := &pubsub_pb.PartialMessagesExtension{
		TopicID:       &topic,
		GroupID:       slotGroupID(1),
		PartsMetadata: []byte{0xFF, 0xFF, 0xFF},
	}
	require.Error(t, m.onIncomingRPC(pid, peers, rpc))
}

// -----------------------------------------------------------------------------
// onEmitGossip
// -----------------------------------------------------------------------------

func TestOnEmitGossipMarksGossipPeer(t *testing.T) {
	m := newPartialUnitManager(t)
	pid := peer.ID("p1")
	peers := map[peer.ID]peerState{pid: {}}

	m.onEmitGossip("t0", slotGroupID(1), []peer.ID{pid}, peers)

	assert.True(t, peers[pid].gossipPeer)
}

func TestOnEmitGossipRespectsDisableFlag(t *testing.T) {
	m := newPartialUnitManager(t)
	m.node.DisableIHaveGossip = true
	pid := peer.ID("p1")
	peers := map[peer.ID]peerState{pid: {}}

	m.onEmitGossip("t0", slotGroupID(1), []peer.ID{pid}, peers)

	assert.False(t, peers[pid].gossipPeer)
}

// -----------------------------------------------------------------------------
// Compile-time interface check
// -----------------------------------------------------------------------------

var _ iter.Seq2[peer.ID, partialmessages.PublishAction] = func(yield func(peer.ID, partialmessages.PublishAction) bool) {}

var _ = slog.Default
