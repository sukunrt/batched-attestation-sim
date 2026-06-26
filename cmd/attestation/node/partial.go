package node

import (
	"encoding/binary"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
	"github.com/ethp2p/simlab/cmd/attestation/verify"
)

// maxIWantPerPosition caps how many gossip peers we ask for any one missing
// committee position per (slot, attestation_data) bucket.
const maxIWantPerPosition = 10

// PartialAttestationEntry holds one committee member's signature plus the
// shared attestation_data it signed. Stored per (bucket, position).
type PartialAttestationEntry struct {
	Position  int
	Signature []byte
	Data      []byte
}

// peerAttestationState captures what a peer has seen and what they have asked for,
// scoped to a single (topic, slot, attestation_data) bucket.
type peerAttestationState struct {
	// available bits indicate committee positions the peer holds (set via
	// peer-sent metadata Have or inferred from received attestations).
	available bitmap.Bitmap
	// pendingWant bits indicate committee positions the peer just requested
	// via metadata Want. Cleared on the next outgoing publish to the peer
	// for this bucket — requests are non-persistent per spec.
	pendingWant bitmap.Bitmap
	// sentAvailable/sentRequests track metadata IDs already advertised to this
	// peer for this bucket, so outgoing metadata can be encoded as deltas.
	sentAvailable bitmap.Bitmap
	sentRequests  bitmap.Bitmap
}

func newPeerAttestationState(committeeSize int) *peerAttestationState {
	return &peerAttestationState{
		available:     newCommitteeBitmap(committeeSize),
		pendingWant:   newCommitteeBitmap(committeeSize),
		sentAvailable: newCommitteeBitmap(committeeSize),
		sentRequests:  newCommitteeBitmap(committeeSize),
	}
}

// peerState tracks per-peer flags that span all buckets at the manager level.
// `gossipPeer` is true once we've seen the peer act like a non-mesh peer
// (i.e., they sent us a Want, or libp2p told us to gossip to them via
// EmitGossip). Per-bucket peer state (available, pendingWant) lives inside
// the AttestationState.
type peerState struct {
	gossipPeer bool
	// sendAvailableList is set when EmitGossip selects this peer for a gossip
	// heartbeat round. The next publish tick includes our Available list to the
	// peer and the flag is cleared (the peer is dropped from peerStates after
	// serving), so Available is advertised at heartbeat cadence rather than
	// re-sent every publish tick in response to the peer's own Available — which
	// re-adds it via onIncomingRPC without setting this flag.
	sendAvailableList bool
}

// AttestationState is the per-(topic, slot, attestation_data) state.
//
// Forks at the same slot get independent state per `data`.
type AttestationState struct {
	data     []byte
	dataHash []byte

	// attestations holds one entry per committee position we hold.
	attestations map[int]*PartialAttestationEntry

	// validating is the set of positions accepted into the verifier but not
	// yet validated; validated is the set of positions the verifier has
	// promoted to forwardable.
	validating map[int]struct{}
	validated  map[int]struct{}

	// sendCount tracks how many peers each attestation has been forwarded to.
	sendCount map[int]int

	// requestCount tracks how many gossip peers we have asked for each
	// missing position. Capped at maxIWantPerPosition.
	requestCount map[int]int

	// peers holds per-peer available/pendingWant for this bucket. The map
	// only contains peers we have actually exchanged messages with for this
	// bucket; absence is equivalent to a zero bitmap.
	peers map[peer.ID]*peerAttestationState

	newSinceLastTick bool
}

func newAttestationState(data []byte, hashes ...[]byte) *AttestationState {
	hash := hashAttestationData(data)
	if len(hashes) > 0 {
		hash = slices.Clone(hashes[0])
	}
	return &AttestationState{
		data:         slices.Clone(data),
		dataHash:     slices.Clone(hash),
		attestations: make(map[int]*PartialAttestationEntry),
		validating:   make(map[int]struct{}),
		validated:    make(map[int]struct{}),
		sendCount:    make(map[int]int),
		requestCount: make(map[int]int),
		peers:        make(map[peer.ID]*peerAttestationState),
	}
}

// PartialAttestationSlotState holds all buckets (per-AttestationData) for a
// (topic, slot).
type PartialAttestationSlotState struct {
	slot            int
	attestationsMap map[string]*AttestationState // string(sha256(attestation_data)) => AttestationState
}

func newSlotState(slot int) *PartialAttestationSlotState {
	return &PartialAttestationSlotState{
		slot:            slot,
		attestationsMap: make(map[string]*AttestationState),
	}
}

// partialAttestationManager is the application-side manager for the
// CommitteeAttestation propagation algorithm.
type partialAttestationManager struct {
	logger *slog.Logger
	node   *Node
	ext    *partialmessages.PartialMessagesExtension[peerState]

	publishStart  time.Time
	slotDuration  time.Duration
	committeeSize int

	topicIndexMap map[string]int // topic name -> stable index, used for log tagging
	identities    attestationIdentityCache
	sentDataFull  map[peer.ID]map[string]struct{}
	sentMetaFull  map[peer.ID]map[string]struct{}

	mu    sync.Mutex
	slots map[string]map[int]*PartialAttestationSlotState

	lifecycleStarted  bool
	activeSlot        int
	highestClosedSlot int
}

func newPartialAttestationManager(
	n *Node,
	publishStart time.Time,
	slotDuration time.Duration,
	topicIndexMap map[string]int,
) *partialAttestationManager {
	logger := slog.With("node", n.Num, "component", "partial")
	if n.CommitteeSize <= 0 {
		panic("CommitteeSize must be set (= num_attestors per topic)")
	}
	m := &partialAttestationManager{
		logger:        logger,
		node:          n,
		publishStart:  publishStart,
		slotDuration:  slotDuration,
		committeeSize: n.CommitteeSize,
		topicIndexMap: topicIndexMap,
		identities:    newAttestationIdentityCache(),
		sentDataFull:  make(map[peer.ID]map[string]struct{}),
		sentMetaFull:  make(map[peer.ID]map[string]struct{}),
		slots:         make(map[string]map[int]*PartialAttestationSlotState),
	}
	return m
}

// slotStartTime returns the absolute wall-clock start of the given slot. Used
// to compute receive latency.
func (m *partialAttestationManager) slotStartTime(slot int) time.Time {
	return m.publishStart.Add(time.Duration(slot-1) * m.slotDuration)
}

func (m *partialAttestationManager) getOrCreateSlotState(topic string, slot int) *PartialAttestationSlotState {
	topicSlots, ok := m.slots[topic]
	if !ok {
		topicSlots = make(map[int]*PartialAttestationSlotState)
		m.slots[topic] = topicSlots
	}
	ss, ok := topicSlots[slot]
	if !ok {
		ss = newSlotState(slot)
		topicSlots[slot] = ss
	}
	return ss
}

func (m *partialAttestationManager) getSlotState(topic string, slot int) *PartialAttestationSlotState {
	topicSlots, ok := m.slots[topic]
	if !ok {
		return nil
	}
	return topicSlots[slot]
}

func (m *partialAttestationManager) SlotStart(slot int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lifecycleStarted = true
	m.activeSlot = slot
	for topic, topicSlots := range m.slots {
		for s := range topicSlots {
			if s < slot {
				delete(topicSlots, s)
			}
		}
		if len(topicSlots) == 0 {
			delete(m.slots, topic)
		}
	}
}

func (m *partialAttestationManager) SlotEnd(slot int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lifecycleStarted = true
	if slot > m.highestClosedSlot {
		m.highestClosedSlot = slot
	}
	if m.activeSlot == slot {
		m.activeSlot = 0
	}
	for topic, topicSlots := range m.slots {
		delete(topicSlots, slot)
		if len(topicSlots) == 0 {
			delete(m.slots, topic)
		}
	}
}

func (m *partialAttestationManager) acceptsSlotLocked(slot int) bool {
	if slot <= m.highestClosedSlot {
		return false
	}
	if m.lifecycleStarted && m.activeSlot != slot {
		return false
	}
	return true
}

func (m *partialAttestationManager) getOrCreateAttestationState(topic string, slot int, data []byte) *AttestationState {
	hash := m.identities.remember(data)
	return m.getOrCreateAttestationStateByHash(topic, slot, data, hash)
}

func (m *partialAttestationManager) getOrCreateAttestationStateByHash(
	topic string,
	slot int,
	data []byte,
	hash []byte,
) *AttestationState {
	ss := m.getOrCreateSlotState(topic, slot)
	key := string(hash)
	b, ok := ss.attestationsMap[key]
	if !ok {
		b = newAttestationState(data, hash)
		ss.attestationsMap[key] = b
	} else if len(b.data) == 0 && len(data) > 0 {
		b.data = slices.Clone(data)
	}
	return b
}

// initAndGetPeerAttestationState returns (and creates as needed) per-bucket
// state for a peer. Caller must hold m.mu.
func initAndGetPeerAttestationState(b *AttestationState, p peer.ID, committeeSize int) *peerAttestationState {
	s, ok := b.peers[p]
	if !ok {
		s = newPeerAttestationState(committeeSize)
		b.peers[p] = s
	}
	return s
}

// publishLocal stores a self-produced attestation in the right bucket and
// marks it validated immediately.
func (m *partialAttestationManager) publishLocal(topic string, slot, position int, sig, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.acceptsSlotLocked(slot) {
		return
	}
	b := m.getOrCreateAttestationState(topic, slot, data)
	if _, ok := b.attestations[position]; ok {
		return
	}
	b.attestations[position] = &PartialAttestationEntry{
		Position:  position,
		Signature: sig,
		Data:      b.data,
	}
	b.validated[position] = struct{}{}
	b.newSinceLastTick = true
}

// addReceived adds attestations received from a peer (pending validation).
// Returns the indices of newly added attestations.
func (b *AttestationState) addReceived(positions []int, signatures [][]byte) []any {
	var newEntries []any
	for i, pos := range positions {
		if _, ok := b.attestations[pos]; ok {
			continue
		}
		pe := &PartialAttestationEntry{
			Position:  pos,
			Signature: signatures[i],
			Data:      b.data,
		}
		b.attestations[pos] = pe
		b.validating[pos] = struct{}{}
		newEntries = append(newEntries, pe)
	}
	return newEntries
}

// markValidated promotes positions from validating to validated after the
// batch verifier callback fires.
func (m *partialAttestationManager) markValidated(topic string, slot int, data []byte, entries []any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ss := m.getSlotState(topic, slot)
	if ss == nil {
		return
	}
	hash := m.identities.remember(data)
	b, ok := ss.attestationsMap[string(hash)]
	if !ok {
		return
	}

	now := time.Now()
	slotStart := m.slotStartTime(slot)
	topicIdx := m.topicIndexMap[topic]
	digest := attDigestHex(data)
	for _, entry := range entries {
		pe := entry.(*PartialAttestationEntry)
		delete(b.validating, pe.Position)
		b.validated[pe.Position] = struct{}{}
		b.newSinceLastTick = true
		latencyMs := now.Sub(slotStart).Milliseconds()
		m.logger.Info("attestation_validated",
			"slot", slot,
			"topic", topicIdx,
			"att_digest", digest,
			"position", pe.Position,
			"latency_ms", latencyMs,
		)
	}
}

// publishActions returns the PublishActionsFn for a given topic and slot.
//
// On each call:
//   - Every gossip peer currently in peerStates receives an "available"
//     envelope and is then dropped from peerStates, so each gossip peer is
//     served once per EmitGossip heartbeat rather than on every tick.
//   - For each missing position any gossip peer advertises in their
//     available, up to maxIWantPerPosition request targets are chosen across
//     all peers; the per-bucket counter is bumped accordingly.
//   - Mesh peers continue to receive data via push. No metadata is sent to
//     mesh peers (per spec).
func (m *partialAttestationManager) publishActions(topic string, slot int) partialmessages.PublishActionsFn[peerState] {
	return func(
		peerStates map[peer.ID]peerState,
		peerRequestsPartial func(peer.ID) bool,
	) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
			m.mu.Lock()
			defer m.mu.Unlock()

			ss := m.getSlotState(topic, slot)
			if ss == nil || len(ss.attestationsMap) == 0 {
				return
			}

			// Per-bucket: select positions to IWANT from each peer based on
			// what they advertise. This bumps requestCount.
			wantPerPeerPerData := make(map[peer.ID]map[string][]int)
			for attDataStr, b := range ss.attestationsMap {
				perPeer := selectIWantTargets(b, peerStates, m.committeeSize)
				for p, positions := range perPeer {
					if _, ok := wantPerPeerPerData[p]; !ok {
						wantPerPeerPerData[p] = make(map[string][]int)
					}
					wantPerPeerPerData[p][attDataStr] = positions
				}
			}

			for p, ps := range peerStates {
				ctrlEnvelope := &pb.ControlEnvelope{}
				dataEnvelope := &pb.BatchedAttestationEnvelope{}

				var totalPositionsSent int
				var attestationDataWithForwards int

				for attDataStr, b := range ss.attestationsMap {
					bps := initAndGetPeerAttestationState(b, p, m.committeeSize)

					// Data: build the BatchedAttestation for this bucket.
					if peerRequestsPartial(p) {
						positions := m.claimAttestationsToSend(b, bps, ps.gossipPeer)
						if len(positions) > 0 {
							batch := m.encodeBatchForPeer(p, b, positions)
							dataEnvelope.Batches = append(dataEnvelope.Batches, batch)
							totalPositionsSent += len(positions)
							attestationDataWithForwards++
							m.logger.Info("partial_send_data",
								"peer", shortPeer(p),
								"slot", slot,
								"topic", m.topicIndexMap[topic],
								"att_digest", attDigestHex(b.data),
								"positions_count", len(positions),
								"batch_bytes", proto.Size(batch),
							)
						}
					}

					// Always clear pendingWants, even if we
					// satisfied none of it — requests are non-persistent.
					bps.pendingWant = newCommitteeBitmap(m.committeeSize)

					// Metadata: only gossip peers. We advertise our Available
					// list only when sendAvailableList is set (once per
					// EmitGossip heartbeat); Requests (Wants) are always served.
					// This keeps available re-advertisement at heartbeat cadence
					// instead of every publish tick.
					if ps.gossipPeer {
						wantList := wantPerPeerPerData[p][attDataStr]
						md := getDeltaAttestationMetadata(b, bps, m.committeeSize, slot, wantList, ps.sendAvailableList)
						if md != nil {
							m.setMetadataIdentityForPeer(p, b, md)
							ctrlEnvelope.Metadatas = append(ctrlEnvelope.Metadatas, md)
							m.logger.Info("partial_send_metadata",
								"peer", shortPeer(p),
								"slot", slot,
								"topic", m.topicIndexMap[topic],
								"att_digest", attDigestHex(b.data),
								"available_ones", metadataOnes(md.AvailableIds, md.Available),
								"requests_ones", metadataOnes(md.RequestsIds, md.Requests),
								"md_bucket_bytes", proto.Size(md),
								"send_want", len(wantList) > 0,
							)
						}
					}
				}

				// We've collected everything this gossip peer needs this
				// round. Drop it from peerStates so we don't re-gossip on
				// every publish tick; the next EmitGossip heartbeat re-adds
				// it (and an incoming Want re-adds it sooner). Mesh peers are
				// re-added each tick by the extension, so they stay.
				if ps.gossipPeer {
					delete(peerStates, p)
				}

				var encodedCtrl, encodedData []byte
				var err error
				if len(ctrlEnvelope.Metadatas) > 0 {
					encodedCtrl, err = proto.Marshal(ctrlEnvelope)
					if err != nil {
						m.logger.Error("marshal control envelope", "err", err)
						encodedCtrl = nil
					}
				}
				if len(dataEnvelope.Batches) > 0 {
					encodedData, err = proto.Marshal(dataEnvelope)
					if err != nil {
						m.logger.Error("marshal data envelope", "err", err)
						encodedData = nil
					}
				}

				if encodedCtrl == nil && encodedData == nil {
					continue
				}

				peerType := "mesh"
				if ps.gossipPeer {
					peerType = "gossip"
				}
				m.logger.Info("partial_send_tick",
					"peer", shortPeer(p),
					"peer_type", peerType,
					"slot", slot,
					"topic", m.topicIndexMap[topic],
					"num_buckets", attestationDataWithForwards,
					"md_bytes", len(encodedCtrl),
					"data_bytes", len(encodedData),
					"total_positions_sent", totalPositionsSent,
				)

				if !yield(p, partialmessages.PublishAction{
					EncodedPartsMetadata:  encodedCtrl,
					EncodedPartialMessage: encodedData,
				}) {
					return
				}
			}

			// Mark all buckets as fully ticked once per call.
			for _, b := range ss.attestationsMap {
				b.newSinceLastTick = false
			}
		}
	}
}

// claimAttestationsToSend returns the set of positions to send to the peer whose
// per-attestation state is bps, and as a side effect updates the bookkeeping
// (sendCount, per-peer available). Caller must hold m.mu.
func (m *partialAttestationManager) claimAttestationsToSend(
	b *AttestationState,
	bps *peerAttestationState,
	gossipPeer bool,
) []int {
	if gossipPeer && bps.pendingWant.OnesCount() <= 0 {
		return nil
	}

	candidates := make([]int, 0, len(b.validated))
	for pos := range b.validated {
		if b.sendCount[pos] >= m.node.MaxPeersPerAttestation {
			continue
		}
		if bps.available.Get(pos) {
			continue
		}
		if gossipPeer && !bps.pendingWant.Get(pos) {
			continue
		}
		candidates = append(candidates, pos)
	}
	slices.Sort(candidates)
	for _, pos := range candidates {
		bps.available.Set(pos)
		b.sendCount[pos]++
	}
	return candidates
}

// selectIWantTargets picks, for each missing position advertised by some
// gossip peer in this bucket, up to (maxIWantPerPosition -
// already-sent) target peers to request it from. Bumps
// b.requestCount and returns the chosen positions per peer.
// Caller must hold m.mu.
func selectIWantTargets(b *AttestationState, peerStates map[peer.ID]peerState, committeeSize int) map[peer.ID][]int {
	candidatesByPos := make(map[int][]peer.ID)
	for p, ps := range peerStates {
		if !ps.gossipPeer {
			continue
		}
		bps := initAndGetPeerAttestationState(b, p, committeeSize)
		for pos := range committeeSize {
			if !bps.available.Get(pos) {
				continue
			}
			if _, have := b.attestations[pos]; have {
				continue
			}
			if b.requestCount[pos] >= maxIWantPerPosition {
				continue
			}
			b.requestCount[pos]++
			candidatesByPos[pos] = append(candidatesByPos[pos], p)
		}
	}
	wantPerPeer := make(map[peer.ID][]int)
	for pos, cands := range candidatesByPos {
		for _, p := range cands {
			wantPerPeer[p] = append(wantPerPeer[p], pos)
		}
	}
	for p := range wantPerPeer {
		slices.Sort(wantPerPeer[p])
	}
	return wantPerPeer
}

// getAttestationMetadata assembles a per-bucket CommitteeAttestationPartsMetadata.
// AvailableIds is populated from the bucket's validated positions only when
// includeAvailable is set (gated to the gossip heartbeat); wantList populates
// RequestsIds regardless. Returns nil if both would be empty.
func getAttestationMetadata(
	b *AttestationState,
	committeeSize int,
	slot int,
	wantList []int,
	includeAvailable bool,
) *pb.CommitteeAttestationPartsMetadata {
	md := &pb.CommitteeAttestationPartsMetadata{
		Slot: int32(slot),
	}

	if includeAvailable && len(b.validated) > 0 {
		md.AvailableIds = validatedIDs(b, nil, committeeSize)
	}

	if len(wantList) > 0 {
		md.RequestsIds = positionsToIDs(wantList, nil, committeeSize)
	}

	if len(md.AvailableIds) == 0 && len(md.RequestsIds) == 0 {
		return nil
	}
	return md
}

// getDeltaAttestationMetadata is the sender-side metadata builder. It emits
// only IDs this peer has not already been told for this bucket and records the
// IDs as sent before returning the metadata.
func getDeltaAttestationMetadata(
	b *AttestationState,
	bps *peerAttestationState,
	committeeSize int,
	slot int,
	wantList []int,
	includeAvailable bool,
) *pb.CommitteeAttestationPartsMetadata {
	ensurePeerMetadataBitmaps(bps, committeeSize)
	md := &pb.CommitteeAttestationPartsMetadata{Slot: int32(slot)}
	if includeAvailable && len(b.validated) > 0 {
		md.AvailableIds = validatedIDs(b, bps.sentAvailable, committeeSize)
		for _, pos := range md.AvailableIds {
			bps.sentAvailable.Set(int(pos))
		}
	}
	if len(wantList) > 0 {
		md.RequestsIds = positionsToIDs(wantList, bps.sentRequests, committeeSize)
		for _, pos := range md.RequestsIds {
			bps.sentRequests.Set(int(pos))
		}
	}
	if len(md.AvailableIds) == 0 && len(md.RequestsIds) == 0 {
		return nil
	}
	return md
}

func ensurePeerMetadataBitmaps(bps *peerAttestationState, committeeSize int) {
	if bps.sentAvailable == nil {
		bps.sentAvailable = newCommitteeBitmap(committeeSize)
	}
	if bps.sentRequests == nil {
		bps.sentRequests = newCommitteeBitmap(committeeSize)
	}
}

func validatedIDs(b *AttestationState, alreadySent bitmap.Bitmap, committeeSize int) []uint32 {
	positions := make([]int, 0, len(b.validated))
	for pos := range b.validated {
		positions = append(positions, pos)
	}
	return positionsToIDs(positions, alreadySent, committeeSize)
}

func positionsToIDs(positions []int, alreadySent bitmap.Bitmap, committeeSize int) []uint32 {
	if len(positions) == 0 {
		return nil
	}
	positions = slices.Clone(positions)
	slices.Sort(positions)
	ids := make([]uint32, 0, len(positions))
	for _, pos := range positions {
		if pos < 0 || pos >= committeeSize {
			continue
		}
		if alreadySent != nil && alreadySent.Get(pos) {
			continue
		}
		ids = append(ids, uint32(pos))
	}
	return ids
}

func metadataBitmap(ids []uint32, legacy []byte, committeeSize int) (bitmap.Bitmap, error) {
	out := newCommitteeBitmap(committeeSize)
	for _, id := range ids {
		if int(id) >= committeeSize {
			return nil, fmt.Errorf("metadata id %d >= committee_size %d", id, committeeSize)
		}
		out.Set(int(id))
	}
	legacyBm := bitmap.Bitmap(legacy)
	for pos := range committeeSize {
		if legacyBm.Get(pos) {
			out.Set(pos)
		}
	}
	return out, nil
}

func metadataOnes(ids []uint32, legacy []byte) int {
	return len(ids) + availableOnes(legacy)
}

// encodeBatch builds a BatchedAttestation for the given positions. Caller must
// ensure all entries exist in b.attestations. AttestorIndices and Signatures
// are emitted in the same order as `positions`.
func (m *partialAttestationManager) encodeBatchForPeer(
	p peer.ID,
	b *AttestationState,
	positions []int,
) *pb.BatchedAttestation {
	idxs := make([]uint32, 0, len(positions))
	sigs := make([][]byte, 0, len(positions))
	for _, pos := range positions {
		idxs = append(idxs, uint32(pos))
		sigs = append(sigs, b.attestations[pos].Signature)
	}
	batch := &pb.BatchedAttestation{
		AttestorIndices: idxs,
		Signatures:      sigs,
	}
	includeFullData := !peerSentFull(m.sentDataFull, p, b.dataHash) && len(b.data) > 0
	setBatchIdentity(batch, b, includeFullData)
	if includeFullData {
		markPeerSentFull(m.sentDataFull, p, b.dataHash)
	}
	return batch
}

func (m *partialAttestationManager) encodeBatch(
	b *AttestationState,
	positions []int,
) *pb.BatchedAttestation {
	return m.encodeBatchForPeer("", b, positions)
}

func (m *partialAttestationManager) setMetadataIdentityForPeer(
	p peer.ID,
	b *AttestationState,
	md *pb.CommitteeAttestationPartsMetadata,
) {
	includeFullData := !peerSentFull(m.sentMetaFull, p, b.dataHash) && len(b.data) > 0
	setMetadataIdentity(md, b, includeFullData)
	if includeFullData {
		markPeerSentFull(m.sentMetaFull, p, b.dataHash)
	}
}

// fanoutPublish eagerly sends a single attestation to all peers via
// PublishPartial. Used by fanout nodes which have no mesh peers and can't
// rely on the tick-based publish loop.
func (m *partialAttestationManager) fanoutPublish(
	ps *pubsub.PubSub,
	topic string,
	slot, position int,
	signature, data []byte,
) {
	batch := &pb.BatchedAttestation{
		AttestationData: data,
		AttestorIndices: []uint32{uint32(position)},
		Signatures:      [][]byte{signature},
	}
	env := &pb.BatchedAttestationEnvelope{Batches: []*pb.BatchedAttestation{batch}}
	encoded, err := proto.Marshal(env)
	if err != nil {
		m.logger.Error("marshal fanout envelope", "err", err)
		return
	}

	digest := attDigestHex(data)
	topicIdx := m.topicIndexMap[topic]

	actions := func(peerStates map[peer.ID]peerState, _ func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
			for p := range peerStates {
				m.logger.Info("partial_fanout_publish",
					"topic", topicIdx,
					"slot", slot,
					"att_digest", digest,
					"position", position,
					"peer", shortPeer(p),
					"batch_bytes", len(encoded),
				)
				if !yield(p, partialmessages.PublishAction{EncodedPartialMessage: encoded}) {
					return
				}
			}
		}
	}

	if err := pubsub.PublishPartial(ps, topic, slotGroupID(slot), actions); err != nil {
		m.logger.Error("fanout publish partial", "topic", topic, "slot", slot, "err", err)
	}
}

// onEmitGossip is called by gossipsub during heartbeat to gossip to non-mesh
// peers. Mark them as gossip peers; the next publish tick serves each one an
// Available envelope and then drops it from peerStates until the following
// heartbeat re-adds it.
func (m *partialAttestationManager) onEmitGossip(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
	if m.node.DisableIHaveGossip {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range gossipPeers {
		ps := peerStates[p]
		ps.gossipPeer = true
		// Advertise our Available list to this peer on the next publish tick.
		// Set only here (per heartbeat), so available re-advertisement runs at
		// heartbeat cadence rather than every publish tick.
		ps.sendAvailableList = true
		peerStates[p] = ps
	}
}

// onIncomingRPC handles incoming partial-extension RPCs from peers.
func (m *partialAttestationManager) onIncomingRPC(from peer.ID, peerStates map[peer.ID]peerState, rpc *pubsub_pb.PartialMessagesExtension) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic := rpc.GetTopicID()
	slot := groupIDToSlot(rpc.GroupID)
	topicIdx := m.topicIndexMap[topic]
	if !m.acceptsSlotLocked(slot) {
		m.logger.Debug("drop stale partial rpc", "from", shortPeer(from), "slot", slot, "topic", topicIdx)
		return nil
	}

	var ctrl pb.ControlEnvelope
	if len(rpc.PartsMetadata) > 0 {
		if err := proto.Unmarshal(rpc.PartsMetadata, &ctrl); err != nil {
			return fmt.Errorf("unmarshal control envelope: %w", err)
		}
	}
	var dataEnv pb.BatchedAttestationEnvelope
	if len(rpc.PartialMessage) > 0 {
		if err := proto.Unmarshal(rpc.PartialMessage, &dataEnv); err != nil {
			return fmt.Errorf("unmarshal data envelope: %w", err)
		}
	}

	m.logger.Info("partial_recv_rpc",
		"from", shortPeer(from),
		"slot", slot,
		"topic", topicIdx,
		"num_metadatas", len(ctrl.Metadatas),
		"num_batches", len(dataEnv.Batches),
		"md_bytes", len(rpc.PartsMetadata),
		"data_bytes", len(rpc.PartialMessage),
	)

	// Process metadatas first so that available/pendingWant is up-to-date
	// when we later infer available from received attestations.
	for _, md := range ctrl.Metadatas {
		data, hash, err := m.identities.resolve(md.AttestationData, md.AttestationDataHash, false)
		if err != nil {
			m.logger.Error("drop partial metadata", "from", shortPeer(from), "err", err)
			continue
		}
		b := m.getOrCreateAttestationStateByHash(topic, slot, data, hash)
		bps := initAndGetPeerAttestationState(b, from, m.committeeSize)

		available, err := metadataBitmap(md.AvailableIds, md.Available, m.committeeSize)
		if err != nil {
			return err
		}
		bps.available.Or(available)

		requests, err := metadataBitmap(md.RequestsIds, md.Requests, m.committeeSize)
		if err != nil {
			return err
		}
		bps.pendingWant.Or(requests)

		// A peer issuing metadata is by definition a gossip peer. Marking it
		// here also re-adds it to peerStates if a prior publish tick dropped
		// it, so its Want is served on the next tick.
		if available.OnesCount() > 0 || requests.OnesCount() > 0 {
			ps := peerStates[from]
			ps.gossipPeer = true
			peerStates[from] = ps
		}

		m.logger.Info("partial_recv_metadata",
			"from", shortPeer(from),
			"slot", slot,
			"topic", topicIdx,
			"att_digest", attDigestHexFor(data, hash),
			"available_ones", available.OnesCount(),
			"requests_ones", requests.OnesCount(),
		)
	}

	// Process data batches.
	for _, batch := range dataEnv.Batches {
		data, hash, err := m.identities.resolve(
			batch.AttestationData,
			batch.AttestationDataHash,
			true,
		)
		if err != nil {
			m.logger.Error("drop partial batch", "from", shortPeer(from), "err", err)
			continue
		}
		b := m.getOrCreateAttestationStateByHash(topic, slot, data, hash)
		bps := initAndGetPeerAttestationState(b, from, m.committeeSize)

		if len(batch.AttestorIndices) != len(batch.Signatures) {
			return fmt.Errorf("attestor_indices=%d != signatures=%d", len(batch.AttestorIndices), len(batch.Signatures))
		}
		positions := make([]int, len(batch.AttestorIndices))
		for i, p := range batch.AttestorIndices {
			if int(p) >= m.committeeSize {
				return fmt.Errorf("attestor index %d >= committee_size %d", p, m.committeeSize)
			}
			positions[i] = int(p)
		}

		newEntries := b.addReceived(positions, batch.Signatures)

		batchBytes := proto.Size(batch)
		m.logger.Info("partial_recv_batch",
			"from", shortPeer(from),
			"slot", slot,
			"topic", topicIdx,
			"att_digest", attDigestHexFor(data, hash),
			"positions_count", len(positions),
			"new_positions", len(newEntries),
			"batch_bytes", batchBytes,
		)

		// Notify the application tracer for each newly-received position.
		if m.node.Tracer != nil {
			slotStart := m.slotStartTime(slot)
			latencyMs := time.Since(slotStart).Milliseconds()
			for _, entry := range newEntries {
				pe := entry.(*PartialAttestationEntry)
				m.node.Tracer.OnPartialReceive(slot, topicIdx, pe.Position, b.data, latencyMs)
			}
		}

		// Infer peer's available |= positions sent.
		for _, pos := range positions {
			bps.available.Set(pos)
		}

		if len(newEntries) > 0 {
			data := b.data
			m.node.verifier.Submit(
				verify.Item{Topic: topic, Slot: slot, Data: data, Attestations: newEntries},
				func(item verify.Item) {
					m.markValidated(item.Topic, item.Slot, item.Data, item.Attestations)
				},
			)
		}
	}

	return nil
}

func (m *partialAttestationManager) publishTick(ps *pubsub.PubSub, topics []string) {
	for _, topic := range topics {
		m.mu.Lock()
		topicSlots := m.slots[topic]
		slots := slices.Collect(maps.Keys(topicSlots))
		m.mu.Unlock()

		for _, slot := range slots {
			actions := m.publishActions(topic, slot)
			if actions == nil {
				continue
			}
			if err := pubsub.PublishPartial(ps, topic, slotGroupID(slot), actions); err != nil {
				m.logger.Error("publish partial", "topic", topic, "slot", slot, "err", err)
			}
		}
	}
}

func (m *partialAttestationManager) runPublishLoop(ctx interface{ Done() <-chan struct{} }, ps *pubsub.PubSub, topics []string) {
	time.Sleep(time.Duration(rand.Int64N(m.node.PublishInterval.Milliseconds())) * time.Millisecond)
	m.publishTick(ps, topics)

	ticker := time.NewTicker(m.node.PublishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.publishTick(ps, topics)
		}
	}
}

func (m *partialAttestationManager) newPartialMessagesExtension() *partialmessages.PartialMessagesExtension[peerState] {
	m.ext = &partialmessages.PartialMessagesExtension[peerState]{
		Logger:             m.logger,
		OnEmitGossip:       m.onEmitGossip,
		OnIncomingRPC:      m.onIncomingRPC,
		GroupTTLByHeatbeat: 10,
	}
	return m.ext
}

// slotGroupID returns the groupID bytes for a slot number.
func slotGroupID(slot int) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(slot))
}

// groupIDToSlot converts a groupID back to a slot number. Bytes shorter than
// 4 are zero-padded on the left.
func groupIDToSlot(groupID []byte) int {
	var buf [4]byte
	if len(groupID) > 4 {
		groupID = groupID[len(groupID)-4:]
	}
	copy(buf[4-len(groupID):], groupID)
	return int(binary.BigEndian.Uint32(buf[:]))
}

// shortPeer returns a short prefix of the peer ID for logging.
func shortPeer(p peer.ID) string {
	s := p.String()
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// newCommitteeBitmap returns a zero bitmap.Bitmap of capacity committeeSize
// bits ((committeeSize+7)/8 bytes).
func newCommitteeBitmap(committeeSize int) bitmap.Bitmap {
	return make(bitmap.Bitmap, (committeeSize+7)/8)
}

// availableOnes counts the data bits in a fixed-width committee bitmap.
func availableOnes(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	return bitmap.Bitmap(b).OnesCount()
}

// requestsOnes is just availableOnes — named for log readability.
func requestsOnes(b []byte) int { return availableOnes(b) }
