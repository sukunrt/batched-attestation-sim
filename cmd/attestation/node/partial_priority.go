package node

import (
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
)

// MaxAttestationsPerMessage is the default per-data-message size cap N: an
// outgoing partial-priority data RPC carries at most this many attestations
// (~96 bytes of signature each, so 30 ≈ 3 KB). Override per node via
// Node.MaxAttestationsPerMessage. It bounds message SIZE only; the per-tick
// send VOLUME is unbounded (everything sendable is sent, just chunked).
const MaxAttestationsPerMessage = 30

// partial-priority reuses partial.go's data model (AttestationState, peerState,
// peerAttestationState, PartialAttestationEntry) and its pure helpers
// (getAttestationMetadata, selectIWantTargets, addReceived, newCommitteeBitmap,
// slotGroupID, shortPeer, attDigestHex, …). The only behavioral divergence is
// the send selection: rather than one big push per peer per tick, it round-
// robins one ≤ N-sized data message to each peer per pass — least-forwarded
// attestations first, committed as drawn so each peer's pick spreads across the
// others — with the gossip metadata advertisement as its own message.

// idxEntry identifies one forwardable attestation across all buckets at a
// (topic, slot): bucketKey is string(attestation_data); pos is the committee
// position. The same position in two forks is two distinct entries with
// independent sendCount.
type idxEntry struct {
	bucketKey string
	pos       int
}

// countLevel holds the entries currently at one sendCount value. entries is an
// append-ordered slice (deterministic, no per-tick sort); at maps an entry to
// its slice index for O(1) swap-delete.
type countLevel struct {
	entries []idxEntry
	at      map[idxEntry]int
}

func (l *countLevel) add(e idxEntry) {
	if l.at == nil {
		l.at = make(map[idxEntry]int)
	}
	if _, ok := l.at[e]; ok {
		return
	}
	l.at[e] = len(l.entries)
	l.entries = append(l.entries, e)
}

func (l *countLevel) remove(e idxEntry) {
	i, ok := l.at[e]
	if !ok {
		return
	}
	last := len(l.entries) - 1
	if i != last {
		moved := l.entries[last]
		l.entries[i] = moved
		l.at[moved] = i
	}
	l.entries = l.entries[:last]
	delete(l.at, e)
}

// prioritySlotState holds all buckets for a (topic, slot) plus a sendCount-
// ordered index over their validated positions. levels[k] holds the entries
// whose current sendCount == k, for k in [0, maxPeers). Selection walks levels
// ascending (least-forwarded first); committing a send moves an entry from
// level k to k+1 in O(1). An entry reaching maxPeers is dropped (it has hit the
// MaxPeersPerAttestation lifetime ceiling and is no longer a candidate).
//
// Invariant: idxEntry{bk,pos} is present in exactly one level k iff
// pos ∈ attestationsMap[bk].validated and sendCount[pos] < maxPeers, with
// k == sendCount[pos]. checkIndexInvariant asserts this in tests.
type prioritySlotState struct {
	slot            int
	attestationsMap map[string]*AttestationState // string(attestation_data) => bucket

	maxPeers  int          // = node.MaxPeersPerAttestation, the level count / cap
	levels    []countLevel // index by sendCount value
	bucketSeq map[string]int
	nextSeq   int
}

func newPrioritySlotState(slot, maxPeers int) *prioritySlotState {
	return &prioritySlotState{
		slot:            slot,
		attestationsMap: make(map[string]*AttestationState),
		maxPeers:        maxPeers,
		levels:          make([]countLevel, maxPeers),
		bucketSeq:       make(map[string]int),
	}
}

func (ss *prioritySlotState) ensureBucketSeq(bk string) {
	if _, ok := ss.bucketSeq[bk]; !ok {
		ss.bucketSeq[bk] = ss.nextSeq
		ss.nextSeq++
	}
}

// indexAddValidated inserts a newly-validated position at its sendCount level.
// A position at/over the lifetime ceiling is never a candidate, so it is
// skipped.
func (ss *prioritySlotState) indexAddValidated(bk string, pos, sendCount int) {
	if sendCount >= ss.maxPeers {
		return
	}
	ss.ensureBucketSeq(bk)
	ss.levels[sendCount].add(idxEntry{bk, pos})
}

// indexBump moves an entry from level `from` to `from+1` after sendCount has
// been incremented. Reaching maxPeers drops the entry entirely.
func (ss *prioritySlotState) indexBump(bk string, pos, from int) {
	e := idxEntry{bk, pos}
	if from < len(ss.levels) {
		ss.levels[from].remove(e)
	}
	if to := from + 1; to < len(ss.levels) {
		ss.levels[to].add(e)
	}
}

// priorityAttestationManager is the partial-priority forwarding strategy: a
// drop-in alternative to partialAttestationManager that keeps every outgoing
// data message small and pushes the least-forwarded attestations first.
type priorityAttestationManager struct {
	logger *slog.Logger
	node   *Node
	ext    *partialmessages.PartialMessagesExtension[peerState]

	publishStart  time.Time
	slotDuration  time.Duration
	committeeSize int
	maxPerMessage int // N: per-data-message attestation cap

	topicIndexMap map[string]int // topic name -> stable index, used for log tagging

	mu    sync.Mutex
	slots map[string]map[int]*prioritySlotState
}

func newPriorityAttestationManager(
	n *Node,
	publishStart time.Time,
	slotDuration time.Duration,
	topicIndexMap map[string]int,
) *priorityAttestationManager {
	logger := slog.With("node", n.Num, "component", "partial-priority")
	if n.CommitteeSize <= 0 {
		panic("CommitteeSize must be set (= num_attestors per topic)")
	}
	maxPerMessage := n.MaxAttestationsPerMessage
	if maxPerMessage <= 0 {
		maxPerMessage = MaxAttestationsPerMessage
	}
	return &priorityAttestationManager{
		logger:        logger,
		node:          n,
		publishStart:  publishStart,
		slotDuration:  slotDuration,
		committeeSize: n.CommitteeSize,
		maxPerMessage: maxPerMessage,
		topicIndexMap: topicIndexMap,
		slots:         make(map[string]map[int]*prioritySlotState),
	}
}

func (m *priorityAttestationManager) slotStartTime(slot int) time.Time {
	return m.publishStart.Add(time.Duration(slot-1) * m.slotDuration)
}

func (m *priorityAttestationManager) getOrCreateSlotState(topic string, slot int) *prioritySlotState {
	topicSlots, ok := m.slots[topic]
	if !ok {
		topicSlots = make(map[int]*prioritySlotState)
		m.slots[topic] = topicSlots
	}
	ss, ok := topicSlots[slot]
	if !ok {
		ss = newPrioritySlotState(slot, m.node.MaxPeersPerAttestation)
		topicSlots[slot] = ss
	}
	return ss
}

func (m *priorityAttestationManager) getSlotState(topic string, slot int) *prioritySlotState {
	topicSlots, ok := m.slots[topic]
	if !ok {
		return nil
	}
	return topicSlots[slot]
}

// getOrCreateBucket returns (creating as needed) the bucket for attestation_data
// within a slot state, registering its stable bucket-order seq. Caller holds m.mu.
func getOrCreateBucket(ss *prioritySlotState, data []byte) *AttestationState {
	key := string(data)
	b, ok := ss.attestationsMap[key]
	if !ok {
		b = newAttestationState(data)
		ss.attestationsMap[key] = b
		ss.ensureBucketSeq(key)
	}
	return b
}

func (m *priorityAttestationManager) getOrCreateAttestationState(topic string, slot int, data []byte) *AttestationState {
	ss := m.getOrCreateSlotState(topic, slot)
	return getOrCreateBucket(ss, data)
}

// publishLocal stores a self-produced attestation in the right bucket, marks it
// validated immediately, and inserts it into the priority index.
func (m *priorityAttestationManager) publishLocal(topic string, slot, position int, sig, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ss := m.getOrCreateSlotState(topic, slot)
	b := getOrCreateBucket(ss, data)
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
	ss.indexAddValidated(string(b.data), position, b.sendCount[position])
}

// markValidated promotes positions from validating to validated after the batch
// verifier callback fires, inserting each into the priority index.
func (m *priorityAttestationManager) markValidated(topic string, slot int, data []byte, entries []any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ss := m.getSlotState(topic, slot)
	if ss == nil {
		return
	}
	b, ok := ss.attestationsMap[string(data)]
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
		ss.indexAddValidated(string(b.data), pe.Position, b.sendCount[pe.Position])
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

// publishActions returns the PublishActionsFn for a (topic, slot). The
// partial-priority send round-robins one ≤ N-sized data message to each
// requesting peer per pass (least-forwarded validated positions first, drawn
// across all buckets and committed as drawn so the next peer's pick reflects
// it), repeating passes until every peer is drained. The gossip metadata
// advertisement is yielded afterward as its own separate message per peer.
func (m *priorityAttestationManager) publishActions(topic string, slot int) partialmessages.PublishActionsFn[peerState] {
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
			topicIdx := m.topicIndexMap[topic]

			// IWANT: per bucket, pick gossip peers to request missing positions
			// from. Bumps requestCount. Unchanged from partial mode.
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

			// DATA: round-robin one ≤ N-sized chunk to each requesting peer per
			// pass, least-forwarded first across all buckets. Each draw commits
			// its positions before the next peer is served, so the next peer's
			// pick reflects what this one just took — spreading scarce
			// attestations across peers instead of draining one peer fully.
			// Repeat passes until every peer is exhausted. requesters is a
			// snapshot so the per-pass order stays stable.
			var requesters []peer.ID
			for p := range peerStates {
				if peerRequestsPartial(p) {
					requesters = append(requesters, p)
				}
			}
			// availEnv advertises our validated bitmap across all buckets. It is
			// constant for the tick (our validated set can't change while m.mu is
			// held), so it is built once and piggybacked on the FIRST data message
			// to each mesh peer (tracked by sentAvailableTo).
			var availEnv []byte
			if m.node.SendAvailableWithData {
				availEnv = m.buildAvailableEnvelope(ss, slot)
			}
			sentAvailableTo := make(map[peer.ID]bool)
			for {
				progressed := false
				for _, p := range requesters {
					chunk, more := m.selectOneChunkForPeer(ss, p, peerStates[p].gossipPeer)
					if chunk == nil {
						continue
					}
					progressed = true
					dataEnv := &pb.BatchedAttestationEnvelope{}
					var total int
					bks := slices.Collect(maps.Keys(chunk))
					slices.SortFunc(bks, func(a, b string) int {
						return ss.bucketSeq[a] - ss.bucketSeq[b]
					})
					for _, bk := range bks {
						b := ss.attestationsMap[bk]
						dataEnv.Batches = append(dataEnv.Batches, m.encodeBatch(b, chunk[bk]))
						total += len(chunk[bk])
					}
					encodedData, err := proto.Marshal(dataEnv)
					if err != nil {
						m.logger.Error("marshal data envelope", "err", err)
						continue
					}
					action := partialmessages.PublishAction{EncodedPartialMessage: encodedData}
					// Piggyback our available bitmap onto this (data-carrying)
					// message for a mesh peer, but ONLY when we still hold more the
					// peer needs beyond this chunk (`more`) — if this message already
					// carries everything we have for the peer, the data conveys our
					// state and the bitmap is pure overhead. Once per tick per peer,
					// gossip peers excluded (they get available via the metadata
					// action below). Never sent without data, so the receiver won't
					// reclassify us as a gossip peer.
					if availEnv != nil && more && !peerStates[p].gossipPeer && !sentAvailableTo[p] {
						action.EncodedPartsMetadata = availEnv
						sentAvailableTo[p] = true
					}
					m.logger.Info("partial_send_tick",
						"peer", shortPeer(p),
						"peer_type", peerTypeLabel(peerStates[p]),
						"slot", slot,
						"topic", topicIdx,
						"num_buckets", len(bks),
						"data_bytes", len(encodedData),
						"md_bytes", len(action.EncodedPartsMetadata),
						"total_positions_sent", total,
					)
					if !yield(p, action) {
						return
					}
				}
				if !progressed {
					break
				}
			}

			// pendingWant clear + gossip METADATA, per peer. Run after all data
			// so a peer's requests stay readable until its data fully drains.
			for p, ps := range peerStates {
				// Clear pendingWants regardless of what we sent — requests are
				// non-persistent (read above by the gossip-peer draw).
				for _, b := range ss.attestationsMap {
					if bps, ok := b.peers[p]; ok {
						bps.pendingWant = newCommitteeBitmap(m.committeeSize)
					}
				}

				// METADATA: gossip peers only, in its own metadata-only action.
				if ps.gossipPeer {
					ctrl := &pb.ControlEnvelope{}
					for attDataStr, b := range ss.attestationsMap {
						wantList := wantPerPeerPerData[p][attDataStr]
						md := getAttestationMetadata(b, m.committeeSize, slot, wantList, ps.sendAvailableList)
						if md != nil {
							ctrl.Metadatas = append(ctrl.Metadatas, md)
						}
					}
					if len(ctrl.Metadatas) > 0 {
						if encodedCtrl, err := proto.Marshal(ctrl); err != nil {
							m.logger.Error("marshal control envelope", "err", err)
						} else {
							m.logger.Info("partial_send_metadata",
								"peer", shortPeer(p),
								"slot", slot,
								"topic", topicIdx,
								"num_buckets", len(ctrl.Metadatas),
								"md_bytes", len(encodedCtrl),
							)
							if !yield(p, partialmessages.PublishAction{EncodedPartsMetadata: encodedCtrl}) {
								return
							}
						}
					}
					// Served this round; drop until the next heartbeat re-adds it.
					delete(peerStates, p)
				}
			}

			for _, b := range ss.attestationsMap {
				b.newSinceLastTick = false
			}
		}
	}
}

func peerTypeLabel(ps peerState) string {
	if ps.gossipPeer {
		return "gossip"
	}
	return "mesh"
}

// buildAvailableEnvelope marshals an available-only ControlEnvelope advertising
// our validated positions for every bucket at this (topic, slot), in bucketSeq
// order. Returns nil when nothing is validated yet (or marshalling fails). Reuses
// getAttestationMetadata so the bitmap encoding matches the gossip metadata path.
// Caller holds m.mu.
func (m *priorityAttestationManager) buildAvailableEnvelope(ss *prioritySlotState, slot int) []byte {
	bks := slices.Collect(maps.Keys(ss.attestationsMap))
	slices.SortFunc(bks, func(a, b string) int { return ss.bucketSeq[a] - ss.bucketSeq[b] })
	ctrl := &pb.ControlEnvelope{}
	for _, bk := range bks {
		if md := getAttestationMetadata(ss.attestationsMap[bk], m.committeeSize, slot, nil, true); md != nil {
			ctrl.Metadatas = append(ctrl.Metadatas, md)
		}
	}
	if len(ctrl.Metadatas) == 0 {
		return nil
	}
	encoded, err := proto.Marshal(ctrl)
	if err != nil {
		m.logger.Error("marshal available envelope", "err", err)
		return nil
	}
	return encoded
}

// selectOneChunkForPeer draws up to maxPerMessage of the positions this peer
// needs, in least-forwarded-first order across all buckets, and commits each
// draw (per-peer available, sendCount, index bump) so the next peer served in
// the same pass sees the updated sendCount. It returns one chunk as a
// bucketKey->positions map in priority order (nil when the peer has nothing left
// to send) plus `more`: whether the peer needs additional positions we hold
// beyond this chunk (i.e. we hit the per-message cap with candidates to spare).
// Caller holds m.mu.
//
// Candidates are collected read-only first — ascending sendCount level, and
// within a level sorted by bucketSeq then position so the order is deterministic
// despite the index's swap-delete reordering — then committed, so the index
// mutation can't perturb the scan. The per-peer available bitmap guards against
// re-drawing a position across passes. Entries in ss.levels are validated and
// under the lifetime ceiling by construction, so no extra guard is needed.
func (m *priorityAttestationManager) selectOneChunkForPeer(ss *prioritySlotState, p peer.ID, gossipPeer bool) (map[string][]int, bool) {
	var drawn []idxEntry
	more := false
	for k := 0; k < len(ss.levels) && !more; k++ {
		var cand []idxEntry
		for _, e := range ss.levels[k].entries {
			b := ss.attestationsMap[e.bucketKey]
			if b == nil {
				continue
			}
			bps := initAndGetPeerAttestationState(b, p, m.committeeSize)
			if bps.available.Get(e.pos) {
				continue
			}
			if gossipPeer && !bps.pendingWant.Get(e.pos) {
				continue
			}
			cand = append(cand, e)
		}
		slices.SortFunc(cand, func(a, b idxEntry) int {
			if d := ss.bucketSeq[a.bucketKey] - ss.bucketSeq[b.bucketKey]; d != 0 {
				return d
			}
			return a.pos - b.pos
		})
		for _, e := range cand {
			if len(drawn) == m.maxPerMessage {
				more = true // a needed candidate exists beyond this chunk
				break
			}
			drawn = append(drawn, e)
		}
	}
	if len(drawn) == 0 {
		return nil, false
	}

	chunk := make(map[string][]int)
	for _, e := range drawn {
		b := ss.attestationsMap[e.bucketKey]
		sc := b.sendCount[e.pos]
		chunk[e.bucketKey] = append(chunk[e.bucketKey], e.pos)
		bps := initAndGetPeerAttestationState(b, p, m.committeeSize)
		bps.available.Set(e.pos)
		b.sendCount[e.pos]++
		ss.indexBump(e.bucketKey, e.pos, sc)
	}
	return chunk, more
}

// encodeBatch builds a BatchedAttestation for the given positions, emitting
// AttestorIndices and Signatures in the same order as positions.
func (m *priorityAttestationManager) encodeBatch(b *AttestationState, positions []int) *pb.BatchedAttestation {
	idxs := make([]uint32, 0, len(positions))
	sigs := make([][]byte, 0, len(positions))
	for _, pos := range positions {
		idxs = append(idxs, uint32(pos))
		sigs = append(sigs, b.attestations[pos].Signature)
	}
	return &pb.BatchedAttestation{
		AttestationData: b.data,
		AttestorIndices: idxs,
		Signatures:      sigs,
	}
}

// fanoutPublish eagerly sends a single attestation to all peers via
// PublishPartial. Unchanged from partial mode: one position, not chunked, and
// it does not touch sendCount (a fanout node sends exactly one attestation).
func (m *priorityAttestationManager) fanoutPublish(
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

// onEmitGossip mirrors partial mode: mark heartbeat-selected peers as gossip
// peers and prime them to receive our Available list on the next tick.
func (m *priorityAttestationManager) onEmitGossip(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
	if m.node.DisableIHaveGossip {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range gossipPeers {
		ps := peerStates[p]
		ps.gossipPeer = true
		ps.sendAvailableList = true
		peerStates[p] = ps
	}
}

// onIncomingRPC handles incoming partial-extension RPCs. Identical to partial
// mode except it routes through the priority slot state / index (received
// positions enter the index when the verifier promotes them in markValidated).
func (m *priorityAttestationManager) onIncomingRPC(from peer.ID, peerStates map[peer.ID]peerState, rpc *pubsub_pb.PartialMessagesExtension) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic := rpc.GetTopicID()
	slot := groupIDToSlot(rpc.GroupID)
	topicIdx := m.topicIndexMap[topic]

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

	// A metadata-only RPC marks the sender as a gossip peer. A mesh peer
	// piggybacks its Available bitmap alongside data (SendAvailableWithData), so
	// an RPC that also carries data must NOT reclassify the sender as gossip — we
	// still record its Available below, just not the gossip flag.
	hasData := len(dataEnv.Batches) > 0

	for _, md := range ctrl.Metadatas {
		b := m.getOrCreateAttestationState(topic, slot, md.AttestationData)
		bps := initAndGetPeerAttestationState(b, from, m.committeeSize)

		available := bitmap.Bitmap(md.Available)
		bps.available.Or(md.Available)

		requests := bitmap.Bitmap(md.Requests)
		bps.pendingWant.Or(md.Requests)

		if !hasData && (available.OnesCount() > 0 || requests.OnesCount() > 0) {
			ps := peerStates[from]
			ps.gossipPeer = true
			peerStates[from] = ps
		}

		m.logger.Info("partial_recv_metadata",
			"from", shortPeer(from),
			"slot", slot,
			"topic", topicIdx,
			"att_digest", attDigestHex(md.AttestationData),
			"available_ones", available.OnesCount(),
			"requests_ones", requests.OnesCount(),
		)
	}

	for _, batch := range dataEnv.Batches {
		b := m.getOrCreateAttestationState(topic, slot, batch.AttestationData)
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

		m.logger.Info("partial_recv_batch",
			"from", shortPeer(from),
			"slot", slot,
			"topic", topicIdx,
			"att_digest", attDigestHex(batch.AttestationData),
			"positions_count", len(positions),
			"new_positions", len(newEntries),
			"batch_bytes", proto.Size(batch),
		)

		if m.node.Tracer != nil {
			slotStart := m.slotStartTime(slot)
			latencyMs := time.Since(slotStart).Milliseconds()
			for _, entry := range newEntries {
				pe := entry.(*PartialAttestationEntry)
				m.node.Tracer.OnPartialReceive(slot, topicIdx, pe.Position, batch.AttestationData, latencyMs)
			}
		}

		for _, pos := range positions {
			bps.available.Set(pos)
		}

		if len(newEntries) > 0 {
			data := b.data
			m.node.verifier.submit(
				verificationItem{Topic: topic, Slot: slot, Data: data, Attestations: newEntries},
				func(item verificationItem) {
					m.markValidated(item.Topic, item.Slot, item.Data, item.Attestations)
				},
			)
		}
	}

	return nil
}

func (m *priorityAttestationManager) publishTick(ps *pubsub.PubSub, topics []string) {
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

func (m *priorityAttestationManager) runPublishLoop(ctx interface{ Done() <-chan struct{} }, ps *pubsub.PubSub, topics []string) {
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

func (m *priorityAttestationManager) newPartialMessagesExtension() *partialmessages.PartialMessagesExtension[peerState] {
	m.ext = &partialmessages.PartialMessagesExtension[peerState]{
		Logger:             m.logger,
		OnEmitGossip:       m.onEmitGossip,
		OnIncomingRPC:      m.onIncomingRPC,
		GroupTTLByHeatbeat: 10,
	}
	return m.ext
}
