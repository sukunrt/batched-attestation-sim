package node

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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

// gossipInterval is the minimum gap between successive IHAVE-style envelopes
// (CommitteeAttestationPartsMetadata) sent to the same gossip peer. Matches
// the gossipsub heartbeat interval.
const gossipInterval = 700 * time.Millisecond

// maxIWantPerPosition caps how many gossip peers we ask for any one missing
// committee position per (slot, attestation_data) bucket.
const maxIWantPerPosition = 2

// PartialAttestationEntry holds one committee member's signature plus the
// shared attestation_data it signed. Stored per (bucket, position).
type PartialAttestationEntry struct {
	Position  int
	Signature []byte
	Data      []byte
}

// bucketPeerState captures what a peer has seen and what they have asked for,
// scoped to a single (topic, slot, attestation_data) bucket.
type bucketPeerState struct {
	// available bits indicate committee positions the peer holds (set via
	// peer-sent metadata Have or inferred from received attestations).
	available bitmap.Bitmap
	// pendingWant bits indicate committee positions the peer just requested
	// via metadata Want. Cleared on the next outgoing publish to the peer
	// for this bucket — requests are non-persistent per spec.
	pendingWant bitmap.Bitmap
}

func newBucketPeerState(committeeSize int) *bucketPeerState {
	return &bucketPeerState{
		available:   newCommitteeBitmap(committeeSize),
		pendingWant: newCommitteeBitmap(committeeSize),
	}
}

// peerState tracks per-peer flags that span all buckets at the manager level.
// `gossipPeer` is true once we've seen the peer act like a non-mesh peer
// (i.e., they sent us a Want, or libp2p told us to gossip to them via
// EmitGossip). Per-bucket peer state (available, pendingWant) lives inside
// the AttDataBucket.
type peerState struct {
	gossipPeer bool
}

// AttDataBucket is the per-(topic, slot, attestation_data) state.
//
// Forks at the same slot get independent buckets, satisfying the spec
// requirement that nodes MUST NOT deduplicate by (slot, committee position).
type AttDataBucket struct {
	data []byte

	// attestations holds one entry per committee position we hold.
	attestations map[int]*PartialAttestationEntry

	// validating is the set of positions accepted into the verifier but not
	// yet validated; validated is the set of positions the verifier has
	// promoted to forwardable.
	validating map[int]struct{}
	validated  map[int]struct{}

	// perSendCount tracks how many peers each position has been pushed to.
	perSendCount map[int]int

	// requestSentCount tracks how many gossip peers we have asked for each
	// missing position. Capped at maxIWantPerPosition.
	requestSentCount map[int]int

	// peers holds per-peer available/pendingWant for this bucket. The map
	// only contains peers we have actually exchanged messages with for this
	// bucket; absence is equivalent to a zero bitmap.
	peers map[peer.ID]*bucketPeerState

	newSinceLastTick bool
}

func newAttDataBucket(data []byte) *AttDataBucket {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return &AttDataBucket{
		data:             dataCopy,
		attestations:     make(map[int]*PartialAttestationEntry),
		validating:       make(map[int]struct{}),
		validated:        make(map[int]struct{}),
		perSendCount:     make(map[int]int),
		requestSentCount: make(map[int]int),
		peers:            make(map[peer.ID]*bucketPeerState),
	}
}

// PartialAttestationSlotState holds all buckets (per-AttestationData) for a
// (topic, slot).
type PartialAttestationSlotState struct {
	slot    int
	buckets map[string]*AttDataBucket // key = string(attestation_data)
}

func newSlotState(slot int) *PartialAttestationSlotState {
	return &PartialAttestationSlotState{
		slot:    slot,
		buckets: make(map[string]*AttDataBucket),
	}
}

// partialAttesattionManager is the application-side manager for the
// CommitteeAttestation propagation algorithm.
type partialAttesattionManager struct {
	logger *slog.Logger
	node   *Node
	ext    *partialmessages.PartialMessagesExtension[peerState]

	publishStart  time.Time
	slotDuration  time.Duration
	committeeSize int

	topicIndexMap map[string]int // topic name -> stable index, used for log tagging

	mu    sync.Mutex
	slots map[string]map[int]*PartialAttestationSlotState

	// peerNextGossipAt is the earliest time at which we will next consider
	// the peer for sending a metadata envelope. One schedule per peer, shared
	// across topics/slots/buckets.
	peerNextGossipAt map[peer.ID]time.Time
}

func newPartialAttestationManager(
	n *Node,
	publishStart time.Time,
	slotDuration time.Duration,
	topicIndexMap map[string]int,
) *partialAttesattionManager {
	logger := slog.With("node", n.Num, "component", "partial")
	if n.CommitteeSize <= 0 {
		// Defensive — main.go should populate this, but keep tests cheap.
		n.CommitteeSize = 2048
	}
	m := &partialAttesattionManager{
		logger:           logger,
		node:             n,
		publishStart:     publishStart,
		slotDuration:     slotDuration,
		committeeSize:    n.CommitteeSize,
		topicIndexMap:    topicIndexMap,
		slots:            make(map[string]map[int]*PartialAttestationSlotState),
		peerNextGossipAt: make(map[peer.ID]time.Time),
	}
	return m
}

// slotStartTime returns the absolute wall-clock start of the given slot. Used
// to compute receive latency.
func (m *partialAttesattionManager) slotStartTime(slot int) time.Time {
	return m.publishStart.Add(time.Duration(slot-1) * m.slotDuration)
}

func (m *partialAttesattionManager) getOrCreateSlotState(topic string, slot int) *PartialAttestationSlotState {
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

func (m *partialAttesattionManager) getSlotState(topic string, slot int) *PartialAttestationSlotState {
	topicSlots, ok := m.slots[topic]
	if !ok {
		return nil
	}
	return topicSlots[slot]
}

func (m *partialAttesattionManager) getOrCreateBucket(topic string, slot int, data []byte) *AttDataBucket {
	ss := m.getOrCreateSlotState(topic, slot)
	key := string(data)
	b, ok := ss.buckets[key]
	if !ok {
		b = newAttDataBucket(data)
		ss.buckets[key] = b
	}
	return b
}

// getBucketPeerState returns (and creates as needed) per-bucket state for a
// peer. Caller must hold m.mu.
func (m *partialAttesattionManager) getBucketPeerState(b *AttDataBucket, p peer.ID) *bucketPeerState {
	s, ok := b.peers[p]
	if !ok {
		s = newBucketPeerState(m.committeeSize)
		b.peers[p] = s
	}
	return s
}

// publishLocal stores a self-produced attestation in the right bucket and
// marks it validated immediately.
func (m *partialAttesattionManager) publishLocal(topic string, slot, position int, sig, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.getOrCreateBucket(topic, slot, data)
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
func (b *AttDataBucket) addReceived(positions []int, signatures [][]byte) []any {
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
func (m *partialAttesattionManager) markValidated(topic string, slot int, data []byte, entries []any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ss := m.getSlotState(topic, slot)
	if ss == nil {
		return
	}
	b, ok := ss.buckets[string(data)]
	if !ok {
		return
	}

	now := time.Now()
	slotStart := m.slotStartTime(slot)
	digest := attDigestHex(data)
	topicIdx := m.topicIndexMap[topic]
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
//   - Up to IHaveGossipDegree gossip peers (eligible by peerNextGossipAt) are
//     randomly chosen to receive an "available" envelope this tick.
//   - For each missing position any gossip peer advertises in their
//     available, up to maxIWantPerPosition request targets are chosen across
//     all peers; the per-bucket counter is bumped accordingly.
//   - Mesh peers continue to receive data via push. No metadata is sent to
//     mesh peers (per spec).
func (m *partialAttesattionManager) publishActions(topic string, slot int) partialmessages.PublishActionsFn[peerState] {
	return func(
		peerStates map[peer.ID]peerState,
		peerRequestsPartial func(peer.ID) bool,
	) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
			m.mu.Lock()
			defer m.mu.Unlock()

			ss := m.getSlotState(topic, slot)
			if ss == nil || len(ss.buckets) == 0 {
				return
			}

			now := time.Now()
			degree := m.node.IHaveGossipDegree
			if degree <= 0 {
				degree = 6
			}
			iHavePeers := m.selectIHaveRecipients(peerStates, now, degree)

			// Per-bucket: select positions to IWANT from each peer based on
			// what they advertise. This bumps requestSentCount.
			wantPerPeerPerBucket := make(map[peer.ID]map[string][]int)
			for key, b := range ss.buckets {
				perPeer := selectIWantTargets(b, peerStates, m.committeeSize)
				for p, positions := range perPeer {
					if _, ok := wantPerPeerPerBucket[p]; !ok {
						wantPerPeerPerBucket[p] = make(map[string][]int)
					}
					wantPerPeerPerBucket[p][key] = positions
				}
			}

			for p := range peerStates {
				ps := peerStates[p]

				ctrlEnv := &pb.ControlEnvelope{}
				dataEnv := &pb.BatchedAttestationEnvelope{}

				var totalPositionsSent int
				var bucketsWithData int

				for key, b := range ss.buckets {
					var canSendData bool
					if ps.gossipPeer {
						bps := b.peers[p]
						canSendData = bps != nil && bps.pendingWant.OnesCount() > 0
					} else {
						canSendData = peerRequestsPartial(p)
					}

					// Data: build the BatchedAttestation for this bucket.
					if canSendData {
						positions := m.selectAndCommitSends(b, p, ps.gossipPeer)
						if len(positions) > 0 {
							batch := m.encodeBatch(b, positions)
							dataEnv.Batches = append(dataEnv.Batches, batch)
							totalPositionsSent += len(positions)
							bucketsWithData++

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

					// Always clear pendingWant for gossip peers, even if we
					// satisfied none of it — requests are non-persistent.
					if ps.gossipPeer {
						if bps, ok := b.peers[p]; ok && bps.pendingWant != nil {
							bps.pendingWant = newCommitteeBitmap(m.committeeSize)
						}
					}

					// Metadata: only gossip peers, only with new info.
					if !ps.gossipPeer {
						continue
					}
					_, sendHave := iHavePeers[p]
					wantList := wantPerPeerPerBucket[p][key]
					md := buildBucketMetadata(b, p, m.committeeSize, slot, sendHave, wantList)
					if md != nil {
						ctrlEnv.Metadatas = append(ctrlEnv.Metadatas, md)
						m.logger.Info("partial_send_metadata",
							"peer", shortPeer(p),
							"slot", slot,
							"topic", m.topicIndexMap[topic],
							"att_digest", attDigestHex(b.data),
							"available_ones", availableOnes(md.Available),
							"requests_ones", requestsOnes(md.Requests),
							"md_bucket_bytes", proto.Size(md),
							"send_have", sendHave,
							"send_want", len(wantList) > 0,
						)
					}
				}

				var encodedCtrl, encodedData []byte
				var err error
				if len(ctrlEnv.Metadatas) > 0 {
					encodedCtrl, err = proto.Marshal(ctrlEnv)
					if err != nil {
						m.logger.Error("marshal control envelope", "err", err)
						encodedCtrl = nil
					}
				}
				if len(dataEnv.Batches) > 0 {
					encodedData, err = proto.Marshal(dataEnv)
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
					"num_buckets", bucketsWithData,
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
			for _, b := range ss.buckets {
				b.newSinceLastTick = false
			}
		}
	}
}

// selectAndCommitSends returns the set of positions to send to peer p for
// bucket b, and as a side effect updates the bookkeeping (perSendCount,
// per-peer available). Caller must hold m.mu.
func (m *partialAttesattionManager) selectAndCommitSends(b *AttDataBucket, p peer.ID, gossipPeer bool) []int {
	bps := b.peers[p]
	candidates := make([]int, 0, len(b.validated))
	for pos := range b.validated {
		if b.perSendCount[pos] >= m.node.MaxPeersPerAttestation {
			continue
		}
		if bps != nil && bps.available != nil && bps.available.Get(pos) {
			continue
		}
		if gossipPeer {
			if bps == nil || bps.pendingWant == nil || !bps.pendingWant.Get(pos) {
				continue
			}
		}
		candidates = append(candidates, pos)
	}
	if len(candidates) == 0 {
		return nil
	}
	slices.Sort(candidates)
	if bps == nil {
		bps = m.getBucketPeerState(b, p)
	}
	for _, pos := range candidates {
		bps.available.Set(pos)
		b.perSendCount[pos]++
	}
	return candidates
}

// selectIHaveRecipients returns the set of gossip peers chosen to receive an
// available-style envelope this tick: eligible (peerNextGossipAt elapsed)
// gossip peers, capped at `degree`.
func (m *partialAttesattionManager) selectIHaveRecipients(
	peerStates map[peer.ID]peerState,
	now time.Time,
	degree int,
) map[peer.ID]struct{} {
	nextGossipAt := now.Add(gossipInterval)
	iHavePeers := make(map[peer.ID]struct{})

	for p, ps := range peerStates {
		if !ps.gossipPeer {
			continue
		}
		next := m.peerNextGossipAt[p]
		if now.After(next) {
			m.peerNextGossipAt[p] = nextGossipAt
			iHavePeers[p] = struct{}{}
			degree--
			if degree <= 0 {
				break
			}
		}
	}
	return iHavePeers
}

// selectIWantTargets picks, for each missing position advertised by some
// gossip peer in this bucket, up to (maxIWantPerPosition -
// already-sent) target peers to request it from. Bumps
// b.requestSentCount and returns the chosen positions per peer.
// Caller must hold m.mu.
func selectIWantTargets(b *AttDataBucket, peerStates map[peer.ID]peerState, committeeSize int) map[peer.ID][]int {
	candidatesByPos := make(map[int][]peer.ID)
	for p, ps := range peerStates {
		if !ps.gossipPeer {
			continue
		}
		bps := b.peers[p]
		if bps == nil || bps.available == nil {
			continue
		}
		for pos := range iterBits(bps.available, committeeSize) {
			if _, have := b.attestations[pos]; have {
				continue
			}
			if b.requestSentCount[pos] >= maxIWantPerPosition {
				continue
			}
			b.requestSentCount[pos]++
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

// buildBucketMetadata assembles a per-bucket CommitteeAttestationPartsMetadata.
// `sendHave` chooses whether to populate `Available`; `wantList` populates
// `Requests`. Returns nil if both would be empty.
func buildBucketMetadata(
	b *AttDataBucket,
	p peer.ID,
	committeeSize int,
	slot int,
	sendHave bool,
	wantList []int,
) *pb.CommitteeAttestationPartsMetadata {
	if !sendHave && len(wantList) == 0 {
		return nil
	}

	md := &pb.CommitteeAttestationPartsMetadata{
		Slot:            int32(slot),
		AttestationData: b.data,
	}

	if sendHave {
		bps := b.peers[p]
		avail := newCommitteeBitmap(committeeSize)
		any := false
		for pos := range b.validated {
			if bps != nil && bps.available != nil && bps.available.Get(pos) {
				continue
			}
			avail.Set(pos)
			any = true
		}
		if any {
			md.Available = []byte(avail)
		}
	}

	if len(wantList) > 0 {
		req := newCommitteeBitmap(committeeSize)
		for _, pos := range wantList {
			req.Set(pos)
		}
		md.Requests = []byte(req)
	}

	if len(md.Available) == 0 && len(md.Requests) == 0 {
		return nil
	}
	return md
}

// encodeBatch builds a BatchedAttestation for the given positions. Caller must
// ensure all entries exist in b.attestations. AttestorIndices and Signatures
// are emitted in the same order as `positions`.
func (m *partialAttesattionManager) encodeBatch(b *AttDataBucket, positions []int) *pb.BatchedAttestation {
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
// PublishPartial. Used by fanout nodes which have no mesh peers and can't
// rely on the tick-based publish loop.
func (m *partialAttesattionManager) fanoutPublish(
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
// peers. Mark them as gossip peers and register them in the IHAVE schedule
// (zero-time = immediately eligible for the next tick).
func (m *partialAttesattionManager) onEmitGossip(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
	if m.node.DisableIHaveGossip {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range gossipPeers {
		ps := peerStates[p]
		if !ps.gossipPeer {
			ps.gossipPeer = true
			peerStates[p] = ps
		}
		m.registerGossipPeer(p)
	}
}

// registerGossipPeer ensures peerNextGossipAt has an entry for p. The default
// zero-time means "immediately eligible". Existing schedules are not reset.
func (m *partialAttesattionManager) registerGossipPeer(p peer.ID) {
	if _, ok := m.peerNextGossipAt[p]; !ok {
		m.peerNextGossipAt[p] = time.Time{}
	}
}

// onIncomingRPC handles incoming partial-extension RPCs from peers.
func (m *partialAttesattionManager) onIncomingRPC(from peer.ID, peerStates map[peer.ID]peerState, rpc *pubsub_pb.PartialMessagesExtension) error {
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

	m.logger.Info("partial_recv_tick",
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
		b := m.getOrCreateBucket(topic, slot, md.AttestationData)
		bps := m.getBucketPeerState(b, from)

		var availOnes, reqOnes int
		if len(md.Available) > 0 {
			availOnes = bitmap.Bitmap(md.Available).OnesCount()
			bps.available = mergeBitmap(bps.available, md.Available, m.committeeSize)
		}
		if len(md.Requests) > 0 {
			reqOnes = bitmap.Bitmap(md.Requests).OnesCount()
			bps.pendingWant = mergeBitmap(bps.pendingWant, md.Requests, m.committeeSize)
		}

		// A peer issuing metadata is by definition a gossip peer.
		if availOnes > 0 || reqOnes > 0 {
			ps := peerStates[from]
			ps.gossipPeer = true
			peerStates[from] = ps
			m.registerGossipPeer(from)
		}

		m.logger.Info("partial_recv_metadata",
			"from", shortPeer(from),
			"slot", slot,
			"topic", topicIdx,
			"att_digest", attDigestHex(md.AttestationData),
			"available_ones", availOnes,
			"requests_ones", reqOnes,
		)
	}

	// Process data batches.
	for _, batch := range dataEnv.Batches {
		b := m.getOrCreateBucket(topic, slot, batch.AttestationData)
		bps := m.getBucketPeerState(b, from)

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
			"att_digest", attDigestHex(batch.AttestationData),
			"positions_count", len(positions),
			"new_positions", len(newEntries),
			"batch_bytes", batchBytes,
		)

		// Notify the application tracer for each newly-received position.
		if m.node.Tracer != nil {
			slotStart := m.slotStartTime(slot)
			latencyMs := time.Since(slotStart).Milliseconds()
			digest := attDigest(batch.AttestationData)
			for _, entry := range newEntries {
				pe := entry.(*PartialAttestationEntry)
				m.node.Tracer.OnPartialReceive(slot, topicIdx, pe.Position, digest, latencyMs)
			}
		}

		// Infer peer's available |= positions sent.
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

func (m *partialAttesattionManager) publishTick(ps *pubsub.PubSub, topics []string) {
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

func (m *partialAttesattionManager) runPublishLoop(ctx interface{ Done() <-chan struct{} }, ps *pubsub.PubSub, topics []string) {
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

func (m *partialAttesattionManager) newPartialMessagesExtension() *partialmessages.PartialMessagesExtension[peerState] {
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

// iterBits yields positions in [0, committeeSize) where the bitmap has bits
// set. Iteration is in ascending order.
func iterBits(b bitmap.Bitmap, committeeSize int) iter.Seq[int] {
	return func(yield func(int) bool) {
		limitBytes := (committeeSize + 7) / 8
		n := len(b)
		if n > limitBytes {
			n = limitBytes
		}
		for byteIdx := 0; byteIdx < n; byteIdx++ {
			by := b[byteIdx]
			if by == 0 {
				continue
			}
			for bit := 0; bit < 8; bit++ {
				pos := byteIdx*8 + bit
				if pos >= committeeSize {
					return
				}
				if by&(1<<uint(bit)) != 0 {
					if !yield(pos) {
						return
					}
				}
			}
		}
	}
}

// mergeBitmap OR-merges incoming bytes into dest. Pads/copies dest to fit the
// committee size. Returns the merged result; dest may be reallocated.
func mergeBitmap(dest bitmap.Bitmap, incoming []byte, committeeSize int) bitmap.Bitmap {
	wantBytes := (committeeSize + 7) / 8
	if len(dest) < wantBytes {
		grown := make(bitmap.Bitmap, wantBytes)
		copy(grown, dest)
		dest = grown
	}
	n := len(incoming)
	if n > wantBytes {
		n = wantBytes
	}
	for i := 0; i < n; i++ {
		dest[i] |= incoming[i]
	}
	return dest
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

// attDigest returns the 8-byte SHA-256 prefix of attestation_data. Used as a
// compact correlation token in tracer events and logs.
func attDigest(data []byte) [8]byte {
	sum := sha256.Sum256(data)
	var out [8]byte
	copy(out[:], sum[:8])
	return out
}

// attDigestHex returns the 16-char hex prefix of attestation_data's SHA-256.
// Cheaper-to-read than the full 32-byte hash; collisions are vanishingly rare
// in a single simulation run.
func attDigestHex(data []byte) string {
	d := attDigest(data)
	return hex.EncodeToString(d[:])
}

