package attprop

import (
	"context"
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
	"github.com/ethp2p/simlab/cmd/attestation/verify"
)

// writerBuf is the buffer depth for the budget-bypassing bitmap and control
// writers. Deep enough that bursts (e.g. a full-bitmap dump across slots plus a
// floor emit) rarely overflow; overflow drops idempotent frames via tryEnqueue.
const writerBuf = 32

// event is the closed union consumed by the single eventloop goroutine
// (§F4 / "Eventloop design"). Every reader, sender, timer driver, and the
// verifier callback posts an event; the eventloop is the sole owner of the
// Manager's mutable state. isEvent is an unexported marker so only this package
// can be an event.
type event interface{ isEvent() }

// inboundDataEvent is one decoded data frame off a peer's push stream.
type inboundDataEvent struct {
	from peer.ID
	env  *pb.BatchedAttestationEnvelope
}

// inboundBitmapEvent is one decoded available-bitmap advertisement off a peer's
// bitmap stream. A peer that sends us bitmaps is a bitmap-mesh peer.
type inboundBitmapEvent struct {
	from peer.ID
	ctrl *pb.ControlEnvelope
}

// inboundControlEvent is one decoded graft/prune RPC off a peer's control
// stream.
type inboundControlEvent struct {
	from peer.ID
	ctrl *pb.AttPropControl
}

// sendDoneEvent signals that a peer's in-flight data send completed (its
// WriteMsg returned — the QUIC-window backpressure signal), so the eventloop
// can select that peer's next data message.
type sendDoneEvent struct {
	peer peer.ID
}

// peerUpEvent signals that the three bidirectional streams to/from a peer are
// established. For the opener side the streams were dialed; for the receiver
// side they were accepted by the stream handlers.
type peerUpEvent struct {
	peer                  peer.ID
	push, bitmap, control network.Stream
}

// peerDownEvent signals that a peer's stream closed or reset; the eventloop
// tears down that peer's sender/role state.
type peerDownEvent struct {
	peer peer.ID
}

// validatedEvent is posted from the verifier callback when a batch finishes:
// the listed entries for (topic, slot, data) are now validated and forwardable
// (§G2).
type validatedEvent struct {
	slot    int
	data    []byte
	entries []any
}

// publishLocalEvent injects this node's own attestation into local state from
// the eventloop goroutine, preserving single-owner discipline (§F4): PublishLocal
// posts it rather than touching state directly.
type publishLocalEvent struct {
	slot int
	pos  int
	sig  []byte
	data []byte
}

// funcEvent runs an arbitrary closure on the eventloop goroutine, then
// re-selects. It is the single-owner-safe way for an external caller to read or
// mutate Manager state without a lock (used by tests to force mesh roles and to
// observe state); production code stays event-driven.
type funcEvent struct{ fn func() }

// tickEvent fires every Config.TickInterval (§F4): the eventloop flushes every
// push peer's pending batch (including partial) for the duration of the pass.
type tickEvent struct{}

// bitmapFloorEvent fires every Config.BitmapFloorInterval (§D2): re-emit the
// current available bitmap to bitmap-mesh peers only if it changed.
type bitmapFloorEvent struct{}

// heartbeatEvent fires every Config.HeartbeatInterval (§C2): run mesh
// maintenance (graft/prune toward target sizes, respecting backoff).
type heartbeatEvent struct{}

func (inboundDataEvent) isEvent()    {}
func (inboundBitmapEvent) isEvent()  {}
func (inboundControlEvent) isEvent() {}
func (sendDoneEvent) isEvent()       {}
func (peerUpEvent) isEvent()         {}
func (peerDownEvent) isEvent()       {}
func (validatedEvent) isEvent()      {}
func (publishLocalEvent) isEvent()   {}
func (funcEvent) isEvent()           {}
func (tickEvent) isEvent()           {}
func (bitmapFloorEvent) isEvent()    {}
func (heartbeatEvent) isEvent()      {}

// peerSender owns one peer's outgoing data stream. The eventloop hands a framed
// message to work (buffered size 1, so the eventloop never blocks on handoff);
// the sender writes it via w.WriteMsg — which blocks under a full QUIC window,
// the backpressure signal — then posts sendDoneEvent. inFlight (owned by the
// eventloop) gates whether the peer can take another message. Bitmap and
// control writers reuse this type but bypass the budget.
type peerSender struct {
	peer     peer.ID
	stream   network.Stream
	w        msgio.WriteCloser
	work     chan []byte
	inFlight bool
}

// newPeerSender wraps a stream's writer and launches the sender goroutine. The
// goroutine drains one frame at a time: WriteMsg blocks on the QUIC window
// (backpressure), then posts sendDone so the eventloop selects the next frame.
// done lets the bitmap/control writers (which bypass the budget and never raise
// sendDoneEvent) skip the signal. The goroutine exits when work is closed.
//
// The data sender uses a size-1 channel with the inFlight flag for strict
// one-in-flight flow control. Bitmap/control writers bypass the budget and never
// signal back, so they use a deeper buffer and a non-blocking enqueue (see
// tryEnqueue) — the eventloop must never block handing off (§F4).
func (m *Manager) newPeerSender(
	p peer.ID,
	s network.Stream,
	buf int,
	done func(),
) *peerSender {
	ps := &peerSender{
		peer:   p,
		stream: s,
		w:      msgio.NewVarintWriter(s),
		work:   make(chan []byte, buf),
	}
	go func() {
		for f := range ps.work {
			if err := writeFrame(ps.w, f); err != nil {
				m.logger.Debug("write frame", "topic", m.cfg.TopicIndex, "peer", shortPeer(p), "err", err)
				m.post(peerDownEvent{peer: p})
				return
			}
			if done != nil {
				done()
			}
		}
	}()
	return ps
}

func (s *peerSender) closeAndReset() {
	close(s.work)
	if s.stream != nil {
		s.stream.Reset()
	}
}

// tryEnqueue hands a frame to a budget-bypassing writer (bitmap/control) without
// blocking the eventloop: if the writer's buffer is full it drops the frame.
// Safe because bitmap advertisements are idempotent (the floor re-emits) and
// graft/prune re-converges on the next heartbeat.
func (m *Manager) tryEnqueue(s *peerSender, frame []byte, what string) {
	select {
	case s.work <- frame:
	default:
		m.logger.Debug("drop frame, writer full", "peer", shortPeer(s.peer), "what", what)
	}
}

// driveTimer posts mk() every interval until ctx is cancelled. A zero interval
// disables the driver (used in tests that pump events by hand). No timer fires
// from inside the eventloop, so the fake clock and the loop stay in lockstep
// under synctest.
func (m *Manager) driveTimer(ctx context.Context, interval time.Duration, mk func() event) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !m.post(mk()) {
				return
			}
		}
	}
}

// run is the single-owner eventloop. All mutable Manager state is touched only
// here (no mutex). It processes one event at a time and re-runs trySelectAndSend
// on every event that can change what's sendable. On ctx cancel it closes every
// writer's work chan (senders exit their range), resets the open streams, and
// stops.
func (m *Manager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			m.shutdown()
			return
		case ev := <-m.events:
			m.dispatch(ev)
		}
	}
}

// dispatch routes one event to its handler and runs trySelectAndSend after the
// events that can change what is sendable (§F4).
func (m *Manager) dispatch(ev event) {
	switch e := ev.(type) {
	case peerUpEvent:
		m.onPeerUp(e)
		m.trySelectAndSend()
	case peerDownEvent:
		m.onPeerDown(e)
	case inboundControlEvent:
		m.onInboundControl(e.from, e.ctrl)
	case inboundBitmapEvent:
		m.onInboundBitmap(e.from, e.ctrl)
		m.trySelectAndSend()
	case inboundDataEvent:
		m.onInboundData(e.from, e.env)
		m.trySelectAndSend()
	case validatedEvent:
		m.onValidated(e)
		m.trySelectAndSend()
	case publishLocalEvent:
		m.onPublishLocal(e)
		m.trySelectAndSend()
	case funcEvent:
		e.fn()
		m.trySelectAndSend()
	case sendDoneEvent:
		m.onSendDone(e.peer)
	case tickEvent:
		m.onTick()
	case bitmapFloorEvent:
		m.emitBitmaps(true)
	case heartbeatEvent:
		m.onHeartbeat()
	}
}

// shutdown closes every per-peer writer so the sender goroutines exit their
// range, and resets the open streams. Readers exit on the resulting EOF/reset.
func (m *Manager) shutdown() {
	for _, s := range m.senders {
		s.closeAndReset()
	}
	for _, s := range m.bitmapWriters {
		s.closeAndReset()
	}
	for _, s := range m.controlWriters {
		s.closeAndReset()
	}
}

// onPeerUp registers the three per-peer stream writers. Data goes on the push
// stream (its sends raise sendDoneEvent so the budget re-selects); bitmap and
// control writers bypass the budget, so they pass a nil done callback.
// Idempotent: if a peer accidentally opens a second set, reset the duplicate.
func (m *Manager) onPeerUp(e peerUpEvent) {
	if _, ok := m.senders[e.peer]; ok {
		e.push.Reset()
		e.bitmap.Reset()
		e.control.Reset()
		return // already up (duplicate event)
	}
	m.senders[e.peer] = m.newPeerSender(e.peer, e.push, 1, func() {
		m.post(sendDoneEvent{peer: e.peer})
	})
	m.bitmapWriters[e.peer] = m.newPeerSender(e.peer, e.bitmap, writerBuf, nil)
	m.controlWriters[e.peer] = m.newPeerSender(e.peer, e.control, writerBuf, nil)
	m.mesh.roles[e.peer] = roleConnected
	m.logger.Debug("peer_up", "topic", m.cfg.TopicIndex, "peer", shortPeer(e.peer))
}

// onPeerDown tears down a peer: close its writers (senders exit), clear its
// in-flight budget charge, and drop its role. Its advertised available bits are
// left in place — holder-count is a local scarcity proxy, and sweeping a
// departed peer's bits out of every bucket would cost more than the small,
// self-correcting skew it leaves. Idempotent: the three readers plus the sender
// can each post a peerDownEvent, but only the first finds the peer present.
func (m *Manager) onPeerDown(e peerDownEvent) {
	if s, ok := m.senders[e.peer]; ok {
		if s.inFlight {
			m.activeData--
		}
		s.closeAndReset()
		delete(m.senders, e.peer)
	}
	if s, ok := m.bitmapWriters[e.peer]; ok {
		s.closeAndReset()
		delete(m.bitmapWriters, e.peer)
	}
	if s, ok := m.controlWriters[e.peer]; ok {
		s.closeAndReset()
		delete(m.controlWriters, e.peer)
	}
	delete(m.mesh.roles, e.peer)
	// Release the open-claim so a reconnect (a fresh inbound stream) re-opens our
	// outbound set instead of being suppressed by a stale claim.
	m.clearSendStreamsOpening(e.peer)
}

// onSendDone clears a peer's data in-flight flag, releases its budget charge,
// and re-selects (a freed slot may unblock a held push/bitmap send) (§F4).
func (m *Manager) onSendDone(p peer.ID) {
	if s, ok := m.senders[p]; ok && s.inFlight {
		s.inFlight = false
		m.activeData--
	}
	m.trySelectAndSend()
}

// onTick runs the tick selection pass: while sendAllToPushMesh is true, partial
// push batches flush too (§F4). The flag is eventloop-local — set, consumed in
// the same synchronous pass, and reset — never observed across a channel hop.
func (m *Manager) onTick() {
	m.sendAllToPushMesh = true
	m.trySelectAndSend()
	m.sendAllToPushMesh = false
}

// onHeartbeat runs mesh maintenance: gather connected candidates, call the mesh
// state machine, and dispatch the resulting graft/prune items on each peer's
// control writer (§C2). Role changes already applied by heartbeat take effect on
// the next selection pass.
func (m *Manager) onHeartbeat() {
	now := time.Now()
	candidates := make([]peer.ID, 0, len(m.senders))
	for p := range m.senders {
		candidates = append(candidates, p)
	}
	grafts, prunes := m.mesh.heartbeat(now, candidates)
	for p, items := range grafts {
		m.sendControl(p, items)
		m.logger.Debug("graft", "topic", m.cfg.TopicIndex, "peer", shortPeer(p), "items", len(items))
		// If we grafted this peer into the bitmap mesh, seed it with our full
		// current bitmap (§D1).
		if m.mesh.role(p) == roleBitmap {
			m.sendFullBitmapTo(p)
		}
	}
	for p, items := range prunes {
		m.sendControl(p, items)
		m.logger.Debug("prune", "topic", m.cfg.TopicIndex, "peer", shortPeer(p), "items", len(items))
	}
}

// sendFullBitmapTo dumps our full current available bitmap (every active slot)
// to one peer on its bitmap writer, bypassing the budget. Used when a peer first
// enters our bitmap mesh (§D1).
func (m *Manager) sendFullBitmapTo(p peer.ID) {
	w, ok := m.bitmapWriters[p]
	if !ok {
		return
	}
	for slot := range m.slots {
		ctrl := m.buildAvailableEnvelope(slot)
		if ctrl == nil {
			continue
		}
		frame, err := proto.Marshal(ctrl)
		if err != nil {
			m.logger.Error("marshal full bitmap", "topic", m.cfg.TopicIndex, "err", err)
			continue
		}
		m.tryEnqueue(w, frame, "bitmap_full")
	}
}

// onInboundControl feeds graft/prune items to the mesh state machine and sends
// any reply on the peer's control writer (§C). A graft may open the peer into a
// mesh; a prune drops it and arms backoff.
func (m *Manager) onInboundControl(from peer.ID, ctrl *pb.AttPropControl) {
	now := time.Now()
	ms := m.mesh
	wasBitmap := ms.role(from) == roleBitmap
	var reply []*pb.AttPropControlItem
	for _, it := range ctrl.Items {
		switch it.Op {
		case pb.AttPropMeshOp_GRAFT:
			reply = append(reply, ms.onGraft(from, it.Mesh, now)...)
		case pb.AttPropMeshOp_PRUNE:
			ms.onPrune(from, it.Mesh, now)
		}
	}
	if len(reply) > 0 {
		m.sendControl(from, reply)
	}
	// A peer that just entered our bitmap mesh gets the full current bitmap so it
	// starts with our complete state (§D1).
	if !wasBitmap && ms.role(from) == roleBitmap {
		m.sendFullBitmapTo(from)
	}
}

// trySelectAndSend is the send scheduler (§F4). It is idempotent and runs on
// every event that can change what is sendable. Push peers are served first and
// are exempt from the budget B; bitmap peers fill spare capacity only while
// activeData < B.
//
//  1. Push, exempt from B: for each push peer with no in-flight send, select its
//     scarcest ≤ N chunk. A full chunk (N items) is sent immediately; a partial
//     chunk is sent only on the tick flush (sendAllToPushMesh), otherwise held.
//  2. Bitmap, gated: for each bitmap peer with no in-flight send while
//     activeData < B, send its scarcest chunk.
func (m *Manager) trySelectAndSend() {
	for p, s := range m.senders {
		if s.inFlight || m.mesh.role(p) != rolePush {
			continue
		}
		chunk, more, ss := m.selectForPeer(p)
		if chunk == nil {
			continue
		}
		// A full chunk goes anytime; a partial chunk waits for the tick flush so
		// small batches don't trickle out (§F4). Full = hit the per-message cap,
		// whether or not candidates remain beyond it (`more`).
		full := more || chunkLen(chunk) >= m.cfg.MaxAttsPerMessage
		if full || m.sendAllToPushMesh {
			m.send(p, ss, chunk)
		} else {
			ss.rollbackChunk(p, chunk)
		}
	}
	for p, s := range m.senders {
		if m.activeData >= m.cfg.SendBudgetB {
			break
		}
		if s.inFlight || m.mesh.role(p) != roleBitmap {
			continue
		}
		chunk, _, ss := m.selectForPeer(p)
		if chunk == nil {
			continue
		}
		m.send(p, ss, chunk)
	}
}

// selectForPeer draws the peer's scarcest chunk from the active slots for this
// manager's topic. It returns the chunk, whether more candidates remain beyond
// it, and the slotState the chunk was drawn from (nil chunk when the peer needs
// nothing). The draw commits holder-count as it goes (§E2); callers that decide
// not to send must rollbackChunk.
func (m *Manager) selectForPeer(p peer.ID) (map[string][]int, bool, *slotState) {
	for _, ss := range m.slots {
		chunk, more := ss.selectOneChunkForPeer(p, m.cfg.MaxAttsPerMessage)
		if chunk != nil {
			return chunk, more, ss
		}
	}
	return nil, false, nil
}

// rollbackChunk undoes a drawn-but-not-sent chunk: clear the peer's available
// bit, decrement holder-count, and bump the index back down one level. Keeps the
// scarcity state honest when a partial push chunk is held for the tick (§F4).
func (ss *slotState) rollbackChunk(p peer.ID, chunk map[string][]int) {
	for bk, positions := range chunk {
		b := ss.buckets[bk]
		if b == nil {
			continue
		}
		bm := b.peerAvailFor(p, ss.committeeSize)
		for _, pos := range positions {
			if !bm.Get(pos) {
				continue
			}
			bm.Clear(pos)
			hc := b.holderCount[pos]
			b.holderCount[pos] = hc - 1
			// Move the index entry back from level hc to hc-1.
			if hc-1 >= 0 {
				ss.indexBumpHolderDown(bk, pos, hc)
			}
		}
	}
}

// indexBumpHolderDown moves a validated entry from level `from` to `from-1`
// after a holder-count decrement (the rollback counterpart of indexBumpHolder).
// Like indexBumpHolder it only moves an entry actually present in `from`.
func (ss *slotState) indexBumpHolderDown(bk string, pos, from int) {
	e := idxEntry{bk, pos}
	if from >= len(ss.levels) || !ss.levels[from].remove(e) {
		return
	}
	if to := from - 1; to >= 0 {
		ss.levels[to].add(e)
	}
}

// send marshals a chunk into a BatchedAttestationEnvelope, hands the frame to
// the peer's data sender (non-blocking; work is buffered size 1), and charges
// the budget. inFlight is cleared by sendDoneEvent when WriteMsg returns. Push
// sends are exempt from B but still tracked in activeData for observability and
// so a draining push send counts against bitmap fill (§F1).
func (m *Manager) send(p peer.ID, ss *slotState, chunk map[string][]int) {
	env := &pb.BatchedAttestationEnvelope{}
	bks := sortedBucketKeys(ss, chunk)
	var total int
	for _, bk := range bks {
		b := ss.buckets[bk]
		env.Batches = append(env.Batches, encodeBatch(b, chunk[bk]))
		total += len(chunk[bk])
	}
	frame, err := proto.Marshal(env)
	if err != nil {
		m.logger.Error("marshal data envelope", "err", err)
		ss.rollbackChunk(p, chunk)
		return
	}
	s := m.senders[p]
	s.inFlight = true
	m.activeData++
	s.work <- frame
	m.logger.Debug("attprop_send_data",
		"peer", shortPeer(p),
		"topic", m.cfg.TopicIndex,
		"role", int(m.mesh.role(p)),
		"slot", ss.slot,
		"num_buckets", len(bks),
		"positions", total,
		"bytes", len(frame),
	)
}

// sendControl marshals graft/prune items and hands them to the peer's control
// writer (bypasses the budget). Dropped silently if the peer has no writer yet.
func (m *Manager) sendControl(p peer.ID, items []*pb.AttPropControlItem) {
	w, ok := m.controlWriters[p]
	if !ok {
		return
	}
	frame, err := proto.Marshal(&pb.AttPropControl{Items: items})
	if err != nil {
		m.logger.Error("marshal control", "topic", m.cfg.TopicIndex, "err", err)
		return
	}
	m.tryEnqueue(w, frame, "control")
}

// onInboundData decodes received batches into the bucket, records the sender as
// a holder (bumps holder-count), submits new positions to the verifier (the
// validated gate, §G2), and emits the recv log + tracer event (§H2). The
// verifier callback posts a validatedEvent back into the eventloop.
//
// att_propagation push streams are per-topic, not per-slot, and the data
// envelope carries no slot. attestation_data is unique per (topic, slot), so the
// slot is resolved by slotForData: an already-known bucket's slot, else the
// latest active slot (the sim runs slots sequentially — see slotForData).
func (m *Manager) onInboundData(from peer.ID, env *pb.BatchedAttestationEnvelope) {
	for _, batch := range env.Batches {
		if len(batch.AttestorIndices) != len(batch.Signatures) {
			m.logger.Warn("attprop_recv_bad_batch", "from", shortPeer(from))
			continue
		}
		positions := make([]int, len(batch.AttestorIndices))
		ok := true
		for i, idx := range batch.AttestorIndices {
			if int(idx) >= m.cfg.CommitteeSize {
				m.logger.Warn("attprop_recv_bad_index", "from", shortPeer(from), "idx", idx)
				ok = false
				break
			}
			positions[i] = int(idx)
		}
		if !ok {
			continue // drop the whole batch; positions[i]↔signatures[i] must align
		}
		slot := m.slotForData(batch.AttestationData)
		ss := m.getOrCreateSlotState(slot)
		b := ss.getOrCreateBucket(batch.AttestationData)
		newEntries := b.addReceived(positions, batch.Signatures)

		// The sender holds everything it sent us — record it (bumps holder-count
		// on the 0→1 flips), so scarcity reflects what the sender has.
		for _, pos := range positions {
			ss.markHolder(b, from, pos)
		}

		m.logger.Info("partial_recv_batch",
			"from", shortPeer(from),
			"slot", slot,
			"topic", m.cfg.TopicIndex,
			"att_digest", attDigestHex(batch.AttestationData),
			"positions_count", len(positions),
			"new_positions", len(newEntries),
			"batch_bytes", proto.Size(batch),
		)

		if m.tracer != nil {
			latencyMs := time.Since(m.slotStartTime(slot)).Milliseconds()
			for _, e := range newEntries {
				pe := e.(*attEntry)
				m.tracer.OnPartialReceive(slot, m.cfg.TopicIndex, pe.Position, batch.AttestationData, latencyMs)
			}
		}

		if len(newEntries) > 0 {
			data := b.data
			m.verifier.Submit(
				verify.Item{Topic: m.cfg.Topic, Slot: slot, Data: data, Attestations: newEntries},
				func(it verify.Item) {
					m.post(validatedEvent{
						slot: it.Slot, data: it.Data, entries: it.Attestations,
					})
				},
			)
		}
	}
}

// onValidated promotes verifier-validated positions to forwardable: move them
// validating→validated, insert into the scarcity index at their current
// holder-count, bump the +K bitmap-trigger counter, and emit the validated log
// (§G2/§H2). trySelectAndSend re-runs after (newly forwardable positions).
func (m *Manager) onValidated(e validatedEvent) {
	ss := m.getSlotState(e.slot)
	if ss == nil {
		return
	}
	b, ok := ss.buckets[string(e.data)]
	if !ok {
		return
	}
	digest := attDigestHex(e.data)
	latencyMs := time.Since(m.slotStartTime(e.slot)).Milliseconds()
	for _, entry := range e.entries {
		pe := entry.(*attEntry)
		if _, done := b.validated[pe.Position]; done {
			continue
		}
		delete(b.validating, pe.Position)
		b.validated[pe.Position] = struct{}{}
		ss.indexAddValidated(string(b.data), pe.Position, b.holderCount[pe.Position])
		ss.validatedSinceEmit++
		m.logger.Info("attestation_validated",
			"slot", e.slot,
			"topic", m.cfg.TopicIndex,
			"att_digest", digest,
			"position", pe.Position,
			"latency_ms", latencyMs,
		)
	}
	m.maybeEmitBitmap(ss)
}

// maybeEmitBitmap fires a +K bitmap advertisement once enough positions have
// validated for the slot since its last emit (§D2). Called from every path that
// validates positions (received-then-verified, and self-published).
func (m *Manager) maybeEmitBitmap(ss *slotState) {
	if ss.validatedSinceEmit >= bitmapTriggerK {
		m.emitBitmaps(false)
	}
}

// onPublishLocal injects this node's own attestation as validated and indexed at
// holder-count 0 (we are the origin; no peer holds it yet) — mirrors
// partial_priority.publishLocal but from the eventloop goroutine (§F4).
func (m *Manager) onPublishLocal(e publishLocalEvent) {
	ss := m.getOrCreateSlotState(e.slot)
	b := ss.getOrCreateBucket(e.data)
	if _, ok := b.atts[e.pos]; ok {
		return
	}
	b.atts[e.pos] = &attEntry{Position: e.pos, Signature: e.sig, Data: b.data}
	b.validated[e.pos] = struct{}{}
	ss.indexAddValidated(string(b.data), e.pos, b.holderCount[e.pos])
	ss.validatedSinceEmit++
	m.logger.Info("self_published",
		"slot", e.slot,
		"topic", m.cfg.TopicIndex,
		"att_digest", attDigestHex(e.data),
		"position", e.pos,
	)
	m.maybeEmitBitmap(ss)
}

// getOrCreateSlotState returns (creating as needed) the per-slot state.
// Eventloop-only (no lock).
func (m *Manager) getOrCreateSlotState(slot int) *slotState {
	ss, ok := m.slots[slot]
	if !ok {
		ss = newSlotState(slot, m.maxPeers(), m.cfg.CommitteeSize)
		m.slots[slot] = ss
	}
	return ss
}

// getSlotState returns the per-slot state or nil. Eventloop-only.
func (m *Manager) getSlotState(slot int) *slotState {
	return m.slots[slot]
}

// maxPeers returns the holder-count ceiling (= MaxPeersPerAtt), at least 1.
func (m *Manager) maxPeers() int {
	if m.cfg.MaxPeersPerAtt > 0 {
		return m.cfg.MaxPeersPerAtt
	}
	return 1
}

// slotStartTime returns the wall-clock start of a slot, for receive-latency
// logging (mirrors partial mode).
func (m *Manager) slotStartTime(slot int) time.Time {
	return m.cfg.PublishStart.Add(time.Duration(slot-1) * m.cfg.SlotDuration)
}

// chunkLen sums the positions across a chunk's buckets.
func chunkLen(chunk map[string][]int) int {
	n := 0
	for _, positions := range chunk {
		n += len(positions)
	}
	return n
}

// sortedBucketKeys returns the chunk's bucket keys in stable bucketSeq order, so
// an envelope's batches are deterministic.
func sortedBucketKeys(ss *slotState, chunk map[string][]int) []string {
	bks := make([]string, 0, len(chunk))
	for bk := range chunk {
		bks = append(bks, bk)
	}
	slices.SortFunc(bks, func(a, b string) int { return ss.bucketSeq[a] - ss.bucketSeq[b] })
	return bks
}

// slotForData resolves the slot that owns a piece of attestation_data on the
// (slot-less) push stream. attestation_data is unique per (topic, slot), so if
// any existing slotState already holds a bucket for this data we reuse its slot
// (learned earlier from our own publish, fanout, or a bitmap advertisement which
// carries Slot). Otherwise we attribute it to the latest active slot: the sim
// drives slots sequentially (publish → drain slotDuration → next), so unseen
// push data belongs to the slot whose window is currently open. Defaults to 1
// when nothing is active yet.
func (m *Manager) slotForData(data []byte) int {
	key := string(data)
	latest := 0
	for slot, ss := range m.slots {
		if _, ok := ss.buckets[key]; ok {
			return slot
		}
		if slot > latest {
			latest = slot
		}
	}
	if latest == 0 {
		return 1
	}
	return latest
}
