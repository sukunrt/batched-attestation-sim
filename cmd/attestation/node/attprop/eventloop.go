package attprop

import (
	"context"
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
	"github.com/ethp2p/simlab/cmd/attestation/verify"
)

// writerBuf is the buffer depth for the budget-bypassing bitmap and control
// writers. Bitmap overflow replaces stale queued advertisements; control
// overflow drops idempotent mesh-management frames via tryEnqueue.
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

type slotLifecycleCmd struct {
	slot  int
	start bool
	done  chan struct{}
}

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
// eventloop) gates whether the peer can take another message. Control writers
// reuse this type but bypass the budget.
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
// done lets control writers (which bypass the budget and never raise
// sendDoneEvent) skip the signal. The goroutine exits when work is closed.
//
// The data sender uses a size-1 channel with the inFlight flag for strict
// one-in-flight flow control. Control writers bypass the budget and never signal
// back, so they use a deeper buffer and a non-blocking enqueue (see tryEnqueue)
// — the eventloop must never block handing off (§F4).
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
	// Closing lets an idle sender goroutine exit its range; Reset unblocks a
	// goroutine stuck in WriteMsg. The eventloop removes the sender from its maps
	// before future enqueues can target this channel.
	close(s.work)
	if s.stream != nil {
		s.stream.Reset()
	}
}

// tryEnqueue hands a frame to a budget-bypassing control writer without blocking
// the eventloop: if the writer's buffer is full it drops the frame. Safe because
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
		case cmd := <-m.lifecycle:
			m.onSlotLifecycle(cmd)
		case ev := <-m.events:
			m.dispatch(ev)
		}
	}
}

func (m *Manager) onSlotLifecycle(cmd slotLifecycleCmd) {
	if cmd.start {
		m.onSlotStart(cmd.slot)
	} else {
		m.onSlotEnd(cmd.slot)
	}
	close(cmd.done)
}

func (m *Manager) onSlotStart(slot int) {
	m.lifecycleStarted = true
	m.activeSlot = slot
	for s := range m.slots {
		if s < slot {
			delete(m.slots, s)
		}
	}
}

func (m *Manager) onSlotEnd(slot int) {
	m.lifecycleStarted = true
	if slot > m.highestClosedSlot {
		m.highestClosedSlot = slot
	}
	if m.activeSlot == slot {
		m.activeSlot = 0
	}
	delete(m.slots, slot)
}

func (m *Manager) acceptsSlot(slot int) bool {
	if slot <= m.highestClosedSlot {
		return false
	}
	if m.lifecycleStarted && m.activeSlot != slot {
		return false
	}
	return true
}

// dispatch routes one event to its handler and runs trySelectAndSend after the
// events that can change what is sendable (§F4).
func (m *Manager) dispatch(ev event) {
	switch e := ev.(type) {
	case peerUpEvent:
		m.onPeerUp(e)
	case peerDownEvent:
		m.onPeerDown(e)
	case inboundControlEvent:
		m.onInboundControl(e.from, e.ctrl)
	case inboundBitmapEvent:
		m.onInboundBitmap(e.from, e.ctrl)
	case inboundDataEvent:
		m.onInboundData(e.from, e.env)
	case validatedEvent:
		m.onValidated(e)
	case publishLocalEvent:
		m.onPublishLocal(e)
	case funcEvent:
		e.fn()
	case sendDoneEvent:
		m.onSendDone(e.peer)
	case tickEvent:
		m.onTick()
	case bitmapFloorEvent:
		m.emitBitmaps()
	case heartbeatEvent:
		m.onHeartbeat()
	case bandwidthEvent:
		m.onBandwidth(e)
		return
	}
	m.trySelectAndSend()
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
	m.bitmapWriters[e.peer] = m.newBitmapWriter(e.peer, e.bitmap)
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
	delete(m.sentFull, e.peer)
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
}

// onTick runs one push-drain selection pass where partial push batches flush too
// (§F4). Later send completions go back to holding partial chunks until the next
// tick.
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
	m.logMeshHeartbeat()
}

// logMeshHeartbeat emits one post-maintenance snapshot per heartbeat: first
// each mesh peer's RTT, then one size/average-RTT row for each mesh.
func (m *Manager) logMeshHeartbeat() {
	pushPeers, bitmapPeers := m.meshPeers()
	m.logMeshPeerRTTs("bitmap", bitmapPeers)
	m.logMeshPeerRTTs("push", pushPeers)
	m.logMeshSummary("bitmap", bitmapPeers)
	m.logMeshSummary("push", pushPeers)
}

func (m *Manager) meshPeers() (pushPeers, bitmapPeers []peer.ID) {
	for p, r := range m.mesh.roles {
		switch r {
		case rolePush:
			pushPeers = append(pushPeers, p)
		case roleBitmap:
			bitmapPeers = append(bitmapPeers, p)
		}
	}
	slices.Sort(pushPeers)
	slices.Sort(bitmapPeers)
	return pushPeers, bitmapPeers
}

func (m *Manager) logMeshPeerRTTs(mesh string, peers []peer.ID) {
	for _, p := range peers {
		rtt, ok := m.peerRTT(p)
		rttMs := int64(-1)
		if ok {
			rttMs = rtt.Milliseconds()
		}
		m.logger.Info("attprop_mesh_peer_rtt",
			"topic", m.cfg.TopicIndex,
			"mesh", mesh,
			"peer", shortPeer(p),
			"peer_id", p.String(),
			"smoothedRTT_ms", rttMs,
		)
	}
}

func (m *Manager) logMeshSummary(mesh string, peers []peer.ID) {
	var total time.Duration
	var samples int
	for _, p := range peers {
		rtt, ok := m.peerRTT(p)
		if !ok {
			continue
		}
		total += rtt
		samples++
	}
	avgMs := int64(-1)
	if samples > 0 {
		avgMs = (total / time.Duration(samples)).Milliseconds()
	}
	msg := "attprop_" + mesh + "_mesh"
	m.logger.Info(msg,
		"topic", m.cfg.TopicIndex,
		"size", len(peers),
		"rtt_ms", avgMs,
		"rtt_samples", samples,
	)
}

func (m *Manager) peerRTT(p peer.ID) (time.Duration, bool) {
	if m.host == nil {
		return 0, false
	}
	for _, c := range m.host.Network().ConnsToPeer(p) {
		var qc *quic.Conn
		if ok := c.As(&qc); ok {
			return qc.ConnectionStats().SmoothedRTT, true
		}
	}
	return 0, false
}

// sendFullBitmapTo queues our full current available state (every active slot)
// to one peer's bitmap writer, bypassing the budget. The writer emits only the
// peer's missing available_ids. Used when a peer first enters our bitmap mesh
// (§D1).
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
		w.enqueueBitmaps(ctrl.Metadatas)
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
//     scarcest <= N chunk. A full chunk (N items) is sent immediately; a partial
//     chunk is selected only during a tick pass (sendAllToPushMesh).
//  2. Bitmap, gated: for each bitmap peer with no in-flight send while
//     activeData < B, send its scarcest chunk, but only from holder-count levels
//     below pushPeers + bitmapPeers/2.
func (m *Manager) trySelectAndSend() {
	for p, s := range m.senders {
		if s.inFlight || m.mesh.role(p) != rolePush {
			continue
		}
		chunk, ss := m.selectForPeer(p, m.sendAllToPushMesh, noHolderCountLimit)
		if chunk == nil {
			continue
		}
		m.send(p, ss, chunk)
	}
	push, bitmapPeers := m.mesh.counts()
	bitmapHolderLimit := (push + (bitmapPeers / 2) + 1)
	for p, s := range m.senders {
		if m.activeData >= m.cfg.SendBudgetB {
			break
		}
		if s.inFlight || m.mesh.role(p) != roleBitmap {
			continue
		}
		chunk, ss := m.selectForPeer(p, true, bitmapHolderLimit)
		if chunk == nil {
			continue
		}
		m.send(p, ss, chunk)
	}
}

// selectForPeer draws the peer's scarcest chunk from the active slots for this
// manager's topic. If allowPartial is false, only full chunks are selected so
// held push sends do not need to be rolled back. maxHolderCount bounds the
// scanned holder-count levels when serving bitmap peers; push peers pass no
// limit. The draw commits holder-count as it goes (§E2).
func (m *Manager) selectForPeer(
	p peer.ID,
	allowPartial bool,
	maxHolderCount int,
) (map[string][]int, *slotState) {
	for _, ss := range m.slots {
		chunk, _ := ss.selectOneChunkForPeer(
			p,
			m.cfg.MaxAttsPerMessage,
			allowPartial,
			maxHolderCount,
		)
		if chunk != nil {
			return chunk, ss
		}
	}
	return nil, nil
}

// rollbackChunk undoes a committed chunk that could not be handed to the sender:
// clear the peer's available bit, decrement holder-count, and bump the index
// back down one level.
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
			if hc-1 >= 0 {
				ss.indexBumpHolderDown(bk, pos, hc)
			}
		}
	}
}

// indexBumpHolderDown moves a validated entry from level `from` to `from-1`
// after a holder-count decrement. Like indexBumpHolder it only moves an entry
// actually present in `from`.
func (ss *slotState) indexBumpHolderDown(bk string, pos, from int) {
	if from >= len(ss.levels) || !ss.levels[from].remove(bk, pos) {
		return
	}
	if to := from - 1; to >= 0 {
		ss.levels[to].add(bk, pos)
	}
}

// send marshals a chunk into a BatchedAttestationEnvelope, hands the frame to
// the peer's data sender (non-blocking; work is buffered size 1), and charges
// the budget. inFlight is cleared by sendDoneEvent when WriteMsg returns. Push
// sends are exempt from B but still tracked in activeData for observability and
// so a draining push send counts against bitmap fill (§F1).
func (m *Manager) send(p peer.ID, ss *slotState, chunk map[string][]int) {
	if m.sentFull == nil {
		m.sentFull = make(map[peer.ID]map[string]struct{})
	}
	if _, ok := m.sentFull[p]; !ok {
		m.sentFull[p] = make(map[string]struct{})
	}
	env := &pb.BatchedAttestationEnvelope{}
	var total int
	var sentFullData [][]byte
	for bk := range chunk {
		b := ss.buckets[bk]
		_, sentFull := m.sentFull[p][string(b.dataHash)]
		env.Batches = append(env.Batches, encodeBatch(b, chunk[bk], !sentFull))
		if !sentFull {
			sentFullData = append(sentFullData, b.dataHash)
		}
		total += len(chunk[bk])
	}
	frame, err := proto.Marshal(env)
	if err != nil {
		m.logger.Error("CRITICAL: marshal data envelope", "err", err)
		ss.rollbackChunk(p, chunk)
		return
	}
	s := m.senders[p]
	select {
	case s.work <- frame:
	default:
		m.logger.Error("CRITICAL: attprop data sender queue full",
			"peer", shortPeer(p),
			"topic", m.cfg.TopicIndex,
			"queued", len(s.work),
			"cap", cap(s.work),
		)
		ss.rollbackChunk(p, chunk)
		return
	}
	s.inFlight = true
	for _, hash := range sentFullData {
		m.sentFull[p][string(hash)] = struct{}{}
	}
	m.activeData++
	pushInFlight, bitmapInFlight := m.dataInFlightCounts()
	pushMeshSize, bitmapMeshSize := m.mesh.counts()
	role := m.mesh.role(p)
	m.logger.Info("attprop_send_data",
		"peer", shortPeer(p),
		"topic", m.cfg.TopicIndex,
		"mesh", role.String(),
		"role", int(role),
		"slot", ss.slot,
		"num_buckets", len(chunk),
		"positions", total,
		"bytes", len(frame),
		"queue_len", len(s.work),
		"queue_cap", cap(s.work),
		"active_data", m.activeData,
		"send_budget_b", m.cfg.SendBudgetB,
		"push_in_flight", pushInFlight,
		"bitmap_in_flight", bitmapInFlight,
		"push_mesh_size", pushMeshSize,
		"bitmap_mesh_size", bitmapMeshSize,
	)
}

func (m *Manager) dataInFlightCounts() (push, bitmap int) {
	for p, s := range m.senders {
		if !s.inFlight {
			continue
		}
		switch m.mesh.role(p) {
		case rolePush:
			push++
		case roleBitmap:
			bitmap++
		}
	}
	return push, bitmap
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
		var hash []byte
		data := batch.AttestationData
		if len(batch.AttestationData) == 0 {
			var ok bool
			hash = batch.AttestationDataHash
			data, ok = m.identities.Get(hash)
			if !ok {
				m.logger.Error("CRITICAL: attprop_drop_data", "from", shortPeer(from), "err", "missing data or hash")
				return
			}
		} else {
			hash = m.identities.Put(data)
		}

		if len(batch.AttestorIndices) != len(batch.Signatures) {
			m.logger.Error("CRITICAL: attprop_recv_bad_batch", "from", shortPeer(from))
			continue
		}

		positions := make([]int, len(batch.AttestorIndices))
		ok := true
		for i, idx := range batch.AttestorIndices {
			if int(idx) >= m.cfg.CommitteeSize {
				m.logger.Error("CRITICAL: attprop_recv_bad_index", "from", shortPeer(from), "idx", idx)
				ok = false
				break
			}
			positions[i] = int(idx)
		}
		if !ok {
			return
		}

		slot := m.slotForHash(hash)
		if !m.acceptsSlot(slot) {
			m.logger.Debug("drop stale attprop data", "from", shortPeer(from), "slot", slot)
			continue
		}
		ss := m.getOrCreateSlotState(slot)
		b := ss.getOrCreateBucket(data, hash)

		// The sender holds everything it sent us — record it (bumps holder-count
		// on the 0→1 flips), so scarcity reflects what the sender has.
		for _, pos := range positions {
			ss.markHolder(b, from, pos)
		}

		newEntries := b.addReceived(positions, batch.Signatures)

		m.logger.Info("partial_recv_batch",
			"from", shortPeer(from),
			"slot", slot,
			"topic", m.cfg.TopicIndex,
			"att_digest", digestHex(data, hash),
			"positions_count", len(positions),
			"new_positions", len(newEntries),
			"batch_bytes", proto.Size(batch),
		)

		if m.tracer != nil {
			latencyMs := time.Since(m.slotStartTime(slot)).Milliseconds()
			for _, e := range newEntries {
				pe := e.(*attEntry)
				m.tracer.OnPartialReceive(slot, m.cfg.TopicIndex, pe.Position, b.data, latencyMs)
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
// holder-count, and emit the validated log (§G2/§H2). trySelectAndSend re-runs
// after (newly forwardable positions).
func (m *Manager) onValidated(e validatedEvent) {
	if !m.acceptsSlot(e.slot) {
		return
	}
	ss := m.getSlotState(e.slot)
	if ss == nil {
		return
	}
	hash := m.identities.Put(e.data)
	b, ok := ss.buckets[string(hash)]
	if !ok {
		return
	}
	latencyMs := time.Since(m.slotStartTime(e.slot)).Milliseconds()
	for _, entry := range e.entries {
		pe := entry.(*attEntry)
		if _, done := b.validated[pe.Position]; done {
			continue
		}
		delete(b.validating, pe.Position)
		b.validated[pe.Position] = struct{}{}
		ss.indexAddValidated(string(b.dataHash), pe.Position, b.holderCount[pe.Position])
		m.logger.Info("attestation_validated",
			"slot", e.slot,
			"topic", m.cfg.TopicIndex,
			"att_digest", hexPrefix(hash),
			"position", pe.Position,
			"latency_ms", latencyMs,
		)
	}
}

// onPublishLocal injects this node's own attestation as validated and indexed at
// holder-count 0 (we are the origin; no peer holds it yet) — mirrors
// partial_priority.publishLocal but from the eventloop goroutine (§F4).
func (m *Manager) onPublishLocal(e publishLocalEvent) {
	if !m.acceptsSlot(e.slot) {
		return
	}
	ss := m.getOrCreateSlotState(e.slot)
	hash := m.identities.Put(e.data)
	b := ss.getOrCreateBucket(e.data, hash)
	if _, ok := b.atts[e.pos]; ok {
		return
	}
	b.atts[e.pos] = &attEntry{Position: e.pos, Signature: e.sig, Data: b.data}
	b.validated[e.pos] = struct{}{}
	ss.indexAddValidated(string(b.dataHash), e.pos, b.holderCount[e.pos])
	m.logger.Info("self_published",
		"slot", e.slot,
		"topic", m.cfg.TopicIndex,
		"att_digest", attDigestHex(e.data),
		"position", e.pos,
	)
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

// maxPeers returns the initial holder-count index capacity, at least 1.
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

func chunkBucketKeys(chunk map[string][]int) []string {
	bks := make([]string, 0, len(chunk))
	for bk := range chunk {
		bks = append(bks, bk)
	}
	return bks
}

// slotForData resolves the slot that owns a piece of attestation_data on the
// (slot-less) push stream. attestation_data is unique per (topic, slot), so if
// any existing slotState already holds a bucket for this data we reuse its slot
// (learned earlier from our own publish, fanout, or a bitmap advertisement which
// carries Slot). Otherwise we attribute it to the slot whose window is currently
// open: the sim drives slots sequentially (publish → drain slotDuration → next),
// so unseen push data belongs to the current wall-clock slot. We cannot infer
// this from existing buckets — a node forwarding only what it receives never
// holds a bucket for the live slot until its first push of that slot arrives, so
// it would otherwise misfile every new slot under the last one it knows.
func (m *Manager) slotForHash(hash []byte) int {
	key := string(hash)
	for slot, ss := range m.slots {
		if _, ok := ss.buckets[key]; ok {
			return slot
		}
	}
	if m.lifecycleStarted {
		return m.activeSlot
	}
	return m.currentSlot()
}

// currentSlot returns the slot whose window is open now, derived from the
// wall-clock distance since slot 1 started. Defaults to 1 before slot 1 begins.
func (m *Manager) currentSlot() int {
	elapsed := time.Since(m.cfg.PublishStart)
	if m.cfg.SlotDuration <= 0 || elapsed < 0 {
		return 1
	}
	return 1 + int(elapsed/m.cfg.SlotDuration)
}
