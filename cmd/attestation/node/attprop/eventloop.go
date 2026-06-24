package attprop

import (
	"context"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

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

// peerUpEvent signals that the three streams to/from a peer are established.
// For the opener side the streams were dialed; for the receiver side they were
// accepted by the stream handlers.
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
	topic   string
	slot    int
	data    []byte
	entries []any
}

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
	w        msgio.WriteCloser
	work     chan []byte
	inFlight bool
}

// run is the single-owner eventloop. It launches the tick/floor/heartbeat timer
// drivers, then processes events one at a time, running trySelectAndSend on
// every event that can change what's sendable. Implemented by the Core agent.
func (m *Manager) run(ctx context.Context) {
	panic("TODO: Core — eventloop.go")
}
