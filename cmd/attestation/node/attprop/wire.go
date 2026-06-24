package attprop

import (
	"context"
	"errors"
	"io"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// wire.go is the att_propagation stream substrate: varint-length-prefixed
// protobuf framing over persistent libp2p streams (the same go-msgio wire
// format gossipsub uses), the three per-topic protocol-ID handlers, the
// lower-peerID-opens dial logic, and the per-stream reader goroutines that
// decode frames into events. It owns no Manager state beyond posting to the
// events channel; the eventloop (Core) is the sole state owner.

// streamKind tags which of the three per-topic streams a reader is draining, so
// it can decode each frame into the right protobuf type and post the matching
// inbound event.
type streamKind int

const (
	kindPush streamKind = iota
	kindBitmap
	kindControl
)

// writeFrame writes one varint-length-prefixed protobuf frame. Blocks under a
// full QUIC flow-control window — that block is the send backpressure signal.
func writeFrame(w msgio.WriteCloser, b []byte) error {
	return w.WriteMsg(b)
}

// newFrameReader wraps a stream in a varint reader capped at cfg.MaxMsgSize,
// matching gossipsub's comm.go.
func (m *Manager) newFrameReader(s network.Stream) msgio.ReadCloser {
	return msgio.NewVarintReaderSize(s, m.cfg.MaxMsgSize)
}

// start registers the three per-topic inbound stream handlers on the host. Each
// handler spawns a reader goroutine that drains framed messages off the
// accepted stream until the remote half-closes (EOF) or resets. The opener
// (lower peer ID) dials in connectPeer; this side receives.
func (m *Manager) start(ctx context.Context) {
	for topicIdx := range m.cfg.Topics {
		m.host.SetStreamHandler(PushProtocol(topicIdx), m.inboundHandler(ctx, kindPush))
		m.host.SetStreamHandler(BitmapProtocol(topicIdx), m.inboundHandler(ctx, kindBitmap))
		m.host.SetStreamHandler(ControlProtocol(topicIdx), m.inboundHandler(ctx, kindControl))
	}
}

// inboundHandler returns a network.StreamHandler that starts a reader goroutine
// for an accepted stream of the given kind.
func (m *Manager) inboundHandler(ctx context.Context, kind streamKind) network.StreamHandler {
	return func(s network.Stream) {
		go m.readLoop(ctx, s, s.Conn().RemotePeer(), kind)
	}
}

// connectPeer opens the three per-topic streams to a peer when we are the
// opener (lower peer ID) and emits a peerUpEvent carrying them. When we are not
// the opener we do nothing here — the peer dials us and our inbound handlers
// accept. Topic 0 is used for the (currently single-topic) sim; the per-topic
// stream set generalises by topic index.
func (m *Manager) connectPeer(p peer.ID) {
	if !weOpen(m.self, p) {
		return
	}
	ctx := context.Background()
	const topicIdx = 0

	push, err := m.host.NewStream(ctx, p, PushProtocol(topicIdx))
	if err != nil {
		m.logger.Error("open push stream", "peer", shortPeer(p), "err", err)
		return
	}
	bm, err := m.host.NewStream(ctx, p, BitmapProtocol(topicIdx))
	if err != nil {
		m.logger.Error("open bitmap stream", "peer", shortPeer(p), "err", err)
		push.Reset()
		return
	}
	ctrl, err := m.host.NewStream(ctx, p, ControlProtocol(topicIdx))
	if err != nil {
		m.logger.Error("open control stream", "peer", shortPeer(p), "err", err)
		push.Reset()
		bm.Reset()
		return
	}

	// The opener also receives on these streams (symmetric meshes), so start a
	// reader for each direction we opened.
	go m.readLoop(ctx, push, p, kindPush)
	go m.readLoop(ctx, bm, p, kindBitmap)
	go m.readLoop(ctx, ctrl, p, kindControl)

	m.post(peerUpEvent{peer: p, push: push, bitmap: bm, control: ctrl})
}

// readLoop drains framed messages off one stream, decodes each into the
// protobuf type for its kind, and posts the matching inbound event. A clean
// half-close (io.EOF at a frame boundary) or a reset ends the loop; either way
// it posts a single peerDownEvent and returns. The reader owns no Manager
// state.
func (m *Manager) readLoop(ctx context.Context, s network.Stream, from peer.ID, kind streamKind) {
	r := m.newFrameReader(s)
	for {
		b, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(b)
			if errors.Is(err, io.EOF) {
				// Clean close at a frame boundary. Be nice and close our side.
				_ = s.Close()
			} else if !errors.Is(err, context.Canceled) {
				s.Reset()
				m.logger.Debug("read frame", "peer", shortPeer(from), "kind", kind, "err", err)
			}
			m.post(peerDownEvent{peer: from})
			return
		}
		if len(b) == 0 {
			r.ReleaseMsg(b)
			continue
		}
		ev, derr := decodeFrame(from, kind, b)
		r.ReleaseMsg(b)
		if derr != nil {
			s.Reset()
			m.logger.Warn("decode frame", "peer", shortPeer(from), "kind", kind, "err", derr)
			m.post(peerDownEvent{peer: from})
			return
		}
		if !m.post(ev) {
			// ctx cancelled; stop reading.
			s.Reset()
			return
		}
		select {
		case <-ctx.Done():
			s.Reset()
			return
		default:
		}
	}
}

// decodeFrame unmarshals one frame into the inbound event for its stream kind.
func decodeFrame(from peer.ID, kind streamKind, b []byte) (event, error) {
	switch kind {
	case kindPush:
		env := &pb.BatchedAttestationEnvelope{}
		if err := proto.Unmarshal(b, env); err != nil {
			return nil, err
		}
		return inboundDataEvent{from: from, env: env}, nil
	case kindBitmap:
		ctrl := &pb.ControlEnvelope{}
		if err := proto.Unmarshal(b, ctrl); err != nil {
			return nil, err
		}
		return inboundBitmapEvent{from: from, ctrl: ctrl}, nil
	case kindControl:
		ctrl := &pb.AttPropControl{}
		if err := proto.Unmarshal(b, ctrl); err != nil {
			return nil, err
		}
		return inboundControlEvent{from: from, ctrl: ctrl}, nil
	default:
		return nil, errUnknownKind
	}
}

var errUnknownKind = errors.New("attprop: unknown stream kind")

// post delivers an event to the eventloop. It returns false if the manager is
// shutting down (the events channel is closed by the eventloop on ctx cancel),
// so readers can stop. Until the eventloop owns the channel it simply enqueues.
func (m *Manager) post(ev event) bool {
	defer func() {
		// A send on a closed channel panics; the eventloop closes events on
		// shutdown. Recover so a racing reader exits quietly instead of
		// crashing the process.
		_ = recover()
	}()
	m.events <- ev
	return true
}

// shortPeer returns a short prefix of the peer ID for logging. Reimplemented
// here (attprop cannot import node); mirrors node.shortPeer.
func shortPeer(p peer.ID) string {
	s := p.String()
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
