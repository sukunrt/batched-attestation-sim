package attprop

import (
	"context"
	"errors"
	"io"
	"math/bits"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// wire.go is the att_propagation stream substrate: varint-length-prefixed
// protobuf framing over persistent bidirectional libp2p streams (the same
// go-msgio wire format gossipsub uses), the three per-topic protocol-ID
// handlers, the dial logic, and the per-stream reader goroutines that decode
// frames into events. It owns no attestation state beyond posting to the events
// channel; the eventloop (Core) is the sole protocol-state owner.

// streamKind tags which of the three per-topic streams a reader is draining, so
// it can decode each frame into the right protobuf type and post the matching
// inbound event.
type streamKind int

const (
	kindPush streamKind = iota
	kindBitmap
	kindControl
)

type peerStreams struct {
	push, bitmap, control network.Stream
}

// writeFrame writes one varint-length-prefixed protobuf frame. Blocks under a
// full QUIC flow-control window — that block is the send backpressure signal.
func writeFrame(w msgio.WriteCloser, b []byte) error {
	return w.WriteMsg(b)
}

func (m *Manager) writeFrameTimed(
	w msgio.WriteCloser,
	b []byte,
	p peer.ID,
	writerType string,
) error {
	start := time.Now()
	err := writeFrame(w, b)
	elapsed := time.Since(start)
	if err == nil {
		m.logger.Info("attprop_write_frame",
			"peer", shortPeer(p),
			"topic", m.cfg.TopicIndex,
			"writer_type", writerType,
			"bytes", len(b),
			"duration_ms", elapsed.Milliseconds(),
		)
	}
	return err
}

// newFrameReader wraps a stream in a varint reader capped at cfg.MaxMsgSize,
// matching gossipsub's comm.go.
func (m *Manager) newFrameReader(s network.Stream) msgio.ReadCloser {
	return msgio.NewVarintReaderSize(s, m.cfg.MaxMsgSize)
}

// start registers the three per-topic inbound stream handlers on the host. Each
// handler records the accepted bidirectional stream as a writer and spawns a
// reader goroutine that drains framed messages until the remote half-closes
// (EOF) or resets.
func (m *Manager) start(ctx context.Context) {
	if m.cfg.Fanout {
		return
	}

	m.host.SetStreamHandler(PushProtocol(m.cfg.TopicIndex), m.inboundHandler(ctx, kindPush))
	m.host.SetStreamHandler(BitmapProtocol(m.cfg.TopicIndex), m.inboundHandler(ctx, kindBitmap))
	m.host.SetStreamHandler(ControlProtocol(m.cfg.TopicIndex), m.inboundHandler(ctx, kindControl))
}

// inboundHandler returns a network.StreamHandler for an accepted stream of the
// given kind. The accepted QUIC stream is bidirectional: we read peer frames from
// it and, once all three message-type streams are accepted, use the same streams
// for writes back to that peer.
func (m *Manager) inboundHandler(
	ctx context.Context,
	kind streamKind,
) network.StreamHandler {
	return func(s network.Stream) {
		p := s.Conn().RemotePeer()
		if streams, ok := m.recordInboundStream(p, kind, s); ok {
			m.post(peerUpEvent{
				peer:    p,
				push:    streams.push,
				bitmap:  streams.bitmap,
				control: streams.control,
			})
		}
		go m.readLoop(ctx, s, p, kind)
	}
}

// connectPeer opens our three bidirectional streams to a peer after the host has
// connected. The accepting side uses those same streams for its writes; it does
// not open a second stream set back.
func (m *Manager) connectPeer(p peer.ID) {
	go m.openSendStreams(context.Background(), p)
}

// openSendStreams opens this node's three bidirectional streams to a peer, starts
// readers for the peer's frames on those same streams, and posts a peerUpEvent
// carrying the writers. The once map guards against opening twice.
func (m *Manager) openSendStreams(ctx context.Context, p peer.ID) {
	pushProto := PushProtocol(m.cfg.TopicIndex)
	bitmapProto := BitmapProtocol(m.cfg.TopicIndex)
	controlProto := ControlProtocol(m.cfg.TopicIndex)
	supports, err := m.peerSupports(p, pushProto, bitmapProto, controlProto)
	if err != nil {
		m.logger.Debug("check attprop protocol support", "topic", m.cfg.TopicIndex,
			"peer", shortPeer(p), "err", err)
		return
	}
	if !supports {
		m.logger.Debug("peer does not support attprop streams", "topic", m.cfg.TopicIndex,
			"peer", shortPeer(p))
		return
	}
	if !m.markSendStreamsOpening(p) {
		return // already opening/open for this peer
	}
	push, err := m.host.NewStream(ctx, p, pushProto)
	if err != nil {
		m.logger.Error("open push stream", "topic", m.cfg.TopicIndex, "peer", shortPeer(p), "err", err)
		m.clearSendStreamsOpening(p)
		return
	}
	bm, err := m.host.NewStream(ctx, p, bitmapProto)
	if err != nil {
		m.logger.Error("open bitmap stream", "topic", m.cfg.TopicIndex, "peer", shortPeer(p), "err", err)
		push.Reset()
		m.clearSendStreamsOpening(p)
		return
	}
	ctrl, err := m.host.NewStream(ctx, p, controlProto)
	if err != nil {
		m.logger.Error("open control stream", "topic", m.cfg.TopicIndex, "peer", shortPeer(p), "err", err)
		push.Reset()
		bm.Reset()
		m.clearSendStreamsOpening(p)
		return
	}
	go m.readLoop(ctx, push, p, kindPush)
	go m.readLoop(ctx, bm, p, kindBitmap)
	go m.readLoop(ctx, ctrl, p, kindControl)
	m.post(peerUpEvent{peer: p, push: push, bitmap: bm, control: ctrl})
}

// recordInboundStream collects the three accepted bidirectional streams for one
// peer. It returns a complete set exactly once, when push/bitmap/control are all
// present; fanout's one-shot push stream never completes a set, which is fine
// because receivers do not write back to fanout leaves.
func (m *Manager) recordInboundStream(
	p peer.ID, kind streamKind, s network.Stream,
) (*peerStreams, bool) {
	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()

	streams := m.pending[p]
	if streams == nil {
		streams = &peerStreams{}
		m.pending[p] = streams
	}
	switch kind {
	case kindPush:
		streams.push = s
	case kindBitmap:
		streams.bitmap = s
	case kindControl:
		streams.control = s
	}
	if streams.push == nil || streams.bitmap == nil || streams.control == nil {
		return nil, false
	}
	delete(m.pending, p)
	return streams, true
}

// readLoop drains framed messages off one stream, decodes each into the
// protobuf type for its kind, and posts the matching inbound event. A clean
// half-close (io.EOF at a frame boundary) or a reset ends the loop; either way
// it posts a single peerDownEvent and returns. The reader owns no Manager
// state.
func (m *Manager) readLoop(
	ctx context.Context,
	s network.Stream,
	from peer.ID,
	kind streamKind,
) {
	r := m.newFrameReader(s)
	for {
		b, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(b)
			if errors.Is(err, io.EOF) {
				// TODO: check if we have len(b) > 0
				// panic if we do. Just want to confirm this works correctly.
				//
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
		m.logReceivedFrame(from, kind, b, ev)
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

func (m *Manager) logReceivedFrame(from peer.ID, kind streamKind, b []byte, ev event) {
	switch kind {
	case kindPush:
		e, ok := ev.(inboundDataEvent)
		if !ok || e.env == nil {
			return
		}
		dataBatches, attCount, attDataBytes, attDataHashBytes, sigBytes := attpropDataStats(e.env)
		m.logger.Info("attprop_data_received",
			"peer", shortPeer(from),
			"topic", m.cfg.TopicIndex,
			"bytes", len(b),
			"data_batches", dataBatches,
			"att_count", attCount,
			"att_data_bytes", attDataBytes,
			"att_data_hash_bytes", attDataHashBytes,
			"sig_bytes", sigBytes,
		)
	case kindBitmap:
		e, ok := ev.(inboundBitmapEvent)
		if !ok || e.ctrl == nil {
			return
		}
		metaCount, availableOnes, attDataHashBytes := attpropMetadataStats(e.ctrl)
		m.logger.Info("attprop_metadata_received",
			"peer", shortPeer(from),
			"topic", m.cfg.TopicIndex,
			"bytes", len(b),
			"meta_count", metaCount,
			"att_data_hash_bytes", attDataHashBytes,
			"available_ones", availableOnes,
		)
	case kindControl:
		e, ok := ev.(inboundControlEvent)
		if !ok || e.ctrl == nil {
			return
		}
		m.logger.Info("attprop_control_received",
			"peer", shortPeer(from),
			"topic", m.cfg.TopicIndex,
			"bytes", len(b),
			"items", len(e.ctrl.Items),
		)
	}
}

func attpropDataStats(env *pb.BatchedAttestationEnvelope) (
	dataBatches int,
	attCount int,
	attDataBytes int,
	attDataHashBytes int,
	sigBytes int,
) {
	for _, batch := range env.Batches {
		if batch == nil {
			continue
		}
		dataBatches++
		attDataBytes += len(batch.AttestationData)
		attDataHashBytes += len(batch.AttestationDataHash)
		attCount += len(batch.Signatures)
		for _, sig := range batch.Signatures {
			sigBytes += len(sig)
		}
	}
	return dataBatches, attCount, attDataBytes, attDataHashBytes, sigBytes
}

func attpropMetadataStats(ctrl *pb.ControlEnvelope) (
	metaCount int,
	availableOnes int,
	attDataHashBytes int,
) {
	for _, md := range ctrl.Metadatas {
		if md == nil {
			continue
		}
		metaCount++
		attDataHashBytes += len(md.AttestationDataHash)
		availableOnes += len(md.AvailableIds)
		for _, b := range md.Available {
			availableOnes += bits.OnesCount8(b)
		}
	}
	return metaCount, availableOnes, attDataHashBytes
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
