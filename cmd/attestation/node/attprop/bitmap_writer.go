package attprop

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

type bitmapKey struct {
	slot int32
	data string
}

// bitmapWriter owns one peer's outgoing bitmap stream. It keeps at most one
// queued advertisement per (slot, attestation_data), replacing stale queued
// state with the newest bitmap instead of dropping the update under backpressure.
type bitmapWriter struct {
	peer   peer.ID
	stream network.Stream
	w      msgio.WriteCloser
	work   chan bitmapKey

	mu      sync.Mutex
	pending map[bitmapKey]*pb.CommitteeAttestationPartsMetadata
	closed  bool
}

func (m *Manager) newBitmapWriter(p peer.ID, s network.Stream, buf int) *bitmapWriter {
	bw := &bitmapWriter{
		peer:    p,
		stream:  s,
		w:       msgio.NewVarintWriter(s),
		work:    make(chan bitmapKey, buf),
		pending: make(map[bitmapKey]*pb.CommitteeAttestationPartsMetadata),
	}
	go func() {
		for key := range bw.work {
			md := bw.pop(key)
			if md == nil {
				continue
			}
			frame, err := proto.Marshal(&pb.ControlEnvelope{
				Metadatas: []*pb.CommitteeAttestationPartsMetadata{md},
			})
			if err != nil {
				m.logger.Error("marshal bitmap", "topic", m.cfg.TopicIndex, "err", err)
				continue
			}
			if err := writeFrame(bw.w, frame); err != nil {
				m.logger.Debug(
					"write bitmap frame",
					"topic", m.cfg.TopicIndex,
					"peer", shortPeer(p),
					"err", err,
				)
				m.post(peerDownEvent{peer: p})
				return
			}
		}
	}()
	return bw
}

func (w *bitmapWriter) enqueueBitmap(md *pb.CommitteeAttestationPartsMetadata) (
	replaced bool,
	dropped bool,
	ok bool,
) {
	if md == nil {
		return false, false, false
	}
	key := bitmapKey{slot: md.Slot, data: string(md.AttestationData)}
	queued := proto.Clone(md).(*pb.CommitteeAttestationPartsMetadata)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false, false, false
	}
	if _, exists := w.pending[key]; exists {
		w.pending[key] = queued
		return true, false, true
	}
	select {
	case w.work <- key:
		w.pending[key] = queued
		return false, false, true
	default:
	}

	select {
	case old := <-w.work:
		delete(w.pending, old)
		dropped = true
	default:
	}
	select {
	case w.work <- key:
		w.pending[key] = queued
		return false, dropped, true
	default:
		return false, dropped, false
	}
}

func (w *bitmapWriter) pop(key bitmapKey) *pb.CommitteeAttestationPartsMetadata {
	w.mu.Lock()
	defer w.mu.Unlock()
	md := w.pending[key]
	delete(w.pending, key)
	return md
}

func (w *bitmapWriter) closeAndReset() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		clear(w.pending)
		close(w.work)
	}
	w.mu.Unlock()
	if w.stream != nil {
		w.stream.Reset()
	}
}

func (m *Manager) enqueueBitmap(
	w *bitmapWriter,
	md *pb.CommitteeAttestationPartsMetadata,
	what string,
) {
	replaced, dropped, ok := w.enqueueBitmap(md)
	if !ok {
		m.logger.Debug("drop bitmap, writer closed", "peer", shortPeer(w.peer), "what", what)
		return
	}
	if replaced {
		m.logger.Debug("replace queued bitmap", "peer", shortPeer(w.peer), "what", what)
	}
	if dropped {
		m.logger.Debug("drop stale bitmap, writer full", "peer", shortPeer(w.peer), "what", what)
	}
}
