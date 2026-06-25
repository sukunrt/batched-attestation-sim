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
	hash string
}

// bitmapWriter owns one peer's outgoing bitmap stream. It keeps at most one
// queued advertisement per (slot, attestation_data), replacing stale queued
// state with the newest bitmap and coalescing pending advertisements into one
// frame per write wakeup.
type bitmapWriter struct {
	peer   peer.ID
	stream network.Stream
	w      msgio.WriteCloser
	work   chan struct{}

	mu       sync.Mutex
	pending  map[bitmapKey]*pb.CommitteeAttestationPartsMetadata
	sentFull map[string]struct{}
	closed   bool
}

func (m *Manager) newBitmapWriter(p peer.ID, s network.Stream) *bitmapWriter {
	bw := &bitmapWriter{
		peer:     p,
		stream:   s,
		w:        msgio.NewVarintWriter(s),
		work:     make(chan struct{}, 1),
		pending:  make(map[bitmapKey]*pb.CommitteeAttestationPartsMetadata),
		sentFull: make(map[string]struct{}),
	}
	go func() {
		for range bw.work {
			md := bw.getNextBitmap()
			if md == nil {
				continue
			}
			frame, err := proto.Marshal(md)
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

func (w *bitmapWriter) enqueueBitmaps(mds []*pb.CommitteeAttestationPartsMetadata) {
	if len(mds) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	for _, md := range mds {
		key := bitmapKey{slot: md.Slot, hash: string(md.AttestationDataHash)}
		queued := proto.Clone(md).(*pb.CommitteeAttestationPartsMetadata)
		if _, sent := w.sentFull[key.hash]; sent {
			queued.AttestationData = nil
		} else if len(queued.AttestationData) == 0 && w.pending[key] != nil {
			queued.AttestationData = w.pending[key].AttestationData
		}
		w.pending[key] = queued
	}
	select {
	case w.work <- struct{}{}:
	default:
	}
}

func (w *bitmapWriter) getNextBitmap() *pb.ControlEnvelope {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.pending) == 0 {
		return nil
	}

	var mds []*pb.CommitteeAttestationPartsMetadata
	for key, v := range w.pending {
		v.AttestationDataHash = []byte(key.hash)
		if _, sent := w.sentFull[key.hash]; sent {
			v.AttestationData = nil
		} else {
			w.sentFull[key.hash] = struct{}{}
			v.AttestationDataHash = nil
		}
		mds = append(mds, v)
	}
	clear(w.pending)
	return &pb.ControlEnvelope{
		Metadatas: mds,
	}
}

func (w *bitmapWriter) closeAndReset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.closed = true
		clear(w.pending)
		close(w.work)
	}
	if w.stream != nil {
		w.stream.Reset()
	}
}
