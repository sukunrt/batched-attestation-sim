package attprop

import (
	"sync"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
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
// queue per (slot, attestation_data), coalescing each queue into one metadata
// item per write wakeup.
type bitmapWriter struct {
	peer   peer.ID
	stream network.Stream
	w      msgio.WriteCloser
	work   chan struct{}

	committeeSize int

	mu            sync.Mutex
	pending       map[bitmapKey][]*pb.CommitteeAttestationPartsMetadata
	sentFull      map[string]struct{}
	sentAvailable map[bitmapKey]bitmap.Bitmap
	closed        bool
}

func (m *Manager) newBitmapWriter(p peer.ID, s network.Stream) *bitmapWriter {
	bw := &bitmapWriter{
		peer:          p,
		stream:        s,
		w:             msgio.NewVarintWriter(s),
		work:          make(chan struct{}, 1),
		committeeSize: m.cfg.CommitteeSize,
		pending:       make(map[bitmapKey][]*pb.CommitteeAttestationPartsMetadata),
		sentFull:      make(map[string]struct{}),
		sentAvailable: make(map[bitmapKey]bitmap.Bitmap),
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
			metaCount, availableOnes, attDataHashBytes := attpropMetadataStats(md)
			attDataBytes := attpropMetadataDataBytes(md)
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
			m.logger.Info("attprop_send_bitmap",
				"peer", shortPeer(p),
				"topic", m.cfg.TopicIndex,
				"bytes", len(frame),
				"meta_count", metaCount,
				"att_data_bytes", attDataBytes,
				"att_data_hash_bytes", attDataHashBytes,
				"available_ones", availableOnes,
			)
		}
	}()
	return bw
}

func attpropMetadataDataBytes(ctrl *pb.ControlEnvelope) int {
	var n int
	for _, md := range ctrl.Metadatas {
		if md == nil {
			continue
		}
		n += len(md.AttestationData)
	}
	return n
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
		w.pending[key] = append(w.pending[key], md)
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
	for key, queued := range w.pending {
		if len(queued) == 0 {
			delete(w.pending, key)
			continue
		}
		last := queued[len(queued)-1]
		if w.sentAvailable[key] == nil {
			w.sentAvailable[key] = newCommitteeBitmap(w.committeeSize)
		}
		ids := w.newAvailableIDs(w.sentAvailable[key], last.Available, queued)
		if len(ids) == 0 {
			continue
		}

		out := &pb.CommitteeAttestationPartsMetadata{
			Slot:                last.Slot,
			AttestationData:     last.AttestationData,
			AttestationDataHash: []byte(key.hash),
			AvailableIds:        ids,
		}
		if _, sent := w.sentFull[key.hash]; sent {
			out.AttestationData = nil
		} else if len(out.AttestationData) > 0 {
			w.sentFull[key.hash] = struct{}{}
			out.AttestationDataHash = nil
		}
		mds = append(mds, out)
	}
	clear(w.pending)
	if len(mds) == 0 {
		return nil
	}
	return &pb.ControlEnvelope{
		Metadatas: mds,
	}
}

func (w *bitmapWriter) newAvailableIDs(
	lastSent bitmap.Bitmap,
	current bitmap.Bitmap,
	queued []*pb.CommitteeAttestationPartsMetadata,
) []uint32 {
	full := newCommitteeBitmap(w.committeeSize)
	for pos := range w.committeeSize {
		if current.Get(pos) {
			full.Set(pos)
		}
	}
	for _, md := range queued {
		for _, id := range md.AvailableIds {
			if int(id) < w.committeeSize {
				full.Set(int(id))
			}
		}
	}

	ids := make([]uint32, 0, 32)
	for pos := range w.committeeSize {
		if !full.Get(pos) || lastSent.Get(pos) {
			continue
		}
		ids = append(ids, uint32(pos))
		lastSent.Set(pos)
	}
	return ids
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
