package attprop

import (
	"slices"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// bitmapTriggerK is the +K count trigger for a bitmap advertisement (§D2): once
// this many positions validate for a slot since its last emit, re-advertise.
const bitmapTriggerK = 30

// bitmap_stream.go implements the bitmap advertisement stream (§D): full
// available-only bitmaps per active bucket, triggered by a +K count change plus
// a periodic floor (re-emit only if changed), sent to bitmap-mesh peers only and
// bypassing the send budget.

// buildAvailableEnvelope assembles an available-only pb.ControlEnvelope (one
// CommitteeAttestationPartsMetadata per bucket, available set from validated, no
// requests) for a (topic, slot), in bucketSeq order. Returns nil when nothing is
// validated yet (§D1). Used to dump the full current bitmap to a peer on a fresh
// Graft:Bitmap; it does not touch lastEmitted (that tracks the floor broadcast,
// not point-to-point dumps). Eventloop-only (no lock).
func (m *Manager) buildAvailableEnvelope(slot int) *pb.ControlEnvelope {
	ss := m.getSlotState(slot)
	if ss == nil {
		return nil
	}
	ctrl := &pb.ControlEnvelope{}
	for _, bk := range sortedBuckets(ss) {
		b := ss.buckets[bk]
		if len(b.validated) == 0 {
			continue
		}
		ctrl.Metadatas = append(ctrl.Metadatas, &pb.CommitteeAttestationPartsMetadata{
			Slot:            int32(slot),
			AttestationData: b.data,
			Available:       []byte(m.validatedBitmap(b)),
		})
	}
	if len(ctrl.Metadatas) == 0 {
		return nil
	}
	return ctrl
}

// validatedBitmap returns a fresh committee bitmap with a bucket's validated
// positions set.
func (m *Manager) validatedBitmap(b *bucket) bitmap.Bitmap {
	avail := newCommitteeBitmap(m.cfg.CommitteeSize)
	for pos := range b.validated {
		avail.Set(pos)
	}
	return avail
}

// sortedBuckets returns a slot's bucket keys in stable bucketSeq order.
func sortedBuckets(ss *slotState) []string {
	bks := make([]string, 0, len(ss.buckets))
	for bk := range ss.buckets {
		bks = append(bks, bk)
	}
	slices.SortFunc(bks, func(a, b string) int { return ss.bucketSeq[a] - ss.bucketSeq[b] })
	return bks
}

// emitBitmaps advertises the current available bitmap to every bitmap-mesh peer
// for the slots that need it, bypassing the send budget (§D3). forced is the
// floor tick: emit a bucket only if its validated bitmap changed since the last
// emit (§D2). When not forced it is the +K trigger: emit slots whose
// since-emit counter has reached K and reset that counter. Eventloop-only.
func (m *Manager) emitBitmaps(forced bool) {
	bitmapPeers := m.bitmapMeshPeers()
	if len(bitmapPeers) == 0 {
		return
	}
	for slot, ss := range m.slots {
		if !forced && ss.validatedSinceEmit < bitmapTriggerK {
			continue
		}
		ctrl := m.changedAvailableEnvelope(ss, slot, forced)
		if ctrl == nil {
			continue
		}
		frame, err := proto.Marshal(ctrl)
		if err != nil {
			m.logger.Error("marshal bitmap envelope", "err", err)
			continue
		}
		for _, p := range bitmapPeers {
			if w, ok := m.bitmapWriters[p]; ok {
				m.tryEnqueue(w, frame, "bitmap")
			}
		}
		ss.validatedSinceEmit = 0
		m.logger.Debug("attprop_emit_bitmap",
			"topic", m.cfg.TopicIndex,
			"slot", slot, "buckets", len(ctrl.Metadatas), "forced", forced,
			"peers", len(bitmapPeers))
	}
}

// changedAvailableEnvelope builds the available envelope for a slot, recording
// each bucket's emitted bitmap as lastEmitted. When onlyChanged (floor), buckets
// whose bitmap is unchanged since lastEmitted are skipped; otherwise all
// validated buckets are emitted (§D2). Returns nil when nothing would be sent.
func (m *Manager) changedAvailableEnvelope(
	ss *slotState, slot int, onlyChanged bool,
) *pb.ControlEnvelope {
	ctrl := &pb.ControlEnvelope{}
	for _, bk := range sortedBuckets(ss) {
		b := ss.buckets[bk]
		if len(b.validated) == 0 {
			continue
		}
		avail := m.validatedBitmap(b)
		if onlyChanged && b.lastEmitted != nil && slices.Equal(b.lastEmitted, avail) {
			continue // floor: unchanged since last emit, skip (§D2)
		}
		b.lastEmitted = avail
		ctrl.Metadatas = append(ctrl.Metadatas, &pb.CommitteeAttestationPartsMetadata{
			Slot:            int32(slot),
			AttestationData: b.data,
			Available:       []byte(avail),
		})
	}
	if len(ctrl.Metadatas) == 0 {
		return nil
	}
	return ctrl
}

// bitmapMeshPeers returns the peers currently in our bitmap mesh, in sorted
// order for deterministic sends.
func (m *Manager) bitmapMeshPeers() []peer.ID {
	var ps []peer.ID
	for p := range m.bitmapWriters {
		if m.mesh.role(p) == roleBitmap {
			ps = append(ps, p)
		}
	}
	slices.Sort(ps)
	return ps
}

// onInboundBitmap folds a peer's advertised available bitmap into our state: for
// each bucket metadata, mark the peer as holding every set bit (markHolder bumps
// holder-count and the scarcity index on each 0→1 flip — §E1). The metadata
// carries the authoritative Slot, so it also seeds the (topic, slot) bucket.
// Eventloop-only. Emits partial_recv_metadata (§H2).
func (m *Manager) onInboundBitmap(from peer.ID, ctrl *pb.ControlEnvelope) {
	for _, md := range ctrl.Metadatas {
		slot := int(md.Slot)
		ss := m.getOrCreateSlotState(slot)
		b := ss.getOrCreateBucket(md.AttestationData)

		avail := bitmap.Bitmap(md.Available)
		flips := 0
		for pos := 0; pos < m.cfg.CommitteeSize; pos++ {
			if avail.Get(pos) && ss.markHolder(b, from, pos) {
				flips++
			}
		}
		m.logger.Info("partial_recv_metadata",
			"from", shortPeer(from),
			"slot", slot,
			"topic", m.cfg.TopicIndex,
			"att_digest", attDigestHex(md.AttestationData),
			"available_ones", avail.OnesCount(),
			"requests_ones", 0,
			"holder_flips", flips,
		)
	}
}
