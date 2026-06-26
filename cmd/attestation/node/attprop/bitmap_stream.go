package attprop

import (
	"fmt"
	"slices"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// bitmap_stream.go implements the bitmap advertisement stream (§D): full
// available state per active bucket is queued internally, triggered by the
// periodic floor tick (re-emit only if changed), then each bitmap writer emits
// per-peer available_ids deltas to bitmap-mesh peers only and bypasses the send
// budget.

// buildAvailableEnvelope assembles an internal full-availability
// pb.ControlEnvelope (one CommitteeAttestationPartsMetadata per bucket,
// available set from validated, no requests) for a (topic, slot). Returns nil
// when nothing is validated yet (§D1). Used to seed a peer's bitmap writer on a
// fresh Graft:Bitmap; it does not touch lastEmitted (that tracks the floor
// broadcast, not point-to-point dumps). Eventloop-only (no lock).
func (m *Manager) buildAvailableEnvelope(slot int) *pb.ControlEnvelope {
	ss := m.getSlotState(slot)
	if ss == nil {
		return nil
	}
	ctrl := &pb.ControlEnvelope{}
	for _, bk := range bucketKeys(ss) {
		b := ss.buckets[bk]
		if len(b.validated) == 0 {
			continue
		}
		ctrl.Metadatas = append(ctrl.Metadatas, &pb.CommitteeAttestationPartsMetadata{
			Slot:                int32(slot),
			AttestationData:     b.data,
			AttestationDataHash: b.dataHash,
			Available:           []byte(m.validatedBitmap(b)),
		})
	}
	if len(ctrl.Metadatas) == 0 {
		return nil
	}
	return ctrl
}

func errMetadataIDOutOfRange(id uint32, committeeSize int) error {
	return fmt.Errorf("metadata id %d >= committee_size %d", id, committeeSize)
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

func bucketKeys(ss *slotState) []string {
	bks := make([]string, 0, len(ss.buckets))
	for bk := range ss.buckets {
		bks = append(bks, bk)
	}
	return bks
}

// for the slots that changed since the last emit, bypassing the send budget
// (§D3). Eventloop-only.
func (m *Manager) emitBitmaps() {
	peerCount := 0
	for p := range m.bitmapWriters {
		if m.mesh.role(p) == roleBitmap {
			peerCount++
		}
	}
	if peerCount == 0 {
		return
	}
	for slot, ss := range m.slots {
		ctrl := m.changedAvailableEnvelope(ss, slot)
		if ctrl == nil {
			continue
		}
		count := 0
		for p, w := range m.bitmapWriters {
			if m.mesh.role(p) != roleBitmap {
				continue
			}
			w.enqueueBitmaps(ctrl.Metadatas)
			count++
		}
		m.logger.Debug("attprop_emit_bitmap",
			"topic", m.cfg.TopicIndex,
			"slot", slot, "buckets", len(ctrl.Metadatas),
			"peers", count)
	}
}

// changedAvailableEnvelope builds the available envelope for a slot, recording
// each bucket's emitted bitmap as lastEmitted. Buckets whose bitmap is unchanged
// since lastEmitted are skipped. Returns nil when nothing would be sent.
//
// We MUST emit the AVAILABLE bitmap.
// deduping into ids happens in the bitmap writer.
func (m *Manager) changedAvailableEnvelope(ss *slotState, slot int) *pb.ControlEnvelope {
	ctrl := &pb.ControlEnvelope{}
	for _, bk := range bucketKeys(ss) {
		b := ss.buckets[bk]
		if len(b.validated) == 0 {
			continue
		}
		avail := m.validatedBitmap(b)
		if b.lastEmitted != nil && slices.Equal(b.lastEmitted, avail) {
			continue // floor: unchanged since last emit, skip (§D2)
		}
		b.lastEmitted = avail
		ctrl.Metadatas = append(ctrl.Metadatas, &pb.CommitteeAttestationPartsMetadata{
			Slot:                int32(slot),
			AttestationData:     b.data,
			AttestationDataHash: b.dataHash,
			Available:           []byte(avail),
		})
	}
	if len(ctrl.Metadatas) == 0 {
		return nil
	}
	return ctrl
}

// onInboundBitmap folds a peer's advertised available bitmap into our state: for
// each bucket metadata, mark the peer as holding every set bit (markHolder bumps
// holder-count and the scarcity index on each 0→1 flip — §E1). The metadata
// carries the authoritative Slot, so it also seeds the (topic, slot) bucket.
// Eventloop-only. Emits partial_recv_metadata (§H2).
func (m *Manager) onInboundBitmap(from peer.ID, ctrl *pb.ControlEnvelope) {
	for _, md := range ctrl.Metadatas {
		slot := int(md.Slot)
		if !m.acceptsSlot(slot) {
			m.logger.Debug("drop stale attprop bitmap", "from", shortPeer(from), "slot", slot)
			continue
		}
		data := md.AttestationData
		var hash []byte
		if len(data) > 0 {
			hash = m.identities.Put(data)
		} else {
			hash = md.AttestationDataHash
			var ok bool
			data, ok = m.identities.Get(hash)
			if !ok {
				m.logger.Error("CRITICAL GOT BITMAP HASH WITHOUT MESSAGE", "from", from)
				return
			}
		}
		ss := m.getOrCreateSlotState(slot)
		b := ss.getOrCreateBucket(data, hash)

		flips := 0
		availableOnes := 0
		for _, id := range md.AvailableIds {
			if int(id) >= m.cfg.CommitteeSize {
				m.logger.Error("CRITICAL: attprop_recv_bad_metadata",
					"from", shortPeer(from),
					"err", errMetadataIDOutOfRange(id, m.cfg.CommitteeSize))
				continue
			}
			availableOnes++
			if ss.markHolder(b, from, int(id)) {
				flips++
			}
		}
		if len(md.Available) > 0 {
			bm := bitmap.Bitmap(md.Available)
			for pos := 0; pos < m.cfg.CommitteeSize; pos++ {
				if bm.Get(pos) {
					availableOnes++
					if ss.markHolder(b, from, pos) {
						flips++
					}
				}
			}
		}
		m.logger.Info("partial_recv_metadata",
			"from", shortPeer(from),
			"slot", slot,
			"topic", m.cfg.TopicIndex,
			"att_digest", digestHex(data, hash),
			"available_ones", availableOnes,
			"requests_ones", 0,
			"holder_flips", flips,
			"available_ids_len", len(md.AvailableIds),
		)
	}
}
