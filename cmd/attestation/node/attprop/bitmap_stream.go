package attprop

import (
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// bitmap_stream.go implements the bitmap advertisement stream (§D): full
// available-only bitmaps per active bucket, triggered by a +K count change plus
// a periodic floor (re-emit only if changed), sent to bitmap-mesh peers only
// and bypassing the send budget. Method bodies are filled by the Core agent.

// buildAvailableEnvelope assembles an available-only pb.ControlEnvelope (one
// CommitteeAttestationPartsMetadata per bucket, available set, no requests) for
// a (topic, slot), in bucketSeq order. Returns nil when nothing is validated
// yet (§D1). Implemented by the Core agent.
func (m *Manager) buildAvailableEnvelope(topic string, slot int) *pb.ControlEnvelope {
	panic("TODO: Core — bitmap_stream.go")
}

// emitBitmaps sends the current available bitmap to every bitmap-mesh peer that
// needs it. forced is true on the floor tick (re-emit only if changed since
// last emit); otherwise it is the +K count trigger. Implemented by the Core
// agent.
func (m *Manager) emitBitmaps(forced bool) {
	panic("TODO: Core — bitmap_stream.go")
}

// onInboundBitmap folds a peer's advertised available bitmap into our state:
// peerAvail.Or, then bump holder-count for every position whose bit newly
// flipped 0→1 (§E1). Implemented by the Core agent.
func (m *Manager) onInboundBitmap(from peer.ID, ctrl *pb.ControlEnvelope) {
	panic("TODO: Core — bitmap_stream.go")
}
