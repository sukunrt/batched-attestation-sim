package node

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// Tracer receives app-level publish/receive events for the simulation. The
// Node calls these on each attestation it publishes or receives so callers can
// record latency, build counters, etc.
//
// Classic-mode methods (OnPublish/OnReceive) consume a full pb.Attestation.
// Partial-mode methods consume just the per-position lifecycle and the
// AttestationData digest used to correlate logs across the publish → forward
// → receive → validate pipeline.
type Tracer interface {
	OnPublish(att *pb.Attestation, topicIndex int)
	OnReceive(nodeNum int, att *pb.Attestation, topicIndex int)

	// OnPartialPublish is called when this node self-publishes an
	// attestation in partial-messages mode. position is the committee
	// position; attData is the raw attestation_data (the tracer derives any
	// digest it needs for correlation).
	OnPartialPublish(slot, topicIndex, position int, attData []byte)

	// OnPartialReceive is called for each newly-received committee position
	// in partial-messages mode (one call per position, not per batch).
	// latencyMs is the wall-clock latency since the slot's nominal start.
	OnPartialReceive(slot, topicIndex, position int, attData []byte, latencyMs int64)
}

// SlogTracer logs publish/receive events at info level via slog. It is the
// production Tracer used by the cmd binary.
type SlogTracer struct {
	logger *slog.Logger
}

func NewSlogTracer(nodeNum int) *SlogTracer {
	return &SlogTracer{logger: slog.With("node", nodeNum)}
}

func (s *SlogTracer) OnPublish(att *pb.Attestation, topicIndex int) {
	s.logger.Info("published", "slot", att.SlotNum, "committee_index", topicIndex, "msg_index", att.MsgIndex)
}

func (s *SlogTracer) OnReceive(nodeNum int, att *pb.Attestation, topicIndex int) {
	latency := time.Now().UnixMilli() - att.ExpectedPublishAtUnixMs
	s.logger.Info("received", "from", att.NodeNum, "slot", att.SlotNum, "committee_index", topicIndex, "msg_index", att.MsgIndex, "latency_ms", latency)
}

func (s *SlogTracer) OnPartialPublish(slot, topicIndex, position int, attData []byte) {
	s.logger.Info("partial_published",
		"slot", slot,
		"committee_index", topicIndex,
		"position", position,
		"att_digest", attDigestHex(attData),
	)
}

func (s *SlogTracer) OnPartialReceive(slot, topicIndex, position int, attData []byte, latencyMs int64) {
	s.logger.Info("partial_received",
		"slot", slot,
		"committee_index", topicIndex,
		"position", position,
		"att_digest", attDigestHex(attData),
		"latency_ms", latencyMs,
	)
}

// attDigest returns the 8-byte SHA-256 prefix of attestation_data. Used as a
// compact correlation token in tracer events and logs.
func attDigest(data []byte) [8]byte {
	sum := sha256.Sum256(data)
	var out [8]byte
	copy(out[:], sum[:8])
	return out
}

// attDigestHex returns the 16-char hex prefix of attestation_data's SHA-256.
// Cheaper-to-read than the full 32-byte hash; collisions are vanishingly rare
// in a single simulation run.
func attDigestHex(data []byte) string {
	d := attDigest(data)
	return hex.EncodeToString(d[:])
}
