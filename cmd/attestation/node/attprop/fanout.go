package attprop

import (
	"context"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// fanout.go implements fanout (originator) behaviour (§G1): a fanout node opens
// a push-protocol stream to its configured peers, sends its single
// BatchedAttestation (one bit set), and resets any inbound stream a peer opens
// — it is a pure leaf injector with no push/bitmap mesh, no scarcity, no graft.

// installFanoutResetHandlers overrides the inbound stream handlers (registered
// by Start) so a fanout node resets every stream a peer opens to it (§G1).
// SetStreamHandler replaces the prior handler, so this is idempotent and safe to
// call from both Run and fanoutPublish. A fanout node never receives.
func (m *Manager) installFanoutResetHandlers() {
	reset := func(s network.Stream) { m.fanoutResetInbound(s) }
	m.host.SetStreamHandler(PushProtocol(m.cfg.TopicIndex), reset)
	m.host.SetStreamHandler(BitmapProtocol(m.cfg.TopicIndex), reset)
	m.host.SetStreamHandler(ControlProtocol(m.cfg.TopicIndex), reset)
}

// fanoutPublish opens a push stream to each peer the host is connected to and
// sends one BatchedAttestation carrying this node's single attestation (one bit
// set). It is the whole fanout protocol: no mesh, no scarcity, no graft. The
// reset handlers are (re)installed first so any inbound stream is rejected
// (§G1). connectedPeers are the host's current peers — in the sim these are the
// fanout node's configured mesh peers (fanout_node_mesh_peers).
func (m *Manager) fanoutPublish(slot, pos int, sig, data []byte) {
	m.installFanoutResetHandlers()

	batch := &pb.BatchedAttestation{
		AttestationData: data,
		AttestorIndices: []uint32{uint32(pos)},
		Signatures:      [][]byte{sig},
	}
	frame, err := proto.Marshal(&pb.BatchedAttestationEnvelope{
		Batches: []*pb.BatchedAttestation{batch},
	})
	if err != nil {
		m.logger.Error("marshal fanout envelope", "err", err)
		return
	}

	digest := attDigestHex(data)
	for _, p := range m.host.Network().Peers() {
		s, err := m.host.NewStream(context.Background(), p, PushProtocol(m.cfg.TopicIndex))
		if err != nil {
			m.logger.Debug("fanout open push stream", "peer", shortPeer(p), "err", err)
			continue
		}
		if err := writeFrame(msgio.NewVarintWriter(s), frame); err != nil {
			m.logger.Debug("fanout write", "peer", shortPeer(p), "err", err)
			s.Reset()
			continue
		}
		// Half-close so the receiver's reader hits a clean EOF after our one
		// frame; we never send more on this stream.
		_ = s.CloseWrite()
		m.logger.Info("partial_fanout_publish",
			"topic", m.cfg.TopicIndex,
			"slot", slot,
			"att_digest", digest,
			"position", pos,
			"peer", shortPeer(p),
			"batch_bytes", len(frame),
		)
	}
}

// fanoutResetInbound resets an inbound stream opened to a fanout node — it never
// receives (§G1).
func (m *Manager) fanoutResetInbound(s network.Stream) {
	s.Reset()
}
