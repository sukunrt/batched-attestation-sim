package attprop

import (
	"context"

	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// fanout.go implements fanout (originator) behaviour (§G1): a fanout node opens
// a push-protocol stream to its connected mesh peers and sends its single
// BatchedAttestation (one bit set). It does not install inbound attprop stream
// handlers, so mesh nodes can identify it as not supporting push/bitmap/control.

// fanoutPublish opens a push stream to each peer the host is connected to and
// sends one BatchedAttestation carrying this node's single attestation (one bit
// set). It is the whole fanout protocol: no mesh, no scarcity, no graft.
// connectedPeers are the host's current peers — in the sim these are the fanout
// node's configured mesh peers (fanout_node_mesh_peers).
func (m *Manager) fanoutPublish(slot, pos int, sig, data []byte) {
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
	pushProto := PushProtocol(m.cfg.TopicIndex)
	for _, p := range m.host.Network().Peers() {
		supports, err := m.peerSupports(p, pushProto)
		if err != nil || !supports {
			m.logger.Error("CRITICAL: fanout peer lacks attprop push protocol",
				"topic", m.cfg.TopicIndex,
				"peer", shortPeer(p),
				"err", err,
			)
			continue
		}
		s, err := m.host.NewStream(context.Background(), p, pushProto)
		if err != nil {
			m.logger.Error("CRITICAL: fanout open push stream",
				"topic", m.cfg.TopicIndex,
				"peer", shortPeer(p),
				"err", err,
			)
			continue
		}
		if err := m.writeFrameTimed(msgio.NewVarintWriter(s), frame, p, "fanout_push"); err != nil {
			m.logger.Error("CRITICAL: fanout write",
				"topic", m.cfg.TopicIndex,
				"peer", shortPeer(p),
				"err", err,
			)
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
