package attprop

import (
	"github.com/libp2p/go-libp2p/core/network"
)

// fanout.go implements fanout (originator) behaviour (§G1): a fanout node opens
// a push-protocol stream to its configured peers, sends its single
// BatchedAttestation (one bit set), and resets any inbound stream a peer opens
// — it is a pure leaf injector with no push/bitmap mesh, no scarcity, no graft.
// Method bodies are filled by the Core agent.

// fanoutPublish opens a push stream to each configured peer and sends one
// BatchedAttestation carrying this node's single attestation. Implemented by
// the Core agent.
func (m *Manager) fanoutPublish(topic string, slot, pos int, sig, data []byte) {
	panic("TODO: Core — fanout.go")
}

// fanoutResetInbound resets an inbound stream opened to a fanout node (it never
// receives). Implemented by the Core agent.
func (m *Manager) fanoutResetInbound(s network.Stream) {
	panic("TODO: Core — fanout.go")
}
