// Package attprop implements the att_propagation attestation-broadcast mode: a
// native libp2p protocol (no gossipsub) with three persistent per-topic streams
// between peers — push (data forwarding), bitmap (validated-bitmap
// advertisement), and control (graft/prune mesh management). Each peer is
// Connected, in the Push Mesh, or in the Bitmap Mesh. The push mesh is roughly
// half of gossipsub's D and pushes all data blindly; the cheap bitmap mesh
// learns who holds what so spare upload can push the scarcest attestations to
// peers that lack them, through a flow-controlled send queue. The design is
// specified in ideas/new-algo-spec.md (sections A–H).
//
// This package is driven by package node: node constructs a Manager and passes
// in its dependencies (host, verifier, tracer), so attprop does NOT import node
// — that would be an import cycle. It reuses the pb wire types, the bitmap
// package, and the verify package.
package attprop

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/ethp2p/simlab/cmd/attestation/verify"
)

// defaultMaxMsgSize is the framing cap for an inbound msgio frame when
// Config.MaxMsgSize is unset. Generous: a full N=30 batch of ~96-byte
// signatures plus a 2000-bit bitmap is a few KB; 1 MiB leaves ample headroom.
const defaultMaxMsgSize = 1 << 20

// PushProtocol is the per-topic data-forwarding stream protocol ID.
func PushProtocol(topicID int) protocol.ID {
	return protocol.ID(fmt.Sprintf("attestation_push_%d", topicID))
}

// BitmapProtocol is the per-topic validated-bitmap advertisement stream
// protocol ID.
func BitmapProtocol(topicID int) protocol.ID {
	return protocol.ID(fmt.Sprintf("attestation_bitmap_%d", topicID))
}

// ControlProtocol is the per-topic graft/prune mesh-management stream protocol
// ID.
func ControlProtocol(topicID int) protocol.ID {
	return protocol.ID(fmt.Sprintf("attestation_control_%d", topicID))
}

// weOpen reports whether self should be the side that opens streams to other.
// The lower peer ID opens (matching the libp2p convention used elsewhere),
// resolving the symmetric "who opens" question deterministically.
func weOpen(self, other peer.ID) bool {
	return self < other
}

// Config carries every tunable the att_propagation Manager needs. Field
// defaults come from the spec sections noted inline; the node/plumbing layer
// fills them from the run config.
type Config struct {
	Logger        *slog.Logger
	NodeNum       int
	Topics        []string  // topic names, indexed by topic ID
	CommitteeSize int       // wire-level bitmap capacity (= num_attestors per topic)
	PublishStart  time.Time // wall-clock start of slot 1 (for latency logging)
	SlotDuration  time.Duration
	Fanout        bool // fanout (originator) node: leaf injector only (§G1)

	// §C1 mesh sizes. push Dlow/D/Dhigh = 4/5/5, bitmap low/target/high =
	// 14/16/16 (D == Dhigh: hard cap, Dlow is the top-up trigger).
	PushDlow, PushD, PushDhigh          int
	BitmapLow, BitmapTarget, BitmapHigh int

	// §F1 SendBudgetB (default 4), §F3 MaxAttsPerMessage N (default 30),
	// §E3 MaxPeersPerAtt lifetime ceiling.
	SendBudgetB, MaxAttsPerMessage, MaxPeersPerAtt int

	// §F4 TickInterval (20ms), §D2 BitmapFloorInterval (~100ms),
	// §C2 HeartbeatInterval (~700ms), §C7 PruneBackoff (60s).
	TickInterval, BitmapFloorInterval, HeartbeatInterval, PruneBackoff time.Duration

	// MaxMsgSize caps a single inbound msgio frame. Zero ⇒ defaultMaxMsgSize.
	MaxMsgSize int
}

// Tracer is the receive-latency sink. It is satisfied structurally by node's
// existing tracer (the OnPartialReceive method), so log/wire keys and analysis
// parsing are reused unchanged (§H2).
type Tracer interface {
	OnPartialReceive(slot, topicIdx, position int, attData []byte, latencyMs int64)
}

// Manager owns all att_propagation state for one node across all its topics. A
// single eventloop goroutine (Run) owns the mutable state; readers and senders
// own none and communicate via the events channel. Dropping gossipsub means no
// external caller races in, so the manager is effectively single-threaded —
// this is what keeps it deterministic under synctest (see the eventloop design
// in the plan).
type Manager struct {
	host     host.Host
	verifier *verify.Verifier
	tracer   Tracer
	cfg      Config
	logger   *slog.Logger

	// self is the local peer ID, cached for weOpen comparisons.
	self peer.ID

	// topicIndex maps a topic name to its stable index (used for protocol IDs
	// and log tagging).
	topicIndex map[string]int

	// events is the single ingress for the eventloop: readers, senders, timer
	// drivers, and the verifier callback all post here; the eventloop is the
	// sole consumer.
	events chan event

	// senders holds the per-peer data sender (push stream). One in-flight data
	// frame per peer; the eventloop hands work off and waits for sendDoneEvent.
	senders map[peer.ID]*peerSender

	// bitmapWriters / controlWriters hold the per-peer bitmap and control stream
	// writers. These bypass the send budget (§F1) — advertisements and mesh
	// management never block behind a data write.
	bitmapWriters  map[peer.ID]*peerSender
	controlWriters map[peer.ID]*peerSender

	// mesh tracks per-peer push/bitmap role and per-mesh prune backoff (§C).
	mesh *meshState

	// slots holds per-(topic, slot) state with the holder-count scarcity index
	// (§E). Keyed topic name -> slot -> slotState.
	slots map[string]map[int]*slotState

	// activeData counts in-flight data sends gated by the budget B. Push sends
	// are exempt (§F1) but still tracked here for observability.
	activeData int

	// sendAllToPushMesh is eventloop-local tick state (§F4): true only during a
	// tickEvent's selection pass so partial push batches flush on the tick;
	// false otherwise. Never observed across a channel hop.
	sendAllToPushMesh bool
}

// New constructs an att_propagation Manager. The node layer wires in the host,
// the shared verifier, a receive tracer, and the resolved Config.
func New(h host.Host, v *verify.Verifier, tr Tracer, cfg Config) *Manager {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.With("node", cfg.NodeNum, "component", "attprop")
	}
	if cfg.MaxMsgSize <= 0 {
		cfg.MaxMsgSize = defaultMaxMsgSize
	}
	topicIndex := make(map[string]int, len(cfg.Topics))
	for i, name := range cfg.Topics {
		topicIndex[name] = i
	}
	return &Manager{
		host:           h,
		verifier:       v,
		tracer:         tr,
		cfg:            cfg,
		logger:         logger,
		self:           h.ID(),
		topicIndex:     topicIndex,
		events:         make(chan event, 256),
		senders:        make(map[peer.ID]*peerSender),
		bitmapWriters:  make(map[peer.ID]*peerSender),
		controlWriters: make(map[peer.ID]*peerSender),
		mesh:           newMeshState(cfg),
		slots:          make(map[string]map[int]*slotState),
	}
}

// Start registers the three per-topic stream handlers on the host so inbound
// streams from the higher-peerID side are accepted. Implemented in wire.go.
func (m *Manager) Start(ctx context.Context) {
	m.start(ctx)
}

// ConnectPeer is called after the host has connected to a peer. If we are the
// opener (lower peer ID) it opens the three streams and emits a peerUpEvent;
// otherwise it relies on the inbound handler registered by Start. Implemented
// in wire.go.
func (m *Manager) ConnectPeer(p peer.ID) {
	m.connectPeer(p)
}

// Run launches the eventloop and the tick/floor/heartbeat timer drivers, then
// blocks until ctx is cancelled (§F4). A fanout node has no eventloop — it only
// installs the reset handlers (it never receives) and returns once ctx is done.
func (m *Manager) Run(ctx context.Context) {
	if m.cfg.Fanout {
		m.installFanoutResetHandlers()
		<-ctx.Done()
		return
	}
	go m.driveTimer(ctx, m.cfg.TickInterval, func() event { return tickEvent{} })
	go m.driveTimer(ctx, m.cfg.BitmapFloorInterval, func() event { return bitmapFloorEvent{} })
	go m.driveTimer(ctx, m.cfg.HeartbeatInterval, func() event { return heartbeatEvent{} })
	m.run(ctx)
}

// PublishLocal injects this mesh node's own attestation into local state as
// validated. It posts a publishLocalEvent so the injection happens on the
// eventloop goroutine, preserving single-owner discipline (§F4).
func (m *Manager) PublishLocal(topic string, slot, pos int, sig, data []byte) {
	m.post(publishLocalEvent{topic: topic, slot: slot, pos: pos, sig: sig, data: data})
}

// FanoutPublish injects a fanout (originator) node's single attestation: opens
// a push stream to each configured peer and sends one BatchedAttestation, then
// resets any inbound stream (§G1). Implemented in fanout.go.
func (m *Manager) FanoutPublish(topic string, slot, pos int, sig, data []byte) {
	m.fanoutPublish(topic, slot, pos, sig, data)
}

// topicIdxOf resolves a topic name to its stable index, or -1 if unknown.
func (m *Manager) topicIdxOf(topic string) int {
	if i, ok := m.topicIndex[topic]; ok {
		return i
	}
	return -1
}
