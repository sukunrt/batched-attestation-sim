// Package attprop implements the att_propagation attestation-broadcast mode: a
// native libp2p protocol (no gossipsub) with three persistent bidirectional
// per-topic streams between peers: push (data forwarding), bitmap
// (validated-bitmap advertisement), and control (graft/prune mesh management). Each peer is
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
	"sync"
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

func (m *Manager) peerSupports(p peer.ID, protocols ...protocol.ID) (bool, error) {
	supported, err := m.host.Peerstore().SupportsProtocols(p, protocols...)
	if err != nil {
		return false, err
	}
	return len(supported) == len(protocols), nil
}

// Config carries every tunable the att_propagation Manager needs. Field
// defaults come from the spec sections noted inline; the node/plumbing layer
// fills them from the run config.
type Config struct {
	Logger        *slog.Logger
	NodeNum       int
	Topic         string    // topic name managed by this Manager
	TopicIndex    int       // stable topic ID used for protocol IDs and log tagging
	CommitteeSize int       // wire-level bitmap capacity (= num_attestors per topic)
	PublishStart  time.Time // wall-clock start of slot 1 (for latency logging)
	SlotDuration  time.Duration
	Fanout        bool // fanout (originator) node: leaf injector only (§G1)

	// §C1 mesh sizes. push Dlow/D/Dhigh = 4/5/5, bitmap low/target/high =
	// 14/16/16 (D == Dhigh: hard cap, Dlow is the top-up trigger).
	PushDlow, PushD, PushDhigh       int
	BitmapDlow, BitmapD, BitmapDhigh int

	// §F1 per-topic SendBudgetB (default 4), §F3 MaxAttsPerMessage N (default 30),
	// and MaxPeersPerAtt as the initial holder-count index capacity.
	SendBudgetB, MaxAttsPerMessage, MaxPeersPerAtt int

	// §F4 TickInterval (20ms), §D2 BitmapFloorInterval (50ms),
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

// Manager owns all att_propagation state for one node on one topic. A single
// eventloop goroutine (Run) owns the mutable state; readers and senders
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

	// events is the single ingress for the eventloop: readers, senders, timer
	// drivers, and the verifier callback all post here; the eventloop is the
	// sole consumer.
	events chan event

	// senders holds the per-peer data sender (push stream). One in-flight data
	// frame per peer; the eventloop hands work off and waits
	// for sendDoneEvent.
	senders map[peer.ID]*peerSender

	// bitmapWriters / controlWriters hold the per-peer bitmap and control stream
	// writers. These bypass the send budget (§F1) — advertisements and mesh
	// management never block behind a data write. Bitmap writers replace stale
	// queued advertisements instead of dropping the newest state.
	bitmapWriters  map[peer.ID]*bitmapWriter
	controlWriters map[peer.ID]*peerSender

	// mesh tracks peer push/bitmap roles and prune backoff (§C).
	mesh *meshState

	// slots holds per-slot state with the holder-count scarcity index (§E).
	slots      map[int]*slotState
	identities attestationIdentityCache
	sentFull   map[peer.ID]map[string]struct{}

	// activeData counts in-flight data sends gated by the budget B. Push sends
	// are exempt (§F1) but still tracked here for observability.
	activeData int

	// sendAllToPushMesh is eventloop-local tick state (§F4): true only during a
	// tickEvent's selection pass so partial push batches flush on the tick; false
	// otherwise. Never observed across a channel hop.
	sendAllToPushMesh bool

	// streamsMu guards stream setup state, which is intentionally outside the
	// eventloop because NewStream and inbound handlers run on libp2p goroutines.
	streamsMu sync.Mutex
	opening   map[peer.ID]struct{}
	pending   map[peer.ID]*peerStreams
}

// New constructs an att_propagation Manager. The node layer wires in the host,
// this topic's verifier, a receive tracer, and the resolved Config.
func New(h host.Host, v *verify.Verifier, tr Tracer, cfg Config) *Manager {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.With("node", cfg.NodeNum, "component", "attprop")
	}
	if cfg.MaxMsgSize <= 0 {
		cfg.MaxMsgSize = defaultMaxMsgSize
	}
	return &Manager{
		host:           h,
		verifier:       v,
		tracer:         tr,
		cfg:            cfg,
		logger:         logger,
		self:           h.ID(),
		events:         make(chan event, 256),
		senders:        make(map[peer.ID]*peerSender),
		bitmapWriters:  make(map[peer.ID]*bitmapWriter),
		controlWriters: make(map[peer.ID]*peerSender),
		mesh:           newMeshState(cfg),
		slots:          make(map[int]*slotState),
		identities:     newAttestationIdentityCache(),
		sentFull:       make(map[peer.ID]map[string]struct{}),
		opening:        make(map[peer.ID]struct{}),
		pending:        make(map[peer.ID]*peerStreams),
	}
}

// markSendStreamsOpening claims the right to open the peer's bidirectional
// stream set, returning false if another goroutine already claimed it. Cleared
// by clearSendStreamsOpening on a failed open so a retry can proceed.
func (m *Manager) markSendStreamsOpening(p peer.ID) bool {
	m.streamsMu.Lock()
	defer m.streamsMu.Unlock()
	if _, ok := m.opening[p]; ok {
		return false
	}
	m.opening[p] = struct{}{}
	return true
}

// clearSendStreamsOpening releases stream setup state so a reconnect can retry.
func (m *Manager) clearSendStreamsOpening(p peer.ID) {
	m.streamsMu.Lock()
	delete(m.opening, p)
	delete(m.pending, p)
	m.streamsMu.Unlock()
}

// Start registers the three per-topic stream handlers on the host so inbound
// bidirectional streams are accepted. Implemented in wire.go.
func (m *Manager) Start(ctx context.Context) {
	m.start(ctx)
}

// ConnectPeer is called after the host has connected to a peer. The caller's
// side opens the three bidirectional streams and emits a peerUpEvent; the peer
// accepts those same streams via the handlers registered by Start. Implemented
// in wire.go.
func (m *Manager) ConnectPeer(p peer.ID) {
	m.connectPeer(p)
}

// Run launches the eventloop and the tick/floor/heartbeat timer drivers, then
// blocks until ctx is cancelled (§F4). A fanout node has no eventloop — it only
// installs the reset handlers (it never receives) and returns once ctx is done.
func (m *Manager) Run(ctx context.Context) {
	if m.cfg.Fanout {
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
func (m *Manager) PublishLocal(slot, pos int, sig, data []byte) {
	m.post(publishLocalEvent{slot: slot, pos: pos, sig: sig, data: data})
}

// FanoutPublish injects a fanout (originator) node's single attestation: opens
// a push stream to each configured peer and sends one BatchedAttestation.
// Implemented in fanout.go.
func (m *Manager) FanoutPublish(slot, pos int, sig, data []byte) {
	m.fanoutPublish(slot, pos, sig, data)
}
