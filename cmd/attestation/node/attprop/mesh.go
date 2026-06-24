package attprop

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// meshRole is a peer's membership in this node's meshes for one topic. A peer
// is Connected (neither mesh), in the Push Mesh, or in the Bitmap Mesh; push and
// bitmap are mutually exclusive (§C5).
type meshRole int

const (
	roleConnected meshRole = iota
	rolePush
	roleBitmap
)

// meshState is the per-topic graft/prune state machine (§C). It tracks each
// peer's role plus a per-mesh prune backoff so a freshly pruned peer is not
// immediately re-grafted (Full = both meshes, §C7). The eventloop owns this
// struct (single-threaded), so the methods take no locks. The mesh-management
// method bodies are filled by the Mesh agent.
type meshState struct {
	cfg Config

	// roles maps a peer to its current role. Absence ⇒ not connected.
	roles map[peer.ID]meshRole

	// backoff maps (peer, mesh) to the wall-clock time before which that peer
	// may not be re-grafted into that mesh. Keyed by pushBackoff/bitmapBackoff
	// helpers; FULL sets both.
	pushBackoff   map[peer.ID]time.Time
	bitmapBackoff map[peer.ID]time.Time
}

// newMeshState initialises an empty per-topic mesh state.
func newMeshState(cfg Config) *meshState {
	return &meshState{
		cfg:           cfg,
		roles:         make(map[peer.ID]meshRole),
		pushBackoff:   make(map[peer.ID]time.Time),
		bitmapBackoff: make(map[peer.ID]time.Time),
	}
}

// role returns the peer's current mesh role (roleConnected if unknown).
// Implemented by the Mesh agent.
func (ms *meshState) role(p peer.ID) meshRole {
	panic("TODO: Mesh — mesh.go")
}

// onGraft handles an inbound Graft for the given mesh and returns the control
// items to reply with (accept = none; redirect or full = prune/graft items per
// §C2/§C3/§C4). Implemented by the Mesh agent.
func (ms *meshState) onGraft(
	p peer.ID,
	mesh pb.AttPropMesh,
	now time.Time,
) (reply []*pb.AttPropControlItem) {
	panic("TODO: Mesh — mesh.go")
}

// onPrune handles an inbound Prune for the given mesh: leave the mesh and arm
// the per-mesh backoff (Full = both). Implemented by the Mesh agent.
func (ms *meshState) onPrune(p peer.ID, mesh pb.AttPropMesh, now time.Time) {
	panic("TODO: Mesh — mesh.go")
}

// heartbeat runs periodic mesh maintenance over the connected-peer candidate
// set: top up push toward target (prefer fresh connected, bitmap-promotion
// fallback — §C6), then bitmap, and prune excess, respecting backoff. It
// returns the graft and prune control items to send per peer. Implemented by
// the Mesh agent.
func (ms *meshState) heartbeat(
	now time.Time,
	candidates []peer.ID,
) (grafts, prunes map[peer.ID][]*pb.AttPropControlItem) {
	panic("TODO: Mesh — mesh.go")
}
