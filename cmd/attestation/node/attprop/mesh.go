package attprop

import (
	"slices"
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
// struct (single-threaded), so the methods take no locks.
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
func (ms *meshState) role(p peer.ID) meshRole {
	return ms.roles[p]
}

// counts returns the current number of push-mesh and bitmap-mesh peers.
func (ms *meshState) counts() (push, bitmap int) {
	for _, r := range ms.roles {
		switch r {
		case rolePush:
			push++
		case roleBitmap:
			bitmap++
		}
	}
	return push, bitmap
}

// inPushBackoff reports whether the peer is still inside its push backoff at now.
func (ms *meshState) inPushBackoff(p peer.ID, now time.Time) bool {
	return now.Before(ms.pushBackoff[p])
}

// inBitmapBackoff reports whether the peer is still inside its bitmap backoff.
func (ms *meshState) inBitmapBackoff(p peer.ID, now time.Time) bool {
	return now.Before(ms.bitmapBackoff[p])
}

// graftItem builds a Graft control item for the given mesh.
func graftItem(mesh pb.AttPropMesh) *pb.AttPropControlItem {
	return &pb.AttPropControlItem{Op: pb.AttPropMeshOp_GRAFT, Mesh: mesh}
}

// pruneItem builds a Prune control item for the given mesh.
func pruneItem(mesh pb.AttPropMesh) *pb.AttPropControlItem {
	return &pb.AttPropControlItem{Op: pb.AttPropMeshOp_PRUNE, Mesh: mesh}
}

// onGraft handles an inbound Graft for the given mesh and returns the control
// items to reply with. An accept replies nil — the meshes are symmetric (§B2a),
// so the peer assumes success unless it is pruned. A redirect or full reply
// carries the prune/graft items per §C2/§C4.
func (ms *meshState) onGraft(
	p peer.ID,
	mesh pb.AttPropMesh,
	now time.Time,
) (reply []*pb.AttPropControlItem) {
	push, bitmap := ms.counts()
	switch mesh {
	case pb.AttPropMesh_PUSH:
		// Reject a push graft while the peer is in push backoff (§C7).
		if ms.inPushBackoff(p, now) {
			return []*pb.AttPropControlItem{pruneItem(pb.AttPropMesh_PUSH)}
		}
		if push < ms.cfg.PushD {
			ms.roles[p] = rolePush
			return nil
		}
		// Push full: redirect to bitmap if there is room and no bitmap backoff
		// (§C4). Otherwise reject both meshes with Prune:Full.
		if bitmap < ms.cfg.BitmapTarget && !ms.inBitmapBackoff(p, now) {
			ms.roles[p] = roleBitmap
			return []*pb.AttPropControlItem{
				pruneItem(pb.AttPropMesh_PUSH),
				graftItem(pb.AttPropMesh_BITMAP),
			}
		}
		return []*pb.AttPropControlItem{pruneItem(pb.AttPropMesh_FULL)}

	case pb.AttPropMesh_BITMAP:
		if ms.inBitmapBackoff(p, now) {
			return []*pb.AttPropControlItem{pruneItem(pb.AttPropMesh_BITMAP)}
		}
		if bitmap < ms.cfg.BitmapTarget {
			ms.roles[p] = roleBitmap
			return nil
		}
		return []*pb.AttPropControlItem{pruneItem(pb.AttPropMesh_BITMAP)}

	default:
		return nil
	}
}

// onPrune handles an inbound Prune: leave the named mesh, drop back to
// connected, and arm the per-mesh backoff. Prune:Full arms both backoffs (§C7).
func (ms *meshState) onPrune(p peer.ID, mesh pb.AttPropMesh, now time.Time) {
	until := now.Add(ms.cfg.PruneBackoff)
	switch mesh {
	case pb.AttPropMesh_PUSH:
		if ms.roles[p] == rolePush {
			ms.roles[p] = roleConnected
		}
		ms.pushBackoff[p] = until
	case pb.AttPropMesh_BITMAP:
		if ms.roles[p] == roleBitmap {
			ms.roles[p] = roleConnected
		}
		ms.bitmapBackoff[p] = until
	case pb.AttPropMesh_FULL:
		ms.roles[p] = roleConnected
		ms.pushBackoff[p] = until
		ms.bitmapBackoff[p] = until
	}
}

// heartbeat runs periodic mesh maintenance (§C2/§C5/§C6) over the connected-peer
// candidate set: top up push toward PushD (preferring fresh connected peers,
// promoting a bitmap peer only as a fallback), prune push excess, top up bitmap
// toward BitmapTarget, then prune bitmap excess. All grafts respect backoff and
// the two meshes stay mutually exclusive. It returns the graft and prune control
// items to send per peer; the eventloop sends them.
func (ms *meshState) heartbeat(
	now time.Time,
	candidates []peer.ID,
) (grafts, prunes map[peer.ID][]*pb.AttPropControlItem) {
	grafts = make(map[peer.ID][]*pb.AttPropControlItem)
	prunes = make(map[peer.ID][]*pb.AttPropControlItem)

	// Deterministic order so tests are stable (mirrors partial_priority.go).
	sorted := append([]peer.ID(nil), candidates...)
	slices.Sort(sorted)

	push, bitmap := ms.counts()

	addGraft := func(p peer.ID, mesh pb.AttPropMesh) {
		grafts[p] = append(grafts[p], graftItem(mesh))
	}
	addPrune := func(p peer.ID, mesh pb.AttPropMesh) {
		prunes[p] = append(prunes[p], pruneItem(mesh))
	}

	// Push top-up: PushDlow is the trigger, PushD the fill target (§C1). Once
	// triggered, fill to target preferring fresh connected peers (not in push
	// backoff); only if none remain, promote a bitmap peer (which vacates a
	// bitmap slot, §C6).
	if push < ms.cfg.PushDlow {
		for push < ms.cfg.PushD {
			p, ok := ms.pickFreshConnected(sorted, now)
			if ok {
				ms.roles[p] = rolePush
				addGraft(p, pb.AttPropMesh_PUSH)
				push++
				continue
			}
			p, ok = ms.pickBitmapToPromote(sorted, now)
			if !ok {
				break
			}
			ms.roles[p] = rolePush
			addGraft(p, pb.AttPropMesh_PUSH)
			push++
			bitmap-- // promotion vacated a bitmap slot
		}
	}

	// Push trim: prune excess above PushD.
	for push > ms.cfg.PushD {
		p, ok := ms.pickOne(sorted, rolePush)
		if !ok {
			break
		}
		ms.roles[p] = roleConnected
		ms.pushBackoff[p] = now.Add(ms.cfg.PruneBackoff)
		addPrune(p, pb.AttPropMesh_PUSH)
		push--
	}

	// Bitmap top-up: BitmapLow is the trigger, BitmapTarget the fill target.
	// Fresh connected peers only (not in bitmap backoff); bitmap never promotes
	// from push — push is the scarcer mesh.
	if bitmap < ms.cfg.BitmapLow {
		for bitmap < ms.cfg.BitmapTarget {
			p, ok := ms.pickFreshConnectedBitmap(sorted, now)
			if !ok {
				break
			}
			ms.roles[p] = roleBitmap
			addGraft(p, pb.AttPropMesh_BITMAP)
			bitmap++
		}
	}

	// Bitmap trim: prune excess above target.
	for bitmap > ms.cfg.BitmapTarget {
		p, ok := ms.pickOne(sorted, roleBitmap)
		if !ok {
			break
		}
		ms.roles[p] = roleConnected
		ms.bitmapBackoff[p] = now.Add(ms.cfg.PruneBackoff)
		addPrune(p, pb.AttPropMesh_BITMAP)
		bitmap--
	}

	return grafts, prunes
}

// pickFreshConnected returns the first connected (non-mesh) candidate not in
// push backoff, in sorted order, for promotion into the push mesh.
func (ms *meshState) pickFreshConnected(sorted []peer.ID, now time.Time) (peer.ID, bool) {
	for _, p := range sorted {
		if ms.roles[p] == roleConnected && !ms.inPushBackoff(p, now) {
			return p, true
		}
	}
	return "", false
}

// pickFreshConnectedBitmap returns the first connected candidate not in bitmap
// backoff, in sorted order, for grafting into the bitmap mesh.
func (ms *meshState) pickFreshConnectedBitmap(sorted []peer.ID, now time.Time) (peer.ID, bool) {
	for _, p := range sorted {
		if ms.roles[p] == roleConnected && !ms.inBitmapBackoff(p, now) {
			return p, true
		}
	}
	return "", false
}

// pickBitmapToPromote returns the first bitmap-mesh candidate not in push
// backoff, in sorted order — the §C6 fallback when no fresh connected peer is
// available to fill the push mesh.
func (ms *meshState) pickBitmapToPromote(sorted []peer.ID, now time.Time) (peer.ID, bool) {
	for _, p := range sorted {
		if ms.roles[p] == roleBitmap && !ms.inPushBackoff(p, now) {
			return p, true
		}
	}
	return "", false
}

// pickOne returns the first candidate currently in the given role, in sorted
// order, for pruning.
func (ms *meshState) pickOne(sorted []peer.ID, want meshRole) (peer.ID, bool) {
	for _, p := range sorted {
		if ms.roles[p] == want {
			return p, true
		}
	}
	return "", false
}
