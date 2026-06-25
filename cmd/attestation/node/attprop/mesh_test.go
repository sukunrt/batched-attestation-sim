package attprop

import (
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// testCfg returns a Config with the §C1 mesh sizes (push 4/5/5, bitmap
// 14/16/16) and a 60s prune backoff (§C7).
func testCfg() Config {
	return Config{
		PushDlow: 4, PushD: 5, PushDhigh: 5,
		BitmapDlow: 14, BitmapD: 16, BitmapDhigh: 16,
		PruneBackoff: 60 * time.Second,
	}
}

func testCfgWithSlack() Config {
	c := testCfg()
	c.PushDhigh = 7
	c.BitmapDhigh = 18
	return c
}

// pid builds a deterministic test peer ID. Lower index sorts first, matching
// the deterministic selection order the state machine uses.
func pid(i int) peer.ID { return peer.ID(fmt.Sprintf("p%02d", i)) }

// seedRole forces a peer into a role without going through graft (test setup).
func seedRole(ms *meshState, p peer.ID, r meshRole) { ms.roles[p] = r }

func ops(items []*pb.AttPropControlItem) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.GetOp().String() + ":" + it.GetMesh().String()
	}
	return out
}

func TestOnGraftPush(t *testing.T) {
	now := time.Unix(1000, 0)
	tests := []struct {
		name       string
		push, bmap int // pre-seeded mesh sizes
		wantReply  []string
		wantRole   meshRole
	}{
		{"accept when push has room", 0, 0, nil, rolePush},
		{"accept at PushD-1", 4, 0, nil, rolePush},
		{"redirect to bitmap when push full", 5, 0,
			[]string{"PRUNE:PUSH", "GRAFT:BITMAP"}, roleBitmap},
		{"prune full when both full", 5, 16, []string{"PRUNE:FULL"}, roleConnected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := newMeshState(testCfg())
			for i := 0; i < tt.push; i++ {
				seedRole(ms, pid(100+i), rolePush)
			}
			for i := 0; i < tt.bmap; i++ {
				seedRole(ms, pid(200+i), roleBitmap)
			}
			p := pid(1)
			reply := ms.onGraft(p, pb.AttPropMesh_PUSH, now)
			assert.Equal(t, tt.wantReply, ops(reply))
			assert.Equal(t, tt.wantRole, ms.role(p))
		})
	}
}

// TestOnGraftPushSequence accepts grafts until push is full, then redirects, then
// rejects with Full once both meshes are full.
func TestOnGraftPushSequence(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())

	// First PushD grafts are accepted into push.
	for i := range 5 {
		reply := ms.onGraft(pid(i), pb.AttPropMesh_PUSH, now)
		require.Nil(t, reply, "graft %d should accept", i)
		require.Equal(t, rolePush, ms.role(pid(i)))
	}
	push, _ := ms.counts()
	require.Equal(t, 5, push)

	// Next pushes redirect to bitmap until bitmap is full (16 slots).
	for i := 5; i < 5+16; i++ {
		reply := ms.onGraft(pid(i), pb.AttPropMesh_PUSH, now)
		require.Equal(t, []string{"PRUNE:PUSH", "GRAFT:BITMAP"}, ops(reply))
		require.Equal(t, roleBitmap, ms.role(pid(i)))
	}
	_, bmap := ms.counts()
	require.Equal(t, 16, bmap)

	// Now both full ⇒ Prune:Full, role stays connected.
	reply := ms.onGraft(pid(100), pb.AttPropMesh_PUSH, now)
	require.Equal(t, []string{"PRUNE:FULL"}, ops(reply))
	require.Equal(t, roleConnected, ms.role(pid(100)))
}

// TestOnGraftBitmap accepts bitmap grafts until full, then rejects with
// Prune:Bitmap.
func TestOnGraftBitmap(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())

	for i := range 16 {
		reply := ms.onGraft(pid(i), pb.AttPropMesh_BITMAP, now)
		require.Nil(t, reply, "bitmap graft %d should accept", i)
		require.Equal(t, roleBitmap, ms.role(pid(i)))
	}
	reply := ms.onGraft(pid(100), pb.AttPropMesh_BITMAP, now)
	require.Equal(t, []string{"PRUNE:BITMAP"}, ops(reply))
	require.Equal(t, roleConnected, ms.role(pid(100)))
}

// TestOnPruneArmsBackoff verifies prune drops to connected and arms the right
// outbound backoff. Inbound grafts ignore that local backoff and are admitted by
// normal capacity checks.
func TestOnPruneArmsBackoff(t *testing.T) {
	now := time.Unix(1000, 0)

	t.Run("prune push arms push backoff only", func(t *testing.T) {
		ms := newMeshState(testCfg())
		p := pid(1)
		seedRole(ms, p, rolePush)
		ms.onPrune(p, pb.AttPropMesh_PUSH, now)
		assert.Equal(t, roleConnected, ms.role(p))
		assert.True(t, ms.inPushBackoff(p, now))
		assert.False(t, ms.inBitmapBackoff(p, now))

		// Graft during backoff is still accepted if we have room.
		reply := ms.onGraft(p, pb.AttPropMesh_PUSH, now.Add(30*time.Second))
		assert.Nil(t, reply)
		assert.Equal(t, rolePush, ms.role(p))
	})

	t.Run("prune bitmap arms bitmap backoff only", func(t *testing.T) {
		ms := newMeshState(testCfg())
		p := pid(1)
		seedRole(ms, p, roleBitmap)
		ms.onPrune(p, pb.AttPropMesh_BITMAP, now)
		assert.Equal(t, roleConnected, ms.role(p))
		assert.True(t, ms.inBitmapBackoff(p, now))
		assert.False(t, ms.inPushBackoff(p, now))

		reply := ms.onGraft(p, pb.AttPropMesh_BITMAP, now.Add(30*time.Second))
		assert.Nil(t, reply)
		assert.Equal(t, roleBitmap, ms.role(p))
	})

	t.Run("prune full arms both backoffs", func(t *testing.T) {
		ms := newMeshState(testCfg())
		p := pid(1)
		seedRole(ms, p, rolePush)
		ms.onPrune(p, pb.AttPropMesh_FULL, now)
		assert.Equal(t, roleConnected, ms.role(p))
		assert.True(t, ms.inPushBackoff(p, now))
		assert.True(t, ms.inBitmapBackoff(p, now))

		// Both push and bitmap grafts can still be accepted during the window.
		assert.Nil(t, ms.onGraft(p, pb.AttPropMesh_PUSH, now.Add(time.Second)))
		ms.roles[p] = roleConnected
		assert.Nil(t, ms.onGraft(p, pb.AttPropMesh_BITMAP, now.Add(time.Second)))
	})
}

// TestOnGraftPushRedirectIgnoresBitmapBackoff: inbound graft handling ignores
// local bitmap backoff and redirects when bitmap has room.
func TestOnGraftPushRedirectIgnoresBitmapBackoff(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())
	for i := range 5 {
		seedRole(ms, pid(100+i), rolePush)
	}
	p := pid(1)
	ms.bitmapBackoff[p] = now.Add(30 * time.Second)
	reply := ms.onGraft(p, pb.AttPropMesh_PUSH, now)
	assert.Equal(t, []string{"PRUNE:PUSH", "GRAFT:BITMAP"}, ops(reply))
	assert.Equal(t, roleBitmap, ms.role(p))
}

func TestOnGraftUsesDhighSlack(t *testing.T) {
	now := time.Unix(1000, 0)

	t.Run("push admits up to Dhigh", func(t *testing.T) {
		ms := newMeshState(testCfgWithSlack())
		for i := range ms.cfg.PushD {
			seedRole(ms, pid(100+i), rolePush)
		}
		for i := range ms.cfg.PushDhigh - ms.cfg.PushD {
			reply := ms.onGraft(pid(i), pb.AttPropMesh_PUSH, now)
			require.Nil(t, reply)
			require.Equal(t, rolePush, ms.role(pid(i)))
		}
		reply := ms.onGraft(pid(99), pb.AttPropMesh_PUSH, now)
		require.Equal(t, []string{"PRUNE:PUSH", "GRAFT:BITMAP"}, ops(reply))
		require.Equal(t, roleBitmap, ms.role(pid(99)))
	})

	t.Run("bitmap admits up to Dhigh", func(t *testing.T) {
		ms := newMeshState(testCfgWithSlack())
		for i := range ms.cfg.BitmapD {
			seedRole(ms, pid(100+i), roleBitmap)
		}
		for i := range ms.cfg.BitmapDhigh - ms.cfg.BitmapD {
			reply := ms.onGraft(pid(i), pb.AttPropMesh_BITMAP, now)
			require.Nil(t, reply)
			require.Equal(t, roleBitmap, ms.role(pid(i)))
		}
		reply := ms.onGraft(pid(99), pb.AttPropMesh_BITMAP, now)
		require.Equal(t, []string{"PRUNE:BITMAP"}, ops(reply))
		require.Equal(t, roleConnected, ms.role(pid(99)))
	})
}

// TestHeartbeatTopUpPrefersFreshConnected: push below Dlow tops up to PushD using
// fresh connected peers in preference to promoting a bitmap peer.
func TestHeartbeatTopUpPrefersFreshConnected(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())

	// push=2 (needs 3 to reach PushD=5). bitmap=16 (at target, so bitmap top-up
	// does NOT fire) and includes pid(60). Exactly 3 fresh connected peers exist
	// — just enough for push — so the only promotion candidate competing is the
	// bitmap peer, which must NOT be chosen while fresh peers remain (§C6).
	seedRole(ms, pid(50), rolePush)
	seedRole(ms, pid(51), rolePush)
	var candidates []peer.ID
	for i := range 16 { // 16 bitmap peers ⇒ bitmap at target
		seedRole(ms, pid(60+i), roleBitmap)
		candidates = append(candidates, pid(60+i))
	}
	for i := range 3 { // 3 fresh connected peers
		candidates = append(candidates, pid(i))
	}
	candidates = append(candidates, pid(50), pid(51))

	grafts, prunes := ms.heartbeat(now, candidates)
	assert.Empty(t, prunes)

	push, bmap := ms.counts()
	assert.Equal(t, 5, push)
	assert.Equal(t, 16, bmap, "no bitmap peer promoted while fresh peers exist")
	for _, p := range []peer.ID{pid(0), pid(1), pid(2)} {
		assert.Equal(t, []string{"GRAFT:PUSH"}, ops(grafts[p]))
		assert.Equal(t, rolePush, ms.role(p))
	}
	for i := range 16 {
		assert.Equal(t, roleBitmap, ms.role(pid(60+i)), "bitmap peer untouched")
	}
}

// TestHeartbeatPromotesBitmapFallback: with no fresh connected peers, push top-up
// promotes a bitmap peer (vacating a bitmap slot, §C6).
func TestHeartbeatPromotesBitmapFallback(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())

	// push=3 (needs 2 more), bitmap=16 (at target), no fresh connected peers.
	for i := range 3 {
		seedRole(ms, pid(50+i), rolePush)
	}
	var candidates []peer.ID
	for i := range 16 {
		seedRole(ms, pid(i), roleBitmap)
		candidates = append(candidates, pid(i))
	}
	candidates = append(candidates, pid(50), pid(51), pid(52))

	grafts, _ := ms.heartbeat(now, candidates)

	push, _ := ms.counts()
	assert.Equal(t, 5, push, "push topped up via bitmap promotion")
	// Two lowest bitmap peers promoted to push.
	for _, p := range []peer.ID{pid(0), pid(1)} {
		assert.Equal(t, rolePush, ms.role(p))
		assert.Equal(t, []string{"GRAFT:PUSH"}, ops(grafts[p]))
	}
	// Promotion vacated 2 bitmap slots (16 ⇒ 14). 14 is exactly BitmapDlow, not
	// below it, so bitmap top-up does not re-trigger and it stays at 14 — no
	// refill churn, and never over target.
	_, bmap := ms.counts()
	assert.Equal(t, 14, bmap)
}

// TestHeartbeatGrowsBitmap: bitmap below BitmapDlow grows toward BitmapD with
// fresh connected peers.
func TestHeartbeatGrowsBitmap(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())

	// push at target so push logic is a no-op; bitmap=10 (< Low=14).
	for i := range 5 {
		seedRole(ms, pid(50+i), rolePush)
	}
	for i := range 10 {
		seedRole(ms, pid(100+i), roleBitmap)
	}
	var candidates []peer.ID
	for i := range 10 { // 10 fresh connected peers
		candidates = append(candidates, pid(i))
	}
	for i := range 5 {
		candidates = append(candidates, pid(50+i))
	}
	for i := range 10 {
		candidates = append(candidates, pid(100+i))
	}

	grafts, prunes := ms.heartbeat(now, candidates)
	assert.Empty(t, prunes)
	push, bmap := ms.counts()
	assert.Equal(t, 5, push)
	assert.Equal(t, 16, bmap, "bitmap grown to target")
	// 6 fresh peers grafted to bitmap (lowest IDs).
	for i := range 6 {
		assert.Equal(t, []string{"GRAFT:BITMAP"}, ops(grafts[pid(i)]))
	}
}

// TestHeartbeatTrimsExcess: push and bitmap above their caps are pruned.
func TestHeartbeatTrimsExcess(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())

	for i := range 7 { // push=7 (> PushD=5)
		seedRole(ms, pid(i), rolePush)
	}
	for i := range 18 { // bitmap=18 (> BitmapD=16)
		seedRole(ms, pid(100+i), roleBitmap)
	}
	var candidates []peer.ID
	for i := range 7 {
		candidates = append(candidates, pid(i))
	}
	for i := range 18 {
		candidates = append(candidates, pid(100+i))
	}

	grafts, prunes := ms.heartbeat(now, candidates)
	assert.Empty(t, grafts)
	push, bmap := ms.counts()
	assert.Equal(t, 5, push)
	assert.Equal(t, 16, bmap)

	// 2 push pruned (lowest IDs) and armed with backoff.
	for _, p := range []peer.ID{pid(0), pid(1)} {
		assert.Equal(t, []string{"PRUNE:PUSH"}, ops(prunes[p]))
		assert.Equal(t, roleConnected, ms.role(p))
		assert.True(t, ms.inPushBackoff(p, now))
	}
	// 2 bitmap pruned.
	for _, p := range []peer.ID{pid(100), pid(101)} {
		assert.Equal(t, []string{"PRUNE:BITMAP"}, ops(prunes[p]))
		assert.True(t, ms.inBitmapBackoff(p, now))
	}
}

func TestHeartbeatTrimsOnlyAboveDhigh(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfgWithSlack())

	for i := range 8 { // push=8 (> PushDhigh=7)
		seedRole(ms, pid(i), rolePush)
	}
	for i := range 19 { // bitmap=19 (> BitmapDhigh=18)
		seedRole(ms, pid(100+i), roleBitmap)
	}
	var candidates []peer.ID
	for i := range 8 {
		candidates = append(candidates, pid(i))
	}
	for i := range 19 {
		candidates = append(candidates, pid(100+i))
	}

	grafts, prunes := ms.heartbeat(now, candidates)
	assert.Empty(t, grafts)
	push, bmap := ms.counts()
	assert.Equal(t, ms.cfg.PushDhigh, push)
	assert.Equal(t, ms.cfg.BitmapDhigh, bmap)
	assert.Len(t, prunes, 2)
}

// TestHeartbeatRespectsBackoff: a peer in backoff is skipped during top-up and a
// fresh peer is grafted in its place. pid(0) is backed off for both meshes so it
// stays connected; pid(1) is fresh and should be grafted to push.
func TestHeartbeatRespectsBackoff(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())

	// push=3 (< Dlow=4 ⇒ top-up triggers, fills toward PushD=5).
	for i := range 3 {
		seedRole(ms, pid(50+i), rolePush)
	}
	// pid(0) backed off for both meshes (Full backoff) ⇒ never grafted.
	ms.pushBackoff[pid(0)] = now.Add(30 * time.Second)
	ms.bitmapBackoff[pid(0)] = now.Add(30 * time.Second)
	candidates := []peer.ID{pid(0), pid(1), pid(50), pid(51), pid(52)}

	grafts, _ := ms.heartbeat(now, candidates)
	assert.Equal(t, roleConnected, ms.role(pid(0)), "backed-off peer not grafted")
	assert.Empty(t, grafts[pid(0)])
	assert.Equal(t, rolePush, ms.role(pid(1)), "fresh peer grafted instead")
	assert.Equal(t, []string{"GRAFT:PUSH"}, ops(grafts[pid(1)]))
}

// TestHeartbeatConvergence: from a degree-~30 all-connected candidate set,
// repeated heartbeats converge to push≈PushD / bitmap≈BitmapD and never
// violate mutual exclusion (a peer is never both push and bitmap).
func TestHeartbeatConvergence(t *testing.T) {
	now := time.Unix(1000, 0)
	ms := newMeshState(testCfg())

	const degree = 30
	var candidates []peer.ID
	for i := range degree {
		candidates = append(candidates, pid(i))
	}

	for round := range 10 {
		ms.heartbeat(now, candidates)
		now = now.Add(700 * time.Millisecond)

		// Mutual exclusion holds every round (roles map is single-valued, but
		// assert the counts are consistent and no peer is double-counted).
		push, bmap := ms.counts()
		assert.LessOrEqual(t, push, ms.cfg.PushD, "round %d push over cap", round)
		assert.LessOrEqual(t, bmap, ms.cfg.BitmapD, "round %d bitmap over cap", round)
	}

	push, bmap := ms.counts()
	assert.Equal(t, ms.cfg.PushD, push, "converged push")
	assert.Equal(t, ms.cfg.BitmapD, bmap, "converged bitmap")

	// Verify mutual exclusion explicitly: tally roles, ensure each peer has one.
	seen := map[meshRole]int{}
	for _, p := range candidates {
		seen[ms.role(p)]++
	}
	assert.Equal(t, ms.cfg.PushD, seen[rolePush])
	assert.Equal(t, ms.cfg.BitmapD, seen[roleBitmap])
	assert.Equal(t, degree-ms.cfg.PushD-ms.cfg.BitmapD, seen[roleConnected])
}
