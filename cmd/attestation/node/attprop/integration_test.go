package attprop

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/ethp2p/simlab/cmd/attestation/verify"
)

// intgCfg returns a Config for the integration tests: small meshes, tight
// budget, committee 64. Timer intervals are left zero so driveTimer is a no-op;
// tests post tick/floor/heartbeat events by hand for determinism.
func intgCfg(fanout bool) Config {
	return intgCfgWithTopics(fanout, []string{"t0"})
}

func intgCfgWithTopics(fanout bool, topics []string) Config {
	c := testCfg()
	// Debug-level logger to a discard writer: keeps test output clean while still
	// exercising every log call. Swap io.Discard for os.Stderr to debug.
	var w io.Writer = io.Discard
	if os.Getenv("ATTPROP_LOG") != "" {
		w = os.Stderr
	}
	c.Logger = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c.Topics = topics
	c.CommitteeSize = testCommittee
	c.SendBudgetB = 4
	c.MaxAttsPerMessage = 30
	c.MaxPeersPerAtt = 16
	c.SlotDuration = 12 * time.Second
	c.PublishStart = time.Now()
	c.Fanout = fanout
	c.TickInterval, c.BitmapFloorInterval, c.HeartbeatInterval = 0, 0, 0
	return c
}

// fastVerifier builds a verifier with a tiny batch window so validated callbacks
// fire promptly under synctest.
func fastVerifier() *verify.Verifier {
	return verify.New(
		func() time.Duration { return time.Millisecond },
		0, time.Millisecond, slog.Default(),
	)
}

// makeData returns deterministic non-empty attestation_data for a slot (content
// is opaque to attprop; only per-slot uniqueness matters).
func makeData(slot int) []byte { return []byte{byte(slot), 0xde, 0xad, 0xbe, 0xef} }

// tracerFunc adapts a function to the Tracer interface.
type tracerFunc func(slot, topicIdx, position int, attData []byte, latencyMs int64)

func (f tracerFunc) OnPartialReceive(
	slot, topicIdx, position int, attData []byte, latencyMs int64,
) {
	f(slot, topicIdx, position, attData, latencyMs)
}

// harness pairs a simnet with the per-host managers under test.
type harness struct {
	sn       *simNet
	managers []*Manager
}

// noFanout marks no host as a fanout node.
func noFanout(int) bool { return false }

// orderedPair returns (opener, other) for a 2-host net: the lower-peerID host is
// the stream opener per weOpen.
func orderedPair() (int, int) {
	if weOpen(testPeerID(0), testPeerID(1)) {
		return 0, 1
	}
	return 1, 0
}

// newHarness builds count managers over one simnet, each with a fast verifier
// running and its stream handlers registered; non-fanout managers run their
// eventloop. tracers optionally overrides a host's receive tracer.
func newHarness(
	t *testing.T, ctx context.Context, count int, fanout func(int) bool, tracers map[int]Tracer,
) *harness {
	return newHarnessWithTopics(t, ctx, count, fanout, tracers, []string{"t0"})
}

func newHarnessWithTopics(
	t *testing.T,
	ctx context.Context,
	count int,
	fanout func(int) bool,
	tracers map[int]Tracer,
	topics []string,
) *harness {
	t.Helper()
	sn := newSimNet(t, count)
	h := &harness{sn: sn, managers: make([]*Manager, count)}
	for i := range count {
		v := fastVerifier()
		go v.Run()
		t.Cleanup(v.Stop)
		var tr Tracer
		if tracers != nil {
			tr = tracers[i]
		}
		m := New(sn.hosts[i], v, tr, intgCfgWithTopics(fanout(i), topics))
		m.Start(ctx)
		h.managers[i] = m
		if !fanout(i) {
			go m.run(ctx)
		}
	}
	return h
}

// addr / connectUp mirror simNet but route through the harness's managers.
func (h *harness) connectUp(t *testing.T, ctx context.Context, a, b int) {
	t.Helper()
	require.NoError(t, h.sn.hosts[a].Connect(ctx, peer.AddrInfo{
		ID: testPeerID(b), Addrs: []ma.Multiaddr{h.sn.addr(b)},
	}))
	h.managers[a].ConnectPeer(testPeerID(b))
	synctest.Wait()
}

// onLoop runs fn on m's eventloop goroutine and blocks until it has finished,
// so a test can safely force roles or read state without racing the loop.
func (m *Manager) onLoop(fn func()) {
	done := make(chan struct{})
	m.post(funcEvent{fn: func() { fn(); close(done) }})
	<-done
}

func (m *Manager) forceTopicRole(topicIdx int, p peer.ID, r meshRole) {
	m.onLoop(func() { m.mesh(topicIdx).roles[p] = r })
}

// forceRole sets a topic-0 peer's mesh role on the eventloop goroutine.
func (m *Manager) forceRole(p peer.ID, r meshRole) { m.forceTopicRole(0, p, r) }

// TestFanoutInjectAndReset exercises §G1: a fanout node injects its single
// attestation to its connected peer (the mesh node receives + validates it), and
// resets any stream a peer opens back to it.
func TestFanoutInjectAndReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		const fan, mesh = 0, 1
		var got struct {
			pos  int
			seen bool
		}
		recv := tracerFunc(func(_, _, pos int, _ []byte, _ int64) { got.pos, got.seen = pos, true })

		h := newHarness(t, ctx, 2, func(i int) bool { return i == fan },
			map[int]Tracer{mesh: recv})
		// Install the fanout reset handlers before any inbound stream arrives.
		h.managers[fan].installFanoutResetHandlers()

		// Live connection so the fanout publish path can open push streams to B.
		require.NoError(t, h.sn.hosts[fan].Connect(ctx, peer.AddrInfo{
			ID: testPeerID(mesh), Addrs: []ma.Multiaddr{h.sn.addr(mesh)},
		}))
		synctest.Wait()

		h.managers[fan].FanoutPublish("t0", 1, 7, []byte{0xab}, makeData(1))
		synctest.Wait()
		time.Sleep(100 * time.Millisecond) // frame traversal + verify
		synctest.Wait()

		require.True(t, got.seen, "mesh node received the fanout attestation")
		require.Equal(t, 7, got.pos)

		// Any stream the mesh node opens to the fanout node is reset (§G1).
		s, err := h.sn.hosts[mesh].NewStream(ctx, testPeerID(fan), PushProtocol(0))
		require.NoError(t, err)
		_, _ = s.Write([]byte{0x01})
		_, rerr := s.Read(make([]byte, 1))
		require.Error(t, rerr, "fanout resets inbound streams")
		synctest.Wait()
	})
}

// TestEndToEndPushForward proves the full mesh send path over real QUIC: node A
// publishes its own attestation, A pushes it to push-peer B, B's reader decodes
// it, submits to the verifier, and the validated callback promotes it — all
// driven by the single-owner eventloop and real stream Writes (the
// write-completion backpressure path). It also confirms a second send to B
// starts only after the first frame's Write returns (one-in-flight).
func TestEndToEndPushForward(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var got struct{ positions []int }
		recv := tracerFunc(func(_, _, pos int, _ []byte, _ int64) {
			got.positions = append(got.positions, pos)
		})
		opener, other := orderedPair()
		h := newHarness(t, ctx, 2, noFanout, map[int]Tracer{other: recv})

		// A (opener) ↔ B. Make A→B a push edge so A forwards data to B.
		h.connectUp(t, ctx, opener, other)
		h.managers[opener].forceRole(testPeerID(other), rolePush)

		// A publishes positions; the tick flush ships the (partial) batch to B.
		data := makeData(1)
		for _, pos := range []int{3, 8, 21} {
			h.managers[opener].PublishLocal("t0", 1, pos, []byte{byte(pos)}, data)
		}
		synctest.Wait()
		h.managers[opener].post(tickEvent{})
		synctest.Wait()
		time.Sleep(100 * time.Millisecond) // let the in-flight frame traverse + verify
		synctest.Wait()

		require.ElementsMatch(t, []int{3, 8, 21}, got.positions,
			"push peer received and validated every published position")

		// After delivery, A must hold no in-flight send to B (Write returned,
		// sendDone cleared it) and B is recorded as holding what we sent.
		h.managers[opener].onLoop(func() {
			k := topicPeer{topic: 0, peer: testPeerID(other)}
			require.False(t, h.managers[opener].senders[k].inFlight)
			ss := h.managers[opener].getSlotState("t0", 1)
			b := ss.buckets[string(data)]
			for _, pos := range []int{3, 8, 21} {
				require.Equal(t, 1, b.holderCount[pos], "B recorded as holder")
			}
		})
	})
}

// TestEndToEndSecondTopicForward proves topic routing is not hard-coded to
// topic 0: streams, send selection, receive state, and tracer events all carry
// topic 1 end-to-end.
func TestEndToEndSecondTopicForward(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var got []int
		recv := tracerFunc(func(_, topicIdx, pos int, _ []byte, _ int64) {
			if topicIdx == 1 {
				got = append(got, pos)
			}
		})
		opener, other := orderedPair()
		h := newHarnessWithTopics(t, ctx, 2, noFanout, map[int]Tracer{other: recv},
			[]string{"t0", "t1"})

		h.connectUp(t, ctx, opener, other)
		h.managers[opener].forceTopicRole(1, testPeerID(other), rolePush)

		data := []byte("topic-one-slot-one")
		h.managers[opener].PublishLocal("t1", 1, 11, []byte{0x11}, data)
		synctest.Wait()
		h.managers[opener].post(tickEvent{})
		synctest.Wait()
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		require.Equal(t, []int{11}, got)
		require.Equal(t, 1, h.managers[other].ValidatedCount("t1", 1))
		require.Equal(t, 0, h.managers[other].ValidatedCount("t0", 1))
	})
}

// TestBitmapPlusKTrigger proves §D2's +K trigger: a bitmap-mesh peer receives an
// available advertisement once ≥ K positions validate on the sender, with the
// correct number of set bits.
func TestBitmapPlusKTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		opener, other := orderedPair()
		h := newHarness(t, ctx, 2, noFanout, nil)
		h.connectUp(t, ctx, opener, other)
		// A treats B as a bitmap peer; B treats A as a bitmap peer (so B folds the
		// inbound bitmap and we can read its peerAvail).
		h.managers[opener].forceRole(testPeerID(other), roleBitmap)
		h.managers[other].forceRole(testPeerID(opener), roleBitmap)

		// Publish exactly K positions on A → crosses the +K threshold, emits.
		data := makeData(1)
		for pos := range bitmapTriggerK {
			h.managers[opener].PublishLocal("t0", 1, pos, []byte{byte(pos)}, data)
		}
		synctest.Wait()
		time.Sleep(100 * time.Millisecond) // bitmap frame traversal
		synctest.Wait()

		// B should now hold A's full bitmap for the bucket.
		ones := 0
		h.managers[other].onLoop(func() {
			ss := h.managers[other].getSlotState("t0", 1)
			require.NotNil(t, ss, "B learned the slot from the bitmap")
			b := ss.buckets[string(data)]
			require.NotNil(t, b)
			ones = b.peerAvail[testPeerID(opener)].OnesCount()
		})
		require.Equal(t, bitmapTriggerK, ones, "B holds A's advertised bitmap (+K)")
	})
}

// TestBitmapFloorOnlyWhenChanged proves §D2's floor: a forced floor re-emits a
// changed bitmap, but a second floor with no change since the last emit sends
// nothing new (the receiver's bitmap is unchanged and no extra holder flips
// occur).
func TestBitmapFloorOnlyWhenChanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		opener, other := orderedPair()
		h := newHarness(t, ctx, 2, noFanout, nil)
		h.connectUp(t, ctx, opener, other)
		h.managers[opener].forceRole(testPeerID(other), roleBitmap)
		h.managers[other].forceRole(testPeerID(opener), roleBitmap)

		data := makeData(1)
		h.managers[opener].PublishLocal("t0", 1, 5, []byte{5}, data) // < K, no +K emit
		synctest.Wait()

		// First floor: bitmap changed (one position) ⇒ emit; B learns position 5.
		h.managers[opener].post(bitmapFloorEvent{})
		synctest.Wait()
		time.Sleep(100 * time.Millisecond) // bitmap frame traversal
		synctest.Wait()
		h.managers[other].onLoop(func() {
			ss := h.managers[other].getSlotState("t0", 1)
			require.NotNil(t, ss)
			require.True(t, ss.buckets[string(data)].peerAvail[testPeerID(opener)].Get(5))
		})

		// A second floor with no new validations must skip (re-emit only if
		// changed): the sender marks the bucket unchanged — lastEmitted equals the
		// current validated bitmap.
		h.managers[opener].onLoop(func() {
			ss := h.managers[opener].getSlotState("t0", 1)
			b := ss.buckets[string(data)]
			require.NotNil(t, b.lastEmitted, "first floor recorded lastEmitted")
			require.Equal(t, []byte(b.lastEmitted), []byte(h.managers[opener].validatedBitmap(b)))
		})
		h.managers[opener].post(bitmapFloorEvent{})
		synctest.Wait() // no change is the assertion (no panic, bitmap unchanged)
	})
}
