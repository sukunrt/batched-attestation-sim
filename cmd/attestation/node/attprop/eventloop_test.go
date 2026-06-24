package attprop

import (
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
	"github.com/ethp2p/simlab/cmd/attestation/verify"
)

// schedManager builds a Manager with no host or streams, suitable for testing
// the send scheduler (trySelectAndSend) in isolation. Each peer gets a fake data
// sender whose work channel is buffered and NOT drained, so a started send stays
// inFlight until the test releases it — letting us count concurrency without a
// real network.
func schedManager(t *testing.T, maxAttsPerMsg, budgetB, maxPeers int) *Manager {
	t.Helper()
	m := &Manager{
		logger: slog.Default(),
		cfg: Config{
			Topics: []string{"t0"}, CommitteeSize: testCommittee,
			MaxAttsPerMessage: maxAttsPerMsg, SendBudgetB: budgetB, MaxPeersPerAtt: maxPeers,
		},
		topicIndex:     map[string]int{"t0": 0},
		events:         make(chan event, 1024),
		senders:        map[topicPeer]*peerSender{},
		bitmapWriters:  map[topicPeer]*peerSender{},
		controlWriters: map[topicPeer]*peerSender{},
		meshes:         map[int]*meshState{0: newMeshState(testCfg())},
		slots:          map[string]map[int]*slotState{},
	}
	return m
}

// addFakeSender wires a peer with a fake data sender (work buffered, not drained)
// and the given mesh role.
func (m *Manager) addFakeSender(p peer.ID, role meshRole) {
	m.senders[topicPeer{topic: 0, peer: p}] = &peerSender{peer: p, work: make(chan []byte, 64)}
	m.mesh(0).roles[p] = role
}

// inFlightByRole counts how many senders of each role currently have a data send
// in flight.
func (m *Manager) inFlightByRole() (push, bitmap int) {
	for k, s := range m.senders {
		if !s.inFlight {
			continue
		}
		switch m.mesh(k.topic).role(k.peer) {
		case rolePush:
			push++
		case roleBitmap:
			bitmap++
		}
	}
	return push, bitmap
}

// seedValidated adds a slot/bucket with the given validated positions at
// holder-count 0, so the scheduler has scarce data to send to every peer.
func (m *Manager) seedValidated(slot int, positions ...int) *slotState {
	ss := m.getOrCreateSlotState("t0", slot)
	b := ss.getOrCreateBucket([]byte("data"))
	for _, pos := range positions {
		b.atts[pos] = &attEntry{Position: pos, Signature: []byte{1}, Data: b.data}
		b.validated[pos] = struct{}{}
		ss.indexAddValidated(string(b.data), pos, 0)
	}
	return ss
}

// TestBudgetGatePushExemptBitmapCapped: with many push and many bitmap peers all
// idle and a large pool of scarce data, one selection pass starts a send to
// EVERY push peer (exempt from B) but no more than B bitmap peers (gated). This
// is the §F1 budget invariant.
func TestBudgetGatePushExemptBitmapCapped(t *testing.T) {
	const budgetB = 4
	m := schedManager(t, 30, budgetB, 1000)
	// Plenty of scarce positions so every peer gets a full chunk.
	pos := make([]int, 0, 60)
	for i := range 60 {
		pos = append(pos, i)
	}
	m.seedValidated(1, pos...)

	const nPush, nBitmap = 6, 10
	for i := range nPush {
		m.addFakeSender(pid(i), rolePush)
	}
	for i := range nBitmap {
		m.addFakeSender(pid(100+i), roleBitmap)
	}

	m.trySelectAndSend()

	push, bitmap := m.inFlightByRole()
	require.Equal(t, nPush, push, "every push peer sends (exempt from B)")
	require.LessOrEqual(t, bitmap, budgetB, "bitmap sends capped at B")
	// Budget counts push+bitmap; push (6) already exceeds B (4), so the gate
	// blocks ALL bitmap fills this pass.
	require.Equal(t, 0, bitmap, "push already over budget ⇒ no bitmap fill")
	require.Equal(t, nPush, m.activeData)
}

// TestBudgetGateBitmapFillsSpare: with few push peers, bitmap peers fill the
// remaining budget up to B total active data sends, never more.
func TestBudgetGateBitmapFillsSpare(t *testing.T) {
	const budgetB = 4
	m := schedManager(t, 30, budgetB, 1000)
	pos := make([]int, 0, 200)
	for i := range 200 {
		pos = append(pos, i)
	}
	m.seedValidated(1, pos...)

	m.addFakeSender(pid(0), rolePush) // 1 push
	for i := range 10 {
		m.addFakeSender(pid(100+i), roleBitmap)
	}

	m.trySelectAndSend()

	push, bitmap := m.inFlightByRole()
	require.Equal(t, 1, push, "the one push peer sends")
	require.Equal(t, budgetB-1, bitmap, "bitmap fills up to B total (1 push + 3 bitmap)")
	require.Equal(t, budgetB, m.activeData)
}

// TestPushPartialHeldUntilTick: a push peer with fewer than N scarce positions
// gets nothing between ticks (partial held), then is flushed on the tick.
func TestPushPartialHeldUntilTick(t *testing.T) {
	m := schedManager(t, 30, 4, 1000)
	m.seedValidated(1, 1, 2, 3) // only 3 < N=30 ⇒ partial

	m.addFakeSender(pid(0), rolePush)

	// Between ticks: partial push batch is held.
	m.trySelectAndSend()
	push, _ := m.inFlightByRole()
	require.Equal(t, 0, push, "partial push batch held between ticks")
	require.Equal(t, 0, m.activeData)

	// Tick flush: the partial batch goes out.
	m.onTick()
	push, _ = m.inFlightByRole()
	require.Equal(t, 1, push, "partial push batch flushed on tick")
}

// TestPushFullSentImmediately: a push peer with ≥ N scarce positions is sent
// immediately between ticks (no tick wait).
func TestPushFullSentImmediately(t *testing.T) {
	m := schedManager(t, 5, 4, 1000)        // N=5
	m.seedValidated(1, 1, 2, 3, 4, 5, 6, 7) // 7 > 5 ⇒ a full chunk plus more

	m.addFakeSender(pid(0), rolePush)
	m.trySelectAndSend()
	push, _ := m.inFlightByRole()
	require.Equal(t, 1, push, "full push batch sent immediately")
}

// TestSendDoneReselects: a peer cannot have two sends in flight; the next send
// starts only after sendDone clears the first. (Models write-completion flow
// control without a real socket.)
func TestSendDoneReselects(t *testing.T) {
	m := schedManager(t, 2, 4, 1000) // N=2 ⇒ full chunks of 2
	m.seedValidated(1, 1, 2, 3, 4, 5, 6)

	m.addFakeSender(pid(0), rolePush)

	m.trySelectAndSend()
	k := topicPeer{topic: 0, peer: pid(0)}
	require.True(t, m.senders[k].inFlight)
	require.Len(t, m.senders[k].work, 1, "one frame handed off")

	// A second pass must NOT start another send while one is in flight.
	m.trySelectAndSend()
	require.Len(t, m.senders[k].work, 1, "still one in-flight, no second send")

	// Drain the first frame and signal completion; the next chunk is selected.
	<-m.senders[k].work
	m.onSendDone(0, pid(0))
	require.True(t, m.senders[k].inFlight, "next send started after sendDone")
	require.Len(t, m.senders[k].work, 1)
}

// TestInboundDataMarksHolder: a received data batch records the sender as a
// holder of every position it sent (popcount-on-flip from data, §E1), and stages
// the positions in the verifier as validating (not yet validated/forwardable).
func TestInboundDataMarksHolder(t *testing.T) {
	m := schedManager(t, 30, 4, 1000)
	m.verifier = verify.New(func() time.Duration { return 0 }, 0, time.Hour, slog.Default())
	// Verifier not Run() — so positions stay "validating" (the callback never
	// fires), proving the receive-vs-validate split (§G2).
	data := makeData(1)
	env := &pb.BatchedAttestationEnvelope{Batches: []*pb.BatchedAttestation{{
		AttestationData: data,
		AttestorIndices: []uint32{2, 9, 40},
		Signatures:      [][]byte{{1}, {2}, {3}},
	}}}
	m.onInboundData(0, pid(7), env)

	ss := m.getSlotState("t0", 1)
	require.NotNil(t, ss)
	b := ss.buckets[string(data)]
	require.NotNil(t, b)
	for _, pos := range []int{2, 9, 40} {
		require.Equal(t, 1, b.holderCount[pos], "sender recorded as holder")
		require.True(t, b.peerAvail[pid(7)].Get(pos))
		_, validating := b.validating[pos]
		require.True(t, validating, "received but not yet validated")
		_, validated := b.validated[pos]
		require.False(t, validated, "not forwardable until verifier promotes (§G2)")
	}
	// Validating positions are NOT in the scarcity index yet.
	require.Equal(t, 0, len(ss.levels[1].entries))
}

// TestInboundDataBadIndexDropsBatch: an out-of-range attestor index voids the
// whole batch (positions[i]↔signatures[i] alignment must hold).
func TestInboundDataBadIndexDropsBatch(t *testing.T) {
	m := schedManager(t, 30, 4, 1000)
	m.verifier = verify.New(func() time.Duration { return 0 }, 0, time.Hour, slog.Default())
	data := makeData(1)
	env := &pb.BatchedAttestationEnvelope{Batches: []*pb.BatchedAttestation{{
		AttestationData: data,
		AttestorIndices: []uint32{2, uint32(testCommittee)}, // second is out of range
		Signatures:      [][]byte{{1}, {2}},
	}}}
	m.onInboundData(0, pid(7), env)

	ss := m.getSlotState("t0", 1)
	if ss != nil {
		if b := ss.buckets[string(data)]; b != nil {
			require.Empty(t, b.atts, "bad batch dropped wholesale")
		}
	}
}
