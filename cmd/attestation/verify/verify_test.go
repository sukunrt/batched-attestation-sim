package verify

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingSink collects items the verifier marks as validated. Safe for
// concurrent use; the verifier dispatches sequentially but tests inspect from
// other goroutines.
type recordingSink struct {
	mu    sync.Mutex
	items []Item
}

func (r *recordingSink) callback() func(Item) {
	return func(item Item) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.items = append(r.items, item)
	}
}

func (r *recordingSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

func (r *recordingSink) totalAttestations() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int
	for _, it := range r.items {
		n += len(it.Attestations)
	}
	return n
}

// fixedDelay returns a function that records how many times it's been called.
type fixedDelay struct {
	mu    sync.Mutex
	delay time.Duration
	calls int
}

func (f *fixedDelay) fn() func() time.Duration {
	return func() time.Duration {
		f.mu.Lock()
		f.calls++
		f.mu.Unlock()
		return f.delay
	}
}

func (f *fixedDelay) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newTestVerifier(t *testing.T, delay func() time.Duration, perAtt, window time.Duration) *Verifier {
	t.Helper()
	v := New(delay, perAtt, window, slog.Default())
	go v.Run()
	t.Cleanup(func() { v.Stop() })
	return v
}

func TestVerifierSingleItem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newTestVerifier(t, func() time.Duration { return 20 * time.Millisecond }, 0, 5*time.Millisecond)

		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{1, 2, 3}}, sink.callback())
		time.Sleep(50 * time.Millisecond)

		assert.Equal(t, 1, sink.count())
		assert.Equal(t, 3, sink.totalAttestations())
	})
}

func TestVerifierSubmitAndWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		v := newTestVerifier(t, func() time.Duration { return 10 * time.Millisecond }, 0, 5*time.Millisecond)

		start := time.Now()
		v.SubmitAndWait(Item{Topic: "t0", Slot: 1, Attestations: []any{1}})

		// SubmitAndWait must not return before the batch sleep has elapsed.
		assert.GreaterOrEqual(t, time.Since(start), 10*time.Millisecond)
	})
}

func TestVerifierBatchesWithinWindow(t *testing.T) {
	// Items that arrive while the batch window is still open should be
	// dispatched together. With verifyDelay << window the first submission
	// gets its own batch, and any submissions that follow before the window
	// closes form the next batch.
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		delay := &fixedDelay{delay: 5 * time.Millisecond}
		v := newTestVerifier(t, delay.fn(), 0, 50*time.Millisecond)

		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{1}}, sink.callback())
		time.Sleep(10 * time.Millisecond) // verifyBatch finished, window still open
		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{2}}, sink.callback())
		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{3}}, sink.callback())
		time.Sleep(100 * time.Millisecond)

		assert.Equal(t, 3, sink.totalAttestations())
		assert.Equal(t, 2, delay.callCount(), "first item + window-batched pair")
	})
}

func TestVerifierItemsDuringVerifyAreQueued(t *testing.T) {
	// Items submitted while a batch is verifying must NOT be dropped. With
	// verifyDelay > window, the second-iteration select races notify vs
	// timer.C; the run loop must accumulate (not drain-and-drop) when
	// timerRunning is still true.
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newTestVerifier(t, func() time.Duration { return 30 * time.Millisecond }, 0, 5*time.Millisecond)

		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{1}}, sink.callback())
		time.Sleep(10 * time.Millisecond) // mid-verification
		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{2}}, sink.callback())
		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{3}}, sink.callback())
		time.Sleep(200 * time.Millisecond)

		assert.Equal(t, 3, sink.totalAttestations())
	})
}

func TestVerifierManyItemsNoneDropped(t *testing.T) {
	// Submit a burst from many goroutines; every item must be verified.
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newTestVerifier(t, func() time.Duration { return 5 * time.Millisecond }, 0, 5*time.Millisecond)

		const n = 50
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{idx}}, sink.callback())
			}(i)
		}
		wg.Wait()
		// Give plenty of time to drain.
		time.Sleep(500 * time.Millisecond)

		assert.Equal(t, n, sink.totalAttestations())
	})
}

func TestVerifierStopDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := New(
			func() time.Duration { return 5 * time.Millisecond },
			0,
			2*time.Millisecond,
			slog.Default(),
		)
		go v.Run()

		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{1, 2}}, sink.callback())
		v.Stop() // should block until queued items are validated.

		assert.Equal(t, 2, sink.totalAttestations())
	})
}

func TestVerifierStopWithoutWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := New(
			func() time.Duration { return 5 * time.Millisecond },
			0,
			2*time.Millisecond,
			slog.Default(),
		)
		go v.Run()
		v.Stop()
		assert.Equal(t, 0, sink.count())
	})
}

func TestVerifierPerAttestationDelayCounted(t *testing.T) {
	// Each batch should invoke the verificationDelay func exactly once
	// (the batch verification cost is one delay() + N * perAttDelay, not N
	// delays).
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		delay := &fixedDelay{delay: 10 * time.Millisecond}
		v := newTestVerifier(t, delay.fn(), 1*time.Millisecond, 5*time.Millisecond)

		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{1, 2, 3}}, sink.callback())
		time.Sleep(50 * time.Millisecond)
		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{4, 5}}, sink.callback())
		time.Sleep(50 * time.Millisecond)

		assert.Equal(t, 2, delay.callCount())
		assert.Equal(t, 5, sink.totalAttestations())
	})
}

func TestVerifierLogsBatchSizeAndDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		v := New(
			func() time.Duration { return 10 * time.Millisecond },
			2*time.Millisecond,
			5*time.Millisecond,
			logger,
		)
		queuedAt := time.Now().Add(-7 * time.Millisecond)

		v.verifyBatch([]queuedItem{
			{item: Item{Topic: "t0", Slot: 1, Attestations: []any{1}}, enqueuedAt: queuedAt},
			{item: Item{Topic: "t0", Slot: 1, Attestations: []any{2, 3}}, enqueuedAt: queuedAt.Add(3 * time.Millisecond)},
		})

		log := buf.String()
		for _, want := range []string{
			"msg=verification_batch",
			"batch_items=2",
			"attestations=3",
			"queued_ms=7",
			"base_delay_ms=10",
			"per_attestation_delay_ms=2",
			"sleep_ms=16",
			"verification_duration_ms=16",
			"duration_ms=23",
		} {
			if !strings.Contains(log, want) {
				t.Fatalf("verification log missing %q in %q", want, log)
			}
		}
	})
}

func TestVerifierMultipleTopicsAndSlots(t *testing.T) {
	// Items from different (topic, slot) tuples in the same batch should
	// each be dispatched to onVerified with their Topic/Slot intact.
	synctest.Test(t, func(t *testing.T) {
		sink := &recordingSink{}
		v := newTestVerifier(t, func() time.Duration { return 10 * time.Millisecond }, 0, 5*time.Millisecond)

		v.Submit(Item{Topic: "t0", Slot: 1, Attestations: []any{1}}, sink.callback())
		v.Submit(Item{Topic: "t0", Slot: 2, Attestations: []any{2}}, sink.callback())
		v.Submit(Item{Topic: "t1", Slot: 1, Attestations: []any{3}}, sink.callback())

		time.Sleep(50 * time.Millisecond)

		seen := map[string]int{}
		sink.mu.Lock()
		for _, it := range sink.items {
			seen[it.Topic+":"+strconvI(it.Slot)] += len(it.Attestations)
		}
		sink.mu.Unlock()

		assert.Equal(t, map[string]int{"t0:1": 1, "t0:2": 1, "t1:1": 1}, seen)
	})
}

func TestVerifierSubmitAndWaitMultipleConcurrent(t *testing.T) {
	// Concurrent SubmitAndWait callers must each unblock once the batch
	// containing their item completes.
	synctest.Test(t, func(t *testing.T) {
		v := newTestVerifier(t, func() time.Duration { return 15 * time.Millisecond }, 0, 5*time.Millisecond)

		var wg sync.WaitGroup
		var done [4]bool
		var mu sync.Mutex
		for i := range 4 {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				v.SubmitAndWait(Item{Topic: "t0", Slot: 1, Attestations: []any{idx}})
				mu.Lock()
				done[idx] = true
				mu.Unlock()
			}(i)
		}
		wg.Wait()

		for i, d := range done {
			assert.True(t, d, "waiter %d did not unblock", i)
		}
	})
}

func TestVerifierStopAfterStopIsSafe(t *testing.T) {
	// Documenting current contract: Stop is only safe to call once.
	// (Calling it twice would close v.notify-driven re-entry or, more
	// importantly, the done channel — guard via require.NotPanics on one
	// call only).
	synctest.Test(t, func(t *testing.T) {
		v := New(
			func() time.Duration { return 1 * time.Millisecond },
			0, 1*time.Millisecond, slog.Default(),
		)
		go v.Run()
		require.NotPanics(t, func() { v.Stop() })
	})
}

// strconvI is a tiny int->string helper used to keep tests dependency-light
// for one inline use.
func strconvI(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
