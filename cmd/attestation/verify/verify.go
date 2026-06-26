// Package verify provides a batch attestation verifier shared by the gossipsub
// partial-message strategies (package node) and the att_propagation native
// protocol (package node/attprop). It lives in its own package so both can
// import it without an import cycle. Item.Attestations is []any so verify needs
// no node types.
package verify

import (
	"log/slog"
	"math"
	"sync"
	"time"
)

// Item represents attestations submitted for batch verification. Data is the
// opaque attestation_data the batched attestations share; the partial-mode
// callback uses it (with Topic, Slot) to find the right state bucket. Empty for
// classic-mode submissions.
type Item struct {
	Slot         int
	Topic        string
	Data         []byte
	Attestations []any
}

// queuedItem pairs a submitted item with its per-submit callback.
type queuedItem struct {
	item       Item
	enqueuedAt time.Time
	onVerified func(Item)
}

// Verifier pipelines attestation verification. Submit pushes items into a
// mutex-protected queue. Run loops on a timer: every batchWindow it swaps out
// the accumulated queue, verifies the batch (sleeping for the verification
// delay), then loops. While one batch verifies, new items accumulate in the
// queue for the next round.
type Verifier struct {
	verificationDelay   func() time.Duration
	perAttestationDelay time.Duration
	batchWindow         time.Duration
	logger              *slog.Logger

	mu      sync.Mutex
	queue   []queuedItem
	notify  chan struct{} // signal that queue is non-empty
	stopped bool
	done    chan struct{}
}

// New constructs a Verifier. Call Run in a goroutine to start the pipeline and
// Stop to drain and shut it down.
func New(
	verificationDelay func() time.Duration,
	perAttestationDelay time.Duration,
	batchWindow time.Duration,
	logger *slog.Logger,
) *Verifier {
	return &Verifier{
		verificationDelay:   verificationDelay,
		perAttestationDelay: perAttestationDelay,
		batchWindow:         batchWindow,
		logger:              logger,
		notify:              make(chan struct{}, 1),
		done:                make(chan struct{}),
	}
}

// Submit enqueues attestations for batch verification. Non-blocking. onVerified
// is invoked once the containing batch finishes its verification sleep; pass nil
// if no callback is needed.
func (v *Verifier) Submit(item Item, onVerified func(Item)) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.queue = append(v.queue, queuedItem{item: item, enqueuedAt: time.Now(), onVerified: onVerified})
	// Non-blocking signal: if notify already has a value, skip.
	select {
	case v.notify <- struct{}{}:
	default:
	}
}

// SubmitAndWait enqueues a verification item and blocks until the batch completes.
func (v *Verifier) SubmitAndWait(item Item) {
	done := make(chan struct{})
	v.Submit(item, func(Item) { close(done) })
	<-done
}

// Run is the pipeline loop. Call as a goroutine.
func (v *Verifier) Run() {
	defer close(v.done)

	timer := time.NewTimer(math.MaxInt64)
	defer timer.Stop()

	timerRunning := false
	for {
		select {
		case <-v.notify:
		case <-timer.C:
			timerRunning = false
		}

		v.mu.Lock()
	OUTER:
		for {
			select {
			case <-v.notify:
			default:
				break OUTER
			}
		}
		stopped := v.stopped
		// While the batch window is still open, accumulate items in the queue
		// without draining it — the timer firing is the signal to verify.
		if timerRunning && !stopped {
			v.mu.Unlock()
			continue
		}
		queueLen := len(v.queue)
		batch := v.queue
		v.queue = nil
		v.mu.Unlock()

		if stopped && queueLen == 0 {
			return
		}
		if queueLen == 0 {
			continue
		}
		timer.Reset(v.batchWindow)
		timerRunning = true
		v.verifyBatch(batch)
		v.mu.Lock()
		stopped = v.stopped
		v.mu.Unlock()
		if stopped {
			return
		}
	}
}

// verifyBatch simulates batch verification and dispatches each item.
func (v *Verifier) verifyBatch(batch []queuedItem) {
	if len(batch) == 0 {
		return
	}
	var totalAttestations int
	validationStart := time.Now()
	oldestQueuedAt := validationStart
	for _, qi := range batch {
		totalAttestations += len(qi.item.Attestations)
		if !qi.enqueuedAt.IsZero() && qi.enqueuedAt.Before(oldestQueuedAt) {
			oldestQueuedAt = qi.enqueuedAt
		}
	}
	baseDelay := v.verificationDelay()
	sleepFor := baseDelay + time.Duration(totalAttestations)*v.perAttestationDelay
	time.Sleep(sleepFor)
	completedAt := time.Now()
	queuedFor := validationStart.Sub(oldestQueuedAt)
	verificationDuration := completedAt.Sub(validationStart)
	totalDuration := completedAt.Sub(oldestQueuedAt)
	if v.logger != nil {
		v.logger.Info("verification_batch",
			"batch_items", len(batch),
			"attestations", totalAttestations,
			"queued_ms", queuedFor.Milliseconds(),
			"base_delay_ms", baseDelay.Milliseconds(),
			"per_attestation_delay_ms", v.perAttestationDelay.Milliseconds(),
			"sleep_ms", sleepFor.Milliseconds(),
			"verification_duration_ms", verificationDuration.Milliseconds(),
			"duration_ms", totalDuration.Milliseconds(),
		)
	}

	for _, qi := range batch {
		if qi.onVerified != nil {
			qi.onVerified(qi.item)
		}
	}
}

// Stop signals the pipeline to drain remaining items and exit.
func (v *Verifier) Stop() {
	v.mu.Lock()
	v.stopped = true
	// Wake up Run in case it's waiting on notify.
	select {
	case v.notify <- struct{}{}:
	default:
	}
	v.mu.Unlock()
	<-v.done
}
