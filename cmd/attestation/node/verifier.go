package node

import (
	"log/slog"
	"math"
	"sync"
	"time"
)

// verificationItem represents attestations submitted for batch verification.
// Data is the opaque attestation_data the batched attestations share; the
// partial-mode callback uses it (with Topic, Slot) to find the right state
// bucket. Empty for classic-mode submissions.
type verificationItem struct {
	Slot         int
	Topic        string
	Data         []byte
	Attestations []any
}

// queuedItem pairs a submitted item with its per-submit callback.
type queuedItem struct {
	item       verificationItem
	onVerified func(verificationItem)
}

// batchVerifier pipelines attestation verification. submit() pushes items into
// a mutex-protected queue. run() loops on a timer: every batchWindow it swaps
// out the accumulated queue, verifies the batch (sleeping for the verification
// delay), then loops. While one batch verifies, new items accumulate in the
// queue for the next round.
type batchVerifier struct {
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

func newBatchVerifier(
	verificationDelay func() time.Duration,
	perAttestationDelay time.Duration,
	batchWindow time.Duration,
	logger *slog.Logger,
) *batchVerifier {
	return &batchVerifier{
		verificationDelay:   verificationDelay,
		perAttestationDelay: perAttestationDelay,
		batchWindow:         batchWindow,
		logger:              logger,
		notify:              make(chan struct{}, 1),
		done:                make(chan struct{}),
	}
}

// submit enqueues attestations for batch verification. Non-blocking. onVerified
// is invoked once the containing batch finishes its verification sleep; pass
// nil if no callback is needed.
func (v *batchVerifier) submit(item verificationItem, onVerified func(verificationItem)) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.queue = append(v.queue, queuedItem{item: item, onVerified: onVerified})
	// Non-blocking signal: if notify already has a value, skip.
	select {
	case v.notify <- struct{}{}:
	default:
	}
}

// submitAndWait enqueues a verification item and blocks until the batch completes.
func (v *batchVerifier) submitAndWait(item verificationItem) {
	done := make(chan struct{})
	v.submit(item, func(verificationItem) { close(done) })
	<-done
}

// run is the pipeline loop. Call as a goroutine.
func (v *batchVerifier) run() {
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
func (v *batchVerifier) verifyBatch(batch []queuedItem) {
	if len(batch) == 0 {
		return
	}
	var totalAttestations int
	for _, qi := range batch {
		totalAttestations += len(qi.item.Attestations)
	}
	time.Sleep(v.verificationDelay() + time.Duration(totalAttestations)*v.perAttestationDelay)

	for _, qi := range batch {
		if qi.onVerified != nil {
			qi.onVerified(qi.item)
		}
	}
}

// stop signals the pipeline to drain remaining items and exit.
func (v *batchVerifier) stop() {
	v.mu.Lock()
	v.stopped = true
	// Wake up run() in case it's waiting on notify.
	select {
	case v.notify <- struct{}{}:
	default:
	}
	v.mu.Unlock()
	<-v.done
}
