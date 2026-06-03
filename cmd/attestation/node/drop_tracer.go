package node

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// DropTracer is a diagnostic pubsub.RawTracer that counts outbound RPCs that
// were accepted into a peer's send queue (SendRPC) versus dropped because the
// per-peer outbound queue was full (DropRPC), split by RPC kind (classic
// publish forwards vs partial-message envelopes vs control-only). It also
// counts RejectMessage reasons so we can tell apart "queue full" forward loss
// from validation-pipeline drops.
//
// All hot-path methods only bump atomic counters; a single goroutine flushes a
// cumulative summary line periodically so logging never runs on the pubsub
// event loop. Enabled only when DROP_TRACE is set in the environment.
type DropTracer struct {
	logger *slog.Logger

	// accepted into the outbound queue
	sentPublishRPC  atomic.Int64
	sentPublishMsgs atomic.Int64
	sentPartialRPC  atomic.Int64
	sentControlRPC  atomic.Int64

	// dropped because the outbound queue was full
	dropPublishRPC  atomic.Int64
	dropPublishMsgs atomic.Int64
	dropPartialRPC  atomic.Int64
	dropControlRPC  atomic.Int64

	// set once any RPC has been dropped; gates the drop_summary log so we
	// only emit when there is an actual drop to report.
	dropped atomic.Bool

	// receive side, for context
	recvPublishMsgs atomic.Int64
	recvPartialRPC  atomic.Int64

	dupMsgs     atomic.Int64
	deliverMsgs atomic.Int64

	mu          sync.Mutex
	rejectByRsn map[string]int
}

var _ pubsub.RawTracer = (*DropTracer)(nil)

// newDropTracer returns a DropTracer that logs cumulative summaries to stderr.
// DIAGNOSTIC BUILD: always enabled; revert before merging.
func newDropTracer(nodeID string) *DropTracer {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("node", nodeID, "component", "drop_tracer")
	return &DropTracer{
		logger:      logger,
		rejectByRsn: make(map[string]int),
	}
}

func (t *DropTracer) start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.flush()
			case <-ctx.Done():
				t.flush()
				return
			}
		}
	}()
}

func (t *DropTracer) flush() {
	if !t.dropped.Load() {
		return
	}

	t.mu.Lock()
	rejThrottle := t.rejectByRsn[pubsub.RejectValidationThrottled]
	rejQueueFull := t.rejectByRsn[pubsub.RejectValidationQueueFull]
	rejIgnored := t.rejectByRsn[pubsub.RejectValidationIgnored]
	rejFailed := t.rejectByRsn[pubsub.RejectValidationFailed]
	t.mu.Unlock()

	t.logger.Info("drop_summary",
		"sent_publish_rpc", t.sentPublishRPC.Load(),
		"sent_publish_msgs", t.sentPublishMsgs.Load(),
		"sent_partial_rpc", t.sentPartialRPC.Load(),
		"sent_control_rpc", t.sentControlRPC.Load(),
		"drop_publish_rpc", t.dropPublishRPC.Load(),
		"drop_publish_msgs", t.dropPublishMsgs.Load(),
		"drop_partial_rpc", t.dropPartialRPC.Load(),
		"drop_control_rpc", t.dropControlRPC.Load(),
		"recv_publish_msgs", t.recvPublishMsgs.Load(),
		"recv_partial_rpc", t.recvPartialRPC.Load(),
		"dup_msgs", t.dupMsgs.Load(),
		"deliver_msgs", t.deliverMsgs.Load(),
		"rej_throttle", rejThrottle,
		"rej_queue_full", rejQueueFull,
		"rej_ignored", rejIgnored,
		"rej_failed", rejFailed,
	)
}

func classifyRPC(rpc *pubsub.RPC) (nPublish int, isPartial bool) {
	return len(rpc.GetPublish()), rpc.GetPartial() != nil
}

func (t *DropTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	nPub, isPartial := classifyRPC(rpc)
	switch {
	case nPub > 0:
		t.sentPublishRPC.Add(1)
		t.sentPublishMsgs.Add(int64(nPub))
	case isPartial:
		t.sentPartialRPC.Add(1)
	default:
		t.sentControlRPC.Add(1)
	}
}

func (t *DropTracer) DropRPC(rpc *pubsub.RPC, p peer.ID) {
	t.dropped.Store(true)
	nPub, isPartial := classifyRPC(rpc)
	switch {
	case nPub > 0:
		t.dropPublishRPC.Add(1)
		t.dropPublishMsgs.Add(int64(nPub))
	case isPartial:
		t.dropPartialRPC.Add(1)
	default:
		t.dropControlRPC.Add(1)
	}
}

func (t *DropTracer) RecvRPC(rpc *pubsub.RPC) {
	nPub, isPartial := classifyRPC(rpc)
	if nPub > 0 {
		t.recvPublishMsgs.Add(int64(nPub))
	}
	if isPartial {
		t.recvPartialRPC.Add(1)
	}
}

func (t *DropTracer) RejectMessage(msg *pubsub.Message, reason string) {
	t.mu.Lock()
	t.rejectByRsn[reason]++
	t.mu.Unlock()
}

func (t *DropTracer) DuplicateMessage(msg *pubsub.Message) { t.dupMsgs.Add(1) }
func (t *DropTracer) DeliverMessage(msg *pubsub.Message)   { t.deliverMsgs.Add(1) }

// no-op RawTracer methods
func (t *DropTracer) OnNewOutboundStream(p peer.ID, proto protocol.ID) {}
func (t *DropTracer) OnClosedOutboundStream(p peer.ID)                  {}
func (t *DropTracer) Join(topic string)                                 {}
func (t *DropTracer) Leave(topic string)                                {}
func (t *DropTracer) Graft(p peer.ID, topic string)                     {}
func (t *DropTracer) Prune(p peer.ID, topic string)                     {}
func (t *DropTracer) ValidateMessage(msg *pubsub.Message)               {}
func (t *DropTracer) ThrottlePeer(p peer.ID)                            {}
func (t *DropTracer) UndeliverableMessage(msg *pubsub.Message)          {}
