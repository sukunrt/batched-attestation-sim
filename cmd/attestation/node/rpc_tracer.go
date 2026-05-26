package node

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

var _ pubsub.RPCTracer = (*RPCTracer)(nil)

type RPCTracer struct {
	nodeID  string
	logger  *slog.Logger
	logFile *os.File

	mx             sync.Mutex
	nextMessageID  int
	MessageIDCount map[string]int
	MessageIDFunc  pubsub.MsgIdFunction
}

func newRPCTracer(filepath string, nodeID string, idFunc pubsub.MsgIdFunction) (*RPCTracer, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(f, nil))

	return &RPCTracer{
		nodeID:         nodeID,
		logger:         logger,
		logFile:        f,
		MessageIDCount: make(map[string]int),
		MessageIDFunc:  idFunc,
	}, nil
}

// NewStderrRPCTracer returns an RPCTracer that writes its structured log
// lines to os.Stderr (mixed with the rest of the node's slog output). Used by
// the production binary so the analyzer can scrape gossip-control bandwidth
// from the standard attestation stderr file.
func NewStderrRPCTracer(nodeID string, idFunc pubsub.MsgIdFunction) *RPCTracer {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return &RPCTracer{
		nodeID:         nodeID,
		logger:         logger,
		MessageIDCount: make(map[string]int),
		MessageIDFunc:  idFunc,
	}
}

func (t *RPCTracer) getMessageID(msgID string) int {
	t.mx.Lock()
	defer t.mx.Unlock()

	c, ok := t.MessageIDCount[msgID]
	if !ok {
		c = t.nextMessageID
		t.nextMessageID++
		t.MessageIDCount[msgID] = c
		t.logger.Info("message_id_mapping",
			"msgID", msgID,
			"message_seq", c,
		)
	}
	return c
}

func (t *RPCTracer) OnRPCSent(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
) {
	type ihaveStats struct {
		count int
		size  int
	}
	topicIHaves := make(map[string]ihaveStats)
	iWantCount := 0
	iWantSize := 0

	if rpc.GetControl() != nil {
		if rpc.GetControl().GetIhave() != nil {
			for _, hv := range rpc.GetControl().GetIhave() {
				msgIDSize := 0
				if len(hv.MessageIDs) > 0 {
					msgIDSize = len(hv.MessageIDs[0])
				}
				s := topicIHaves[hv.GetTopicID()]
				s.count += len(hv.MessageIDs)
				s.size += len(hv.MessageIDs) * msgIDSize
				topicIHaves[hv.GetTopicID()] = s
			}
		}
		if rpc.GetControl().GetIwant() != nil {
			for _, wt := range rpc.GetControl().GetIwant() {
				msgIDSize := 0
				if len(wt.MessageIDs) > 0 {
					msgIDSize = len(wt.MessageIDs[0])
				}
				iWantCount += len(wt.MessageIDs)
				iWantSize += len(wt.MessageIDs) * msgIDSize
			}
		}
	}
	t.logger.Info("rpc_sent",
		"peerID", peerID.ShortString(),
		"total_size", proto.Size(rpc),
		"iwant_count", iWantCount,
		"iwant_size", iWantSize,
		"took", duration.Milliseconds(),
	)
	for topic, s := range topicIHaves {
		t.logger.Info("topic_ihave_sent",
			"peerID", peerID.ShortString(),
			"topic", topic,
			"ihave_count", s.count,
			"ihave_size", s.size,
			"took", duration.Milliseconds(),
		)
	}

	if rpc.GetControl() != nil {
		for _, graft := range rpc.GetControl().GetGraft() {
			t.logger.Info("graft_sent",
				"peerID", peerID.ShortString(),
				"topic", graft.GetTopicID(),
			)
		}
		for _, prune := range rpc.GetControl().GetPrune() {
			t.logger.Info("prune_sent",
				"peerID", peerID.ShortString(),
				"topic", prune.GetTopicID(),
			)
		}
	}

	for _, msg := range rpc.GetPublish() {
		msgID := t.MessageIDFunc(msg)
		messageSeq := t.getMessageID(msgID)
		sz := proto.Size(msg)
		t.logger.Info("topic_message_sent",
			"message_seq", messageSeq,
			"bytes", sz,
			"peerID", peerID.ShortString(),
			"topic", msg.GetTopic(),
			"took", duration.Milliseconds(),
		)
	}

	if partial := rpc.GetPartial(); partial != nil {
		t.logger.Info("partial_sent",
			"peerID", peerID.ShortString(),
			"topic", partial.GetTopicID(),
			"partial_bytes", len(partial.GetPartialMessage()),
			"metadata_bytes", len(partial.GetPartsMetadata()),
			"took", duration.Milliseconds(),
		)
	}
}

func (t *RPCTracer) OnRPCReceived(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
) {
	type ihaveStats struct {
		count int
		size  int
	}
	topicIHaves := make(map[string]ihaveStats)
	iWantCount := 0
	iWantSize := 0

	if rpc.GetControl() != nil {
		if rpc.GetControl().GetIhave() != nil {
			for _, hv := range rpc.GetControl().GetIhave() {
				msgIDSize := 0
				if len(hv.MessageIDs) > 0 {
					msgIDSize = len(hv.MessageIDs[0])
				}
				s := topicIHaves[hv.GetTopicID()]
				s.count += len(hv.MessageIDs)
				s.size += len(hv.MessageIDs) * msgIDSize
				topicIHaves[hv.GetTopicID()] = s
			}
		}
		if rpc.GetControl().GetIwant() != nil {
			for _, wt := range rpc.GetControl().GetIwant() {
				msgIDSize := 0
				if len(wt.MessageIDs) > 0 {
					msgIDSize = len(wt.MessageIDs[0])
				}
				iWantCount += len(wt.MessageIDs)
				iWantSize += len(wt.MessageIDs) * msgIDSize
			}
		}
	}
	t.logger.Info("rpc_received",
		"peerID", peerID.ShortString(),
		"total_size", proto.Size(rpc),
		"iwant_count", iWantCount,
		"iwant_size", iWantSize,
		"took", duration.Milliseconds(),
	)
	for topic, s := range topicIHaves {
		t.logger.Info("topic_ihave_received",
			"peerID", peerID.ShortString(),
			"topic", topic,
			"ihave_count", s.count,
			"ihave_size", s.size,
			"took", duration.Milliseconds(),
		)
	}

	if rpc.GetControl() != nil {
		for _, graft := range rpc.GetControl().GetGraft() {
			t.logger.Info("graft_received",
				"peerID", peerID.ShortString(),
				"topic", graft.GetTopicID(),
			)
		}
		for _, prune := range rpc.GetControl().GetPrune() {
			t.logger.Info("prune_received",
				"peerID", peerID.ShortString(),
				"topic", prune.GetTopicID(),
			)
		}
	}

	for _, msg := range rpc.GetPublish() {
		msgID := t.MessageIDFunc(msg)
		messageSeq := t.getMessageID(msgID)
		sz := proto.Size(msg)
		t.logger.Info("topic_message_received",
			"message_seq", messageSeq,
			"bytes", sz,
			"peerID", peerID.ShortString(),
			"topic", msg.GetTopic(),
			"took", duration.Milliseconds(),
		)
	}

	if partial := rpc.GetPartial(); partial != nil {
		t.logger.Info("partial_received",
			"peerID", peerID.ShortString(),
			"topic", partial.GetTopicID(),
			"partial_bytes", len(partial.GetPartialMessage()),
			"metadata_bytes", len(partial.GetPartsMetadata()),
			"took", duration.Milliseconds(),
		)
	}
}

func (t *RPCTracer) OnPeerRTT(peerID peer.ID, topic string, rtt time.Duration, transport string, remoteAddr string) {
	t.logger.Info("mesh_peer_rtt",
		"peerID", peerID.ShortString(),
		"topic", topic,
		"smoothedRTT_ms", rtt.Milliseconds(),
		"transport", transport,
		"remote_addr", remoteAddr,
	)
}

func (t *RPCTracer) OnMeshSize(topic string, size int) {
	t.logger.Info("mesh_size", "topic", topic, "size", size)
}

func (t *RPCTracer) Close() error {
	return t.logFile.Close()
}
