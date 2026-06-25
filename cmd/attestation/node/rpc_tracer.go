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
	gproto "google.golang.org/protobuf/proto"

	attpb "github.com/ethp2p/simlab/cmd/attestation/pb"
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

// classicAttStats decodes a classic-mode Attestation payload (the Data field of
// a gossipsub message) and returns the attestation_data and signature byte
// counts plus the att_digest. Best-effort: on decode failure it returns zero
// values and an empty digest so the caller still emits a stable log line.
func classicAttStats(data []byte) (attDataBytes, sigBytes int, digest string) {
	var att attpb.Attestation
	if err := gproto.Unmarshal(data, &att); err != nil {
		return 0, 0, ""
	}
	return len(att.GetData()), len(att.GetSignature()), attDigestHex(att.GetData())
}

// partialDataStats decodes a BatchedAttestationEnvelope (the partial-message
// data blob) and returns the number of batches, the attestations conveyed (one
// per signature), and the attestation_data and signature byte counts. The
// attestation_data byte count is summed once per batch, so it reflects the
// deduplicated cost partial mode pays versus classic. Best-effort on failure.
func partialDataStats(blob []byte) (
	batches int,
	attCount int,
	attDataBytes int,
	attDataHashBytes int,
	sigBytes int,
) {
	if len(blob) == 0 {
		return 0, 0, 0, 0, 0
	}
	var env attpb.BatchedAttestationEnvelope
	if err := gproto.Unmarshal(blob, &env); err != nil {
		return 0, 0, 0, 0, 0
	}
	batches = len(env.GetBatches())
	for _, b := range env.GetBatches() {
		attDataBytes += len(b.GetAttestationData())
		attDataHashBytes += len(b.GetAttestationDataHash())
		sigs := b.GetSignatures()
		attCount += len(sigs)
		for _, s := range sigs {
			sigBytes += len(s)
		}
	}
	return batches, attCount, attDataBytes, attDataHashBytes, sigBytes
}

// partialMetaStats decodes a ControlEnvelope (the partial-message metadata blob)
// and returns the number of per-bucket metadata entries plus the total
// available/requests bits advertised across them. Best-effort on failure.
func partialMetaStats(blob []byte) (mdCount, availOnes, reqOnes, attDataHashBytes int) {
	if len(blob) == 0 {
		return 0, 0, 0, 0
	}
	var ctrl attpb.ControlEnvelope
	if err := gproto.Unmarshal(blob, &ctrl); err != nil {
		return 0, 0, 0, 0
	}
	mdCount = len(ctrl.GetMetadatas())
	for _, md := range ctrl.GetMetadatas() {
		availOnes += availableOnes(md.GetAvailable())
		reqOnes += requestsOnes(md.GetRequests())
		attDataHashBytes += len(md.GetAttestationDataHash())
	}
	return mdCount, availOnes, reqOnes, attDataHashBytes
}

// OnRPCSent dispatches an outgoing RPC to the classic-gossipsub and
// partial-message loggers. The two cover disjoint parts of the RPC (publishes
// vs the partial extension payload), so both run unconditionally regardless of
// the node's mode.
func (t *RPCTracer) OnRPCSent(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
) {
	t.onClassicRPCSent(peerID, duration, rpc)
	t.onPartialRPCSent(peerID, duration, rpc)
}

// onClassicRPCSent logs the classic-gossipsub portion of an outgoing RPC: the
// rpc_sent summary, IHAVE/IWANT control, GRAFT/PRUNE control, and full
// topic_message_sent publishes. IHAVE/IWANT gossip is shared with
// partial-message mode, so this runs for every RPC.
func (t *RPCTracer) onClassicRPCSent(
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
		"peer", shortPeer(peerID),
		"total_size", proto.Size(rpc),
		"iwant_count", iWantCount,
		"iwant_size", iWantSize,
		"took", duration.Milliseconds(),
	)
	for topic, s := range topicIHaves {
		t.logger.Info("topic_ihave_sent",
			"peer", shortPeer(peerID),
			"topic", topic,
			"ihave_count", s.count,
			"ihave_size", s.size,
			"took", duration.Milliseconds(),
		)
	}

	if rpc.GetControl() != nil {
		for _, graft := range rpc.GetControl().GetGraft() {
			t.logger.Info("graft_sent",
				"peer", shortPeer(peerID),
				"topic", graft.GetTopicID(),
			)
		}
		for _, prune := range rpc.GetControl().GetPrune() {
			t.logger.Info("prune_sent",
				"peer", shortPeer(peerID),
				"topic", prune.GetTopicID(),
			)
		}
	}

	for _, msg := range rpc.GetPublish() {
		msgID := t.MessageIDFunc(msg)
		messageSeq := t.getMessageID(msgID)
		sz := proto.Size(msg)
		attDataBytes, sigBytes, digest := classicAttStats(msg.GetData())
		t.logger.Info("topic_message_sent",
			"message_seq", messageSeq,
			"bytes", sz,
			"att_count", 1,
			"att_data_bytes", attDataBytes,
			"sig_bytes", sigBytes,
			"att_digest", digest,
			"peer", shortPeer(peerID),
			"topic", msg.GetTopic(),
			"took", duration.Milliseconds(),
		)
	}
}

// onPartialRPCSent logs the partial-message extension payload of an outgoing
// RPC (the partial_sent line). It is a no-op for classic-mode RPCs, which carry
// no partial payload.
func (t *RPCTracer) onPartialRPCSent(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
) {
	partial := rpc.GetPartial()
	if partial == nil {
		return
	}
	dataBatches, attCount, attDataBytes, attDataHashBytes, sigBytes := partialDataStats(partial.GetPartialMessage())
	metaCount, availOnes, reqOnes, metaHashBytes := partialMetaStats(partial.GetPartsMetadata())
	t.logger.Info("partial_sent",
		"peer", shortPeer(peerID),
		"topic", partial.GetTopicID(),
		"partial_bytes", len(partial.GetPartialMessage()),
		"metadata_bytes", len(partial.GetPartsMetadata()),
		"data_batches", dataBatches,
		"att_count", attCount,
		"att_data_bytes", attDataBytes,
		"att_data_hash_bytes", attDataHashBytes,
		"sig_bytes", sigBytes,
		"meta_count", metaCount,
		"metadata_att_data_hash_bytes", metaHashBytes,
		"available_ones", availOnes,
		"requests_ones", reqOnes,
		"took", duration.Milliseconds(),
	)
}

// OnRPCReceived dispatches an incoming RPC to the classic-gossipsub and
// partial-message loggers. The two cover disjoint parts of the RPC (publishes
// vs the partial extension payload), so both run unconditionally regardless of
// the node's mode.
func (t *RPCTracer) OnRPCReceived(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
) {
	t.onClassicRPCReceived(peerID, duration, rpc)
	t.onPartialRPCReceived(peerID, duration, rpc)
}

// onClassicRPCReceived logs the classic-gossipsub portion of an incoming RPC:
// the rpc_received summary, IHAVE/IWANT control, GRAFT/PRUNE control, and full
// topic_message_received publishes. IHAVE/IWANT gossip is shared with
// partial-message mode, so this runs for every RPC.
func (t *RPCTracer) onClassicRPCReceived(
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
		"peer", shortPeer(peerID),
		"total_size", proto.Size(rpc),
		"iwant_count", iWantCount,
		"iwant_size", iWantSize,
		"took", duration.Milliseconds(),
	)
	for topic, s := range topicIHaves {
		t.logger.Info("topic_ihave_received",
			"peer", shortPeer(peerID),
			"topic", topic,
			"ihave_count", s.count,
			"ihave_size", s.size,
			"took", duration.Milliseconds(),
		)
	}

	if rpc.GetControl() != nil {
		for _, graft := range rpc.GetControl().GetGraft() {
			t.logger.Info("graft_received",
				"peer", shortPeer(peerID),
				"topic", graft.GetTopicID(),
			)
		}
		for _, prune := range rpc.GetControl().GetPrune() {
			t.logger.Info("prune_received",
				"peer", shortPeer(peerID),
				"topic", prune.GetTopicID(),
			)
		}
	}

	for _, msg := range rpc.GetPublish() {
		msgID := t.MessageIDFunc(msg)
		messageSeq := t.getMessageID(msgID)
		sz := proto.Size(msg)
		attDataBytes, sigBytes, digest := classicAttStats(msg.GetData())
		t.logger.Info("topic_message_received",
			"message_seq", messageSeq,
			"bytes", sz,
			"att_count", 1,
			"att_data_bytes", attDataBytes,
			"sig_bytes", sigBytes,
			"att_digest", digest,
			"peer", shortPeer(peerID),
			"topic", msg.GetTopic(),
			"took", duration.Milliseconds(),
		)
	}
}

// onPartialRPCReceived logs the partial-message extension payload of an incoming
// RPC (the partial_received line). It is a no-op for classic-mode RPCs, which
// carry no partial payload.
func (t *RPCTracer) onPartialRPCReceived(
	peerID peer.ID,
	duration time.Duration,
	rpc *pb.RPC,
) {
	partial := rpc.GetPartial()
	if partial == nil {
		return
	}
	dataBatches, attCount, attDataBytes, attDataHashBytes, sigBytes := partialDataStats(partial.GetPartialMessage())
	metaCount, availOnes, reqOnes, metaHashBytes := partialMetaStats(partial.GetPartsMetadata())
	t.logger.Info("partial_received",
		"peer", shortPeer(peerID),
		"topic", partial.GetTopicID(),
		"partial_bytes", len(partial.GetPartialMessage()),
		"metadata_bytes", len(partial.GetPartsMetadata()),
		"data_batches", dataBatches,
		"att_count", attCount,
		"att_data_bytes", attDataBytes,
		"att_data_hash_bytes", attDataHashBytes,
		"sig_bytes", sigBytes,
		"meta_count", metaCount,
		"metadata_att_data_hash_bytes", metaHashBytes,
		"available_ones", availOnes,
		"requests_ones", reqOnes,
		"took", duration.Milliseconds(),
	)
}

func (t *RPCTracer) OnPeerRTT(peerID peer.ID, topic string, rtt time.Duration, transport string, remoteAddr string) {
	t.logger.Info("mesh_peer_rtt",
		"peer", shortPeer(peerID),
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
