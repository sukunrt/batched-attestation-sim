package node

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// GossipsubParams holds the mesh-size parameters that drive gossipsub's
// peer-selection logic. Carries yaml tags so it can be unmarshaled directly
// from the simulation config file.
type GossipsubParams struct {
	D     int `yaml:"D"`
	Dlow  int `yaml:"Dlow"`
	Dhigh int `yaml:"Dhigh"`
}

// Node is a single simulated attestor node. All fields are populated by the
// caller before Start; the Node itself does not load configuration.
type Node struct {
	Num                    int
	PublishSlots           map[int]struct{}
	NumTopics              int
	NumMessagesPerAttestor int
	BandwidthLogFrequency  time.Duration

	AttestationDataSize        int
	SignatureSize              int
	VerificationDelay          func() time.Duration
	PublishDelay               func() time.Duration
	VerificationBatchWindow    time.Duration
	PerAttestationVerification time.Duration

	DisableIHaveGossip bool
	GossipsubParams    GossipsubParams

	Host      host.Host
	Network   Network
	Tracer    Tracer
	RPCTracer pubsub.RPCTracer

	Fanout           bool
	FanoutTopicIndex int // -1 for mesh nodes (join all topics)

	// Partial messages fields
	UsePartialMessages        bool
	MaxPeersPerAttestation    int
	DivergentAttestorFraction float64
	PublishInterval           time.Duration
	IHaveGossipDegree         int
	// CommitteeSize is the wire-level capacity for attestor bitmaps and the
	// upper bound on a node's committee position. Set by the caller from
	// SimConfig.EffectiveCommitteeSize.
	CommitteeSize int

	// PublishStart is the wall-clock time at which slot 1 starts publishing.
	// Used by the partial-messages path to map slot numbers to absolute
	// timestamps for latency logging.
	PublishStart time.Time
	// SlotDuration is the duration of each simulation slot.
	SlotDuration time.Duration

	logger   *slog.Logger
	ps       *pubsub.PubSub
	verifier *batchVerifier
	topics   []*pubsub.Topic
	subs     []*pubsub.Subscription
	partial  *partialAttesattionManager
}

// Start brings the node up. ctx is used as the pubsub lifecycle context;
// cancel it (via Stop or in tests) to release pubsub's worker goroutines.
func (n *Node) Start(ctx context.Context) {
	params := pubsub.DefaultGossipSubParams()
	params.D = n.GossipsubParams.D
	params.Dlo = n.GossipsubParams.Dlow
	params.Dhi = n.GossipsubParams.Dhigh
	params.Dout = 1
	params.Dlazy = 6
	params.HeartbeatInterval = 700 * time.Millisecond
	params.FanoutTTL = 60 * time.Second
	params.HistoryLength = 6
	params.HistoryGossip = 3

	opts := []pubsub.Option{
		pubsub.WithGossipSubParams(params),
		pubsub.WithMessageIdFn(MessageIDFunc),
	}
	if n.RPCTracer != nil {
		opts = append(opts, pubsub.WithRPCTracer(n.RPCTracer))
	}

	n.verifier = newBatchVerifier(
		n.VerificationDelay,
		n.PerAttestationVerification,
		n.VerificationBatchWindow,
		slog.With("node", n.Num, "component", "verifier"),
	)
	go n.verifier.run()

	topicIndexMap := make(map[string]int, n.NumTopics)
	for i := range n.NumTopics {
		topicIndexMap[topicName(i)] = i
	}

	if n.UsePartialMessages {
		n.partial = newPartialAttestationManager(n, n.PublishStart, n.SlotDuration, topicIndexMap)
		opts = append(opts, pubsub.WithPartialMessagesExtension(n.partial.newPartialMessagesExtension()))
	}
	ps, err := pubsub.NewGossipSub(ctx, n.Host, opts...)
	if err != nil {
		log.Fatalf("create gossipsub: %v", err)
	}
	n.ps = ps

	n.logger = slog.New(
		slog.NewTextHandler(
			os.Stderr,
			&slog.HandlerOptions{
				AddSource: true,
				Level:     slog.LevelInfo,
			},
		),
	)
	n.logger = n.logger.With("node", n.Num)
	n.logger.Info("started", "fanout", n.Fanout)
}

func topicName(index int) string {
	return fmt.Sprintf("/eth2/00000000/beacon_attestation_%d/ssz_snappy", index)
}

func (n *Node) JoinTopics() {
	joinOne := func(name string, subscribe bool) {
		// Only register topic validator for non-partial mode.
		// In partial mode, validation is handled by the partial message manager.
		if !n.UsePartialMessages {
			err := n.ps.RegisterTopicValidator(name, func(_ context.Context, _ peer.ID, _ *pubsub.Message) pubsub.ValidationResult {
				n.verifier.submitAndWait(verificationItem{Attestations: []any{nil}})
				return pubsub.ValidationAccept
			})
			if err != nil {
				log.Fatalf("register validator: %v", err)
			}
		}

		var topicOpts []pubsub.TopicOpt
		if n.UsePartialMessages {
			topicOpts = append(topicOpts, pubsub.RequestPartialMessages())
		}
		topic, err := n.ps.Join(name, topicOpts...)
		if err != nil {
			log.Fatalf("join topic: %v", err)
		}
		n.topics = append(n.topics, topic)
		if subscribe {
			sub, err := topic.Subscribe(pubsub.WithBufferSize(4096))
			if err != nil {
				log.Fatalf("subscribe: %v", err)
			}
			n.subs = append(n.subs, sub)
		}
		n.logger.Info("joined topic", "topic", name)
	}

	if n.Fanout {
		joinOne(topicName(n.FanoutTopicIndex), false)
	} else {
		for i := range n.NumTopics {
			joinOne(topicName(i), true)
		}
	}
}

// ConnectToPeers dials the given node numbers and registers them as peers.
func (n *Node) ConnectToPeers(peers []int) {
	ctx := context.Background()
	var wg sync.WaitGroup
	semaCh := make(chan struct{}, 20)
	for _, peerNum := range peers {
		if peerNum <= n.Num {
			continue
		}
		semaCh <- struct{}{}
		wg.Go(func() {
			defer func() { <-semaCh }()
			peerID, err := PeerIDFromNodeNum(peerNum)
			if err != nil {
				n.logger.Error("peer ID failed", "peer", peerNum, "err", err)
				return
			}

			addr := n.Network.PeerAddr(peerNum)
			err = n.Host.Connect(ctx, peer.AddrInfo{ID: peerID, Addrs: []ma.Multiaddr{addr}})
			if err != nil {
				n.logger.Error("connect failed", "peer", peerNum, "err", err)
			} else {
				n.logger.Info("connected", "peer", peerNum)
			}
		})
	}
	wg.Wait()
}

// Run reads from the topic and logs messages received.
// In parallel, it publishes to the topic if slotNum is in
// its publishSlots; payload is AttestationDataSize + SignatureSize bytes.
func (n *Node) Run(numSlots int, slotDuration time.Duration) {
	if n.UsePartialMessages {
		n.runPartial(numSlots, slotDuration)
		return
	}
	n.runClassic(numSlots, slotDuration)
}

func (n *Node) runReceiverClassic(ctx context.Context, sub *pubsub.Subscription, topicIndex int) {
	var att pb.Attestation
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		if err := proto.Unmarshal(msg.Data, &att); err != nil {
			n.logger.Error("unmarshal failed", "err", err)
			continue
		}
		n.Tracer.OnReceive(n.Num, &att, topicIndex)
	}
}

func (n *Node) runPublisherClassic(ctx context.Context, numSlots int, slotDuration time.Duration) {
	if n.NumMessagesPerAttestor <= 0 {
		n.NumMessagesPerAttestor = 1
	}

	runSlot := func(slot int) {
		if _, ok := n.PublishSlots[slot]; !ok {
			return
		}

		expectedPublishAt := time.Now()
		if n.PublishDelay != nil {
			if delay := n.PublishDelay(); delay > 0 {
				time.Sleep(delay)
			}
		}

		for msgIdx := range n.NumMessagesPerAttestor {
			for i, topic := range n.topics {
				data := n.attestationDataForSlot(slot, topic.String())
				sig := make([]byte, n.SignatureSize)
				crand.Read(sig)
				att := n.attestationDataForSlotClassic(slot, data, sig, msgIdx, expectedPublishAt)
				buf, err := proto.Marshal(att)
				if err != nil {
					n.logger.Error("marshal failed", "slot", slot, "msg_index", msgIdx, "err", err)
					continue
				}
				if err := topic.Publish(ctx, buf); err != nil {
					n.logger.Error("publish failed", "slot", slot, "topic", i, "msg_index", msgIdx, "err", err)
				} else {
					n.Tracer.OnPublish(att, i)
				}
			}
		}
	}

	for slot := 1; slot <= numSlots; slot++ {
		n.logger.Info("starting slot", "slot", slot)

		runSlot(slot)

		slotEndTime := time.Now().Add(slotDuration)
		time.Sleep(time.Until(slotEndTime))

		n.logger.Info("slot complete", "slot", slot)
	}

}

func (n *Node) attestationDataForSlotClassic(
	slot int,
	data []byte,
	sig []byte,
	msgIdx int,
	expectedPublishAt time.Time,
) *pb.Attestation {
	return &pb.Attestation{
		NodeNum:                 int32(n.Num),
		Data:                    data,
		Signature:               sig,
		SlotNum:                 int32(slot),
		MsgIndex:                int32(msgIdx),
		PublishAtUnixMs:         time.Now().UnixMilli(),
		ExpectedPublishAtUnixMs: expectedPublishAt.UnixMilli(),
	}
}

func (n *Node) runClassic(numSlots int, slotDuration time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	for i, sub := range n.subs {
		wg.Go(func() {
			n.runReceiverClassic(ctx, sub, i)
		})
	}

	n.runPublisherClassic(ctx, numSlots, slotDuration)

	// give the receiver a moment to process in-flight messages, then stop it
	time.Sleep(slotDuration)
	n.verifier.stop()
	cancel()
	wg.Wait()
}

// attestationDataForSlot returns deterministic attestation data for (slot, index).
// index=0 is the majority view; higher indices represent divergent chain head views.
func (n *Node) attestationDataForSlot(slot int, topic string) []byte {
	idx := 0
	if rand.Float64() < n.DivergentAttestorFraction {
		idx = 1
	}
	sha := sha256.New()
	_, err := sha.Write(binary.LittleEndian.AppendUint16(nil, uint16(slot)))
	if err != nil {
		log.Fatal("sha panic")
	}
	_, err = sha.Write(binary.LittleEndian.AppendUint16(nil, uint16(idx)))
	if err != nil {
		log.Fatal("sha panic")
	}
	_, err = sha.Write([]byte(topic))
	if err != nil {
		log.Fatal("sha panic")
	}
	var buf [32]byte
	sha.Sum(buf[:0])
	r := rand.NewChaCha8(buf)
	data := make([]byte, n.AttestationDataSize)
	r.Read(data)
	return data
}

func (n *Node) runPartial(numSlots int, slotDuration time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())

	if !n.Fanout {
		var topicNames []string
		for _, t := range n.topics {
			topicNames = append(topicNames, t.String())
		}
		go n.partial.runPublishLoop(ctx, n.ps, topicNames)
	}

	for slot := 1; slot <= numSlots; slot++ {
		n.logger.Info("starting slot", "slot", slot)
		slotEndTime := time.Now().Add(slotDuration)

		if _, ok := n.PublishSlots[slot]; ok {
			n.selfPublish(slot)
		}

		time.Sleep(time.Until(slotEndTime))
		n.logger.Info("slot complete", "slot", slot)
	}

	time.Sleep(slotDuration)
	n.verifier.stop()
	cancel()
}

// selfPublish creates this node's own attestation and adds it to local
// partial state.
//
// Committee position assignment is static: position == n.Num. main.go asserts
// n.Num < CommitteeSize at startup.
func (n *Node) selfPublish(slot int) {
	sig := make([]byte, n.SignatureSize)
	crand.Read(sig)

	publishTime := time.Now()

	if n.PublishDelay != nil {
		if delay := n.PublishDelay(); delay > 0 {
			time.Sleep(delay)
		}
	}

	position := n.Num
	for _, topic := range n.topics {
		topicName := topic.String()
		data := n.attestationDataForSlot(slot, topicName)
		digest := attDigest(data)
		topicIdx := 0
		if n.partial != nil {
			topicIdx = n.partial.topicIndexMap[topicName]
		}
		if n.Fanout {
			n.partial.fanoutPublish(n.ps, topicName, slot, position, sig, data)
		} else {
			n.partial.publishLocal(topicName, slot, position, sig, data)
		}
		n.logger.Info("self_published",
			"slot", slot,
			"topic", topicIdx,
			"position", position,
			"att_digest", hex.EncodeToString(digest[:]),
			"at", publishTime.UnixMilli(),
		)
		if n.Tracer != nil {
			n.Tracer.OnPartialPublish(slot, topicIdx, position, digest)
		}
	}
}

// ReportBandwidth logs per-interval QUIC byte counts summed across all
// connections. Intended to be run in its own goroutine; returns when ctx is
// canceled.
func (n *Node) ReportBandwidth(ctx context.Context, freq time.Duration) {
	ticker := time.NewTicker(freq)
	defer ticker.Stop()
	var prevSent, prevRecv int
	for {
		select {
		case <-ticker.C:
			var sent, recv int
			var qc *quic.Conn
			for _, c := range n.Host.Network().Conns() {
				if ok := c.As(&qc); ok {
					s := qc.ConnectionStats()
					sent += int(s.BytesSent)
					recv += int(s.BytesReceived)
				}
			}
			ds, dr := sent-prevSent, recv-prevRecv
			prevSent, prevRecv = sent, recv
			sbps := int(float64(ds*8) / freq.Seconds())
			rbps := int(float64(dr*8) / freq.Seconds())
			if sbps < 10 && rbps < 10 {
				continue
			}
			n.logger.Info("bandwidth",
				"sentbps", sbps, "receivedbps", rbps,
				"sentBytesTotal", sent, "receivedBytesTotal", recv)
		case <-ctx.Done():
			return
		}
	}
}
