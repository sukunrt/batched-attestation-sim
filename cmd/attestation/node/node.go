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
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/simlab/cmd/attestation/node/attprop"
	"github.com/ethp2p/simlab/cmd/attestation/pb"
	"github.com/ethp2p/simlab/cmd/attestation/verify"
)

// GossipsubParams holds the mesh-size parameters that drive gossipsub's
// peer-selection logic. Carries yaml tags so it can be unmarshaled directly
// from the simulation config file.
type GossipsubParams struct {
	D     int `yaml:"D"`
	Dlow  int `yaml:"Dlow"`
	Dhigh int `yaml:"Dhigh"`
}

// TopicMembership is one entry in a node's committee membership: the topic
// index plus this node's committee position within that topic. Position is
// in [0, num_attestors).
type TopicMembership struct {
	TopicIndex int
	Position   int
}

// Node is a single simulated attestor node. All fields are populated by the
// caller before Start; the Node itself does not load configuration.
type Node struct {
	Num                   int
	PublishSlots          map[int]struct{}
	NumTopics             int
	BandwidthLogFrequency time.Duration

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

	Fanout bool

	// CommitteeMemberships lists the (topic, position) pairs this node is a
	// committee member of. Fanout nodes have exactly one entry (their assigned
	// topic). Mesh nodes have zero or more entries. Empty = pure relay node.
	CommitteeMemberships []TopicMembership

	// Partial messages fields
	UsePartialMessages        bool
	MaxPeersPerAttestation    int
	DivergentAttestorFraction float64
	PublishInterval           time.Duration

	// PartialPriorityMode selects the partial-priority forwarding strategy
	// (size-capped, least-forwarded-first) instead of the default partial
	// push. MaxAttestationsPerMessage caps attestations per outgoing data
	// message (0 = default 30). Only meaningful when partial messages are on.
	PartialPriorityMode       bool
	MaxAttestationsPerMessage int
	// SendAvailableWithData piggybacks our validated available_ids delta onto a
	// data message to each mesh peer per tick (partial-priority only), so peers
	// learn our state and stop forwarding duplicates back. Never sent without
	// data, so the receiver does not misclassify us as a gossip peer.
	SendAvailableWithData bool
	// CommitteeSize is the wire-level committee-position capacity. All
	// committees share the same size (= num_attestors per topic).
	CommitteeSize int

	// PublishStart is the wall-clock time at which slot 1 starts publishing.
	// Used by the partial-messages path to map slot numbers to absolute
	// timestamps for latency logging.
	PublishStart time.Time
	// SlotDuration is the duration of each simulation slot.
	SlotDuration time.Duration

	// AttPropagation selects the native att_propagation protocol (no gossipsub).
	// AttProp holds its tunables. Mutually exclusive with the partial-message
	// modes — main.go rejects the combination.
	AttPropagation bool
	AttProp        AttPropParams

	logger          *slog.Logger
	ps              *pubsub.PubSub
	verifier        *verify.Verifier
	topics          []*pubsub.Topic
	subs            []*pubsub.Subscription
	partial         *partialAttestationManager
	partialPriority *priorityAttestationManager
	attProp         map[string]*attprop.Manager
	attPropPeers    []peer.ID
}

// AttPropParams are the att_propagation mesh/send tunables (spec §C1/§D2/§F),
// resolved with defaults by the config layer and copied into attprop.Config in
// Start. Kept as a Node-level struct so the construction literal stays compact.
type AttPropParams struct {
	PushDlow, PushD, PushDhigh                                         int
	BitmapDlow, BitmapD, BitmapDhigh                                   int
	SendBudgetB, MaxAttsPerMessage, MaxPeersPerAtt                     int
	TickInterval, BitmapFloorInterval, HeartbeatInterval, PruneBackoff time.Duration
}

// partialManager is the runtime surface both partial-message strategies share.
// Its methods don't expose the per-peer generic state, so a single dispatch
// point routes the publish loop, fanout, self-publish, and extension install to
// whichever strategy is active. Test helpers still reach the concrete managers
// (n.partial / n.partialPriority) for their internal state.
type partialManager interface {
	runPublishLoop(ctx interface{ Done() <-chan struct{} }, ps *pubsub.PubSub, topics []string)
	fanoutPublish(ps *pubsub.PubSub, topic string, slot, position int, signature, data []byte)
	publishLocal(topic string, slot, position int, sig, data []byte)
	newPartialMessagesExtension() *partialmessages.PartialMessagesExtension[peerState]
	SlotStart(slot int)
	SlotEnd(slot int)
}

// partialModeActive reports whether either partial-message strategy is enabled.
func (n *Node) partialModeActive() bool {
	return n.UsePartialMessages || n.PartialPriorityMode
}

// activePartialManager returns the partial-message strategy in use, or nil when
// neither is enabled.
func (n *Node) activePartialManager() partialManager {
	if n.partialPriority != nil {
		return n.partialPriority
	}
	if n.partial != nil {
		return n.partial
	}
	return nil
}

// Start brings the node up. ctx is used as the pubsub lifecycle context;
// cancel it (via Stop or in tests) to release pubsub's worker goroutines.
func (n *Node) Start(ctx context.Context) {
	if n.AttPropagation {
		n.startAttProp(ctx)
		return
	}

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
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		// prysm defaults.
		pubsub.WithPeerOutboundQueueSize(1000),
		pubsub.WithValidateQueueSize(600),
	}
	if n.RPCTracer != nil {
		opts = append(opts, pubsub.WithRPCTracer(n.RPCTracer))
	}
	dropTracer := newDropTracer(fmt.Sprintf("node%d", n.Num))
	if dropTracer != nil {
		opts = append(opts, pubsub.WithRawTracer(dropTracer))
	}

	n.verifier = verify.New(
		n.VerificationDelay,
		n.PerAttestationVerification,
		n.VerificationBatchWindow,
		slog.With("node", n.Num, "component", "verifier"),
	)
	go n.verifier.Run()

	topicIndexMap := make(map[string]int, n.NumTopics)
	for i := range n.NumTopics {
		topicIndexMap[topicName(i)] = i
	}

	// partial-priority takes precedence: it is a drop-in alternative strategy
	// over the same libp2p partial-messages extension.
	switch {
	case n.PartialPriorityMode:
		n.partialPriority = newPriorityAttestationManager(n, n.PublishStart, n.SlotDuration, topicIndexMap)
	case n.UsePartialMessages:
		n.partial = newPartialAttestationManager(n, n.PublishStart, n.SlotDuration, topicIndexMap)
	}
	if pm := n.activePartialManager(); pm != nil {
		opts = append(opts, pubsub.WithPartialMessagesExtension(pm.newPartialMessagesExtension()))
	}
	ps, err := pubsub.NewGossipSub(ctx, n.Host, opts...)
	if err != nil {
		log.Fatalf("create gossipsub: %v", err)
	}
	n.ps = ps

	if dropTracer != nil {
		dropTracer.start(ctx)
	}

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

// startAttProp brings the node up in att_propagation mode: build one manager per
// topic and register each manager's stream handlers. No gossipsub is created and
// no topics are joined — the native protocol replaces both.
func (n *Node) startAttProp(ctx context.Context) {
	n.logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	})).With("node", n.Num)

	p := n.AttProp
	n.attProp = make(map[string]*attprop.Manager, n.NumTopics)
	for topicIdx := range n.NumTopics {
		name := topicName(topicIdx)
		v := verify.New(
			n.VerificationDelay,
			n.PerAttestationVerification,
			n.VerificationBatchWindow,
			slog.With("node", n.Num, "component", "verifier", "topic", topicIdx),
		)
		go v.Run()
		cfg := attprop.Config{
			Logger:              slog.With("node", n.Num, "component", "attprop", "topic", topicIdx),
			NodeNum:             n.Num,
			Topic:               name,
			TopicIndex:          topicIdx,
			CommitteeSize:       n.CommitteeSize,
			PublishStart:        n.PublishStart,
			SlotDuration:        n.SlotDuration,
			Fanout:              n.Fanout,
			PushDlow:            p.PushDlow,
			PushD:               p.PushD,
			PushDhigh:           p.PushDhigh,
			BitmapDlow:          p.BitmapDlow,
			BitmapD:             p.BitmapD,
			BitmapDhigh:         p.BitmapDhigh,
			SendBudgetB:         p.SendBudgetB,
			MaxAttsPerMessage:   p.MaxAttsPerMessage,
			MaxPeersPerAtt:      p.MaxPeersPerAtt,
			TickInterval:        p.TickInterval,
			BitmapFloorInterval: p.BitmapFloorInterval,
			HeartbeatInterval:   p.HeartbeatInterval,
			PruneBackoff:        p.PruneBackoff,
		}
		m := attprop.New(n.Host, v, n.attPropTracer(), cfg)
		m.Start(ctx)
		// Run each topic eventloop under the node's lifecycle context, not a
		// short-lived one scoped to Run(): it must outlive Run so post-run reads
		// in tests still reach the eventloop. Cancelling ctx tears it down.
		go m.Run(ctx)
		if topicIdx == 0 && n.BandwidthLogFrequency > 0 {
			go m.ReportBandwidth(ctx, n.BandwidthLogFrequency)
		}
		context.AfterFunc(ctx, v.Stop)
		n.attProp[name] = m
	}

	n.logger.Info("started", "fanout", n.Fanout, "mode", "att_propagation")
}

// attPropTracer returns the receive-latency sink for attprop. n.Tracer
// satisfies attprop.Tracer structurally; a nil Tracer yields a no-op so attprop
// never nil-derefs.
func (n *Node) attPropTracer() attprop.Tracer {
	if n.Tracer == nil {
		return noopAttPropTracer{}
	}
	return n.Tracer
}

// noopAttPropTracer discards receive events (used when Node.Tracer is nil).
type noopAttPropTracer struct{}

func (noopAttPropTracer) OnPartialReceive(_, _, _ int, _ []byte, _ int64) {}

func (n *Node) JoinTopics() {
	if n.AttPropagation {
		// Opening attprop streams is equivalent to joining the topic: handlers were
		// installed in Start, but outbound streams wait for the startup jitter.
		if !n.Fanout {
			for _, peerID := range n.attPropPeers {
				for _, m := range n.attProp {
					m.ConnectPeer(peerID)
				}
			}
		}
		return
	}
	joinOne := func(name string, subscribe bool) {
		// Only register topic validator for non-partial mode.
		// In partial mode, validation is handled by the partial message manager.
		if !n.partialModeActive() {
			err := n.ps.RegisterTopicValidator(name, func(_ context.Context, _ peer.ID, _ *pubsub.Message) pubsub.ValidationResult {
				n.verifier.SubmitAndWait(verify.Item{Attestations: []any{nil}})
				return pubsub.ValidationAccept
			})
			if err != nil {
				log.Fatalf("register validator: %v", err)
			}
		}

		var topicOpts []pubsub.TopicOpt
		if n.partialModeActive() {
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
		joinOne(topicName(n.CommitteeMemberships[0].TopicIndex), false)
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
	var mu sync.Mutex
	var attPropPeers []peer.ID
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
				return
			}
			n.logger.Info("connected", "peer", peerNum)
			if n.AttPropagation {
				mu.Lock()
				attPropPeers = append(attPropPeers, peerID)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if n.AttPropagation {
		n.attPropPeers = append(n.attPropPeers, attPropPeers...)
	}
}

// Run reads from the topic and logs messages received.
// In parallel, it publishes to the topic if slotNum is in
// its publishSlots; payload is AttestationDataSize + SignatureSize bytes.
func (n *Node) Run(numSlots int, slotDuration time.Duration) {
	if n.AttPropagation {
		n.runAttProp(numSlots, slotDuration)
		return
	}
	if n.partialModeActive() {
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
		latencyMs := time.Since(n.slotStartTime(int(att.SlotNum))).Milliseconds()
		n.Tracer.OnReceive(n.Num, &att, topicIndex, latencyMs)
	}
}

func (n *Node) runPublisherClassic(ctx context.Context, numSlots int, slotDuration time.Duration) {
	runSlot := func(slot int) {
		if _, ok := n.PublishSlots[slot]; !ok {
			return
		}

		if n.PublishDelay != nil {
			if delay := n.PublishDelay(); delay > 0 {
				time.Sleep(delay)
			}
		}

		for i, topic := range n.topics {
			data := n.attestationDataForSlot(slot, topic.String())
			sig := make([]byte, n.SignatureSize)
			crand.Read(sig)
			att := n.attestationDataForSlotClassic(slot, data, sig)
			buf, err := proto.Marshal(att)
			if err != nil {
				n.logger.Error("marshal failed", "slot", slot, "err", err)
				continue
			}
			if err := topic.Publish(ctx, buf); err != nil {
				n.logger.Error("publish failed", "slot", slot, "topic", i, "err", err)
			} else {
				n.Tracer.OnPublish(att, i)
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

func (n *Node) attestationDataForSlotClassic(slot int, data, sig []byte) *pb.Attestation {
	return &pb.Attestation{
		NodeNum:   int32(n.Num),
		Data:      data,
		Signature: sig,
		SlotNum:   int32(slot),
	}
}

// slotStartTime is the nominal wall-clock start of a slot — the reference point
// for time-to-receive, matching partial.go's slotStartTime.
func (n *Node) slotStartTime(slot int) time.Time {
	return n.PublishStart.Add(time.Duration(slot-1) * n.SlotDuration)
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
	n.verifier.Stop()
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
		go n.activePartialManager().runPublishLoop(ctx, n.ps, topicNames)
	}

	for slot := 1; slot <= numSlots; slot++ {
		n.logger.Info("starting slot", "slot", slot)
		n.activePartialManager().SlotStart(slot)
		slotEndTime := time.Now().Add(slotDuration)

		if _, ok := n.PublishSlots[slot]; ok {
			n.selfPublish(slot)
		}

		time.Sleep(time.Until(slotEndTime))
		n.activePartialManager().SlotEnd(slot)
		n.logger.Info("slot complete", "slot", slot)
	}

	time.Sleep(slotDuration)
	n.verifier.Stop()
	cancel()
}

// selfPublish creates this node's own attestation and adds it to local
// partial state. Iterates the node's committee memberships; each entry
// supplies the topic and the node's committee position within that topic.
func (n *Node) selfPublish(slot int) {
	sig := make([]byte, n.SignatureSize)
	crand.Read(sig)

	publishTime := time.Now()

	if n.PublishDelay != nil {
		if delay := n.PublishDelay(); delay > 0 {
			time.Sleep(delay)
		}
	}

	for _, m := range n.CommitteeMemberships {
		topicName := topicName(m.TopicIndex)
		data := n.attestationDataForSlot(slot, topicName)
		digest := attDigest(data)
		if n.Fanout {
			n.activePartialManager().fanoutPublish(n.ps, topicName, slot, m.Position, sig, data)
		} else {
			n.activePartialManager().publishLocal(topicName, slot, m.Position, sig, data)
		}
		n.logger.Info("self_published",
			"slot", slot,
			"topic", m.TopicIndex,
			"position", m.Position,
			"att_digest", hex.EncodeToString(digest[:]),
			"at", publishTime.UnixMilli(),
		)
		if n.Tracer != nil {
			n.Tracer.OnPartialPublish(slot, m.TopicIndex, m.Position, data)
		}
	}
}

// runAttProp drives the node in att_propagation mode. It mirrors runPartial:
// launch the Manager eventloop (or, for a fanout node, just its reset
// handlers), publish this node's own attestation per committee membership each
// publish slot, then drain and stop. The mode shares partial mode's slot
// timing and self-publish log keys (§H2) so analysis parsing is unchanged.
func (n *Node) runAttProp(numSlots int, slotDuration time.Duration) {
	// The eventloop and verifier are started in startAttProp under the node's
	// lifecycle context and torn down when it is cancelled, so this only drives
	// the publish schedule and then drains.
	for slot := 1; slot <= numSlots; slot++ {
		n.logger.Info("starting slot", "slot", slot)
		for _, m := range n.attProp {
			m.SlotStart(slot)
		}
		slotEndTime := time.Now().Add(slotDuration)

		if _, ok := n.PublishSlots[slot]; ok {
			n.selfPublishAttProp(slot)
		}

		time.Sleep(time.Until(slotEndTime))
		for _, m := range n.attProp {
			m.SlotEnd(slot)
		}
		n.logger.Info("slot complete", "slot", slot)
	}

	time.Sleep(slotDuration)
}

// selfPublishAttProp injects this node's own attestation into attprop for each
// committee membership. Fanout nodes leaf-inject via FanoutPublish; mesh nodes
// add to local validated state via PublishLocal. Mirrors selfPublish.
func (n *Node) selfPublishAttProp(slot int) {
	sig := make([]byte, n.SignatureSize)
	crand.Read(sig)

	publishTime := time.Now()
	if n.PublishDelay != nil {
		if delay := n.PublishDelay(); delay > 0 {
			time.Sleep(delay)
		}
	}

	for _, m := range n.CommitteeMemberships {
		name := topicName(m.TopicIndex)
		data := n.attestationDataForSlot(slot, name)
		digest := attDigest(data)
		if n.Fanout {
			n.attProp[name].FanoutPublish(slot, m.Position, sig, data)
		} else {
			n.attProp[name].PublishLocal(slot, m.Position, sig, data)
		}
		n.logger.Info("self_published",
			"slot", slot,
			"topic", m.TopicIndex,
			"position", m.Position,
			"att_digest", hex.EncodeToString(digest[:]),
			"at", publishTime.UnixMilli(),
		)
		if n.Tracer != nil {
			n.Tracer.OnPartialPublish(slot, m.TopicIndex, m.Position, data)
		}
	}
}

// ReportBandwidth logs per-interval QUIC byte counts summed across all
// connections. Intended to be run in its own goroutine; returns when ctx is
// canceled.
func (n *Node) ReportBandwidth(ctx context.Context, freq time.Duration) {
	if n.AttPropagation {
		return
	}
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
