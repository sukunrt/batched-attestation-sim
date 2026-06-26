package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
	"go.uber.org/fx"

	"github.com/ethp2p/simlab/cmd/attestation/node"
)

const listenPort = 8000
const minJoinDelay = 5 * time.Second

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	var (
		configFile           = flag.String("config-file", "", "Path to YAML config file")
		nodeNum              = flag.Int("node-num", 0, "Node number")
		publishSlotsStr      = flag.String("publish-slots", "", "Comma-separated slot numbers to publish in")
		peerNumsStr          = flag.String("peer-nums", "", "Comma-separated peer node numbers")
		publishMode          = flag.String("publish-mode", "mesh", "Publish mode: mesh or fanout")
		disableIHaveGossip   = flag.Bool("disable-ihave-gossip", false, "Disable IHAVE gossip")
		committeeMemberships = flag.String("committee-memberships", "", "Semicolon-separated topic:position pairs, e.g. 0:7;1:42")
		usePartialMessages   = flag.Bool("use-partial-messages", false, "Use partial messages (list of attestor IDs + ephemeral iwant)")
		partialPriority      = flag.Bool("partial-priority", false, "Use partial-priority forwarding (size-capped, least-forwarded-first)")
		maxAttsPerMessage    = flag.Int("max-attestations-per-message", 0, "Max attestations per outgoing partial-priority/att-propagation data message (0 = default)")
		sendAvailWithData    = flag.Bool("send-available-with-data", false, "Piggyback our validated bitmap onto the first data message to each mesh peer per tick (partial-priority only)")
		attPropagation       = flag.Bool("att-propagation", false, "Use the att_propagation native protocol (no gossipsub; mutually exclusive with partial modes)")
		attPropPushDlow      = flag.Int("attprop-push-dlow", 0, "att_propagation push Dlow (0 = config/default)")
		attPropPushD         = flag.Int("attprop-push-d", 0, "att_propagation push D (0 = config/default)")
		attPropPushDhigh     = flag.Int("attprop-push-dhigh", 0, "att_propagation push Dhigh (0 = config/default)")
		attPropBitmapDlow    = flag.Int("attprop-bitmap-dlow", 0, "att_propagation bitmap Dlow (0 allowed)")
		attPropBitmapD       = flag.Int("attprop-bitmap-d", 0, "att_propagation bitmap D (0 allowed)")
		attPropBitmapDhigh   = flag.Int("attprop-bitmap-dhigh", 0, "att_propagation bitmap Dhigh (0 allowed)")
		attPropPushBitmap    = flag.Bool("attprop-enable-push-mesh-bitmap", false, "Send att_propagation bitmaps to push-mesh peers on every push tick")
		gossipsubParams      = flag.String("gossipsub-params", "", "Per-node gossipsub mesh override, e.g. Dlow:8,D:12,Dhigh:16; empty uses the config value")
	)
	flag.Parse()
	flagsSet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		flagsSet[f.Name] = true
	})

	if *configFile == "" {
		log.Fatal("-config-file is required")
	}
	sim, err := LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if *gossipsubParams != "" {
		p, err := parseGossipsubParams(*gossipsubParams)
		if err != nil {
			log.Fatalf("gossipsub-params: %v", err)
		}
		sim.GossipsubParams = p
	}
	if *maxAttsPerMessage > 0 {
		sim.MaxAttestationsPerMessage = *maxAttsPerMessage
	}
	applyPositive := func(flagVal *int, dst *int) {
		if *flagVal > 0 {
			*dst = *flagVal
		}
	}
	applyPositive(attPropPushDlow, &sim.AttPropPushDlow)
	applyPositive(attPropPushD, &sim.AttPropPushD)
	applyPositive(attPropPushDhigh, &sim.AttPropPushDhigh)
	if flagsSet["attprop-bitmap-dlow"] {
		sim.AttPropBitmapDlow = *attPropBitmapDlow
	}
	if flagsSet["attprop-bitmap-d"] {
		sim.AttPropBitmapD = *attPropBitmapD
	}
	if flagsSet["attprop-bitmap-dhigh"] {
		sim.AttPropBitmapDhigh = *attPropBitmapDhigh
	}
	if *attPropPushBitmap {
		sim.EnablePushMeshBitmap = true
	}

	publishSlots := make(map[int]struct{})
	for _, s := range parseIntList(*publishSlotsStr) {
		publishSlots[s] = struct{}{}
	}

	publishStart := time.Now().Add(2 * time.Minute)

	numTopics := sim.NumTopics
	if numTopics <= 0 {
		numTopics = 1
	}

	usePartial := *usePartialMessages || sim.UsePartialMessages
	usePriority := *partialPriority || sim.PartialPriorityMode
	useAttProp := *attPropagation || sim.AttPropagation
	sendAvailWithDataOn := *sendAvailWithData || sim.SendAvailableWithData
	if err := checkModeExclusion(usePartial, usePriority, useAttProp); err != nil {
		log.Fatalf("mode config: %v", err)
	}
	memberships := parseCommitteeMemberships(*committeeMemberships)
	for _, m := range memberships {
		if m.TopicIndex < 0 || m.TopicIndex >= numTopics {
			log.Fatalf("committee membership topic %d out of range [0, %d)", m.TopicIndex, numTopics)
		}
		if m.Position < 0 || m.Position >= sim.NumAttestors {
			log.Fatalf("committee membership position %d out of range [0, num_attestors=%d)", m.Position, sim.NumAttestors)
		}
	}

	n := &node.Node{
		Num:                        *nodeNum,
		PublishSlots:               publishSlots,
		NumTopics:                  numTopics,
		CommitteeMemberships:       memberships,
		Fanout:                     *publishMode == "fanout",
		DisableIHaveGossip:         *disableIHaveGossip,
		GossipsubParams:            sim.GossipsubParams,
		VerificationDelay:          sim.ValidationDelayFunc(),
		PublishDelay:               sim.PublishDelayFunc(),
		BandwidthLogFrequency:      sim.BandwidthLogFrequency(),
		Host:                       newShadowHost(*nodeNum),
		Network:                    &shadowNetwork{},
		Tracer:                     node.NewSlogTracer(*nodeNum),
		RPCTracer:                  node.NewStderrRPCTracer(fmt.Sprintf("node%d", *nodeNum), node.MessageIDFunc),
		UsePartialMessages:         usePartial,
		PartialPriorityMode:        usePriority,
		MaxAttestationsPerMessage:  sim.EffectiveMaxAttestationsPerMessage(),
		SendAvailableWithData:      sendAvailWithDataOn,
		AttestationDataSize:        sim.AttestationDataSize,
		SignatureSize:              sim.SignatureSize,
		MaxPeersPerAttestation:     sim.EffectiveMaxPeersPerAttestation(),
		DivergentAttestorFraction:  sim.DivergentAttestorFraction,
		PublishInterval:            sim.PublishInterval(),
		VerificationBatchWindow:    sim.ValidationBatchWindow(),
		PerAttestationVerification: sim.PerAttestationValidation(),
		CommitteeSize:              sim.NumAttestors,
		PublishStart:               publishStart,
		SlotDuration:               sim.SlotDuration(),
		AttPropagation:             useAttProp,
		AttProp:                    sim.AttPropConfig(),
	}

	// add some jitter so that all nodes aren't doing heartbeat in sync
	time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
	n.Start(context.Background())

	// Start gossipsub/partial bandwidth reporting as early as possible (right
	// after host is created). Attprop bypasses gossipsub, so its package owns
	// the equivalent reporter.
	if freq := n.BandwidthLogFrequency; freq > 0 && !useAttProp {
		go n.ReportBandwidth(context.Background(), freq)
	}

	// Join topic AFTER connecting to peers
	n.ConnectToPeers(parseIntList(*peerNumsStr))
	slog.With("node", *nodeNum).Info("all peers connected")

	time.Sleep(minJoinDelay + time.Duration(rand.IntN(30000))*time.Millisecond)
	n.JoinTopics()

	time.Sleep(time.Until(publishStart))
	n.Run(sim.NumSlots, sim.SlotDuration())

	time.Sleep(30 * time.Second)
}

// parseCommitteeMemberships parses a "t0:p0;t1:p1;..." string into
// TopicMembership entries. Empty input returns nil.
func parseCommitteeMemberships(s string) []node.TopicMembership {
	if s == "" {
		return nil
	}
	var out []node.TopicMembership
	for entry := range strings.SplitSeq(s, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		topicStr, posStr, ok := strings.Cut(entry, ":")
		if !ok {
			log.Fatalf("invalid committee membership entry %q (want topic:position)", entry)
		}
		topic, err := strconv.Atoi(strings.TrimSpace(topicStr))
		if err != nil {
			log.Fatalf("invalid topic index %q: %v", topicStr, err)
		}
		pos, err := strconv.Atoi(strings.TrimSpace(posStr))
		if err != nil {
			log.Fatalf("invalid position %q: %v", posStr, err)
		}
		out = append(out, node.TopicMembership{TopicIndex: topic, Position: pos})
	}
	return out
}

// checkModeExclusion rejects incompatible mode combinations. att_propagation is
// a native protocol that replaces gossipsub, so it cannot run alongside either
// partial-message mode. The two partial modes are themselves selected by
// precedence elsewhere (priority wins), so only att_propagation needs guarding.
// Returns an error (not a fatal) so it is unit-testable.
func checkModeExclusion(usePartial, usePriority, useAttProp bool) error {
	if useAttProp && (usePartial || usePriority) {
		return fmt.Errorf(
			"att_propagation is mutually exclusive with use_partial_messages and partial_priority")
	}
	return nil
}

// parseGossipsubParams parses a "Dlow:8,D:12,Dhigh:16" string into
// GossipsubParams. All three keys are required, must be positive, and must
// satisfy Dlow <= D <= Dhigh. Returns an error rather than fataling so the
// parsing is unit-testable; the caller decides how to handle a bad flag.
func parseGossipsubParams(s string) (node.GossipsubParams, error) {
	var p node.GossipsubParams
	seen := map[string]bool{}
	for entry := range strings.SplitSeq(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, valStr, ok := strings.Cut(entry, ":")
		if !ok {
			return p, fmt.Errorf("invalid entry %q (want key:value)", entry)
		}
		key = strings.TrimSpace(key)
		val, err := strconv.Atoi(strings.TrimSpace(valStr))
		if err != nil {
			return p, fmt.Errorf("invalid value %q: %w", valStr, err)
		}
		switch key {
		case "Dlow":
			p.Dlow = val
		case "D":
			p.D = val
		case "Dhigh":
			p.Dhigh = val
		default:
			return p, fmt.Errorf("unknown key %q (want Dlow, D, Dhigh)", key)
		}
		seen[key] = true
	}
	if !seen["Dlow"] || !seen["D"] || !seen["Dhigh"] {
		return p, fmt.Errorf("%q must set Dlow, D, and Dhigh", s)
	}
	if p.Dlow <= 0 || p.D <= 0 || p.Dhigh <= 0 {
		return p, fmt.Errorf("%q must be positive", s)
	}
	if p.Dlow > p.D || p.D > p.Dhigh {
		return p, fmt.Errorf("%q must satisfy Dlow <= D <= Dhigh", s)
	}
	return p, nil
}

func parseIntList(s string) []int {
	if s == "" {
		return nil
	}
	var result []int
	for p := range strings.SplitSeq(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			log.Fatalf("invalid integer %q: %v", p, err)
		}
		result = append(result, n)
	}
	return result
}

// shadowNetwork implements node.Network for the Shadow discrete-event
// simulator: peer addresses are resolved via DNS using the "node<N>" hostname
// convention.
type shadowNetwork struct{}

func newShadowHost(nodeNum int) host.Host {
	privKey := node.NodePrivateKey(nodeNum)

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: listenPort})
	if err != nil {
		log.Fatalf("listen udp: %v", err)
	}
	sconn := newShadowUDPConn(conn)

	maddr := ma.StringCast(fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", listenPort))
	h, err := libp2p.New(
		quicWithPacketConn(sconn),
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(maddr),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.ResourceManager(&libp2pnet.NullResourceManager{}),
		libp2p.ConnectionManager(&connmgr.NullConnMgr{}),
		libp2p.ForceReachabilityPublic(),
	)
	if err != nil {
		log.Fatalf("create host: %v", err)
	}
	return h
}

func (s *shadowNetwork) PeerAddr(nodeNum int) ma.Multiaddr {
	hostname := fmt.Sprintf("node%d", nodeNum)
	addrs, err := net.LookupHost(hostname)
	if err != nil || len(addrs) == 0 {
		log.Fatalf("resolve %s: %v", hostname, err)
	}
	maddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", addrs[0], listenPort))
	if err != nil {
		log.Fatalf("multiaddr for %s: %v", hostname, err)
	}
	return maddr
}

// Serialized UDP writes for Shadow simulator.
type shadowUDPConn struct {
	net.PacketConn
	ch chan struct{}
}

func newShadowUDPConn(pc net.PacketConn) *shadowUDPConn {
	return &shadowUDPConn{PacketConn: pc, ch: make(chan struct{}, 1)}
}

func (s *shadowUDPConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	s.ch <- struct{}{}
	defer func() { <-s.ch }()
	return s.PacketConn.WriteTo(p, addr)
}

// QUIC with custom packet conn for Shadow.
type sourceIPSelector struct {
	ip atomic.Pointer[net.IP]
}

func (m *sourceIPSelector) PreferredSourceIPForDestination(_ *net.UDPAddr) (net.IP, error) {
	return *m.ip.Load(), nil
}

func quicWithPacketConn(conn net.PacketConn) libp2p.Option {
	ca := conn.LocalAddr().(*net.UDPAddr)
	sel := &sourceIPSelector{}
	sel.ip.Store(&ca.IP)
	reuseOpts := []quicreuse.Option{
		quicreuse.OverrideSourceIPSelector(func() (quicreuse.SourceIPSelector, error) {
			return sel, nil
		}),
		quicreuse.OverrideListenUDP(func(_ string, address *net.UDPAddr) (net.PacketConn, error) {
			if ca.IP.Equal(address.IP) && ca.Port == address.Port {
				return conn, nil
			}
			return nil, fmt.Errorf("invalid listen address: %s, wanted: %s", address, ca)
		}),
	}
	return libp2p.QUICReuse(
		func(l fx.Lifecycle, resetKey quic.StatelessResetKey, tokenKey quic.TokenGeneratorKey, opts ...quicreuse.Option) (*quicreuse.ConnManager, error) {
			cm, err := quicreuse.NewConnManager(resetKey, tokenKey, opts...)
			if err != nil {
				return nil, err
			}
			l.Append(fx.StopHook(func() error { return cm.Close() }))
			return cm, nil
		}, reuseOpts...)
}
