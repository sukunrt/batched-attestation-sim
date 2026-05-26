package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

// TestRPCTracerLogLines spins up a 10-node QUIC simnet, runs gossipsub on each
// node with an RPCTracer attached, publishes a message, and verifies that every
// log line the tracer emits is actually printed.
func TestRPCTracerLogLines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const numHosts = 10
		const topicName = "/test/tracer/1.0.0"

		net, meta, err := simlibp2p.SimpleLibp2pNetwork(
			[]simlibp2p.NodeLinkSettingsAndCount{{
				LinkSettings: simnet.NodeBiDiLinkSettings{
					Downlink: simnet.LinkSettings{BitsPerSecond: 1024 * simlibp2p.OneMbps},
					Uplink:   simnet.LinkSettings{BitsPerSecond: 1024 * simlibp2p.OneMbps},
				},
				Count: numHosts,
			}},
			simnet.StaticLatency(5*time.Millisecond),
			simlibp2p.NetworkSettings{},
		)
		if err != nil {
			t.Fatalf("simlibp2p: %v", err)
		}
		net.Start()
		t.Cleanup(func() {
			for _, h := range meta.Nodes {
				_ = h.Close()
			}
			net.Close()
		})

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		idFn := MessageIDFunc

		tmpDir := t.TempDir()
		tracers := make([]*RPCTracer, numHosts)
		pubsubs := make([]*pubsub.PubSub, numHosts)
		logPaths := make([]string, numHosts)
		for i, h := range meta.Nodes {
			logPaths[i] = filepath.Join(tmpDir, fmt.Sprintf("node-%d.log", i))
			tr, err := newRPCTracer(logPaths[i], h.ID().String(), idFn)
			if err != nil {
				t.Fatalf("newRPCTracer: %v", err)
			}
			tracers[i] = tr

			// reportRTT only fires every 100 heartbeat ticks (gossipsub.go).
			// Shrink HeartbeatInterval so 100 ticks fit in the test budget.
			params := pubsub.DefaultGossipSubParams()
			params.HeartbeatInitialDelay = 50 * time.Millisecond
			params.HeartbeatInterval = 50 * time.Millisecond

			ps, err := pubsub.NewGossipSub(ctx, h,
				pubsub.WithGossipSubParams(params),
				pubsub.WithMessageIdFn(idFn),
				pubsub.WithRPCTracer(tr),
			)
			if err != nil {
				t.Fatalf("NewGossipSub: %v", err)
			}
			pubsubs[i] = ps
		}

		for i, a := range meta.Nodes {
			for j := i + 1; j < numHosts; j++ {
				b := meta.Nodes[j]
				if err := a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
					t.Fatalf("connect %d->%d: %v", i, j, err)
				}
			}
		}

		subs := make([]*pubsub.Subscription, numHosts)
		topics := make([]*pubsub.Topic, numHosts)
		for i, ps := range pubsubs {
			tp, err := ps.Join(topicName)
			if err != nil {
				t.Fatalf("join: %v", err)
			}
			topics[i] = tp
			s, err := tp.Subscribe()
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			subs[i] = s
		}

		// Let the mesh form and at least one heartbeat fire.
		time.Sleep(500 * time.Millisecond)

		if err := topics[0].Publish(ctx, []byte("hello world")); err != nil {
			t.Fatalf("publish: %v", err)
		}

		for i := 1; i < numHosts; i++ {
			rctx, c := context.WithTimeout(ctx, 3*time.Second)
			if _, err := subs[i].Next(rctx); err != nil {
				c()
				t.Fatalf("node %d recv: %v", i, err)
			}
			c()
		}

		// reportRTT fires at heartbeat tick 100. With HeartbeatInterval=50ms that
		// is ~5s after pubsub start; give it some headroom.
		time.Sleep(6 * time.Second)

		for _, tr := range tracers {
			if err := tr.Close(); err != nil {
				t.Fatalf("close tracer: %v", err)
			}
		}

		logs := make([]string, numHosts)
		for i, p := range logPaths {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatalf("read log %d: %v", i, err)
			}
			logs[i] = string(b)
		}

		// every node sends RPCs, receives RPCs, grafts onto the mesh,
		// emits mesh_size after a heartbeat, and reports QUIC RTT to mesh peers.
		mustOnAll := []string{
			"msg=rpc_sent",
			"msg=rpc_received",
			"msg=graft_sent",
			"msg=graft_received",
			"msg=mesh_size",
			"msg=mesh_peer_rtt",
		}
		for _, label := range mustOnAll {
			for i, l := range logs {
				if !strings.Contains(l, label) {
					t.Errorf("node %d missing %q", i, label)
				}
			}
		}

		// only the publisher logs topic_message_sent + message_id_mapping.
		if !strings.Contains(logs[0], "msg=topic_message_sent") {
			t.Errorf("publisher missing topic_message_sent")
		}
		if !strings.Contains(logs[0], "msg=message_id_mapping") {
			t.Errorf("publisher missing message_id_mapping")
		}

		// every non-publisher receives the message.
		for i := 1; i < numHosts; i++ {
			if !strings.Contains(logs[i], "msg=topic_message_received") {
				t.Errorf("node %d missing topic_message_received", i)
			}
		}
	})
}
