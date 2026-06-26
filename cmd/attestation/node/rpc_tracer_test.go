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
	pspb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
	gproto "google.golang.org/protobuf/proto"

	attpb "github.com/ethp2p/simlab/cmd/attestation/pb"
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

		att := &attpb.Attestation{
			SlotNum:   1,
			Data:      make([]byte, 128),
			Signature: make([]byte, 96),
		}
		buf, err := gproto.Marshal(att)
		if err != nil {
			t.Fatalf("marshal attestation: %v", err)
		}
		if err := topics[0].Publish(ctx, buf); err != nil {
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

		// the decoded attestation byte breakdown is logged on both sides.
		if !strings.Contains(logs[0], "att_data_bytes=128") || !strings.Contains(logs[0], "sig_bytes=96") {
			t.Errorf("publisher topic_message_sent missing attestation byte breakdown")
		}
		for i := 1; i < numHosts; i++ {
			if !strings.Contains(logs[i], "att_data_bytes=128") || !strings.Contains(logs[i], "sig_bytes=96") {
				t.Errorf("node %d topic_message_received missing attestation byte breakdown", i)
			}
		}
	})
}

// tracerTestPeer is a deterministic peer.ID for unit tests that drive the
// tracer directly without a network.
var tracerTestPeer = peer.ID("test-peer-id-1234567890")

// captureTracerOutput runs fn against a fresh tracer writing to a temp file and
// returns the file contents.
func captureTracerOutput(t *testing.T, fn func(tr *RPCTracer)) string {
	t.Helper()
	lp := filepath.Join(t.TempDir(), "trace.log")
	tr, err := newRPCTracer(lp, "node0", MessageIDFunc)
	if err != nil {
		t.Fatalf("newRPCTracer: %v", err)
	}
	fn(tr)
	if err := tr.Close(); err != nil {
		t.Fatalf("close tracer: %v", err)
	}
	b, err := os.ReadFile(lp)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	return string(b)
}

// logLineWith returns the first log line whose msg= matches msgKey.
func logLineWith(t *testing.T, content, msgKey string) string {
	t.Helper()
	for ln := range strings.SplitSeq(content, "\n") {
		if strings.Contains(ln, "msg="+msgKey) {
			return ln
		}
	}
	t.Fatalf("no line with msg=%s in:\n%s", msgKey, content)
	return ""
}

// assertLineHas fails if line is missing any of the given key=value substrings.
func assertLineHas(t *testing.T, line string, subs ...string) {
	t.Helper()
	for _, s := range subs {
		if !strings.Contains(line, s) {
			t.Errorf("line missing %q:\n%s", s, line)
		}
	}
}

// TestRPCTracerClassicFields drives OnRPCSent with a classic publish RPC and
// checks the attestation-aware fields decoded onto topic_message_sent.
func TestRPCTracerClassicFields(t *testing.T) {
	data, err := gproto.Marshal(&attpb.Attestation{
		SlotNum:   1,
		Data:      make([]byte, 128),
		Signature: make([]byte, 96),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	topic := "t0"
	rpc := &pspb.RPC{Publish: []*pspb.Message{{Topic: &topic, Data: data}}}

	content := captureTracerOutput(t, func(tr *RPCTracer) {
		tr.OnRPCSent(tracerTestPeer, time.Millisecond, rpc)
	})
	line := logLineWith(t, content, "topic_message_sent")
	assertLineHas(t, line, "att_count=1", "att_data_bytes=128", "sig_bytes=96", "att_digest=")
}

// TestRPCTracerPartialFields drives OnRPCReceived with a partial RPC carrying a
// data envelope (1 batch, 3 signatures over 128-byte data) plus a metadata
// envelope (2 available, 1 requested) and checks the decoded accounting fields.
func TestRPCTracerPartialFields(t *testing.T) {
	encData, err := gproto.Marshal(&attpb.BatchedAttestationEnvelope{
		Batches: []*attpb.BatchedAttestation{{
			AttestationData: make([]byte, 128),
			AttestorIndices: []uint32{0, 1, 2},
			Signatures:      [][]byte{make([]byte, 96), make([]byte, 96), make([]byte, 96)},
		}},
	})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}

	encMeta, err := gproto.Marshal(&attpb.ControlEnvelope{
		Metadatas: []*attpb.CommitteeAttestationPartsMetadata{{
			Slot:            1,
			AttestationData: make([]byte, 128),
			AvailableIds:    []uint32{0, 1},
			RequestsIds:     []uint32{3},
		}},
	})
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}

	topic := "t0"
	rpc := &pspb.RPC{Partial: &pspb.PartialMessagesExtension{
		TopicID:        &topic,
		PartialMessage: encData,
		PartsMetadata:  encMeta,
	}}

	content := captureTracerOutput(t, func(tr *RPCTracer) {
		tr.OnRPCReceived(tracerTestPeer, time.Millisecond, rpc)
	})
	line := logLineWith(t, content, "partial_received")
	assertLineHas(t, line,
		"data_batches=1", "att_count=3", "att_data_bytes=128", "sig_bytes=288",
		"meta_count=1", "available_ones=2", "requests_ones=1",
	)
}

// TestAttStatsHelpers checks the decode helpers, including the best-effort path:
// malformed input must yield zero values rather than panicking, which is what
// keeps the log lines stable on bad input.
func TestAttStatsHelpers(t *testing.T) {
	// classic happy path
	data, err := gproto.Marshal(&attpb.Attestation{Data: make([]byte, 50), Signature: make([]byte, 96)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if d, s, dig := classicAttStats(data); d != 50 || s != 96 || dig == "" {
		t.Errorf("classicAttStats = (%d, %d, %q), want (50, 96, non-empty)", d, s, dig)
	}
	if d, s, dig := classicAttStats([]byte{0xFF}); d != 0 || s != 0 || dig != "" {
		t.Errorf("classicAttStats(bad) = (%d, %d, %q), want (0, 0, \"\")", d, s, dig)
	}

	// partial data: 2 batches, 3+1 signatures, 2x128 data bytes, 4x96 sig bytes
	encData, err := gproto.Marshal(&attpb.BatchedAttestationEnvelope{
		Batches: []*attpb.BatchedAttestation{
			{AttestationData: make([]byte, 128), Signatures: [][]byte{make([]byte, 96), make([]byte, 96), make([]byte, 96)}},
			{AttestationData: make([]byte, 128), Signatures: [][]byte{make([]byte, 96)}},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if b, c, dB, hB, sB := partialDataStats(encData); b != 2 || c != 4 || dB != 256 || hB != 0 || sB != 384 {
		t.Errorf("partialDataStats = (%d, %d, %d, %d, %d), want (2, 4, 256, 0, 384)", b, c, dB, hB, sB)
	}
	if b, c, dB, hB, sB := partialDataStats([]byte{0xFF}); b != 0 || c != 0 || dB != 0 || hB != 0 || sB != 0 {
		t.Errorf("partialDataStats(bad) = (%d, %d, %d, %d, %d), want zeros", b, c, dB, hB, sB)
	}
	if b, _, _, _, _ := partialDataStats(nil); b != 0 {
		t.Errorf("partialDataStats(nil) batches = %d, want 0", b)
	}

	// partial metadata: 2 metadatas, (2+1) available IDs, (1+0) request IDs
	encMeta, err := gproto.Marshal(&attpb.ControlEnvelope{
		Metadatas: []*attpb.CommitteeAttestationPartsMetadata{
			{AvailableIds: []uint32{0, 5}, RequestsIds: []uint32{2}},
			{AvailableIds: []uint32{7}},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if m, av, rq, hB := partialMetaStats(encMeta); m != 2 || av != 3 || rq != 1 || hB != 0 {
		t.Errorf("partialMetaStats = (%d, %d, %d, %d), want (2, 3, 1, 0)", m, av, rq, hB)
	}
	if m, av, rq, hB := partialMetaStats([]byte{0xFF}); m != 0 || av != 0 || rq != 0 || hB != 0 {
		t.Errorf("partialMetaStats(bad) = (%d, %d, %d, %d), want zeros", m, av, rq, hB)
	}
}
