package node

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// smallAttPropParams tunes att_propagation for fast multi-node synctest runs:
// small meshes so a sparse topology can fill them, with the same non-zero timers
// as attPropParams.
func smallAttPropParams() AttPropParams {
	p := attPropParams()
	p.PushDlow, p.PushD, p.PushDhigh = 2, 3, 3
	p.BitmapDlow, p.BitmapD, p.BitmapDhigh = 3, 4, 4
	p.MaxPeersPerAtt = 8
	return p
}

// openStreams makes the lower-peerID node (the protocol's opener, weOpen) dial
// the other over QUIC and open the three att_propagation streams, picking the
// opener by peer ID (not node number) so streams come up regardless of how
// peer-ID order relates to node-num order.
func openStreams(t *testing.T, nw *testNetwork, nodes []*Node, a, b int) {
	t.Helper()
	ida, err := PeerIDFromNodeNum(a)
	if err != nil {
		t.Fatalf("peer id %d: %v", a, err)
	}
	idb, err := PeerIDFromNodeNum(b)
	if err != nil {
		t.Fatalf("peer id %d: %v", b, err)
	}
	opener, other, otherID := a, b, idb
	if idb < ida {
		opener, other, otherID = b, a, ida
	}
	err = nw.hosts[opener].Connect(context.Background(), peer.AddrInfo{
		ID: otherID, Addrs: []ma.Multiaddr{nw.PeerAddr(other)},
	})
	if err != nil {
		t.Fatalf("connect %d->%d: %v", opener, other, err)
	}
	for _, m := range nodes[opener].attProp {
		m.ConnectPeer(otherID)
	}
}

// TestAttProp32MeshPropagation is the full-protocol integration gate: 32 mesh
// nodes on one topic over a sparse (circulant degree-8) topology, each owning a
// distinct committee position and self-publishing it at slot 1. The mesh forms
// (graft/prune over the fake-clock heartbeat) and scarcity forwarding floods all
// 32 positions to every node. We assert every node validates all 32.
func TestAttProp32MeshPropagation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes      = 4
			committeeSize = 4
			degreeHalf    = 2 // each node links to (i±1..i±2) ⇒ complete K4
			slotDuration  = 2 * time.Second
		)

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		params := smallAttPropParams()

		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			nodes[i] = &Node{
				Num:                  i,
				PublishSlots:         map[int]struct{}{1: {}},
				NumTopics:            1,
				CommitteeMemberships: []TopicMembership{{TopicIndex: 0, Position: i}},
				CommitteeSize:        committeeSize,
				AttestationDataSize:  64,
				SignatureSize:        36,
				VerificationDelay:    testVerificationDelay,
				Network:              nw,
				Tracer:               tr,
				AttPropagation:       true,
				AttProp:              params,
				PublishStart:         time.Now(),
				SlotDuration:         slotDuration,
				Host:                 nw.hosts[i],
			}
			nodes[i].Start(ctx)
		}

		// Circulant degree-8 topology: every (i, i+k mod n) for k in 1..4. Each
		// unordered pair appears once.
		for i := range numNodes {
			for k := 1; k <= degreeHalf; k++ {
				openStreams(t, nw, nodes, i, (i+k)%numNodes)
			}
		}
		synctest.Wait()

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() { n.Run(1, slotDuration) })
		}
		wg.Wait()

		topic0 := topicName(0)
		full, minSeen := 0, committeeSize
		for i, n := range nodes {
			got := n.attProp[topic0].ValidatedCount(1)
			if got < minSeen {
				minSeen = got
			}
			if got == committeeSize {
				full++
			} else {
				t.Logf("node %d validated %d/%d", i, got, committeeSize)
			}
		}
		t.Logf("propagation: %d/%d nodes full; min=%d/%d", full, numNodes, minSeen, committeeSize)
		if full != numNodes {
			t.Fatalf("incomplete propagation: %d/%d nodes reached all %d (min=%d)",
				full, numNodes, committeeSize, minSeen)
		}
	})
}

func TestAttPropTwoTopicMeshPropagation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes      = 2
			numTopics     = 2
			committeeSize = 2
			slotDuration  = 2 * time.Second
		)

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		params := smallAttPropParams()

		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			nodes[i] = &Node{
				Num:          i,
				PublishSlots: map[int]struct{}{1: {}},
				NumTopics:    numTopics,
				CommitteeMemberships: []TopicMembership{
					{TopicIndex: 0, Position: i},
					{TopicIndex: 1, Position: i},
				},
				CommitteeSize:       committeeSize,
				AttestationDataSize: 64,
				SignatureSize:       36,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
				AttPropagation:      true,
				AttProp:             params,
				PublishStart:        time.Now(),
				SlotDuration:        slotDuration,
				Host:                nw.hosts[i],
			}
			nodes[i].Start(ctx)
		}

		openStreams(t, nw, nodes, 0, 1)
		synctest.Wait()

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() { n.Run(1, slotDuration) })
		}
		wg.Wait()

		for topic := range numTopics {
			name := topicName(topic)
			for i, n := range nodes {
				got := n.attProp[name].ValidatedCount(1)
				if got != committeeSize {
					t.Fatalf("node %d topic %d validated %d/%d", i, topic, got, committeeSize)
				}
			}
		}
	})
}

// TestAttProp32FanoutInjection exercises the fanout (leaf-injector) path at
// scale: 32 fanout nodes each own a distinct position and inject it directly to
// every receiver. Receivers are plain mesh nodes that only receive (they are not
// linked to each other), so each must collect all 32 positions purely from
// direct fanout injection — isolating the fanout path from mesh forwarding.
func TestAttProp32FanoutInjection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numFanout     = 32
			numReceivers  = 3
			committeeSize = 32
			slotDuration  = 2 * time.Second
		)
		total := numReceivers + numFanout // receivers are nodes [0,numReceivers)

		nw := newSimTestNetwork(t, total)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()

		nodes := make([]*Node, total)
		mk := func(num int, fanout bool, memberships []TopicMembership, pubSlots map[int]struct{}) *Node {
			return &Node{
				Num:                  num,
				PublishSlots:         pubSlots,
				NumTopics:            1,
				CommitteeMemberships: memberships,
				CommitteeSize:        committeeSize,
				AttestationDataSize:  64,
				SignatureSize:        36,
				Fanout:               fanout,
				VerificationDelay:    testVerificationDelay,
				Network:              nw,
				Tracer:               tr,
				AttPropagation:       true,
				AttProp:              smallAttPropParams(),
				PublishStart:         time.Now(),
				SlotDuration:         slotDuration,
				Host:                 nw.hosts[num],
			}
		}
		for r := range numReceivers {
			nodes[r] = mk(r, false, nil, nil)
		}
		for f := range numFanout {
			num := numReceivers + f
			nodes[num] = mk(num, true,
				[]TopicMembership{{TopicIndex: 0, Position: f}},
				map[int]struct{}{1: {}})
		}
		for _, n := range nodes {
			n.Start(ctx)
		}

		// Each fanout dials every receiver; FanoutPublish then opens a push stream
		// to each connected peer. Receivers are not linked to each other.
		for f := range numFanout {
			fn := nodes[numReceivers+f]
			for r := range numReceivers {
				rid, err := PeerIDFromNodeNum(r)
				if err != nil {
					t.Fatalf("peer id %d: %v", r, err)
				}
				err = fn.Host.Connect(context.Background(), peer.AddrInfo{
					ID: rid, Addrs: []ma.Multiaddr{nw.PeerAddr(r)},
				})
				if err != nil {
					t.Fatalf("fanout %d connect receiver %d: %v", f, r, err)
				}
			}
		}
		synctest.Wait()

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() { n.Run(1, slotDuration) })
		}
		wg.Wait()

		topic0 := topicName(0)
		for r := range numReceivers {
			got := nodes[r].attProp[topic0].ValidatedCount(1)
			if got != numFanout {
				t.Fatalf("receiver %d validated %d/%d injected positions", r, got, numFanout)
			}
		}
	})
}
