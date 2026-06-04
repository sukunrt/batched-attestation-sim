package node

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
)

// metadataSendTracer records, per destination peer, the (fake-clock) timestamps
// at which this node sent an RPC carrying a non-empty PartsMetadata envelope
// (the partial-message "available"/"requests" control blob). It lets a test
// measure the cadence at which a node re-advertises its available list to a
// gossip peer.
type metadataSendTracer struct {
	mu    sync.Mutex
	sends map[peer.ID][]time.Time
}

func newMetadataSendTracer() *metadataSendTracer {
	return &metadataSendTracer{sends: map[peer.ID][]time.Time{}}
}

func (c *metadataSendTracer) OnRPCSent(p peer.ID, _ time.Duration, rpc *pubsub_pb.RPC) {
	pm := rpc.GetPartial()
	if pm == nil || len(pm.GetPartsMetadata()) == 0 {
		return
	}
	c.mu.Lock()
	c.sends[p] = append(c.sends[p], time.Now())
	c.mu.Unlock()
}

func (c *metadataSendTracer) OnRPCReceived(peer.ID, time.Duration, *pubsub_pb.RPC) {}
func (c *metadataSendTracer) OnPeerRTT(peer.ID, string, time.Duration, string, string) {}
func (c *metadataSendTracer) OnMeshSize(string, int)                                  {}
func (c *metadataSendTracer) Close() error                                            { return nil }

// maxSendsInWindow returns the peer that received the most metadata sends within
// [start, end] and that count.
func (c *metadataSendTracer) maxSendsInWindow(start, end time.Time) (peer.ID, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var bestPeer peer.ID
	best := 0
	for p, ts := range c.sends {
		n := 0
		for _, t := range ts {
			if !t.Before(start) && !t.After(end) {
				n++
			}
		}
		if n > best {
			best = n
			bestPeer = p
		}
	}
	return bestPeer, best
}

// TestE2EGossipAvailableCadence pins down how often a node re-advertises its
// "available" list to a gossip (non-mesh) peer once a slot has fully propagated
// and no Wants remain.
//
// The intended design advertises available at the gossip heartbeat cadence
// (HeartbeatInterval = 700ms, set in node.go), so across a 1.4s steady-state
// window a node should send metadata to any single gossip peer only ~2 times.
//
// If, instead, receiving a peer's available re-adds it to peerStates every tick
// (a mutual available<->available ping-pong), the node re-sends metadata to that
// peer on every publish tick (20ms) — ~70 times in the same window. This test
// fails in that case.
func TestE2EGossipAvailableCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes        = 8
			numSlots        = 1
			slotDuration    = 8 * time.Second
			publishInterval = 20 * time.Millisecond
			heartbeat       = 700 * time.Millisecond // node.go HeartbeatInterval
			measureNode     = 0
		)
		// Tight mesh so most of the 7 connections become gossip peers.
		tightMesh := GossipsubParams{D: 4, Dlow: 4, Dhigh: 5}

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)

		mdTracer := newMetadataSendTracer()
		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			opts := defaultE2EOpts(i, 1, publishStart, slotDuration, numNodes)
			opts.gossipsubParams = tightMesh
			opts.publishInterval = publishInterval
			if i == measureNode {
				opts.rpcTracer = mdTracer
			}
			nodes[i] = newE2EPartialNode(opts, nw, tr)
		}

		// Full TCP-level mesh; gossipsub keeps D peers in each topic mesh and
		// treats the rest as gossip peers.
		conn := map[int][]int{}
		for i := range numNodes {
			peers := make([]int, 0, numNodes-1)
			for j := range numNodes {
				if j != i {
					peers = append(peers, j)
				}
			}
			conn[i] = peers
		}

		for _, n := range nodes {
			n.Start(ctx)
		}
		time.Sleep(1 * time.Second)
		for src, dsts := range conn {
			nodes[src].ConnectToPeers(dsts)
		}
		for _, n := range nodes {
			n.JoinTopics()
		}
		time.Sleep(2 * time.Second)
		time.Sleep(time.Until(publishStart))

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Add(1)
			go func(n *Node) {
				defer wg.Done()
				n.Run(numSlots, slotDuration)
			}(n)
		}

		// Let slot 1 fully propagate: every node ends up holding every position,
		// so available is full everywhere and no Wants remain. 3s is several
		// heartbeats in — safely steady state.
		time.Sleep(3 * time.Second)

		winStart := time.Now()
		const window = 1400 * time.Millisecond // 2 heartbeats
		time.Sleep(window)
		winEnd := time.Now()

		wg.Wait()

		busiest, maxSends := mdTracer.maxSendsInWindow(winStart, winEnd)
		ticksInWindow := int(window / publishInterval)     // 70
		hbInWindow := float64(window) / float64(heartbeat) // ~2
		t.Logf("steady-state metadata sends to busiest gossip peer %s over %s: %d (publish ticks in window=%d, heartbeats in window=%.1f)",
			shortPeer(busiest), window, maxSends, ticksInWindow, hbInWindow)

		// Intended: available advertised ~once per heartbeat, so only a handful
		// of metadata sends to any one peer across two heartbeats. Per-tick
		// re-advertisement pushes this toward ticksInWindow (~70).
		assert.LessOrEqual(t, maxSends, 6,
			"available re-advertised %d times to one gossip peer over %s; expected heartbeat cadence (~%.0f sends), not per-tick (~%d sends)",
			maxSends, window, hbInWindow, ticksInWindow)
	})
}
