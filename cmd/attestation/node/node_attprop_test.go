package node

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// attPropParams returns sane att_propagation tunables for tests: small meshes,
// non-zero timers (zero would disable the drivers), committee 64.
func attPropParams() AttPropParams {
	return AttPropParams{
		PushDlow: 4, PushD: 5, PushDhigh: 5,
		BitmapDlow: 14, BitmapD: 16, BitmapDhigh: 16,
		SendBudgetB: 4, MaxAttsPerMessage: 30, MaxPeersPerAtt: 16,
		TickInterval:        20 * time.Millisecond,
		BitmapFloorInterval: 100 * time.Millisecond,
		HeartbeatInterval:   700 * time.Millisecond,
		PruneBackoff:        60 * time.Second,
	}
}

// TestAttPropStartNoGossipsub asserts that starting a node in att_propagation
// mode wires the attprop Manager and creates NO gossipsub instance: ps stays
// nil and no topics are joined.
func TestAttPropStartNoGossipsub(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw := newSimTestNetwork(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		n := &Node{
			Num:                 0,
			NumTopics:           1,
			CommitteeSize:       64,
			AttestationDataSize: 64,
			SignatureSize:       36,
			VerificationDelay:   testVerificationDelay,
			Network:             nw,
			Host:                nw.hosts[0],
			AttPropagation:      true,
			AttProp:             attPropParams(),
			PublishStart:        time.Now(),
			SlotDuration:        time.Second,
		}
		n.Start(ctx)

		if n.attProp == nil {
			t.Fatal("att_propagation: attProp manager map is nil after Start")
		}
		if len(n.attProp) != 1 || n.attProp[topicName(0)] == nil {
			t.Fatalf("att_propagation: managers=%v, want one manager keyed by topic", n.attProp)
		}
		if n.ps != nil {
			t.Fatal("att_propagation: gossipsub (ps) must not be created")
		}

		// JoinTopics opens attprop streams, but never creates gossipsub topics.
		n.JoinTopics()
		if len(n.topics) != 0 || len(n.subs) != 0 {
			t.Fatalf("att_propagation: JoinTopics created topics=%d subs=%d, want 0",
				len(n.topics), len(n.subs))
		}
	})
}

// TestAttPropFanoutToMesh is a 2-node att_propagation smoke test: a fanout node
// injects its single attestation to a mesh peer, and the mesh node receives and
// validates it (the tracer's OnPartialReceive fires for the right position).
func TestAttPropFanoutToMesh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numSlots     = 2
			slotDuration = 500 * time.Millisecond
		)

		nw := newSimTestNetwork(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()

		// node 0: fanout (publishes slot 1, never receives).
		// node 1: mesh (receives the fanout's injection).
		nodes := []*Node{
			{
				Num:                  0,
				PublishSlots:         map[int]struct{}{1: {}},
				NumTopics:            1,
				CommitteeMemberships: []TopicMembership{{TopicIndex: 0, Position: 7}},
				CommitteeSize:        64,
				AttestationDataSize:  64,
				SignatureSize:        36,
				Fanout:               true,
				VerificationDelay:    testVerificationDelay,
				Network:              nw,
				Tracer:               tr,
				AttPropagation:       true,
				AttProp:              attPropParams(),
			},
			{
				Num:                 1,
				NumTopics:           1,
				CommitteeSize:       64,
				AttestationDataSize: 64,
				SignatureSize:       36,
				VerificationDelay:   testVerificationDelay,
				Network:             nw,
				Tracer:              tr,
				AttPropagation:      true,
				AttProp:             attPropParams(),
			},
		}
		publishStart := time.Now().Add(time.Second)
		for _, n := range nodes {
			n.Host = nw.hosts[n.Num]
			n.PublishStart = publishStart
			n.SlotDuration = slotDuration
			n.Start(ctx)
		}
		time.Sleep(time.Second)

		// Fanout (lower peer ID side) dials the mesh node so a connection exists
		// for the push stream.
		nodes[0].ConnectToPeers([]int{1})
		time.Sleep(time.Second)

		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Go(func() { n.Run(numSlots, slotDuration) })
		}
		wg.Wait()

		// Only the mesh node can receive (the fanout resets every inbound stream),
		// so a validated position 7 on topic 0 proves the mesh path end-to-end.
		tr.mu.Lock()
		var meshRecvPos7 bool
		for _, e := range tr.partialReceives {
			if e.position == 7 && e.topicIndex == 0 {
				meshRecvPos7 = true
			}
		}
		recv := append([]partialEvent(nil), tr.partialReceives...)
		tr.mu.Unlock()

		if !meshRecvPos7 {
			t.Fatalf("mesh node did not validate fanout's position 7; receives=%+v", recv)
		}
	})
}
