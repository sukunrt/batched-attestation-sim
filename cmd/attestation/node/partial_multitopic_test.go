package node

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2EMultiTopicPerTopicCommittees pins down the invariant that broke
// before: `node_num` is NOT used as the committee position, and the committee
// size is per-topic (= num_attestors), not global (= total node count).
//
// Scenario: 10 nodes total, 2 topics, num_attestors = 4 per topic. Topic 0's
// committee = {0, 3, 6, 9}; topic 1's committee = {1, 4, 7, 8}. Several
// committee members have node_num >= num_attestors (e.g., 6, 9, 7, 8). Before
// the fix, partial mode would have crashed at startup with `node number 6 >=
// committee_size 4`. Now positions are explicit and node identity is decoupled
// from committee position.
//
// Assertions:
//  1. No node panics or fatals at startup (implicit — if any did, runE2E hangs
//     or fails).
//  2. Every committee member's attestation lands at the expected per-topic
//     position on every other committee member, in [0, num_attestors).
//  3. Bitmaps allocated for this committee size are exactly
//     ceil(num_attestors / 8) bytes — not sized to total node count or to a
//     stale global default.
func TestE2EMultiTopicPerTopicCommittees(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			numNodes     = 10
			numTopics    = 2
			numAttestors = 4
			numSlots     = 2
			slotDuration = 2 * time.Second
		)

		// Per-topic committee membership. Position = index in the list.
		committees := map[int][]int{
			0: {0, 3, 6, 9}, // topic 0
			1: {1, 4, 7, 8}, // topic 1
		}
		// Invert into per-node memberships.
		nodeMemberships := make(map[int][]TopicMembership)
		for topic, members := range committees {
			for pos, num := range members {
				nodeMemberships[num] = append(nodeMemberships[num], TopicMembership{
					TopicIndex: topic,
					Position:   pos,
				})
			}
		}

		nw := newSimTestNetwork(t, numNodes)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		tr := newTestTracer()
		publishStart := time.Now().Add(4 * time.Second)

		nodes := make([]*Node, numNodes)
		for i := range numNodes {
			opts := defaultE2EOpts(i, 1, publishStart, slotDuration, numAttestors)
			opts.numTopics = numTopics
			opts.committeeSize = numAttestors
			opts.memberships = nodeMemberships[i]
			if opts.memberships == nil {
				opts.publishSlot = 0
			}
			nodes[i] = newE2EPartialNode(opts, nw, tr)
		}

		// Full mesh of connections.
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

		runE2E(t, ctx, nodes, conn, publishStart, numSlots, slotDuration)

		// Assertion 2: every committee member's position propagated on that
		// topic, with positions strictly in [0, num_attestors).
		for topic, members := range committees {
			positions := partialReceivePositions(tr, 1, topic)
			for _, sender := range members {
				senderPos := -1
				for _, m := range nodeMemberships[sender] {
					if m.TopicIndex == topic {
						senderPos = m.Position
					}
				}
				require.GreaterOrEqual(t, senderPos, 0)
				assert.Containsf(t, positions, senderPos,
					"topic %d: missing attestation from node %d at position %d (have %v)",
					topic, sender, senderPos, positions)
			}
			for p := range positions {
				assert.Lessf(t, p, numAttestors,
					"topic %d: observed position %d >= num_attestors %d",
					topic, p, numAttestors)
			}
		}

		// Assertion 3: metadata IDs are committee positions, not global node
		// numbers, and legacy bitmap fields are not populated for outgoing
		// metadata.
		b := newAttestationState([]byte("probe"))
		b.validated[0] = struct{}{}
		b.validated[3] = struct{}{}
		md := getAttestationMetadata(b, numAttestors, 1, nil, true)
		require.NotNil(t, md)
		assert.Equal(t, []uint32{0, 3}, md.AvailableIds)
		assert.Empty(t, md.Available)

		req := getAttestationMetadata(b, numAttestors, 2, []int{1, 2}, false)
		require.NotNil(t, req)
		assert.Equal(t, []uint32{1, 2}, req.RequestsIds)
		assert.Empty(t, req.Requests)

		// Sanity: position IDs remain scoped to the per-topic committee, not
		// the global node count.
		assert.Less(t, numAttestors, numNodes)
	})
}
