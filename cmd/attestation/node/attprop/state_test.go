package attprop

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// checkIndexInvariant asserts the holder-count index invariant (§E): an entry
// {bk,pos} is present in exactly one level k iff the position is validated and
// its holderCount == k.
func checkIndexInvariant(t *testing.T, ss *slotState) {
	t.Helper()
	// Forward: every validated position is indexed at its holder count.
	for bk, b := range ss.buckets {
		for pos := range b.validated {
			hc := b.holderCount[pos]
			require.Less(t, hc, len(ss.levels), "index must grow to holder count")
			in := levelContains(ss.levels[hc], bk, pos)
			require.Truef(t, in, "validated pos %d must be in level %d", pos, hc)
			for k := range ss.levels {
				if k == hc {
					continue
				}
				dup := levelContains(ss.levels[k], bk, pos)
				require.Falsef(t, dup, "pos %d indexed in level %d, want only %d", pos, k, hc)
			}
		}
	}

	// Reverse: every indexed entry is validated and at its level.
	for k := range ss.levels {
		lvl := &ss.levels[k]
		for bk, positions := range lvl.entries {
			b := ss.buckets[bk]
			require.NotNil(t, b)
			for pos := range positions {
				_, val := b.validated[pos]
				require.Truef(t, val, "indexed entry {%q,%d} must be validated", bk, pos)
				require.Equalf(t, k, b.holderCount[pos],
					"entry {%q,%d} at level %d but hc %d", bk, pos, k, b.holderCount[pos])
			}
		}
	}
}

func levelContains(l countLevel, bucketKey string, pos int) bool {
	positions := l.entries[bucketKey]
	if positions == nil {
		return false
	}
	_, ok := positions[pos]
	return ok
}

func levelLen(l countLevel) int {
	n := 0
	for _, positions := range l.entries {
		n += len(positions)
	}
	return n
}

const testCommittee = 64

// newTestSlot builds a slotState with one bucket and the given validated
// positions, all at holder-count 0.
func newTestSlot(maxPeers int, validated ...int) (*slotState, *bucket) {
	ss := newSlotState(1, maxPeers, testCommittee)
	data := []byte("data-A")
	b := ss.getOrCreateBucket(data)
	for _, pos := range validated {
		b.atts[pos] = &attEntry{Position: pos, Signature: []byte{1}, Data: b.data}
		b.validated[pos] = struct{}{}
		ss.indexAddValidated(string(b.data), pos, 0)
	}
	return ss, b
}

// TestIndexAddValidatedInvariant: positions land at their holder-count level,
// and the holder-count index grows when needed.
func TestIndexAddValidatedInvariant(t *testing.T) {
	ss := newSlotState(1, 3, testCommittee)
	b := ss.getOrCreateBucket([]byte("d"))
	for _, pos := range []int{5, 9, 1} {
		b.validated[pos] = struct{}{}
		ss.indexAddValidated(string(b.data), pos, 0)
	}
	// One at hc 2, one beyond the initial index size.
	b.validated[20] = struct{}{}
	b.holderCount[20] = 2
	ss.indexAddValidated(string(b.data), 20, 2)
	b.validated[21] = struct{}{}
	b.holderCount[21] = 3
	ss.indexAddValidated(string(b.data), 21, 3)

	require.Equal(t, 3, levelLen(ss.levels[0]))
	require.Equal(t, 1, levelLen(ss.levels[2]))
	require.GreaterOrEqual(t, len(ss.levels), 4)
	require.True(t, levelContains(ss.levels[3], string(b.data), 21),
		"index must grow for higher holder-count positions")
	checkIndexInvariant(t, ss)
}

// TestScarcityAscendingOrder: selectOneChunkForPeer returns positions
// scarcest-first across holder-count levels (lower holder-count drawn before
// higher), regardless of position number.
func TestScarcityAscendingOrder(t *testing.T) {
	ss := newSlotState(1, 5, testCommittee)
	b := ss.getOrCreateBucket([]byte("d"))
	// Higher position numbers given LOWER holder-count, so a position-only sort
	// would disagree with scarcity order — proving the level walk dominates.
	for _, x := range []struct{ pos, hc int }{{30, 0}, {20, 1}, {10, 2}} {
		b.atts[x.pos] = &attEntry{Position: x.pos, Data: b.data}
		b.validated[x.pos] = struct{}{}
		b.holderCount[x.pos] = x.hc
		ss.indexAddValidated(string(b.data), x.pos, x.hc)
	}
	checkIndexInvariant(t, ss)

	// Cap large enough to take all three: all levels are visited scarcest-first.
	chunk, held := ss.selectOneChunkForPeer(pid(1), 10, true, noHolderCountLimit)
	require.False(t, held)
	require.ElementsMatch(t, []int{30, 20, 10}, chunk[string(b.data)])
}

// TestScarcityChunkCap: with more validated positions than the cap, the chunk
// holds the maxN scarcest positions.
func TestScarcityChunkCap(t *testing.T) {
	ss, b := newTestSlot(10, 1, 2, 3, 4, 5) // all hc 0
	checkIndexInvariant(t, ss)

	chunk, held := ss.selectOneChunkForPeer(pid(1), 3, true, noHolderCountLimit)
	require.False(t, held)
	require.Equal(t, 3, chunkLen(chunk))
	require.Subset(t, []int{1, 2, 3, 4, 5}, chunk[string(b.data)])
}

func TestSelectionHolderCountLimit(t *testing.T) {
	ss := newSlotState(1, 2, testCommittee)
	b := ss.getOrCreateBucket([]byte("d"))
	for _, x := range []struct{ pos, hc int }{{10, 0}, {20, 1}, {30, 2}} {
		b.atts[x.pos] = &attEntry{Position: x.pos, Data: b.data}
		b.validated[x.pos] = struct{}{}
		b.holderCount[x.pos] = x.hc
		ss.indexAddValidated(string(b.data), x.pos, x.hc)
	}
	checkIndexInvariant(t, ss)

	chunk, held := ss.selectOneChunkForPeer(pid(1), 10, true, 2)
	require.False(t, held)
	require.ElementsMatch(t, []int{10, 20}, chunk[string(b.data)])
	require.False(t, b.peerAvail[pid(1)].Get(30), "level 2 is outside limit < 2")

	chunk, held = ss.selectOneChunkForPeer(pid(2), 10, true, noHolderCountLimit)
	require.False(t, held)
	require.ElementsMatch(t, []int{10, 20, 30}, chunk[string(b.data)])
	checkIndexInvariant(t, ss)
}

// TestSelectCanHoldPartialWithoutCommit: non-tick push selection can decline a
// partial chunk before mutating holder-count or peer available state.
func TestSelectCanHoldPartialWithoutCommit(t *testing.T) {
	ss, b := newTestSlot(10, 1, 2)

	chunk, held := ss.selectOneChunkForPeer(pid(1), 3, false, noHolderCountLimit)
	require.Nil(t, chunk)
	require.True(t, held)
	for _, pos := range []int{1, 2} {
		require.Equal(t, 0, b.holderCount[pos])
		require.False(t, b.peerAvail[pid(1)].Get(pos))
	}
	require.Equal(t, 2, levelLen(ss.levels[0]))
	checkIndexInvariant(t, ss)

	chunk, held = ss.selectOneChunkForPeer(pid(1), 2, false, noHolderCountLimit)
	require.False(t, held)
	require.ElementsMatch(t, []int{1, 2}, chunk[string(b.data)])
}

// TestCommitAsDrawnReordersLevels: a draw commits holder-count++ per position,
// so the entries move up a level and the peer's available bit is set (no
// re-draw next pass).
func TestCommitAsDrawnReordersLevels(t *testing.T) {
	ss, b := newTestSlot(10, 1, 2, 3)
	chunk, _ := ss.selectOneChunkForPeer(pid(1), 3, true, noHolderCountLimit)
	require.ElementsMatch(t, []int{1, 2, 3}, chunk[string(b.data)])

	// After the draw: holderCount 1 for each, entries now in level 1, peer holds.
	for _, pos := range []int{1, 2, 3} {
		require.Equal(t, 1, b.holderCount[pos])
		require.True(t, b.peerAvail[pid(1)].Get(pos))
	}
	require.Equal(t, 0, levelLen(ss.levels[0]))
	require.Equal(t, 3, levelLen(ss.levels[1]))
	checkIndexInvariant(t, ss)

	// Same peer again ⇒ nothing left (it already holds all).
	chunk2, held2 := ss.selectOneChunkForPeer(pid(1), 3, true, noHolderCountLimit)
	require.Nil(t, chunk2)
	require.False(t, held2)
}

// TestCommitAsDrawnSpreadsAcrossPeers: serving peer A then peer B in one pass —
// B's pick reflects A's commits, so a second peer still draws the same scarce
// positions (it lacks them) but holder-count is now 1 from A's draw.
func TestCommitAsDrawnSpreadsAcrossPeers(t *testing.T) {
	ss, b := newTestSlot(10, 1, 2, 3)

	cA, _ := ss.selectOneChunkForPeer(pid(1), 3, true, noHolderCountLimit)
	require.ElementsMatch(t, []int{1, 2, 3}, cA[string(b.data)])
	for _, pos := range []int{1, 2, 3} {
		require.Equal(t, 1, b.holderCount[pos], "after A: hc 1")
	}

	// B lacks 1,2,3 (A's draw only set A's available), so B draws them too; each
	// goes from hc 1 → 2.
	cB, _ := ss.selectOneChunkForPeer(pid(2), 3, true, noHolderCountLimit)
	require.ElementsMatch(t, []int{1, 2, 3}, cB[string(b.data)])
	for _, pos := range []int{1, 2, 3} {
		require.Equal(t, 2, b.holderCount[pos], "after B: hc 2")
	}
	checkIndexInvariant(t, ss)
}

// TestPopcountOnFlip: markHolder bumps holder-count only on a genuine 0→1 flip
// of a peer's available bit; a repeat is a no-op.
func TestPopcountOnFlip(t *testing.T) {
	ss, b := newTestSlot(10, 7)
	require.Equal(t, 0, b.holderCount[7])

	require.True(t, ss.markHolder(b, pid(1), 7), "first flip")
	require.Equal(t, 1, b.holderCount[7])
	require.False(t, ss.markHolder(b, pid(1), 7), "repeat: no flip")
	require.Equal(t, 1, b.holderCount[7])

	require.True(t, ss.markHolder(b, pid(2), 7), "second peer flips")
	require.Equal(t, 2, b.holderCount[7])
	checkIndexInvariant(t, ss)
}

// TestRollbackChunkRestores: a drawn-but-held chunk is fully reverted —
// holder-count, the peer's available bit, and the index level all return.
func TestRollbackChunkRestores(t *testing.T) {
	ss, b := newTestSlot(10, 1, 2)
	chunk, _ := ss.selectOneChunkForPeer(pid(1), 2, true, noHolderCountLimit)
	require.Equal(t, 2, chunkLen(chunk))
	require.Equal(t, 2, levelLen(ss.levels[1]))

	ss.rollbackChunk(pid(1), chunk)
	for _, pos := range []int{1, 2} {
		require.Equal(t, 0, b.holderCount[pos], "hc restored")
		require.False(t, b.peerAvail[pid(1)].Get(pos), "available bit cleared")
	}
	require.Equal(t, 2, levelLen(ss.levels[0]), "entries back at level 0")
	require.Equal(t, 0, levelLen(ss.levels[1]))
	checkIndexInvariant(t, ss)
}
