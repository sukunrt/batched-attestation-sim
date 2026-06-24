package attprop

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// checkIndexInvariant asserts the holder-count index invariant (§E): an entry
// {bk,pos} is present in exactly one level k iff the position is validated and
// its holderCount == k < ceiling. It also checks each level's at-map agrees with
// its entries slice (the O(1) swap-delete bookkeeping).
func checkIndexInvariant(t *testing.T, ss *slotState) {
	t.Helper()
	ceiling := len(ss.levels)

	// Forward: every validated position under the ceiling is indexed at its hc.
	for bk, b := range ss.buckets {
		for pos := range b.validated {
			hc := b.holderCount[pos]
			e := idxEntry{bk, pos}
			if hc >= ceiling {
				for k := range ss.levels {
					_, in := ss.levels[k].at[e]
					require.Falsef(t, in, "pos %d (hc %d ≥ ceiling) must not be indexed", pos, hc)
				}
				continue
			}
			_, in := ss.levels[hc].at[e]
			require.Truef(t, in, "validated pos %d must be in level %d", pos, hc)
			for k := range ss.levels {
				if k == hc {
					continue
				}
				_, dup := ss.levels[k].at[e]
				require.Falsef(t, dup, "pos %d indexed in level %d, want only %d", pos, k, hc)
			}
		}
	}

	// Reverse: every indexed entry is validated, under the ceiling, at its level.
	for k := range ss.levels {
		lvl := &ss.levels[k]
		require.Equal(t, len(lvl.entries), len(lvl.at), "level %d at/entries size", k)
		for i, e := range lvl.entries {
			require.Equal(t, i, lvl.at[e], "level %d at[%v] index", k, e)
			b := ss.buckets[e.bucketKey]
			require.NotNil(t, b)
			_, val := b.validated[e.pos]
			require.Truef(t, val, "indexed entry %v must be validated", e)
			require.Equalf(t, k, b.holderCount[e.pos], "entry %v at level %d but hc %d",
				e, k, b.holderCount[e.pos])
		}
	}
}

const testCommittee = 64

// newTestSlot builds a slotState with one bucket and the given validated
// positions, all at holder-count 0, ceiling = maxPeers.
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
// and a position at/over the ceiling is never indexed.
func TestIndexAddValidatedInvariant(t *testing.T) {
	ss := newSlotState(1, 3, testCommittee) // ceiling 3
	b := ss.getOrCreateBucket([]byte("d"))
	for _, pos := range []int{5, 9, 1} {
		b.validated[pos] = struct{}{}
		ss.indexAddValidated(string(b.data), pos, 0)
	}
	// One at hc 2 (still < ceiling), one at hc 3 (== ceiling: dropped).
	b.validated[20] = struct{}{}
	b.holderCount[20] = 2
	ss.indexAddValidated(string(b.data), 20, 2)
	b.validated[21] = struct{}{}
	b.holderCount[21] = 3
	ss.indexAddValidated(string(b.data), 21, 3)

	require.Len(t, ss.levels[0].entries, 3)
	require.Len(t, ss.levels[2].entries, 1)
	_, in21 := ss.levels[0].at[idxEntry{string(b.data), 21}]
	require.False(t, in21, "ceiling position must not be indexed")
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

	// Cap large enough to take all three: order must be scarcest-first 30,20,10.
	chunk, more := ss.selectOneChunkForPeer(pid(1), 10)
	require.False(t, more)
	require.Equal(t, []int{30, 20, 10}, chunk[string(b.data)])
}

// TestScarcityChunkCapAndMore: with more validated positions than the cap, the
// chunk holds the maxN scarcest and reports more=true.
func TestScarcityChunkCapAndMore(t *testing.T) {
	ss, b := newTestSlot(10, 1, 2, 3, 4, 5) // all hc 0
	checkIndexInvariant(t, ss)

	chunk, more := ss.selectOneChunkForPeer(pid(1), 3)
	require.True(t, more, "5 candidates, cap 3 ⇒ more")
	require.Equal(t, 3, chunkLen(chunk))
	// Deterministic: lowest positions first within the single hc-0 level.
	require.Equal(t, []int{1, 2, 3}, chunk[string(b.data)])
}

// TestCommitAsDrawnReordersLevels: a draw commits holder-count++ per position,
// so the entries move up a level and the peer's available bit is set (no
// re-draw next pass).
func TestCommitAsDrawnReordersLevels(t *testing.T) {
	ss, b := newTestSlot(10, 1, 2, 3)
	chunk, _ := ss.selectOneChunkForPeer(pid(1), 3)
	require.Equal(t, []int{1, 2, 3}, chunk[string(b.data)])

	// After the draw: holderCount 1 for each, entries now in level 1, peer holds.
	for _, pos := range []int{1, 2, 3} {
		require.Equal(t, 1, b.holderCount[pos])
		require.True(t, b.peerAvail[pid(1)].Get(pos))
	}
	require.Len(t, ss.levels[0].entries, 0)
	require.Len(t, ss.levels[1].entries, 3)
	checkIndexInvariant(t, ss)

	// Same peer again ⇒ nothing left (it already holds all).
	chunk2, more2 := ss.selectOneChunkForPeer(pid(1), 3)
	require.Nil(t, chunk2)
	require.False(t, more2)
}

// TestCommitAsDrawnSpreadsAcrossPeers: serving peer A then peer B in one pass —
// B's pick reflects A's commits, so a second peer still draws the same scarce
// positions (it lacks them) but holder-count is now 1 from A's draw.
func TestCommitAsDrawnSpreadsAcrossPeers(t *testing.T) {
	ss, b := newTestSlot(10, 1, 2, 3)

	cA, _ := ss.selectOneChunkForPeer(pid(1), 3)
	require.Equal(t, []int{1, 2, 3}, cA[string(b.data)])
	for _, pos := range []int{1, 2, 3} {
		require.Equal(t, 1, b.holderCount[pos], "after A: hc 1")
	}

	// B lacks 1,2,3 (A's draw only set A's available), so B draws them too; each
	// goes from hc 1 → 2.
	cB, _ := ss.selectOneChunkForPeer(pid(2), 3)
	require.Equal(t, []int{1, 2, 3}, cB[string(b.data)])
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
	chunk, _ := ss.selectOneChunkForPeer(pid(1), 2)
	require.Equal(t, 2, chunkLen(chunk))
	require.Len(t, ss.levels[1].entries, 2)

	ss.rollbackChunk(pid(1), chunk)
	for _, pos := range []int{1, 2} {
		require.Equal(t, 0, b.holderCount[pos], "hc restored")
		require.False(t, b.peerAvail[pid(1)].Get(pos), "available bit cleared")
	}
	require.Len(t, ss.levels[0].entries, 2, "entries back at level 0")
	require.Len(t, ss.levels[1].entries, 0)
	checkIndexInvariant(t, ss)
}
