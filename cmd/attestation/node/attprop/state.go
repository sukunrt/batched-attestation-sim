package attprop

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

// attDigestHex returns the hex-encoded 8-byte SHA-256 prefix of
// attestation_data, the correlation token reused across the partial/attprop log
// pipeline (§H2). Reimplemented here because attprop cannot import node; mirrors
// node.attDigestHex.
func attDigestHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

// newCommitteeBitmap returns a zero bitmap.Bitmap of capacity n bits
// ((n+7)/8 bytes). Reimplemented here because attprop cannot import node (§B
// reuse map); mirrors node.newCommitteeBitmap.
func newCommitteeBitmap(n int) bitmap.Bitmap {
	return make(bitmap.Bitmap, (n+7)/8)
}

// attEntry holds one committee member's signature plus the shared
// attestation_data it signed, stored per (bucket, position). Mirrors node's
// PartialAttestationEntry.
type attEntry struct {
	Position  int
	Signature []byte
	Data      []byte
}

// candidate identifies one forwardable attestation across all buckets at a
// (topic, slot): bucketKey is string(attestation_data); pos is the committee
// position. The same position in two forks is two distinct candidates.
type candidate struct {
	bucketKey string
	pos       int
}

const noHolderCountLimit = -1

// countLevel holds the entries currently at one holder-count value.
type countLevel struct {
	entries map[string]map[int]struct{}
}

func (l *countLevel) add(bucketKey string, pos int) {
	if l.entries == nil {
		l.entries = make(map[string]map[int]struct{})
	}
	positions := l.entries[bucketKey]
	if positions == nil {
		positions = make(map[int]struct{})
		l.entries[bucketKey] = positions
	}
	positions[pos] = struct{}{}
}

// remove deletes e and reports whether it was present (so callers can move an
// entry only when it was actually indexed).
func (l *countLevel) remove(bucketKey string, pos int) bool {
	positions := l.entries[bucketKey]
	if positions == nil {
		return false
	}
	if _, ok := positions[pos]; !ok {
		return false
	}
	delete(positions, pos)
	if len(positions) == 0 {
		delete(l.entries, bucketKey)
	}
	return true
}

// bucket is the per-(topic, slot, attestation_data) state. Forks at the same
// slot get independent buckets. atts holds the entries we possess; validating /
// validated track verifier progress (only validated positions are forwardable,
// count toward the +K bitmap trigger, and bump holder-count — §G2). holderCount
// is the popcount over peers' available bit for each position (the scarcity
// metric, §E1); peerAvail is each peer's advertised/inferred available bitmap.
type bucket struct {
	data       []byte
	atts       map[int]*attEntry
	validating map[int]struct{}
	validated  map[int]struct{}

	holderCount map[int]int
	peerAvail   map[peer.ID]bitmap.Bitmap

	// lastEmitted is the validated bitmap last advertised on the bitmap stream,
	// for the floor "re-emit only if changed" check (§D2). nil until first emit.
	lastEmitted bitmap.Bitmap
}

// slotState holds all buckets for a (topic, slot) plus a holder-count-ordered
// index over their validated positions. levels[k] holds the entries whose
// current holder-count == k; selection walks levels ascending (scarcest first).
// The index grows with observed holders so push peers can still receive items
// above the bitmap-forwarding limit.
type slotState struct {
	slot          int
	committeeSize int // wire-level bitmap capacity, for sizing per-peer available
	buckets       map[string]*bucket
	levels        []countLevel

	// validatedSinceEmit counts positions validated since the last bitmap
	// advertisement for this slot; reaching bitmapTriggerK fires a +K emit (§D2).
	validatedSinceEmit int
}

// newBucket initialises empty per-(topic, slot, attestation_data) state. data is
// cloned so the caller's frame buffer can be reused.
func newBucket(data []byte) *bucket {
	return &bucket{
		data:        slices.Clone(data),
		atts:        make(map[int]*attEntry),
		validating:  make(map[int]struct{}),
		validated:   make(map[int]struct{}),
		holderCount: make(map[int]int),
		peerAvail:   make(map[peer.ID]bitmap.Bitmap),
	}
}

// newSlotState initialises a slot's state with a holder-count index pre-sized to
// maxPeers levels. The index grows if holder counts exceed that initial size.
// committeeSize sizes the per-peer available bitmaps.
func newSlotState(slot, maxPeers, committeeSize int) *slotState {
	return &slotState{
		slot:          slot,
		committeeSize: committeeSize,
		buckets:       make(map[string]*bucket),
		levels:        make([]countLevel, maxPeers),
	}
}

func (ss *slotState) ensureLevel(k int) {
	for len(ss.levels) <= k {
		ss.levels = append(ss.levels, countLevel{})
	}
}

// getOrCreateBucket returns (creating as needed) the bucket for attestation_data
// within this slot.
func (ss *slotState) getOrCreateBucket(data []byte) *bucket {
	key := string(data)
	b, ok := ss.buckets[key]
	if !ok {
		b = newBucket(data)
		ss.buckets[key] = b
	}
	return b
}

// peerAvailFor returns (creating as needed) a peer's available bitmap for the
// bucket. Absence is equivalent to all-clear.
func (b *bucket) peerAvailFor(p peer.ID, committeeSize int) bitmap.Bitmap {
	bm, ok := b.peerAvail[p]
	if !ok {
		bm = newCommitteeBitmap(committeeSize)
		b.peerAvail[p] = bm
	}
	return bm
}

// addReceived stores attestations received from a peer (pending validation) and
// returns the entries that were newly added (already-held positions are
// skipped). Mirrors node's AttestationState.addReceived.
func (b *bucket) addReceived(positions []int, signatures [][]byte) []any {
	var newEntries []any
	for i, pos := range positions {
		if _, ok := b.atts[pos]; ok {
			continue
		}
		e := &attEntry{Position: pos, Signature: signatures[i], Data: b.data}
		b.atts[pos] = e
		b.validating[pos] = struct{}{}
		newEntries = append(newEntries, e)
	}
	return newEntries
}

// indexAddValidated inserts a newly-validated position at its holder-count
// level.
func (ss *slotState) indexAddValidated(bk string, pos, holderCount int) {
	ss.ensureLevel(holderCount)
	ss.levels[holderCount].add(bk, pos)
}

// markHolder records that peer p now holds position pos in bucket b: it sets the
// peer's available bit, and — only on a genuine 0→1 flip — increments
// holderCount and bumps the scarcity index. Returns whether the flip happened.
// This is the single funnel for every "peer learned a position" signal (§E1):
// inbound bitmap, inbound data, and our own send all route through it, so the
// holder-count and the index stay consistent by construction.
func (ss *slotState) markHolder(b *bucket, p peer.ID, pos int) bool {
	bm := b.peerAvailFor(p, ss.committeeSize)
	if bm.Get(pos) {
		return false
	}
	bm.Set(pos)
	hc := b.holderCount[pos]
	b.holderCount[pos] = hc + 1
	// Only validated positions live in the index; bump is a no-op otherwise.
	bk := string(b.data)

	if hc >= len(ss.levels) || !ss.levels[hc].remove(bk, pos) {
		return true
	}
	ss.ensureLevel(hc + 1)
	ss.levels[hc+1].add(bk, pos)
	return true
}

// selectOneChunkForPeer draws up to maxN of the scarcest positions peer p lacks,
// ascending holder-count (scarcest first), then commits each draw via markHolder
// so the next peer served in the same pass sees the updated holder-count — the
// commit-as-drawn spreading of §E2. maxHolderCount is an exclusive upper bound
// on selected holder-count levels; noHolderCountLimit scans all levels. It
// returns the chunk as bucketKey->positions in priority order (nil when the peer
// has nothing left to receive within the limit). If allowPartial is false, a
// non-full chunk is held by returning nil, held=true before committing it.
//
// Candidates are collected read-only per level first, then committed, so the
// index mutation in markHolder can't perturb the in-progress scan. Entries in
// ss.levels are validated by construction.
func (ss *slotState) selectOneChunkForPeer(
	p peer.ID,
	maxN int,
	allowPartial bool,
	maxHolderCount int,
) (chunk map[string][]int, held bool) {
	var drawn []candidate
	levelLimit := len(ss.levels)
	if maxHolderCount >= 0 && maxHolderCount < levelLimit {
		levelLimit = maxHolderCount
	}
OUTER:
	for k := 0; k < levelLimit; k++ {
		for bucketKey, positions := range ss.levels[k].entries {
			b := ss.buckets[bucketKey]
			if b == nil {
				continue
			}
			peerAvail := b.peerAvailFor(p, ss.committeeSize)
			for pos := range positions {
				if peerAvail.Get(pos) {
					continue
				}
				if len(drawn) == maxN {
					break OUTER
				}
				drawn = append(drawn, candidate{bucketKey, pos})
			}
		}
	}
	if len(drawn) == 0 {
		return nil, false
	}
	if !allowPartial && len(drawn) < maxN {
		return nil, true
	}

	chunk = make(map[string][]int)
	for _, e := range drawn {
		b := ss.buckets[e.bucketKey]
		chunk[e.bucketKey] = append(chunk[e.bucketKey], e.pos)
		ss.markHolder(b, p, e.pos)
	}
	return chunk, false
}

// encodeBatch builds a BatchedAttestation for the given positions, emitting
// AttestorIndices and Signatures in the same order as positions. Caller must
// ensure every position exists in b.atts.
func encodeBatch(b *bucket, positions []int) *pb.BatchedAttestation {
	idxs := make([]uint32, 0, len(positions))
	sigs := make([][]byte, 0, len(positions))
	for _, pos := range positions {
		idxs = append(idxs, uint32(pos))
		sigs = append(sigs, b.atts[pos].Signature)
	}
	return &pb.BatchedAttestation{
		AttestationData: b.data,
		AttestorIndices: idxs,
		Signatures:      sigs,
	}
}
