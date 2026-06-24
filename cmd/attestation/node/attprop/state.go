package attprop

import (
	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	"github.com/libp2p/go-libp2p/core/peer"
)

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

// idxEntry identifies one forwardable attestation across all buckets at a
// (topic, slot): bucketKey is string(attestation_data); pos is the committee
// position. The same position in two forks is two distinct entries.
type idxEntry struct {
	bucketKey string
	pos       int
}

// countLevel holds the entries currently at one holder-count value. entries is
// an append-ordered slice (deterministic, no per-pass sort); at maps an entry
// to its slice index for O(1) swap-delete. Mirrors node/partial_priority.go's
// countLevel verbatim.
type countLevel struct {
	entries []idxEntry
	at      map[idxEntry]int
}

func (l *countLevel) add(e idxEntry) {
	if l.at == nil {
		l.at = make(map[idxEntry]int)
	}
	if _, ok := l.at[e]; ok {
		return
	}
	l.at[e] = len(l.entries)
	l.entries = append(l.entries, e)
}

func (l *countLevel) remove(e idxEntry) {
	i, ok := l.at[e]
	if !ok {
		return
	}
	last := len(l.entries) - 1
	if i != last {
		moved := l.entries[last]
		l.entries[i] = moved
		l.at[moved] = i
	}
	l.entries = l.entries[:last]
	delete(l.at, e)
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
}

// slotState holds all buckets for a (topic, slot) plus a holder-count-ordered
// index over their validated positions. levels[k] holds the entries whose
// current holder-count == k; selection walks levels ascending (scarcest first).
// bucketSeq gives each bucket a stable order for deterministic tie-breaking.
// Mirrors node/partial_priority.go's prioritySlotState but keyed by holder-count
// instead of sendCount.
type slotState struct {
	slot      int
	buckets   map[string]*bucket
	levels    []countLevel
	bucketSeq map[string]int
	nextSeq   int
}

// indexAddValidated inserts a newly-validated position at its holder-count
// level. Implemented by the Core agent.
func (ss *slotState) indexAddValidated(bk string, pos, holderCount int) {
	panic("TODO: Core — state.go")
}

// indexBumpHolder moves an entry up one level after a peer's available bit for
// the position flipped 0→1 (via bitmap, their send, or our send). Implemented
// by the Core agent.
func (ss *slotState) indexBumpHolder(bk string, pos, from int) {
	panic("TODO: Core — state.go")
}

// selectOneChunkForPeer draws up to maxN of the scarcest positions the peer
// lacks (ascending holder-count, committed as drawn so the next peer served
// sees the update — §E2), returning the chunk as bucketKey->positions in
// priority order plus whether more remain beyond this chunk. Implemented by the
// Core agent.
func (ss *slotState) selectOneChunkForPeer(
	p peer.ID,
	maxN int,
) (chunk map[string][]int, more bool) {
	panic("TODO: Core — state.go")
}
