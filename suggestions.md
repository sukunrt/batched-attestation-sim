# Code Simplification Suggestions

These are candidate cleanup changes for the attestation simulator, especially
`cmd/attestation/node/attprop`. They are suggestions only; no implementation
changes have been made.

## Best First Changes

1. Extract shared attestation identity helpers into a small shared package.

   `cmd/attestation/node/att_data_hash.go` and
   `cmd/attestation/node/attprop/att_data_hash.go` are near-duplicates. The
   same applies to `attDigestHex` and `newCommitteeBitmap` in
   `cmd/attestation/node/attprop/state.go`. A package such as
   `cmd/attestation/attwire` or `cmd/attestation/internal/attmsg` would remove
   drift without creating an import cycle.

2. Replace `map[string][]int` chunks with an ordered chunk type.

   `selectOneChunkForPeer` says it returns positions in priority order, but it
   returns a map and later send encoding iterates maps again. Use a type such
   as:

   ```go
   type selectedAtt struct {
       bucketKey string
       pos       int
   }

   type selectedChunk []selectedAtt
   ```

   Then encode or group only at the boundary. This would simplify rollback,
   preserve scarcity order, and make tests and traces deterministic.

3. Make the send scheduler explicit instead of using transient manager state.

   `dispatch` always calls `trySelectAndSend` after every event, while `onTick`
   also calls it with `sendAllToPushMesh` temporarily flipped. Passing a
   `sendPolicy` into `trySelectAndSend` would remove `sendAllToPushMesh` and
   the double-call surprise.

4. Collapse per-peer writer maps into a `peerConn`.

   `Manager` keeps `senders`, `bitmapWriters`, and `controlWriters` separately,
   so peer teardown has to coordinate all three maps. A single structure would
   make lifecycle code clearer:

   ```go
   type peerConn struct {
       data    *peerSender
       bitmap  *bitmapWriter
       control *peerSender
   }
   ```

   With `peers map[peer.ID]*peerConn`, `onPeerDown`, `sendControl`,
   `bitmapMeshPeers`, and shutdown become smaller and less error-prone.

## Attprop-Specific Cleanup

5. Table-drive the three stream kinds.

   `wire.go` repeats push, bitmap, and control setup in handlers, dialing,
   read-loop decoding, and logging. A `streamSpec` table with kind, protocol,
   decode, and log behavior would cut boilerplate while keeping the wire
   behavior unchanged.

6. Sort slots, buckets, and pending bitmap metadata where order leaks into
   behavior or tests.

   Peers are already sorted in some paths, but slots, buckets, and pending
   metadata still use raw map order. Sorting those would make runs easier to
   compare and reduce flaky expectations.

7. Consider adding slot to attprop data frames.

   `onInboundData` has to infer slot from known hashes or wall-clock state via
   `slotForHash` / `currentSlot`. That is clever but non-obvious. Adding an
   attprop-specific data envelope with a slot field would remove a lot of
   reasoning. This is a wire-shape change, so it is higher risk than the other
   suggestions.

8. Reduce config repacking.

   Attprop tunables are flat in the Go config, flags, node params, and then
   copied again into `attprop.Config`. A shared `attprop.Tunables` struct plus
   one `WithDefaults` method would reduce field plumbing.

## Recommended Sequence

Start with shared helpers, ordered chunks, and scheduler cleanup. Those are
small, testable, and should simplify attprop without changing the protocol.
