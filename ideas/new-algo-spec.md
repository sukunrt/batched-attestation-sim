# New Attestation Broadcast — Design Spec (WIP)

**Status:** design LOCKED across A–H (2026-06-24). Ready to prototype. This file is the handoff
state — another agent can pick up from here; build order is in "Prototype plan" at the bottom.

**Source idea:** `ideas/new-algo.md`. Motivation: `ideas/analysis-straggler.md` (the tail we are
attacking). Closest existing strategy: `ideas/partial-priority.md` +
`cmd/attestation/node/partial_priority.go`.

## Core idea (one paragraph)
Drop gossipsub. Run a native libp2p protocol with three **per-topic** streams between peers
(push / bitmap / control). Each peer is **Connected**, in the **Push Mesh**, or in the **Bitmap
Mesh**. Halve the blind push mesh (~D/2); use a cheap bitmap mesh to learn who holds what; spend
spare upload pushing the *scarcest* attestations to bitmap peers that lack them, through a
flow-controlled send queue that caps parallel data sends and reacts to real wire completion. Goal:
cut the straggler tail / duplicate receives, which the analysis shows is throughput / mop-up
bound, not knowledge bound.

## Streams (per topic, named by topic id)
Protocol IDs are namespaced per topic so the protocol is multi-topic-ready by construction even
though we run a single topic for now. **Every peer has all three; the peer with the lower peerID
opens them** (resolves the symmetric "who opens" question):
- `attestation_push_<topic_id>` — DATA forwarding. To push peers: all our data, eagerly. To bitmap
  peers: opportunistic scarce data when a send slot is free.
- `attestation_bitmap_<topic_id>` — our validated bitmap, to bitmap-mesh peers only.
- `attestation_control_<topic_id>` — mesh management: Graft / Prune for both meshes, with backoff.

Mesh state (push set, bitmap set, backoff timers) is keyed **per topic**. Fanout (originator) nodes
are the exception: they just open a DATA stream to their configured peers, push their one
attestation, and **reset any inbound stream** (pure leaf injectors — see G).

## How to read this doc
Each section (A–H) lists decisions. `DECIDED:` = locked. `OPEN:` = still discussing.
`DEFAULT:` = my proposed answer pending sukun's confirmation. Update `DECIDED` as we go.

## Reuse map (from existing code)
- **Host:** `n.Host` (`cmd/attestation/node/node.go`) is a full libp2p host — register stream
  handlers and open streams directly. Transport is QUIC.
- **Data model (mirror, don't import):** the sub-package reimplements the per-(topic,slot,data)
  state and a count-bucketed index, modeled on `AttestationState` / `peerAttestationState` /
  `prioritySlotState` / `countLevel` from `partial_priority.go`, but keyed by **holder-count** (E).
  It can't import `node` (H1).
- **Verifier (reuse):** `batchVerifier` — submit received batch, callback promotes to validated.
- **Wire types (reuse):** `pb.BatchedAttestationEnvelope`, `pb.CommitteeAttestationPartsMetadata`,
  `pb.ControlEnvelope`, the `bitmap` package.
- **Wiring pattern:** mirror how `priorityAttestationManager` slots into `node.go` (new manager,
  new `Node.<X>Mode` flag, simctl plumbing, a new analysis mode).

## Decisions

### A. Scope & goal
- A1 DECIDED: Run a **single topic** in the sim, but make the protocol topic-scoped via per-topic
  stream protocol IDs (`attestation_{push,bitmap,control}_<topic_id>`). Mesh state keyed per topic
  ⇒ multi-topic-ready by construction. Control stream carries the graft/prune for both meshes.
- A2 DECIDED: Primary success metric = **reduce the p90–p99.9 tail** of time-to-receive vs the
  partial-priority baseline.
- A3 DECIDED: **Tail wins.** Accept a slower exponential phase / median if the p90–p99.9 tail
  improves. No hard "must not regress" constraint.

### B. Substrate & streams
- B1 DECIDED: Native libp2p protocol; drop gossipsub + the partialmessages extension entirely.
- B2 DECIDED (from A1): **3 per-topic streams** — push / bitmap / control. Length-prefixed
  protobuf frames, serial writes per stream.
- B2a DECIDED: **Symmetric** push mesh (gossipsub-style) — graft = mutual forwarding; one number
  is the push-mesh size. Bitmap mesh likewise bidirectional (both ends send bitmaps).
- B2b DECIDED: **Persistent** streams, length-prefixed protobuf frames. **Every peer gets all 3
  streams** (data/bitmap/control); mesh membership is explicit STATE (negotiated on control), not
  encoded by stream existence. What we send depends on role: data → push peers (all) + bitmap peers
  (opportunistic); bitmaps → bitmap peers only; control → always. Eager-vs-lazy stream open and
  shared-bidirectional vs one-per-direction are impl details.
- B3 DECIDED: Reuse `pb.*` wire types, `bitmap`, and `batchVerifier` (via interface, see H1). The
  per-(topic,slot,data) state is **reimplemented** with a holder-count index (E), not the
  sendCount-keyed types verbatim.
- B4 DECIDED (from A1): graft/prune ride the **dedicated control stream** (not folded into
  bitmap). Need a small new control proto (Graft/Prune × {push,bitmap,full}).
- B5 DECIDED: **No network IWANT / peer-to-peer requests.** Forwarding is sender-driven push; the
  bitmaps received from peers tell the sender what each peer lacks and what's scarce, so it picks
  the right message to push when it has spare send capacity. The only "pull" is the send queue's
  INTERNAL flow control (it requests the next-priority message from the selector when a slot frees
  — see F4), never a wire message.

### C. Mesh management (DECIDED — see state machine)
- C1 Sizes (configurable): **push Dlow/D/Dhigh = 4/5/5**, **bitmap low/target/high = 14/16/16**.
  Note D == Dhigh (no overshoot): meshes are hard-capped at target; Dlow is the top-up trigger.
  Over a degree-~30 topology that leaves ~9 connected-but-unmeshed peers as graft candidates.
- C2 Heartbeat maintenance, ~700ms.
- C3 Symmetric meshes (from B2a).
- C4 Push-full rule: **redirect to bitmap** — Prune:Push + Graft:Bitmap; Prune:Full if bitmap full.
- C5 Push / bitmap **mutually exclusive**.
- C6 Push top-up **prefers fresh connected (non-mesh) peers**; promote a bitmap peer into push only
  as a fallback when no fresh connected candidate exists — so we don't vacate (and then refill) a
  bitmap-mesh slot.
- C7 Prune backoff 60s per mesh (Full = both) — DEFAULT, configurable.

**Graft/Prune state machine (per topic):**
1. Connect → open control stream. Fill push first (Graft:Push toward fresh neighbors up to D=5),
   then bitmap (Graft:Bitmap up to 16).
2. Recv Graft:Push → push < 5: accept (open push stream, symmetric). Push full, bitmap < 16:
   Prune:Push + Graft:Bitmap (redirect). Both full: Prune:Full.
3. Recv Graft:Bitmap → bitmap < 16: accept (open bitmap stream). Full: Prune:Bitmap.
4. Recv Prune:X → leave mesh X, close its stream, 60s backoff for X (Full = both).
5. Heartbeat ~700ms → push < 4: graft fresh connected peers (fallback: promote a bitmap peer);
   push > 5: prune excess; bitmap < 14: graft toward 16; bitmap > 16: prune excess. Respect backoff.

### D. Bitmap stream (DECIDED)
- D1 Full bitmap per bucket (idempotent, ~250 B for 2000). Reuse
  `CommitteeAttestationPartsMetadata` with `available` only (no `requests` — there is no IWANT),
  one per active bucket, on the bitmap stream. Send full current bitmap on a fresh Graft:Bitmap.
- D2 Trigger = **count (+K, K=30 default) PLUS a periodic floor**. Floor interval sits between the
  20 ms push tick and the 700 ms heartbeat — default **50 ms** (configurable). Floor re-emits the
  current bitmap only if it changed since last emit.
- D3 Bitmaps only to bitmap-mesh peers; push peers infer our state from the data we send them.

### E. Priority / least-frequent (DECIDED: single scarcity metric)
- E1 **Scarcity is the single priority metric for ALL sends** (push and bitmap). Scarcity of a
  position = number of *peers* known to possess it, counted across **all** peers (push + bitmap). A
  peer is "known to possess" pos when (a) its received bitmap has the bit, (b) it sent us the
  position, or (c) **we sent it to them** ("once we send something to a peer, we record that that
  peer possesses it"). Lowest holder-count = scarcest = sent first. Replaces sendCount as the key.
- E2 For a peer with a free send slot, pick the globally-scarcest position(s) that peer LACKS,
  send, then mark the peer as possessing them (holder-count++ each) so the next peer served sees
  the update — same "commit as drawn" spreading as partial-priority.
- E3 Push forwarding has no holder-count ceiling: push peers receive anything they lack. Bitmap
  forwarding is gated to holder-count levels `< pushPeers + bitmapPeers/2`.

**Index:** reuse partial-priority's count-bucketed index (`prioritySlotState` / `countLevel`) but
key levels by **holder-count** instead of sendCount. holder-count[pos] = popcount over peers of
their `available` bit; increment when any peer's `available[pos]` flips 0→1 (bitmap / their send /
our send). Selection walks levels ascending (scarcest first). Deferred to F: message granularity
(one scarcest item vs a chunk of ≤ N scarcest items the peer lacks).

### F. Send queue (the core — DECIDED)

**Streams (all 3 exist for every peer; role decides what we send):**
- `attestation_push_<topic>` = **DATA stream**. We send data to push peers (all, eagerly) and
  bitmap peers (opportunistic, when a slot is free). The send budget governs this stream.
- `attestation_bitmap_<topic>` = **bitmap-advertisement stream**. We send bitmaps only to
  bitmap-mesh peers (D3); tiny, immediate, BYPASS the budget — so advertisements never block
  behind a data write.
- `attestation_control_<topic>` = graft/prune.
- Mesh membership (push / bitmap / neither) is explicit state, NOT stream existence.

**Concurrency (send budget):**
- F1 Budget **B (default 4, configurable)**. **Push data sends always proceed** (exempt) — one
  in-flight message per push peer, so push may transiently exceed B (e.g. 4 push + 1 draining
  bitmap = 5). **Bitmap data sends start only while total active data sends < B**, in their own
  goroutine pool (re-check the gate when each finishes). One in-flight message per peer everywhere.
- F2 Completion + backpressure = the data stream **Write returning** (QUIC window; blocks under
  congestion) → triggers the peer's next message.
- F3 Message = **≤ N scarcest items the peer lacks** (N=30, configurable), scarcity-ordered,
  committed as drawn (holder-count++). **Push peers receive ALL our data** over time; **bitmap
  peers receive scarce chunks only when a slot is free and holder-count level is below
  `pushPeers + bitmapPeers/2`.**
- F4 A single **eventloop** owns send selection. When a send slot frees, the per-peer sender
  signals "empty send space" and the eventloop picks the next message, **push-mesh first, then
  bitmap**:
  1. Take the next push-peer message (scarcest ≤ N items it lacks).
  2. **Full batch (N items)** → send it.
  3. **Partial batch (< N)** → send it only if `sendAllToPushMesh` is true (tick flush); otherwise
     send a **bitmap-peer** message (scarcity-ordered) instead.
  `sendAllToPushMesh` = false between ticks, set true each 20 ms tick; while true the loop flushes
  every push peer's pending batch (incl. partial), then sets it back to false. Net: full push
  batches go immediately anytime; partial push batches wait for the tick (batching floor) and
  bitmap fills the gap; push latency is bounded by the 20 ms tick. (50 ms floor still drives
  bitmap advertisements on the bitmap stream.)

### G. Fanout & receive (DECIDED)
- G1 Fanout (originator) nodes: open a DATA stream to their configured peers
  (`fanout_node_mesh_peers`, configurable) and send their single attestation; no push/bitmap mesh,
  no scarcity, no graft. They **reset any inbound stream** a peer opens — pure leaf injectors.
- G2 Received data → **batch verifier → validated** is the single gate: only validated positions
  are forwardable, count toward the +K bitmap trigger, and bump holder-count. Raw-received doesn't
  count until validated. Reuse the existing `batchVerifier`.

### H. Code shape & measurement (DECIDED)
- H1 **New sub-package `cmd/attestation/node/attprop/`** (mode / config flag **`att_propagation`**),
  **driven by Node** (Node constructs it and passes deps in, so the package does NOT import `node` —
  avoids a cycle). Reuse `pb.*` + `bitmap` (already separate packages). The per-(topic,slot,data)
  state + holder-count scarcity index is reimplemented here (it needs holder-count, not sendCount).
  `batchVerifier` is shared via a small interface passed in (or moved to a shared package) — resolve
  at prototyping. Node gets `Node.AttPropagation bool` (+ tunables) and routes Start/Run to it.
- H2 Reuse partial's log/wire keys (`partial_recv_batch`, `attestation_validated`, …) so
  `prelim-analysis.py` / arrival-CDF parsing is unchanged; add `graft` / `prune` / eventloop keys.
  Analysis mode detection learns `att_propagation`.

## Open design risks / notes
- Halving the push mesh slows the exponential phase; the bet is that informed bitmap-mesh sends
  recover (and beat) it on the tail. (See A3.)
- Scarcity (holder-count per position) IS the priority index — it replaces sendCount and is fed by
  bitmaps + our sends + their sends. Maintained incrementally (popcount-on-flip).
- "Send completes when Write returns" is a QUIC send-buffer proxy, not peer receipt — fine as a
  backpressure signal, not as a delivery guarantee.
- Prototyping risk (test harness) — MOSTLY RESOLVED: raw streams + framing are deterministic under
  `synctest` (`TestAttPropRawStreamSmoke`); the full eventloop + sender pool still needs the same
  care when built, but the substrate is proven.
- Prototyping risk (backpressure) — RESOLVED: simnet runs real libp2p/QUIC, so stream Write blocks
  under a full flow-control window — the "Write returns = slot frees" signal is real.
- Scarcity (holder-count) is a deliberate **local proxy for how far a position has propagated**:
  low count ≈ under-propagated ≈ send first. Built from every signal we get — bitmap-mesh
  advertisements, data any peer pushes us (push peers forward us the actual messages), and our own
  sends. It's a local sample that varies per node and tightens late via bitmap advertisements,
  exactly when mop-up needs it.

## Prototype plan (build order)
De-risk first, then the vertical slice, then the scarcity/bitmap layer, then measure.

0. **De-risk (simnet+synctest) — DONE ✓.** (b) Backpressure is a non-issue: simnet runs real
   libp2p/QUIC, so Write blocks under a full flow-control window. (a) A custom raw-stream protocol
   with persistent length-prefixed protobuf framing runs deterministically under `synctest` —
   proven by `cmd/attestation/node/attprop_smoke_test.go` (`TestAttPropRawStreamSmoke`). Substrate
   confirmed; the full eventloop + sender pool still needs the same synctest care when built.
1. **Wire + streams:** control proto (Graft/Prune × {push,bitmap,full}); reuse pb data/bitmap
   types; open the 3 per-topic streams (lower-peerID opens); length-prefixed framing; handlers
   route to the manager.
2. **Mesh (C):** graft/prune state machine + heartbeat + backoff. Test convergence (push≈5,
   bitmap≈16) over a degree-~30 topology.
3. **State + scarcity index (E):** per-(topic,slot,data) buckets, holder-count levels, per-peer
   `available`, validated gate via `batchVerifier`.
4. **Send eventloop (F) — the core:** budget B, push-priority, `sendAllToPushMesh` tick batching,
   bitmap fill, Write-completion. Reuse the de-risked sender.
5. **Bitmap stream (D):** full-bitmap advertisements, +K trigger + 50 ms floor, feed holder-count.
6. **Fanout (G):** eager inject to `fanout_node_mesh_peers`; reset inbound.
7. **Plumbing (H):** `att_propagation` flag through simctl / config / main; analysis mode detection.
8. **Measure:** Shadow smoke, then the 500/2000 comparison vs partial-priority on the p90–p99.9 tail.
