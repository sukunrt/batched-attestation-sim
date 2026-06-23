# Partial messages with priority (`partial-priority`)

## Context

Today's partial-message mode (`cmd/attestation/node/partial.go`) pushes each mesh peer **every**
validated attestation position it is missing in one big message per tick, bounded only by the
per-attestation lifetime cap `MaxPeersPerAttestation`. One large message delays first-byte delivery
and bunches a node's bandwidth into a single burst.

`partial-priority` is a second, independent forwarding strategy with one goal: **keep every outgoing
gossipsub data message small** (≤ 30 attestations ≈ ~3 KB of signatures). It still sends a peer
everything it can (nothing is dropped), but splits the send into several small messages per tick and
orders them so the **least-forwarded attestations go out first**. Small messages let a peer start
re-forwarding the first chunk immediately instead of waiting for one big blob, and the priority order
pushes scarce (under-propagated) attestations to the front so they spread fast.

It is a drop-in *alternative* to classic partial mode, selected by a config flag, so experiments can
compare **classic vs partial vs partial-priority** the same way classic-vs-partial is compared today.

## Data model (recap)

- A **topic** has a fixed committee of size `num_attestors`; a committee **position** is an index
  `[0, num_attestors)`.
- One **attestation** = one `(topic, attestation_data, position)` tuple = one signature (~96 bytes).
  "30 attestations" = 30 signatures, so a capped message is ~3 KB.
- State is keyed per `(topic, slot)` → `attestationsMap: map[string(attestation_data)]*AttestationState`.
  Each `attestation_data` is a **bucket** (forks at the same slot coexist as separate buckets).
- Per bucket: `validated` (positions ready to forward), `sendCount[pos]` (how many peers we've already
  forwarded each position to — the **forward count / priority key**), and `peers[peerID].available`
  (bitmap of positions that peer already has).

## The algorithm

Runs in the periodic publish tick (`publish_interval_ms`). `publishActions` processes one
`(topic, slot)` and returns an iterator yielding `(peer, PublishAction)`. **The same peer may be
yielded multiple times** — each yield becomes one separate RPC (confirmed: the extension's publish
loop in `partialmessages/partialmsgs.go` does `for p, action := range actions { sendRPC(p, …) }`
with no dedup). That is how we send a peer several small messages in one tick.

**Per peer, per `(topic, slot)`:**

1. **Candidates** = positions across all buckets where, with `bps = bucket.peers[peer]`:
   `position ∈ bucket.validated`, `bucket.sendCount[position] < MaxPeersPerAttestation` (lifetime
   ceiling kept), `!bps.available.Get(position)` (peer lacks it), and — for a **gossip peer** —
   `bps.pendingWant.Get(position)` (only what it explicitly requested).
2. **Order** candidates by `sendCount` ascending (least-forwarded first); tie-break by position, then
   by a stable bucket order. Walk this order, **skipping positions the peer already has**, and keep
   drawing the next ones (if the first 30 in global order include some the peer holds, continue to
   #31, #32, … until the chunk is full of positions the peer actually needs).
3. **Chunk** the peer's needed candidates into groups of `N = max_attestations_per_message`
   (default **30**), preserving priority order. Each chunk → one `BatchedAttestationEnvelope` (one
   `BatchedAttestation` per bucket represented in that chunk) → one `PublishAction` yielded for this
   peer. Several chunks ⇒ several yields ⇒ several small RPCs. **Nothing sendable is dropped** — the
   30 is a per-*message* size, not a per-tick throttle.
4. **Commit** each sent position as it goes into a chunk: `bps.available.Set(position)` and
   `bucket.sendCount[position]++`.
5. **Metadata** (the per-bucket `available`/`requests` advertisement) is sent in **its own**
   `PublishAction`: when the peer has any metadata to advertise, yield one metadata-only action
   (`EncodedPartsMetadata` set, `EncodedPartialMessage` empty), separate from the data chunks. Each
   data-chunk action carries only `EncodedPartialMessage`. This keeps every action single-purpose,
   sends the advertisement exactly once, and naturally covers the metadata-but-no-data case (a gossip
   peer with an advertisement but nothing to push this tick still gets its one metadata action).
   `pendingWant` clearing and IWANT-target selection stay per-bucket and unchanged from classic
   partial mode.
6. Proceed to the next peer, which now sees the **updated** `sendCount` values.

### Efficiency (important — sims are large)
Do **not** re-sort all candidates per peer per tick. Maintain, per `(topic, slot)` slot-state, a
structure that keeps validated entries ordered by `sendCount` cheaply — recommended: **bucket
`(bucket, position)` entries by their integer `sendCount` value** (0,1,2,…,Max). Selection walks
count-buckets ascending; committing a send moves an entry from bucket `k` to `k+1` in O(1). A min-heap
keyed by `sendCount` is an acceptable alternative. The naive O(n log n) re-sort per peer will make
large simulations too slow.

### Why it works
- **Small messages:** each RPC carries ≤ 30 signatures (~3 KB), so a peer receives and can re-forward
  the first chunk quickly instead of blocking on one large message.
- **Spread:** the priority order puts the least-forwarded attestations in the earliest (lowest-latency)
  chunk, and the per-peer sequential `sendCount` bump means scarce attestations reach fresh peers
  before popular ones consume the per-position budget.
- **Convergence:** `bps.available` prevents re-sending a position to a peer that has it, so over ticks
  each peer that needs a position receives it exactly once, as long as demand stays under
  `MaxPeersPerAttestation` (same guarantee classic partial mode gives today).

## Decisions

- **Independent copy, nothing shared.** Implement as a standalone `partialPriorityManager` in a new
  file `cmd/attestation/node/partial_priority.go`, with its own copy of the publish/receive pipeline
  and prefixed state types (`priorityAttestationState`, `priorityPeerState`, …). Reuse **only**
  primitives/constants: the `pb.*` wire types, the `bitmap` package, `*Node` / `verifier` / `Tracer`,
  `slotGroupID`, the committee-bitmap helper, and shared named constants. `partial.go` stays
  **untouched**. (A separate `node/partialpriority/` package would drop the type-name prefixes but
  needs Node's deps passed explicitly instead of `*Node` to avoid an import cycle — variant, not the
  default.)
- **Send everything sendable, chunked** into ≤ N messages (the intent: reduce message size, don't
  throttle volume). The 30-cap is the per-message chunk size; `MaxPeersPerAttestation` is the only
  volume throttle and is **kept** as the lifetime ceiling. Set it generously (≥ D) in priority
  configs.
- **Same rule for the gossip-IWANT path.** A gossip peer's requested positions are answered in full,
  also chunked into ≤ N messages and priority-ordered — every outgoing data message stays short.
- **`max_attestations_per_message` is a config knob**, default 30, so it can be swept.
- **Fanout eager-publish unchanged** (`fanoutPublish`): one position to all peers, not chunked, does
  not touch `sendCount`. A fanout node sends a single attestation, so a cap is irrelevant there.
- **Per-peer iteration stays Go's randomized map order.** Sequential `sendCount` bumps + randomized
  order spread load without systematically favoring any peer. Multi-peer tests therefore assert only
  aggregate invariants, never which peer got which position.
- **`sendCount` stays per-bucket** (per `attestation_data`): position 5 in fork A is a different
  signature than position 5 in fork B, so their forward counts are independent.

## Implementation outline (Go)

- **`cmd/attestation/node/partial_priority.go` (new)** — full copy of the partial pipeline as
  `partialPriorityManager`. Only behavioral divergence from `partial.go`: the per-peer data selection
  (replaces `claimAttestationsToSend` + the per-bucket data loop in `publishActions`) becomes the
  cross-bucket, priority-ordered, chunked, multi-yield selection above, backed by the count-bucketed
  structure. Batch/metadata encoding (`encodeBatch`, `getAttestationMetadata` equivalents) copied
  verbatim. Plus `partial_priority_test.go`.
- **`node.go`** — add `PartialPriorityMode bool` and `MaxAttestationsPerMessage int` to `Node`. In
  `Node.Start`, when `PartialPriorityMode` is set, construct `partialPriorityManager` instead of
  `partialAttestationManager` and route the publish loop / fanout publish / incoming-RPC handler to it
  (mirror the existing `newPartialAttestationManager` wiring).
- **`config.go`** — add `PartialPriorityMode bool` and `MaxAttestationsPerMessage int` to `SimConfig`;
  add `EffectiveMaxAttestationsPerMessage()` returning 30 when unset (mirror
  `EffectiveMaxPeersPerAttestation`).
- **`main.go`** — add `-partial-priority` and `-max-attestations-per-message` flags (mirror
  `-use-partial-messages`); OR with config; populate the two new `Node` fields.
- **`simctl/config.py`, `simctl/runner.py`** — add `partial_priority: bool = False` and
  `max_attestations_per_message: int = 30` to `AttestationSimConfig` (and the experiment params
  struct). In `generate_shadow_yaml`, append `-partial-priority` and
  `-max-attestations-per-message=<n>` to each host's args when configured (next to the existing
  `-use-partial-messages` append).
- **`README.md`, `AGENTS.md`** — document the new mode and config fields (note it keeps libp2p
  partial-messages + IHAVE/IWANT gossip "as usual").

## Analysis changes (process partial-priority runs)

partial-priority emits the **same wire/log keys as partial mode** (`partial_received`, `att_count`,
`att_data_bytes`, `sig_bytes`, `metadata_bytes`, parts metadata), so the byte-split and latency
parsing need **no change**. Only mode detection and the comparison/plot drivers learn a third mode:

- **`analysis/prelim-analysis.py`** — `analyze_run` (line 227) currently does
  `mode = "partial" if sim.get("use_partial_messages") else "classic"`. Extend to:
  `"partial-priority"` when `sim.get("partial_priority")`, else `"partial"` when
  `use_partial_messages`, else `"classic"`. In `class_bw`, treat `partial-priority` like `partial`
  for `control_recv` (parts metadata, not IHAVE/IWANT). In `main`, generalize the
  comparison driver (lines 323-340): with classic as the baseline, run `print_comparison(classic, …)`
  against **each** non-classic mode present (partial and/or partial-priority), instead of the current
  hard-coded classic-vs-partial pair. Keep the existing uniform-vs-tiered branch working within each
  mode.
- **`analysis/arrival_cdf.py`** — `mode_of` (line 68) and the `styles` dict (lines 131-132): add a
  `partial-priority` mode and a distinct line style (e.g. dotted) so a 3-way run overlays cleanly.
  The PARTIAL_PAT regex already matches partial-priority's `partial_received` lines unchanged.
- **`analysis/plot_arrival_latency_cdf.py`** — same one-line mode recognition (wherever it checks
  `use_partial_messages` to pick classic vs partial) so it labels/overlays the partial-priority run;
  parsing is identical to partial.

## Files to touch

`cmd/attestation/node/partial_priority.go` (new), `…/partial_priority_test.go` (new),
`cmd/attestation/node/node.go`, `cmd/attestation/config.go`, `cmd/attestation/main.go`,
`simctl/config.py`, `simctl/runner.py`, `analysis/prelim-analysis.py`, `analysis/arrival_cdf.py`,
`analysis/plot_arrival_latency_cdf.py`, `README.md`, `AGENTS.md`. **`partial.go` unchanged.**

## Testing (simnet + synctest / unit, no Shadow)

Build on `partial_unit_test.go` helpers: `newPartialUnitManager`, `runPublishActions`, `makePeers`,
`peerAcceptsPartial`. Core selection tests are single-peer (deterministic); multi-peer tests assert
only aggregates.

1. **Priority order** — one bucket, 50 validated positions with `sendCount[pos] = pos`, one mesh peer,
   `MaxPeersPerAttestation` large, `N = 30`. Assert the peer is yielded twice: first message =
   positions `0..29`, second = `30..49`; `sendCount` bumped for all sent.
2. **Skip-and-continue** — peer already `available` for some low-priority positions (e.g. 5, 8).
   Assert those are skipped and the next-in-order positions take their place, so the first message
   still holds 30 positions the peer needs.
3. **Cross-bucket chunk** — two buckets at one `(topic, slot)` with interleaved `sendCount`. Assert
   each emitted chunk holds ≤ 30 positions drawn from the global lowest-`sendCount` set across both
   buckets, regrouped into one `BatchedAttestation` per bucket. (Fails against per-bucket code →
   proves the cross-bucket lift.)
4. **Gossip want, chunked** — a gossip peer with `pendingWant` for > 30 positions. Assert it is
   answered in full across multiple ≤ 30 messages, priority-ordered.
5. **Under-cap** — fewer than 30 candidates → a single message with all of them.
6. **Lifetime ceiling** — a position at `sendCount == MaxPeersPerAttestation` is never a candidate.
7. **E2E (simnet + `synctest.Test` via `runE2E`)** — small topology where a node accumulates > 30
   attestations; assert every outgoing data batch has ≤ 30 positions and all attestations eventually
   reach the intended receivers (`expectAttestation`).

**Done when:** `go build/vet ./...` clean, `go test ./cmd/attestation/node/` (incl. new tests) green,
and a smoke config with `partial_priority: true` emits the new CLI flags in a host's `shadow.yaml`
args. Do not run a long Shadow sim unless asked.
