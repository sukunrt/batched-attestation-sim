# AGENTS.md

## Repository Overview

This repo contains `simctl`, a Python CLI for generating and running Shadow simulations, plus a
Go attestation simulator in `cmd/attestation`.

Always inspect this file before working in the repo. Do not run long Shadow simulations unless the
user explicitly asks you to run them.

`README.md` is the user-facing project documentation. Keep this file compatible with the README:
agent-only process notes belong here, while install, usage, config shape, and run-output
documentation should be reflected in the README when behavior changes.

## Attestation CLI

Use the `attestations` subcommand for a single attestation simulation:

```bash
uv run simctl attestations --config <config.yaml> --output-dir <runs-dir>
```

Use the `experiment` subcommand when comparing multiple simulation variants that share one
generated topology:

```bash
uv run simctl experiment --config <experiment.yaml> --output-dir <runs-dir>
```

## How Attestation Runs Work

`simctl/main.py` defines the CLI. For local `attestations` runs it loads an attestation YAML
config, then calls `simctl/runner.py::run_simulation`.

`run_simulation` does the following:

1. Generates or reuses a topology.
2. Computes peer lists from topology edges.
3. Computes a publish schedule where fanout nodes publish every slot and mesh publishers fill the
   remaining `num_attestors` budget.
4. Writes `topology.json`, `config.yaml`, `schedule.json`, and `shadow.yaml` into a unique run
   directory.
5. Builds `cmd/attestation` into the run directory.
6. Runs `shadow shadow.yaml` from that run directory.

Fanout topology is controlled by `topology.fanout_nodes_per_topic`. These are extra nodes
numbered after the mesh nodes. For example, `num_nodes: 32`, `num_topics: 1`, and
`fanout_nodes_per_topic: 10` creates 32 mesh nodes plus 10 fanout nodes, 42 total simulated
hosts.

`topology.fanout_node_mesh_peers` controls how many random mesh nodes each fanout node connects
to.

## Remote Runs & Result Analysis

Remote runs (`--remote sukun@ethp2p` on `attestations` or `experiment`) rsync the working tree,
build on the remote, run Shadow, then **pack the run/experiment directory into a `.tar.gz` under
`runs/` on the remote and delete the unpacked tree**. The tarball is not downloaded automatically,
and the unpacked tree is large (tens of GB), so fetch the tarball — never the directory:

```bash
# 1. fetch the tarball back
rsync -ah --info=progress2 \
  sukun@ethp2p:/home/sukun/batched-attestation-sim/runs/<name>.tar.gz runs/
# 2. extract (experiment dir lands at runs/<name>/exp-<ts>-<hash>/)
tar xzf runs/<name>.tar.gz -C runs/
```

Then run the classic-vs-partial analysis on the experiment dir (the one holding `topology.json`
plus a `runs/` subdir):

```bash
uv run python analysis/prelim-analysis.py runs/<name>/exp-<ts>-<hash>
```

It parses each host's `shadow.data/hosts/*/attestation.*.stderr` (last slot only) and prints
time-to-receive percentiles plus the received-byte split (att_data / signature / control) for
classic vs partial. Don't pass a `timeout` — large topologies take minutes to parse.

### Arrival-latency plot

`analysis/plot_arrival_latency_cdf.py` plots the per-attestation arrival-latency (time-to-receive)
CDF from a single node's perspective for one topic/slot, overlaying the classic and partial runs:

```bash
uv run python analysis/plot_arrival_latency_cdf.py runs/<name>/exp-<ts>-<hash> \
  --node 3 --topic 0 --slot 2
```

Defaults are `--node 3 --topic 0 --slot 2`; the plot is written to
`graphs/arrival_latency_node<N>_topic<T>_slot<S>.png` (`graphs/` is checked in). Super nodes are
mesh nodes (not in `fanout_nodes`) with `upload_bw_mbps >= 1024`; `committee_index` in the receive
log lines is the topic index.

## Classic Vs Partial Messages

The Go simulator is in `cmd/attestation`.

Classic mode is the default when `use_partial_messages` is false or omitted:

1. Nodes register normal gossipsub topic validators that submit each incoming message to the
   node-owned batch verifier and block until the batch finishes.
2. Publishers marshal full `pb.Attestation` messages.
3. Publishers call `topic.Publish` on each joined topic.
4. Receivers read from subscriptions and log received attestations.

Partial-message mode is enabled with:

```yaml
use_partial_messages: true
```

The partial-message path implements the spec at
`../ethp2p/specs/draft-committee-attestation.md`. Wire shape:

- `BatchedAttestation { attestation_data | attestation_data_hash, attestor_indices[],
  signatures[] }`.
- `CommitteeAttestationPartsMetadata { slot, attestation_data | attestation_data_hash,
  available_ids[], requests_ids[] }`. Legacy `available` / `requests` bitmap fields remain
  decode-compatible only; new sends leave them empty.
- `ControlEnvelope` wraps `repeated CommitteeAttestationPartsMetadata` per peer per tick.
- `BatchedAttestationEnvelope` wraps `repeated BatchedAttestation` per peer per tick.

Each topic has a fixed committee of size `num_attestors` chosen once at sim
build by simctl: `fanout_nodes_per_topic` fanout nodes assigned to that topic
(positions `[0, fnpt)`) plus `num_attestors - fnpt` mesh nodes drawn from the
global mesh pool (positions `[fnpt, num_attestors)`). Committee membership is
written to `schedule.json` as `committee_membership: {topic_id: [node_nums…]}`
and plumbed to each Go process via the `-committee-memberships=t0:p0;t1:p1`
CLI flag. Position is the index into the per-topic committee list; node
identity (`node_num`) is decoupled from committee position. The
`attestor_indices`, `available_ids`, and `requests_ids` values are committee positions, not node
numbers. Metadata IDs are sent as per-peer, per-bucket deltas: once a peer has been informed about
a position for a bucket, later metadata to that peer omits it.

In partial-message mode:

1. Nodes install libp2p's partial-messages extension with `pubsub.WithPartialMessagesExtension`.
2. Topic joins use `pubsub.RequestPartialMessages()`.
3. Mesh nodes run a periodic partial publish loop controlled by `publish_interval_ms` (default 20ms).
4. Fanout nodes eagerly call `PublishPartial` for their single attestation because they do not
   rely on the mesh tick loop. Fanout sends one BatchedAttestation with one bit set.
5. State is keyed per `(topic, slot, sha256(attestation_data))` bucket — forks at the same slot
   coexist as independent buckets, satisfying the spec rule that nodes MUST NOT deduplicate by
   `(slot, committee_position)`. Each manager caches
   `sha256(attestation_data) => attestation_data`; each peer receives full `attestation_data` once
   per partial payload kind, then hash-only.
6. Validation goes through the same node-owned batch verifier as classic mode, but is entered
   from the partial RPC handler: received attestations are enqueued with a per-submit callback
   that promotes them to validated state after the batch sleep. Normal topic validators are not
   registered in this mode.

Per spec, mesh peers receive only `BatchedAttestation` (no metadata); gossip
peers receive metadata advertising `available_ids` and may include `requests_ids`.
Requests are non-persistent — satisfied on the next outgoing publish tick and
cleared, never queued. Outbound request IDs are also delta-compressed per peer/bucket, so the same
peer is not repeatedly asked for the same position in later metadata.

In this repo, "classic gossipsub" with partial messages means normal gossipsub IHAVE/IWANT gossip
remains enabled. Configure it with:

```yaml
disable_ihave_gossip: false
```

Do not set `disable_ihave_gossip: true` unless the user wants the no-IHAVE variant.

### Partial-Priority Messages

`partial-priority` is a second, independent partial-message forwarding strategy, selected with:

```yaml
partial_priority: true
max_attestations_per_message: 30  # optional, default 30 (the per-message size cap N)
send_available_with_data: true    # optional, default false; see below
```

It is a drop-in alternative to partial mode over the same libp2p partial-messages extension (same
wire types, same `RequestPartialMessages`, IHAVE/IWANT gossip as usual, same receive/validation
path). The **only** behavioral divergence is the send: instead of one large push per peer per tick,
it round-robins one ≤ N-sized data message to each requesting peer per pass, repeating passes until
every peer is drained. Each message holds the least-forwarded validated attestations that peer
lacks, drawn across all buckets in `sendCount` order; every draw is committed (bumps `sendCount`)
before the next peer is served, so a peer's pick reflects what the prior peers just took —
spreading scarce attestations across peers instead of draining one peer fully first. The gossip
`available_ids`/`requests_ids` advertisement is its own separate metadata-only message. Nothing sendable is
dropped — N caps message size, not per-tick volume; `max_peers_per_attestation` stays the only
volume throttle (the per-position lifetime ceiling), so set it generously (≥ D).

`send_available_with_data: true` (partial-priority only, default false) piggybacks the node's own
validated `available_ids` delta for every bucket onto a mesh peer's data message, so the peer
learns our state and stops forwarding us positions we already hold. The metadata is attached
**only when we still hold more positions the peer needs than fit in this message** (`more` from
`selectOneChunkForPeer`) — if the message already carries everything we have for the peer, the data
itself conveys our state and the metadata would be pure overhead. It is sent at most once per peer per
tick, rides the same RPC as the data (one `CommitteeAttestationPartsMetadata` per bucket,
`available_ids` only), and is never a standalone message. That last point is required: a peer is
classified as a **gossip peer** when it sends a **metadata-only** RPC, so the receiver only flips a
sender to gossip when the RPC carries no data batches — a mesh peer's available-plus-data piggyback
keeps it classified as mesh. Gossip peers are skipped (they already get `available_ids` via their
separate metadata-only message).

**Measured neutral at full load.** At 500-mesh / 2000-attestor / 2-topic load this did **not** cut
the straggler tail or reduce duplicate receives (`att_recv` flat) — it added ~+30–46% control bytes
for no latency change. That tail is throughput/mop-up bound (see `ideas/analysis-straggler.md`), not
knowledge-bound, so advertising state doesn't help there. Kept default-off as a tool to revisit at
lower D or against the throughput angle.

Implementation: `cmd/attestation/node/partial_priority.go` (`priorityAttestationManager`). It
**reuses** partial.go's data model (`AttestationState`, `peerState`, `peerAttestationState`,
`PartialAttestationEntry`) and pure helpers (`getAttestationMetadata`, `selectIWantTargets`,
`newCommitteeBitmap`, `slotGroupID`, …); only the manager, the `prioritySlotState` (which adds a
`sendCount`-bucketed index for O(1) priority ordering), the send selection, and the
index-maintaining methods are new. `partial.go` is untouched. `node.go` routes to it via the
`partialManager` interface and `Node.PartialPriorityMode`; `Node` holds the two managers as
separate fields so tests can reach each one's internals. partial-priority emits the **same**
log/wire keys as partial mode, so analysis parsing is unchanged — only mode detection learns the
third mode (`partial_priority` in config ⇒ `"partial-priority"`).

### att_propagation

`att_propagation` is a third forwarding mode that **drops gossipsub entirely** and runs a native
libp2p protocol instead. Enable it with:

```yaml
att_propagation: true
max_attestations_per_message: 30   # optional, the per-message size cap N (reused from partial-priority)
# bitmap D values are literal, including 0; other 0-valued tunables use the spec default
attprop_push_dlow: 0
attprop_push_d: 0
attprop_push_dhigh: 0
attprop_bitmap_dlow: 0
attprop_bitmap_d: 0
attprop_bitmap_dhigh: 0
attprop_send_budget_b: 0
attprop_max_peers_per_att: 0
attprop_tick_interval_ms: 0
attprop_bitmap_floor_interval_ms: 0
attprop_heartbeat_interval_ms: 0
attprop_prune_backoff_seconds: 0
```

It is mutually exclusive with `use_partial_messages` and `partial_priority` (the Go side fatals on
the combination; simctl's Pydantic models also reject it early). Each peer connection uses **three
persistent bidirectional per-topic streams** — push (eager batched data), bitmap (periodic have-set
advertisement), and control (graft/prune mesh maintenance) — rather than relying on gossipsub's
IHAVE/IWANT. Each node owns one independent `attprop.Manager` per topic, keyed by topic name; each
manager has its own eventloop, mesh, slot state, verifier, stream state, and send budget. Forwarding
is driven by **holder-count scarcity**: push peers receive missing attestations regardless of holder
count, while bitmap peers receive only entries with holder count below
`pushPeers + bitmapPeers/2`, throttled by the per-topic send budget `B`.
Buckets are keyed by `sha256(attestation_data)`. Push and bitmap streams each send full
`attestation_data` once per peer stream for a bucket, then send only `attestation_data_hash`; each
manager caches `sha256(attestation_data) => attestation_data` for validation and tracing. Bitmap
streams queue the latest full available state internally, but each per-peer writer emits only
`available_ids` deltas and leaves legacy bitmap fields empty.

Mode bool plumbing mirrors `partial_priority`: simctl writes the `att_propagation` key (plus the
`attprop_*` tunables and `max_attestations_per_message`) into `config.yaml` from the Pydantic model,
and `runner.generate_shadow_yaml` emits `-att-propagation` (plus `-max-attestations-per-message=N`)
on the process args. The `attprop_*` tunables ride only in `config.yaml` (the Go side reads them
directly) — they have **no** CLI flags. The Go batch verifier was extracted into its own
`cmd/attestation/verify` package, reused by the partial strategies (package `node`) and each
topic-local att_propagation manager (`cmd/attestation/node/attprop`).

att_propagation reuses the partial-mode app-level receive keys — it logs `partial_received`,
`partial_recv_batch`, `partial_recv_metadata`, `attestation_validated`, etc. via the same
`SlogTracer.OnPartialReceive` path — and adds native wire-accounting keys from the stream reader:
`attprop_data_received`, `attprop_metadata_received`, and `attprop_control_received`. Mode
detection labels it `att-propagation` (the first/most-specific branch). `analysis/prelim-analysis.py`
parses the attprop wire keys for exact att_data/signature/control byte split and can print a
standalone attprop-only run when no classic baseline is present.

### Partial-mode log keys

Structured slog lines emitted by partial mode (use `att_digest` — the hex-encoded
8-byte SHA-256 prefix of `attestation_data` — to correlate across the pipeline):

- `self_published` — this node published its own attestation (per topic per slot).
- `partial_send_tick` — outgoing PublishAction summary per (peer, tick).
- `partial_send_data` — per-bucket data sent to one peer (positions, batch bytes).
- `partial_send_metadata` — per-bucket metadata sent to one gossip peer.
- `partial_fanout_publish` — fanout node's eager batch send.
- `partial_recv_tick` — incoming RPC summary per (peer, tick).
- `partial_recv_metadata` — per-bucket metadata received.
- `partial_recv_batch` — per-bucket data batch received.
- `attestation_validated` — verifier callback promoted a position to validated.
- `rpc_sent` / `rpc_received` / `topic_ihave_*` / `topic_message_*` / `partial_sent` / `partial_received` — wire-level lines from `rpc_tracer.go`, attestation-aware (`att_count`, `att_data_bytes`, `sig_bytes`; partial adds `data_batches` / `meta_count` / `available_ones` / `requests_ones`); consumed by `analysis/prelim-analysis.py` to compare classic vs partial.


## Timing Notes

`cmd/attestation/main.go` sets slot 1 to start two minutes after process startup. Configured
`stop_time_minutes` must be long enough to cover startup, slot duration times `num_slots`, and
the final 30 second drain sleep.

## Config Files

Single-run configs use this shape:

```yaml
simulation:
  topology:
    ...
  gossipsub_params:
    D: 8
    Dlow: 6
    Dhigh: 12
  ...
```

Experiment configs use this shape:

```yaml
topology:
  ...
simulations:
  - gossipsub_params:
      D: 8
      Dlow: 6
      Dhigh: 12
    ...
```

### Per-tier mesh degree (`supernode_d` / `homenode_d`)

The sim config also takes optional `supernode_d` and `homenode_d` blocks (each a full
`Dlow`/`D`/`Dhigh` triple). `runner.generate_shadow_yaml` resolves each node's degree by
bandwidth — super (`upload_bw_mbps >= SUPER_NODE_UPLOAD_MBPS`, i.e. `1024`) uses `supernode_d`,
home uses `homenode_d`, each falling back to `gossipsub_params` — and passes it to *every* node
via the `-gossipsub-params=Dlow:8,D:12,Dhigh:16` CLI flag. `cmd/attestation/main.go`'s
`parseGossipsubParams` parses that flag and overrides `sim.GossipsubParams` for the process.
`analysis/prelim-analysis.py` labels a run `tiered` when either tier block is present and compares
uniform-D vs tiered-D within each mode.

## Testing

Prefer simnet-backed Go tests driven by `testing/synctest` over running Shadow from tests. The
`Network` interface in `cmd/attestation/node/network.go` lets `Node` run against either the
production Shadow network or a simnet-backed test network (`github.com/marcopolo/simnet`). Tests
wrap the simnet run in `synctest.Test` so the fake clock and goroutine scheduler stay in sync,
giving deterministic, fast network tests. Existing tests under `cmd/attestation/node` (for
example `node_test.go`, `rpc_tracer_test.go`, `partial_end_to_end_test.go`, `partial_unit_test.go`,
`verifier_test.go`) follow this pattern. New tests should use simnet + synctest unless they
specifically exercise Shadow-side behavior.

## Development Notes

Prefer small, focused changes. The repo may use jj;
prefer jj over git for version-control operations when needed.

When updating commands, config fields, run artifacts, or behavior documented here, update
`README.md` in the same change unless the information is intentionally agent-only.
