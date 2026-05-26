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

- `BatchedAttestation { attestation_data, attestor_indices (bitmap[committee_size]), signatures[] }`.
- `CommitteeAttestationPartsMetadata { slot, attestation_data, available (bitmap), requests (bitmap) }`.
- `ControlEnvelope` wraps `repeated CommitteeAttestationPartsMetadata` per peer per tick.
- `BatchedAttestationEnvelope` wraps `repeated BatchedAttestation` per peer per tick.

The `attestor_indices` / `available` / `requests` fields are fixed-width bitmaps
of length `committee_size` bits (default 2048, configurable per run with
`committee_size:` in YAML). Bit `i` set means committee position `i` contributed
or is wanted/held. Committee position is statically assigned: position == node
number. Startup asserts `node_num < committee_size`.

In partial-message mode:

1. Nodes install libp2p's partial-messages extension with `pubsub.WithPartialMessagesExtension`.
2. Topic joins use `pubsub.RequestPartialMessages()`.
3. Mesh nodes run a periodic partial publish loop controlled by `publish_interval_ms` (default 20ms).
4. Fanout nodes eagerly call `PublishPartial` for their single attestation because they do not
   rely on the mesh tick loop. Fanout sends one BatchedAttestation with one bit set.
5. State is keyed per `(topic, slot, attestation_data)` bucket — forks at the same slot coexist
   as independent buckets, satisfying the spec rule that nodes MUST NOT deduplicate by
   `(slot, committee_position)`.
6. Validation goes through the same node-owned batch verifier as classic mode, but is entered
   from the partial RPC handler: received attestations are enqueued with a per-submit callback
   that promotes them to validated state after the batch sleep. Normal topic validators are not
   registered in this mode.

Per spec, mesh peers receive only `BatchedAttestation` (no metadata); gossip
peers receive metadata advertising `available` and may include `requests`.
Requests are non-persistent — satisfied on the next outgoing publish tick and
cleared, never queued.

In this repo, "classic gossipsub" with partial messages means normal gossipsub IHAVE/IWANT gossip
remains enabled. Configure it with:

```yaml
disable_ihave_gossip: false
```

Do not set `disable_ihave_gossip: true` unless the user wants the no-IHAVE variant.

`ihave_gossip_degree: 0` means use the default degree of 6, matching gossipsub `Dlazy`.

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
- `rpc_sent` / `rpc_received` / `partial_sent` / `partial_received` — wire-level RPC counters from `rpc_tracer.go`.


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
