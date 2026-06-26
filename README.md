# simlab

`simlab` contains `simctl`, a Python CLI for generating and running Shadow simulations, plus a
Go attestation simulator in `cmd/attestation`.

The main workflow is attestation gossip simulation. `simctl` generates or reuses a topology,
writes the Shadow configuration and per-node schedules, builds the Go simulator, and runs Shadow
from a unique output directory.

## Requirements

- Python 3.13 or newer
- `uv`
- Go 1.25.4 or a compatible newer Go toolchain for `cmd/attestation`
- Shadow available as `shadow` on `PATH` when running simulations

## Project Layout

- `simctl/`: Python CLI, config models, topology generation, Shadow runner, and experiment
  runner.
- `cmd/attestation/`: Go libp2p GossipSub attestation simulator.
- `configs/`: Example single-run and experiment YAML configs.
- `data/`: Country latency and weight inputs used by topology generation.
- `runs/`: Suggested local output location for generated run artifacts.

## CLI Usage

Generate a topology JSON file:

```bash
uv run simctl topology \
  --num-nodes 32 \
  --degree 6 \
  --type random \
  --output-file topology.json
```

Run one attestation simulation:

```bash
uv run simctl attestations \
  --config configs/attestations_32mesh_10fanout_partial_classic.yaml \
  --output-dir runs/attestation
```

Run an experiment with multiple simulation variants sharing one generated topology:

```bash
uv run simctl experiment \
  --config configs/smoke_32.yaml \
  --output-dir runs/experiments
```

Run on a remote host by adding `--remote user@host`. Add `--dry-run` to print remote commands
without executing them.

## Attestation Configs

Single-run configs use a top-level `simulation` object:

```yaml
simulation:
  topology:
    num_nodes: 32
    degree: 20
    type: random
  gossipsub_params:
    D: 8
    Dlow: 6
    Dhigh: 12
  num_topics: 1
  num_slots: 3
  num_attestors: 42
```

Experiment configs define one shared topology and a list of simulation variants:

```yaml
topology:
  num_nodes: 32
  degree: 6
  type: random

simulations:
  - gossipsub_params:
      D: 8
      Dlow: 6
      Dhigh: 12
    num_topics: 1
    num_slots: 3
    num_attestors: 16
```

Topology fanout is controlled by `topology.fanout_nodes_per_topic`. Fanout nodes are extra nodes
numbered after the mesh nodes. For example, `num_nodes: 32`, `num_topics: 1`, and
`fanout_nodes_per_topic: 10` creates 32 mesh nodes plus 10 fanout nodes.

`topology.fanout_node_mesh_peers` controls how many random mesh nodes each fanout node connects
to.

### Per-tier mesh degree

By default every node uses the same `gossipsub_params` (`Dlow`/`D`/`Dhigh`). To give
high-bandwidth "super" nodes a larger mesh than home nodes, set `supernode_d` and/or `homenode_d`
(each a full `Dlow`/`D`/`Dhigh` block). Super nodes (`upload_bw_mbps >= 1024`, set by
`topology.super_node_fraction`) use `supernode_d`; home nodes use `homenode_d`. Either falls back
to `gossipsub_params` when unset, so a uniform run needs only `gossipsub_params`.

```yaml
supernode_d:
  Dlow: 8
  D: 12
  Dhigh: 16
homenode_d:
  Dlow: 3
  D: 4
  Dhigh: 5
```

## Classic And Partial Messages

Classic mode is the default when `use_partial_messages` is false or omitted. Publishers send full
attestation messages through normal GossipSub topics.

Enable partial-message mode with:

```yaml
use_partial_messages: true
```

In partial-message mode the simulator uses libp2p's partial-messages extension to batch
attestations that share the same attestation data, following the `draft-committee-attestation`
spec. Each topic is a fixed committee of `num_attestors` members, chosen once when the simulation
is built: the topic's `fanout_nodes_per_topic` fanout nodes plus enough mesh nodes to reach
`num_attestors`.

Partial data and metadata identify each attestation-data bucket by `sha256(attestation_data)`.
For each peer, the full `attestation_data` is sent once per partial payload kind; later messages
for the same bucket carry only the hash. Metadata advertises committee-position deltas with
`available_ids` and `requests_ids`; legacy bitmap fields are still decoded for compatibility, but
new outgoing metadata does not populate them.

Classic GossipSub IHAVE/IWANT gossip remains enabled by default, including in partial-message
mode:

```yaml
disable_ihave_gossip: false
```

Set `disable_ihave_gossip: true` only for a no-IHAVE variant.

### Partial-priority forwarding

`partial-priority` is a second partial-message forwarding strategy. It keeps every outgoing data
message small and pushes the least-forwarded attestations first: instead of one large push per peer
per tick, it round-robins one small data message (≤ `max_attestations_per_message` attestations,
default 30) to each peer per pass and repeats until every peer is served. Each message carries the
scarcest (under-propagated) attestations that peer still lacks, and every send is counted before the
next peer is picked — so scarce attestations spread across peers instead of piling onto whichever
peer is served first. Nothing sendable is dropped — the cap is a per-message size, not a per-tick
throttle. It runs over the same libp2p partial-messages extension and keeps IHAVE/IWANT gossip as
usual. Enable it with:

```yaml
partial_priority: true
max_attestations_per_message: 30  # optional, default 30
send_available_with_data: true    # optional, default false
```

It is a drop-in alternative to partial mode, so experiments can compare classic vs partial vs
partial-priority the same way classic-vs-partial is compared today. Set `max_peers_per_attestation`
generously (≥ D) in partial-priority configs — it stays the only volume throttle (the per-position
lifetime ceiling).

`send_available_with_data: true` (default off) piggybacks the node's own validated
`available_ids` delta onto a mesh peer's data message so peers learn our state and stop forwarding
us attestations we already hold. It is attached only when we still hold more than fits the message
(otherwise the data already conveys our state), at most once per peer per tick, and only on a
message that also carries data (never on its own), so a mesh peer is not mistaken for a pull-only
gossip peer. Note: at full
2000-attestor load this measured neutral — no tail improvement and no drop in duplicate receives,
for ~+30–46% control bytes — because that tail is throughput-bound, not knowledge-bound. Kept
default-off as a tool to revisit.

### att_propagation

`att_propagation` is a third forwarding mode that replaces GossipSub with a native libp2p protocol.
Each peer connection keeps three persistent bidirectional per-topic streams — push (eager batched
data), bitmap (periodic have-set advertisement), and control (mesh graft/prune) — and each topic
runs an independent manager/eventloop with its own mesh, state, verifier, and send budget. Push
peers receive all missing attestations regardless of holder count; bitmap peers receive only scarce
items with holder count below `pushPeers + bitmapPeers/2`.

Push and bitmap streams send the full `attestation_data` once per peer stream for each bucket, then
refer to it by `sha256(attestation_data)`. Bitmap-stream advertisements use per-peer
`available_ids` deltas.
Enable it with:

```yaml
att_propagation: true
# Optional: form the bitmap mesh, but suppress outbound bitmap advertisements.
disable_bitmap_sends: false
# Optional: also send available-bitmaps to push-mesh peers every push tick.
enable_push_mesh_bitmap: false
```

It is mutually exclusive with `use_partial_messages` and `partial_priority`; setting more than one
is rejected. The per-message size cap reuses `max_attestations_per_message` (default 30). Bitmap
mesh sizes are literal, including `0`; configure them if you want bitmap peers. Other tunables use
`0` to take the protocol default:

| Key | Default | Meaning |
| --- | --- | --- |
| `disable_bitmap_sends` | false | form bitmap mesh peers but do not send bitmap advertisements |
| `enable_push_mesh_bitmap` | false | send available-bitmaps to push-mesh peers on each push tick, using the bitmap stream |
| `attprop_push_dlow` / `attprop_push_d` / `attprop_push_dhigh` | 4 / 5 / 5 | push-mesh sizes (low = top-up trigger, D = high = hard cap) |
| `attprop_bitmap_dlow` / `attprop_bitmap_d` / `attprop_bitmap_dhigh` | 0 / 0 / 0 | bitmap-mesh sizes |
| `attprop_send_budget_b` | 4 | per-topic per-tick send budget B |
| `attprop_max_peers_per_att` | 30 | initial holder-count index capacity |
| `attprop_tick_interval_ms` | 20 | send tick |
| `attprop_bitmap_floor_interval_ms` | 50 | bitmap advertisement interval |
| `attprop_heartbeat_interval_ms` | 700 | mesh-maintenance heartbeat |
| `attprop_prune_backoff_seconds` | 60 | backoff after a prune before re-grafting |

`analysis/prelim-analysis.py` labels this mode `att-propagation` and parses its native
`attprop_*_received` wire-accounting logs for the received-byte split. Each attprop heartbeat also
logs `attprop_mesh_peer_rtt` for every push/bitmap mesh peer, followed by separate
`attprop_bitmap_mesh` and `attprop_push_mesh` summary rows with `size` and average `rtt_ms`.
Attprop bandwidth logging keeps the node-wide `bandwidth` row, including
`push_pending_send_avg` across push peers, and also emits `attprop_peer_bandwidth` per peer with
`role=push|bitmap|conn`; push peers also include `push_pending_send_size`, the count of validated
local attestations not yet marked sent to that peer.
Outbound attprop logging includes `attprop_send_data` when a data frame is enqueued to a peer
(`mesh`, `positions`, queue depth, in-flight counts, budget, and mesh sizes) and
`attprop_send_bitmap` from the bitmap writer for each bitmap frame written. Every successful
attprop writer call also emits `attprop_write_frame` with `writer_type`, `bytes`, and
`duration_ms`.
All modes using the shared batch verifier emit `verification_batch` with `batch_items`,
`attestations`, `queued_ms`, `base_delay_ms`, `per_attestation_delay_ms`, `sleep_ms`,
`verification_duration_ms`, and `duration_ms` (oldest queued item through validation completion).

## Analysis

`analysis/prelim-analysis.py <run-or-experiment-dir>` prints time-to-receive latency percentiles
and a received-bytes breakdown (attestation data, signatures, and control), scoped to the last
slot. With a single mesh degree it compares classic against each non-classic mode present (partial,
partial-priority, and/or att-propagation); with no baseline it prints standalone run tables. When an
experiment mixes tiered-D and uniform-D variants it compares uniform-D vs tiered-D within each mode.

`analysis/plot_arrival_latency_cdf.py` (single node, classic vs partial vs partial-priority) and
`analysis/plot_arrival_delays_cdf.py` (sim vs mainnet) write arrival-delay CDF plots to `graphs/`.
Pass `--runs <dir> <dir> --labels <a> <b>` to overlay any two run directories (for example
tiered-D vs uniform-D).

## Run Outputs

Each attestation run creates a unique directory under the requested output directory. A run
directory contains:

- `topology.json`: Generated or reused topology.
- `config.yaml`: Concrete simulator config, including computed attestor lists.
- `schedule.json`: Fanout nodes, publish schedule, and peer lists.
- `shadow.yaml`: Generated Shadow config.
- `attestation`: Built Go simulator binary.
- `shadow.log`: Shadow stdout and stderr.

Experiment runs create an experiment directory with `experiment.yaml`, shared `topology.json`,
`manifest.json`, and per-variant run directories under `runs/`.

## Timing

Slot 1 starts about two minutes after the simulator starts. Set `stop_time_minutes` long enough to
cover that startup, `slot_duration_seconds * num_slots`, and a final ~30 second drain.

## Development

Check that the Python CLI is installed correctly with:

```bash
uv run simctl --help
```

Run Go tests with:

```bash
cd cmd/attestation
go test ./...
```

Build the Go simulator with:

```bash
cd cmd/attestation
go build -buildvcs=false -o attestation .
```
