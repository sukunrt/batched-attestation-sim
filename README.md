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

## Classic And Partial Messages

Classic mode is the default when `use_partial_messages` is false or omitted. Publishers send full
attestation messages through normal GossipSub topics.

Enable partial-message mode with:

```yaml
use_partial_messages: true
```

In partial-message mode, the simulator installs libp2p's partial-messages extension, joins topics
with partial-message requests, and batches attestations by shared attestation data. The wire
format follows the `draft-committee-attestation` spec:

- `BatchedAttestation { attestation_data, attestor_indices (bitmap[num_attestors]), signatures[] }`
- `CommitteeAttestationPartsMetadata { slot, attestation_data, available, requests }`
- Each PublishAction carries a `ControlEnvelope` (per-bucket metadata list) and a
  `BatchedAttestationEnvelope` (per-bucket batch list).

Each topic has a fixed committee of `num_attestors` members chosen once at sim build:
`fanout_nodes_per_topic` fanout nodes assigned to that topic (positions `[0, fnpt)`) plus
`num_attestors - fnpt` mesh nodes drawn from the global mesh pool (positions
`[fnpt, num_attestors)`). Committee membership is written to `schedule.json` as
`committee_membership: {topic_id: [node_nums…]}` and passed to each Go process via the
`-committee-memberships=t0:p0;t1:p1` CLI flag. Position is the index into the per-topic
committee list; node identity is decoupled from committee position.

`attestor_indices`, `available`, and `requests` are fixed-width bitmaps of length
`num_attestors` bits (`ceil(num_attestors / 8)` bytes). Bit `i` set means the committee member
at position `i` is involved.

State is keyed per `(topic, slot, attestation_data)` bucket — forks coexist as independent
buckets so two divergent attestation data variants at the same slot do not get deduplicated by
committee position.

Verification goes through the same per-node batch verifier as classic mode — both modes enqueue
items, sleep once per batch, and confirm via callback — only the entry point differs (classic
blocks inside the topic validator; partial fires-and-forgets from the RPC handler and promotes
attestations to validated state in the callback).

Classic GossipSub IHAVE/IWANT gossip remains enabled by default, including in partial-message
mode:

```yaml
disable_ihave_gossip: false
```

Set `disable_ihave_gossip: true` only for a no-IHAVE variant. `ihave_gossip_degree: 0` uses the
default degree of 6, matching GossipSub `Dlazy`.

### Partial-mode log keys

Structured slog lines emitted by partial mode (use `att_digest` — the hex-encoded 8-byte SHA-256
prefix of `attestation_data` — to correlate across the pipeline):

- `self_published`, `attestation_validated` — per-position publish/validate lifecycle.
- `partial_send_tick`, `partial_send_data`, `partial_send_metadata` — outgoing per-(peer, tick)
  and per-bucket detail.
- `partial_fanout_publish` — fanout publisher's eager batch send.
- `partial_recv_tick`, `partial_recv_metadata`, `partial_recv_batch` — incoming per-(peer, tick)
  and per-bucket detail.

Wire-level tracer lines (`topic_message_*`, `partial_*`) are attestation-aware (`att_count`,
`att_data_bytes`, `sig_bytes`, …) for bandwidth analysis.

## Analysis

`analysis/prelim-analysis.py <dir>` prints a classic-vs-partial comparison (time-to-receive-95%
latency and a received-bytes composition table) for a run or experiment directory.

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

`cmd/attestation/main.go` starts slot 1 approximately two minutes after process startup. Set
`stop_time_minutes` long enough to cover startup, `slot_duration_seconds * num_slots`, and the
final 30 second drain sleep.

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

Avoid running long Shadow simulations unless you intentionally want to spend the time and
resources.
