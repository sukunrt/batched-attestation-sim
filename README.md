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

In partial-message mode the simulator uses libp2p's partial-messages extension to batch
attestations that share the same attestation data, following the `draft-committee-attestation`
spec. Each topic is a fixed committee of `num_attestors` members, chosen once when the simulation
is built: the topic's `fanout_nodes_per_topic` fanout nodes plus enough mesh nodes to reach
`num_attestors`.

Classic GossipSub IHAVE/IWANT gossip remains enabled by default, including in partial-message
mode:

```yaml
disable_ihave_gossip: false
```

Set `disable_ihave_gossip: true` only for a no-IHAVE variant.

## Analysis

`analysis/prelim-analysis.py <run-or-experiment-dir>` prints a classic-vs-partial comparison:
time-to-receive latency percentiles and a received-bytes breakdown (attestation data, signatures,
and control), scoped to the last slot.

`analysis/plot_arrival_latency_cdf.py` (single node, classic vs partial) and
`analysis/plot_arrival_delays_cdf.py` (sim vs mainnet) write arrival-delay CDF plots to `graphs/`.

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
