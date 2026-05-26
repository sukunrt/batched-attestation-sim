"""Shadow simulation runner for attestations."""

import json
import random
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from simctl.config import AttestationConfig, AttestationSimConfig, GossipsubParams
from simctl.manifest import format_dir_timestamp, random_suffix
from simctl.topology import (
    Topology,
    generate_random_topology,
    generate_ring_topology,
)


def get_simlab_root() -> Path:
    """Get the simlab project root directory."""
    return Path(__file__).parent.parent.resolve()


def generate_gml(topology: Topology, latency_multiple: float = 1.0) -> str:
    """Generate GML graph for Shadow network simulation."""
    lines = ["graph [", "  directed 0"]

    for node in topology.nodes:
        lines.extend(
            [
                "  node [",
                f"    id {node.num}",
                f'    host_bandwidth_up "{node.upload_bw_mbps} Mbit"',
                f'    host_bandwidth_down "{node.download_bw_mbps} Mbit"',
                "  ]",
            ]
        )

    for node in topology.nodes:
        lines.extend(
            [
                "  edge [",
                f"    source {node.num}",
                f"    target {node.num}",
                '    latency "1 ms"',
                "    packet_loss 0.0",
                "  ]",
            ]
        )

    for edge in topology.edges:
        lines.extend(
            [
                "  edge [",
                f"    source {edge.source}",
                f"    target {edge.target}",
                f'    latency "{round(edge.latency_ms * latency_multiple)} ms"',
                "    packet_loss 0.0",
                "  ]",
            ]
        )

    lines.append("]")
    return "\n".join(lines)


def compute_publish_schedule(
    num_nodes: int,
    num_slots: int,
    num_attestors: int,
    fanout_nodes: set[int] | None = None,
    fanout_nodes_per_topic: int = 0,
) -> dict[int, list[int]]:
    """Compute which slots each node publishes in.

    Fanout nodes publish in every slot. The remaining publisher budget per slot
    is filled by randomly selecting from mesh (non-fanout) nodes.
    num_attestors is per-topic: mesh_attestors = num_attestors - fanout_nodes_per_topic.

    For slot k, seed RNG with k and pick remaining publishers from mesh nodes.
    Returns mapping from node_num to sorted list of slot numbers.
    """
    if fanout_nodes is None:
        fanout_nodes = set()

    schedule: dict[int, list[int]] = {i: [] for i in range(num_nodes)}
    mesh_nodes = [i for i in range(num_nodes) if i not in fanout_nodes]
    mesh_attestors = num_attestors - fanout_nodes_per_topic

    for slot in range(1, num_slots + 1):
        # All fanout nodes publish every slot
        for node_num in fanout_nodes:
            schedule[node_num].append(slot)

        # Fill remaining budget from mesh nodes
        if mesh_attestors > 0:
            rng = random.Random(slot)
            publishers = rng.sample(mesh_nodes, k=mesh_attestors)
            for node_num in publishers:
                schedule[node_num].append(slot)

    return schedule


def compute_peer_lists(topology: Topology) -> dict[int, list[int]]:
    """Extract per-node peer lists from topology edges.

    Returns mapping from node_num to sorted list of peer node numbers.
    """
    peers: dict[int, set[int]] = {node.num: set() for node in topology.nodes}

    for edge in topology.edges:
        peers[edge.source].add(edge.target)
        peers[edge.target].add(edge.source)

    return {node_num: sorted(peer_set) for node_num, peer_set in peers.items()}


def build_attestation(output_path: Path) -> None:
    """Build the attestation Go binary."""
    src_dir = get_simlab_root() / "cmd" / "attestation"
    cmd = [
        "go",
        "build",
        "-buildvcs=false",
        "-o",
        str(output_path.resolve()),
        ".",
    ]
    subprocess.run(cmd, check=True, cwd=src_dir)


def generate_shadow_yaml(
    config: AttestationSimConfig,
    topology: Topology,
    binary_path: str,
    config_file_path: str,
    publish_schedule: dict[int, list[int]],
    peer_lists: dict[int, list[int]],
) -> dict[str, Any]:
    """Generate Shadow configuration dictionary for attestations."""
    fanout_nodes = set(topology.fanout_nodes)
    sorted_fanout = sorted(fanout_nodes)
    fanout_nodes_per_topic = config.topology.fanout_nodes_per_topic
    gml = generate_gml(topology, latency_multiple=config.topology.latency_multiple)

    hosts: dict[str, Any] = {}
    for node in topology.nodes:
        node_num = node.num

        is_fanout = node_num in fanout_nodes
        publish_mode = "fanout" if is_fanout else "mesh"
        args_parts = [
            f"-config-file={config_file_path}",
            f"-node-num={node_num}",
            f"-publish-mode={publish_mode}",
        ]
        if is_fanout and fanout_nodes_per_topic > 0:
            fanout_offset = sorted_fanout.index(node_num)
            topic_idx = fanout_offset // fanout_nodes_per_topic
            args_parts.append(f"-fanout-topic-index={topic_idx}")
        if config.disable_ihave_gossip:
            args_parts.append("-disable-ihave-gossip")
        if config.use_partial_messages:
            args_parts.append("-use-partial-messages")

        slots = publish_schedule.get(node_num, [])
        if slots:
            args_parts.append(f"-publish-slots={','.join(str(s) for s in slots)}")

        peers = peer_lists.get(node_num, [])
        if peers:
            args_parts.append(f"-peer-nums={','.join(str(p) for p in peers)}")

        hosts[f"node{node_num}"] = {
            "network_node_id": node_num,
            "processes": [
                {
                    "path": binary_path,
                    "args": " ".join(args_parts),
                    "start_time": "0 sec",
                }
            ],
        }

    stop_minutes = int(config.stop_time_minutes)

    return {
        "general": {
            "stop_time": f"{stop_minutes} min",
            "log_level": config.log_level,
            "progress": True,
            "heartbeat_interval": "10s",
        },
        "network": {
            "graph": {
                "type": "gml",
                "inline": gml,
            },
            "use_shortest_path": True,
        },
        "hosts": hosts,
    }


def write_shadow_config(config_dict: dict[str, Any], path: Path) -> None:
    """Write Shadow configuration to YAML file."""
    with open(path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)


def get_run_dir(base_dir: Path, num_nodes: int, gossip: GossipsubParams) -> Path:
    """Generate a unique run directory name."""
    now = datetime.now()
    timestamp = format_dir_timestamp(now)
    suffix = random_suffix()
    return base_dir / f"run-{timestamp}-{suffix}-n{num_nodes}-D{gossip.D}-Dlo{gossip.Dlow}-Dhi{gossip.Dhigh}"


def _create_run_dir(output_dir: Path, num_nodes: int, gossip: GossipsubParams) -> Path:
    """Create a unique run directory with retry logic.

    Attempts to create a unique directory up to 20 times to handle
    potential collisions from concurrent runs.

    Returns the created directory path.
    Raises RuntimeError if unable to create after 20 attempts.
    """
    for _ in range(20):
        candidate = get_run_dir(output_dir, num_nodes, gossip)
        try:
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        except FileExistsError:
            continue

    raise RuntimeError("Failed to create unique run directory after 20 attempts")


def run_simulation(
    config: AttestationConfig,
    output_dir: Path,
    *,
    topology: Topology | None = None,
    binary_src: Path | None = None,
) -> tuple[Path, subprocess.CompletedProcess]:
    """Run an attestation Shadow simulation.

    Args:
        config: Full simulation config.
        output_dir: Parent directory for run output.
        topology: Pre-generated topology to reuse. If None, generates from config.
        binary_src: Path to pre-built binary to copy. If None, builds from source.

    Returns (run_dir, completed_process).
    """
    sim = config.simulation

    # Generate or reuse topology
    if topology is None:
        topo_cfg = sim.topology
        total_fanout = topo_cfg.fanout_nodes_per_topic * sim.num_topics
        if topo_cfg.type == "random":
            topology = generate_random_topology(
                num_nodes=topo_cfg.num_nodes,
                degree=topo_cfg.degree,
                seed=topo_cfg.seed,
                super_node_fraction=topo_cfg.super_node_fraction,
                fanout_nodes=total_fanout,
                fanout_node_mesh_peers=topo_cfg.fanout_node_mesh_peers,
                min_latency_ms=topo_cfg.min_node_to_node_latency_ms,
            )
        else:
            topology = generate_ring_topology(
                num_nodes=topo_cfg.num_nodes,
                seed=topo_cfg.seed,
                super_node_fraction=topo_cfg.super_node_fraction,
                min_latency_ms=topo_cfg.min_node_to_node_latency_ms,
            )

    peer_lists = compute_peer_lists(topology)

    publish_schedule = compute_publish_schedule(
        num_nodes=len(topology.nodes),
        num_slots=sim.num_slots,
        num_attestors=sim.num_attestors,
        fanout_nodes=topology.fanout_nodes,
        fanout_nodes_per_topic=sim.topology.fanout_nodes_per_topic,
    )

    # Compute per-topic attestor lists for partial messages.
    # Each topic's list is sorted(mesh_nodes ∪ fanout_nodes_for_topic).
    # A node's attestor index is its position in its topic's list.
    fanout_nodes_set = set(topology.fanout_nodes)
    sorted_fanout = sorted(fanout_nodes_set)
    mesh_nodes = sorted(n.num for n in topology.nodes if n.num not in fanout_nodes_set)
    fnpt = sim.topology.fanout_nodes_per_topic
    attestor_lists: list[list[int]] = []
    for t in range(sim.num_topics):
        topic_fanout = sorted_fanout[t * fnpt : (t + 1) * fnpt]
        attestor_lists.append(sorted(mesh_nodes + topic_fanout))

    # Create unique run directory
    run_dir = _create_run_dir(output_dir, num_nodes=len(topology.nodes), gossip=sim.gossipsub_params)

    # Save mesh topology and config
    topology.save(run_dir / "topology.json")
    config_data = config.model_dump()
    config_data["simulation"]["attestor_lists"] = attestor_lists
    config_file = run_dir / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f, default_flow_style=False)

    # Save fanout nodes and publish schedule
    with open(run_dir / "schedule.json", "w") as f:
        json.dump(
            {
                "fanout_nodes": sorted(topology.fanout_nodes),
                "publish_schedule": {str(k): v for k, v in publish_schedule.items()},
                "peer_lists": {str(k): v for k, v in peer_lists.items()},
            },
            f,
            indent=2,
        )

    # Build or copy Go binary
    bin_dest = run_dir / "attestation"
    if binary_src is not None:
        import shutil

        shutil.copy2(binary_src, bin_dest)
    else:
        build_attestation(bin_dest)

    # Generate and write Shadow config
    shadow_config = generate_shadow_yaml(
        config=sim,
        topology=topology,
        binary_path="./attestation",
        config_file_path=str(config_file.resolve()),
        publish_schedule=publish_schedule,
        peer_lists=peer_lists,
    )
    write_shadow_config(shadow_config, run_dir / "shadow.yaml")

    # Run Shadow
    log_path = run_dir / "shadow.log"
    with open(log_path, "w") as log_file:
        result = subprocess.run(
            ["shadow", "shadow.yaml"],
            cwd=run_dir,
            stdout=log_file,
            stderr=log_file,
        )

    print(f"Shadow completed. Results in: {run_dir}")
    print(f"Exit code: {result.returncode}")

    return run_dir, result
