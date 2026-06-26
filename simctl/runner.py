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


# Upload bandwidth (Mbps) at or above which a node is a "super" node. Mirrors
# the threshold used in topology.get_bandwidth and the analysis scripts.
SUPER_NODE_UPLOAD_MBPS = 1024


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


def compute_committees(
    topology: Topology,
    num_topics: int,
    num_attestors: int,
    fanout_nodes_per_topic: int,
    seed: int,
) -> dict[int, list[int]]:
    """Compute fixed per-topic committees of size num_attestors.

    Each topic's committee = (fanout_nodes_per_topic fanout nodes assigned to
    that topic) + (num_attestors - fanout_nodes_per_topic mesh nodes drawn
    from the global mesh pool). The list is ordered: fanout positions
    [0, fnpt), mesh positions [fnpt, num_attestors). Position == index in
    the list.

    Returns mapping from topic_id to ordered list of node_nums.
    """
    fanout_set = set(topology.fanout_nodes)
    sorted_fanout = sorted(fanout_set)
    mesh_nodes = sorted(n.num for n in topology.nodes if n.num not in fanout_set)
    fnpt = fanout_nodes_per_topic
    mesh_attestors = num_attestors - fnpt

    if mesh_attestors < 0:
        raise ValueError(
            f"num_attestors ({num_attestors}) < fanout_nodes_per_topic ({fnpt})"
        )
    if mesh_attestors > len(mesh_nodes):
        raise ValueError(
            f"need {mesh_attestors} mesh attestors per topic, but only {len(mesh_nodes)} mesh nodes available"
        )
    if fnpt > 0 and len(sorted_fanout) < num_topics * fnpt:
        raise ValueError(
            f"need {num_topics * fnpt} fanout nodes total, but only {len(sorted_fanout)} available"
        )

    committees: dict[int, list[int]] = {}
    for t in range(num_topics):
        topic_fanout = sorted_fanout[t * fnpt : (t + 1) * fnpt]
        rng = random.Random(f"{seed}:{t}")
        topic_mesh = sorted(rng.sample(mesh_nodes, k=mesh_attestors)) if mesh_attestors > 0 else []
        committees[t] = list(topic_fanout) + topic_mesh
    return committees


def memberships_per_node(committees: dict[int, list[int]]) -> dict[int, list[tuple[int, int]]]:
    """Invert committees → per-node list of (topic_id, position) entries."""
    result: dict[int, list[tuple[int, int]]] = {}
    for topic, members in committees.items():
        for position, node_num in enumerate(members):
            result.setdefault(node_num, []).append((topic, position))
    for entries in result.values():
        entries.sort()
    return result


def compute_publish_schedule(
    num_slots: int,
    committees: dict[int, list[int]],
) -> dict[int, list[int]]:
    """Every committee member publishes every slot.

    Returns mapping from node_num to sorted list of slot numbers.
    """
    publishers: set[int] = set()
    for members in committees.values():
        publishers.update(members)
    return {n: list(range(1, num_slots + 1)) for n in sorted(publishers)}


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
    memberships: dict[int, list[tuple[int, int]]],
) -> dict[str, Any]:
    """Generate Shadow configuration dictionary for attestations."""
    fanout_nodes = set(topology.fanout_nodes)
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

        # Resolve this node's mesh degree by tier and pass it explicitly, so
        # super and home nodes can run different D. Falls back to the shared
        # gossipsub_params when a tier override is unset.
        is_super = node.upload_bw_mbps >= SUPER_NODE_UPLOAD_MBPS
        tier_d = config.supernode_d if is_super else config.homenode_d
        eff_d = tier_d if tier_d is not None else config.gossipsub_params
        args_parts.append(f"-gossipsub-params=Dlow:{eff_d.Dlow},D:{eff_d.D},Dhigh:{eff_d.Dhigh}")

        if config.disable_ihave_gossip:
            args_parts.append("-disable-ihave-gossip")
        if config.use_partial_messages:
            args_parts.append("-use-partial-messages")
        if config.partial_priority:
            args_parts.append("-partial-priority")
            args_parts.append(
                f"-max-attestations-per-message={config.max_attestations_per_message}"
            )
            if config.send_available_with_data:
                args_parts.append("-send-available-with-data")
        if config.att_propagation:
            args_parts.append("-att-propagation")
            args_parts.append(
                f"-max-attestations-per-message={config.max_attestations_per_message}"
            )
            if config.enable_push_mesh_bitmap:
                args_parts.append("-attprop-enable-push-mesh-bitmap")
            for flag, value in (
                ("attprop-push-dlow", config.attprop_push_dlow),
                ("attprop-push-d", config.attprop_push_d),
                ("attprop-push-dhigh", config.attprop_push_dhigh),
                ("attprop-bitmap-dlow", config.attprop_bitmap_dlow),
                ("attprop-bitmap-d", config.attprop_bitmap_d),
                ("attprop-bitmap-dhigh", config.attprop_bitmap_dhigh),
            ):
                if flag.startswith("attprop-bitmap-") or value > 0:
                    args_parts.append(f"-{flag}={value}")

        node_memberships = memberships.get(node_num, [])
        if node_memberships:
            args_parts.append(
                "-committee-memberships=" + ";".join(f"{t}:{p}" for t, p in node_memberships)
            )

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

    committees = compute_committees(
        topology=topology,
        num_topics=sim.num_topics,
        num_attestors=sim.num_attestors,
        fanout_nodes_per_topic=sim.topology.fanout_nodes_per_topic,
        seed=sim.topology.seed,
    )
    memberships = memberships_per_node(committees)
    publish_schedule = compute_publish_schedule(num_slots=sim.num_slots, committees=committees)

    # Create unique run directory
    run_dir = _create_run_dir(output_dir, num_nodes=len(topology.nodes), gossip=sim.gossipsub_params)

    # Save mesh topology and config
    topology.save(run_dir / "topology.json")
    config_data = config.model_dump()
    config_file = run_dir / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f, default_flow_style=False)

    # Save fanout nodes, publish schedule, and per-topic committee membership
    with open(run_dir / "schedule.json", "w") as f:
        json.dump(
            {
                "fanout_nodes": sorted(topology.fanout_nodes),
                "committee_membership": {str(t): members for t, members in committees.items()},
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
        memberships=memberships,
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
