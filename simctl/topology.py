"""Topology generation with latency computation."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path


@dataclass
class NodeSpec:
    num: int
    upload_bw_mbps: int
    download_bw_mbps: int
    country: str


@dataclass
class Edge:
    source: int
    target: int
    latency_ms: int


@dataclass
class Topology:
    nodes: list[NodeSpec]
    edges: list[Edge]
    fanout_nodes: set[int]

    def to_dict(self) -> dict:
        return {
            "nodes": [
                {
                    "num": n.num,
                    "upload_bw_mbps": n.upload_bw_mbps,
                    "download_bw_mbps": n.download_bw_mbps,
                    "country": n.country,
                }
                for n in self.nodes
            ],
            "edges": [
                {"source": e.source, "target": e.target, "latency_ms": e.latency_ms}
                for e in self.edges
            ],
            "fanout_nodes": sorted(self.fanout_nodes),
        }

    def save(self, path: Path) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)


def load_topology(path: Path) -> Topology:
    """Load a topology from a JSON file."""
    with open(path) as f:
        data = json.load(f)
    nodes = [
        NodeSpec(
            num=n["num"],
            upload_bw_mbps=n["upload_bw_mbps"],
            download_bw_mbps=n["download_bw_mbps"],
            country=n["country"],
        )
        for n in data["nodes"]
    ]
    edges = [
        Edge(source=e["source"], target=e["target"], latency_ms=e["latency_ms"])
        for e in data["edges"]
    ]
    # Ensure bidirectionality: if (u,v) exists but (v,u) doesn't, add (v,u).
    existing = {(e.source, e.target): e.latency_ms for e in edges}
    for (u, v), latency in list(existing.items()):
        if (v, u) not in existing:
            edges.append(Edge(source=v, target=u, latency_ms=latency))
            existing[(v, u)] = latency
    return Topology(
        nodes=nodes, edges=edges, fanout_nodes=set(data.get("fanout_nodes", []))
    )


def load_latencies() -> dict[str, dict[str, int]]:
    """Load country-to-country latencies."""
    data_dir = Path(__file__).parent.parent / "data"
    with open(data_dir / "country_latencies.json") as f:
        return json.load(f)


def load_weights() -> dict[str, int]:
    """Load country weights for random selection."""
    data_dir = Path(__file__).parent.parent / "data"
    with open(data_dir / "country_weights.json") as f:
        return json.load(f)


class CountrySelector:
    def __init__(self, weights: dict[str, int], rng: random.Random):
        self.countries = list(weights.keys())
        self.cumulative = []
        total = 0
        for c in self.countries:
            total += weights[c]
            self.cumulative.append(total)
        self.total = total
        self.rng = rng

    def select(self) -> str:
        r = self.rng.randint(0, self.total - 1)
        for i, cum in enumerate(self.cumulative):
            if r < cum:
                return self.countries[i]
        return self.countries[-1]


def get_bandwidth(
    node_num: int, super_node_fraction: float, rng: random.Random
) -> tuple[int, int]:
    """Determine upload/download bandwidth for a node."""
    if node_num == 0:
        if super_node_fraction > 0.0001:
            return 1024, 1024  # Super node
        return 25, 50  # Block builder

    if rng.random() < super_node_fraction:
        return 1024, 1024
    return 25, 50  # Regular node


def generate_random_topology(
    num_nodes: int,
    degree: int,
    seed: int,
    super_node_fraction: float = 0.0,
    fanout_nodes: int = 0,
    fanout_node_mesh_peers: int = 1,
    min_latency_ms: int = 0,
) -> Topology:
    """Generate a random topology with the given parameters.

    Creates num_nodes mesh nodes connected with the given degree.
    If fanout_nodes > 0, creates additional fanout nodes (numbered
    num_nodes..num_nodes+fanout_nodes-1) each connected to
    fanout_node_mesh_peers random mesh nodes.
    """
    rng = random.Random(seed)
    weights = load_weights()
    latencies = load_latencies()
    selector = CountrySelector(weights, rng)

    # Create mesh nodes
    nodes = []
    for i in range(num_nodes):
        up, down = get_bandwidth(i, super_node_fraction, rng)
        nodes.append(
            NodeSpec(
                num=i,
                upload_bw_mbps=up,
                download_bw_mbps=down,
                country=selector.select(),
            )
        )

    # Create mesh edges - first ensure connectivity
    adjacency: dict[int, set[int]] = {i: set() for i in range(num_nodes)}
    for i in range(1, num_nodes):
        j = rng.randint(0, i - 1)
        adjacency[i].add(j)
        adjacency[j].add(i)

    # Add more edges to reach desired degree
    max_attempts = num_nodes * 10
    for u in range(num_nodes):
        attempts = 0
        while len(adjacency[u]) < degree and attempts < max_attempts:
            v = rng.randint(0, num_nodes - 1)
            if v != u and v not in adjacency[u] and len(adjacency[v]) < degree:
                adjacency[u].add(v)
                adjacency[v].add(u)
            attempts += 1

    # Convert to edges with latencies
    edges = []
    for u, neighbors in adjacency.items():
        for v in neighbors:
            src_country = nodes[u].country
            dst_country = nodes[v].country
            latency = max(
                latencies.get(src_country, {}).get(dst_country, 100), min_latency_ms
            )
            edges.append(Edge(source=u, target=v, latency_ms=latency))

    # Create fanout nodes and connect each to random mesh peers
    fanout_node_nums = set()
    if fanout_nodes > 0:
        fanout_rng = random.Random(seed + 1000)
        mesh_ids = list(range(num_nodes))
        k = min(fanout_node_mesh_peers, num_nodes)
        for i in range(fanout_nodes):
            node_num = num_nodes + i
            up, down = get_bandwidth(node_num, super_node_fraction, fanout_rng)
            node = NodeSpec(
                num=node_num,
                upload_bw_mbps=up,
                download_bw_mbps=down,
                country=selector.select(),
            )
            nodes.append(node)
            fanout_node_nums.add(node_num)
            peers = sorted(fanout_rng.sample(mesh_ids, k=k))
            for peer_num in peers:
                lat = max(
                    latencies.get(node.country, {}).get(nodes[peer_num].country, 100),
                    min_latency_ms,
                )
                edges.append(Edge(source=node_num, target=peer_num, latency_ms=lat))
                edges.append(Edge(source=peer_num, target=node_num, latency_ms=lat))

    return Topology(nodes=nodes, edges=edges, fanout_nodes=fanout_node_nums)


def generate_ring_topology(
    num_nodes: int,
    seed: int,
    super_node_fraction: float = 0.0,
    min_latency_ms: int = 0,
) -> Topology:
    """Generate a ring topology."""
    rng = random.Random(seed)
    weights = load_weights()
    latencies = load_latencies()
    selector = CountrySelector(weights, rng)

    nodes = []
    for i in range(num_nodes):
        up, down = get_bandwidth(i, super_node_fraction, rng)
        nodes.append(
            NodeSpec(
                num=i,
                upload_bw_mbps=up,
                download_bw_mbps=down,
                country=selector.select(),
            )
        )

    edges = []
    for i in range(num_nodes):
        j = (i + 1) % num_nodes
        src_country = nodes[i].country
        dst_country = nodes[j].country
        latency = max(
            latencies.get(src_country, {}).get(dst_country, 100), min_latency_ms
        )
        edges.append(Edge(source=i, target=j, latency_ms=latency))

    return Topology(nodes=nodes, edges=edges, fanout_nodes=set())
