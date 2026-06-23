"""Per-tier gossipsub D flag emission in generate_shadow_yaml."""

from simctl.config import AttestationSimConfig, GossipsubParams, TopologyConfig
from simctl.runner import generate_shadow_yaml
from simctl.topology import Edge, NodeSpec, Topology


def _topology() -> Topology:
    """One super node (node0, 1024 Mbps) and one home node (node1, 25 Mbps)."""
    return Topology(
        nodes=[
            NodeSpec(num=0, upload_bw_mbps=1024, download_bw_mbps=1024, country="x"),
            NodeSpec(num=1, upload_bw_mbps=25, download_bw_mbps=50, country="y"),
        ],
        edges=[
            Edge(source=0, target=1, latency_ms=10),
            Edge(source=1, target=0, latency_ms=10),
        ],
        fanout_nodes=set(),
    )


def _shadow(cfg: AttestationSimConfig) -> dict:
    return generate_shadow_yaml(
        config=cfg,
        topology=_topology(),
        binary_path="./attestation",
        config_file_path="/tmp/config.yaml",
        publish_schedule={},
        peer_lists={},
        memberships={},
    )


def _args(shadow: dict, node: str) -> str:
    return shadow["hosts"][node]["processes"][0]["args"]


def test_tiered_d_emits_per_node():
    cfg = AttestationSimConfig(
        topology=TopologyConfig(num_nodes=2),
        supernode_d=GossipsubParams(Dlow=8, D=12, Dhigh=16),
        homenode_d=GossipsubParams(Dlow=3, D=4, Dhigh=5),
    )
    shadow = _shadow(cfg)
    assert "-gossipsub-params=Dlow:8,D:12,Dhigh:16" in _args(shadow, "node0")
    assert "-gossipsub-params=Dlow:3,D:4,Dhigh:5" in _args(shadow, "node1")


def test_uniform_d_falls_back_to_default():
    cfg = AttestationSimConfig(
        topology=TopologyConfig(num_nodes=2),
        gossipsub_params=GossipsubParams(Dlow=6, D=8, Dhigh=12),
    )
    shadow = _shadow(cfg)
    for node in ("node0", "node1"):
        assert "-gossipsub-params=Dlow:6,D:8,Dhigh:12" in _args(shadow, node)
