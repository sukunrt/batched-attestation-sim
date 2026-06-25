"""att_propagation flag emission in generate_shadow_yaml + config plumbing."""

import pytest

from simctl.config import AttestationSimConfig, AttestationSimParams, TopologyConfig
from simctl.experiment import _sim_params_to_config
from simctl.runner import generate_shadow_yaml
from simctl.topology import Edge, NodeSpec, Topology

ATTPROP_FIELDS = (
    "att_propagation",
    "attprop_push_dlow",
    "attprop_push_d",
    "attprop_push_dhigh",
    "attprop_bitmap_dlow",
    "attprop_bitmap_d",
    "attprop_bitmap_dhigh",
    "attprop_send_budget_b",
    "attprop_max_peers_per_att",
    "attprop_tick_interval_ms",
    "attprop_bitmap_floor_interval_ms",
    "attprop_heartbeat_interval_ms",
    "attprop_prune_backoff_seconds",
)


def _topology() -> Topology:
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


def _args(cfg: AttestationSimConfig, node: str) -> str:
    shadow = generate_shadow_yaml(
        config=cfg,
        topology=_topology(),
        binary_path="./attestation",
        config_file_path="/tmp/config.yaml",
        publish_schedule={},
        peer_lists={},
        memberships={},
    )
    return shadow["hosts"][node]["processes"][0]["args"]


def test_att_propagation_emits_flag_and_n():
    cfg = AttestationSimConfig(
        topology=TopologyConfig(num_nodes=2),
        att_propagation=True,
        max_attestations_per_message=40,
        attprop_push_dlow=6,
        attprop_push_d=8,
        attprop_push_dhigh=12,
        attprop_bitmap_dlow=10,
        attprop_bitmap_d=14,
        attprop_bitmap_dhigh=16,
    )
    args = _args(cfg, "node0")
    assert "-att-propagation" in args
    assert "-max-attestations-per-message=40" in args
    assert "-attprop-push-dlow=6" in args
    assert "-attprop-push-d=8" in args
    assert "-attprop-push-dhigh=12" in args
    assert "-attprop-bitmap-dlow=10" in args
    assert "-attprop-bitmap-d=14" in args
    assert "-attprop-bitmap-dhigh=16" in args
    # att_propagation drops gossipsub, so no partial-mode flags ride along.
    assert "-use-partial-messages" not in args
    assert "-partial-priority" not in args


def test_no_att_propagation_flag_when_off():
    cfg = AttestationSimConfig(topology=TopologyConfig(num_nodes=2))
    assert "-att-propagation" not in _args(cfg, "node0")


def test_tunables_survive_experiment_params_to_config():
    params = AttestationSimParams(
        att_propagation=True,
        attprop_send_budget_b=8,
        attprop_heartbeat_interval_ms=900,
    )
    full = _sim_params_to_config(params, _experiment(params))
    for field in ATTPROP_FIELDS:
        assert getattr(full.simulation, field) == getattr(params, field)


def test_mutual_exclusion_rejected():
    for clash in ("use_partial_messages", "partial_priority"):
        with pytest.raises(ValueError, match="mutually exclusive"):
            AttestationSimConfig(
                topology=TopologyConfig(num_nodes=2),
                att_propagation=True,
                **{clash: True},
            )
        with pytest.raises(ValueError, match="mutually exclusive"):
            AttestationSimParams(att_propagation=True, **{clash: True})


def _experiment(params: AttestationSimParams):
    from simctl.config import AttestationExperimentConfig

    return AttestationExperimentConfig(
        topology=TopologyConfig(num_nodes=2),
        simulations=[params],
    )
