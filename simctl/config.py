"""Configuration schema and validation."""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, model_validator


class AlgorithmConfig(BaseModel):
    """Algorithm-specific configuration."""

    name: Literal["RS", "RS-ChunkLen", "RLNC", "RLNC-ChunkLen", "gossipsub"]

    # RS options
    data_shards: int = 16
    parity_shards: int = 16

    # Chunk options (RS-ChunkLen, RLNC-ChunkLen)
    chunk_len: int | None = None

    # RLNC options
    num_chunks: int = 16
    num_chunks_per_generation: int = 0

    # Bitmap options
    enable_bitmaps: bool = True
    bitmap_threshold: int = 100

    # Per-algorithm override for publish wait (optional, used in experiments)
    publish_wait_seconds: float | None = None


class TopologyConfig(BaseModel):
    """Topology configuration."""

    file: str | None = None
    num_nodes: int = 10
    degree: int = 4
    type: Literal["random", "ring"] = "random"
    seed: int = 42
    super_node_fraction: float = 0.0
    fanout_nodes_per_topic: int = 0
    fanout_node_mesh_peers: int = 1
    latency_multiple: float = 1.0
    min_node_to_node_latency_ms: int = 0


class SimulationConfig(BaseModel):
    """Simulation run configuration."""

    num_messages: int = 1
    message_size: int = 100_000
    publish_wait_seconds: float = 10.0
    stop_time_minutes: float = 30.0
    log_level: Literal["debug", "info", "warn", "error"] = "info"
    seed: int = 42
    bandwidth_log_frequency_ms: int = 100


class RunConfig(BaseModel):
    """Root configuration for a single simulation run."""

    algorithm: AlgorithmConfig
    topology: TopologyConfig
    simulation: SimulationConfig


def load_config(path: Path) -> RunConfig:
    """Load and validate configuration from YAML file."""
    import yaml

    with open(path) as f:
        data = yaml.safe_load(f)
    return RunConfig(**data)


def save_config(config: RunConfig, path: Path) -> None:
    """Save configuration to YAML file."""
    import yaml

    with open(path, "w") as f:
        yaml.dump(config.model_dump(), f, default_flow_style=False)


class GossipsubParams(BaseModel):
    """GossipSub protocol parameters."""

    Dlow: int = 6
    D: int = 8
    Dhigh: int = 12


class AttestationSimConfig(BaseModel):
    """Simulation parameters for attestations."""

    topology: TopologyConfig
    gossipsub_params: GossipsubParams = GossipsubParams()
    # Optional per-tier mesh degree. Super nodes (upload_bw_mbps >= 1024) use
    # supernode_d, home nodes use homenode_d; either falls back to
    # gossipsub_params when unset.
    supernode_d: GossipsubParams | None = None
    homenode_d: GossipsubParams | None = None
    num_topics: int = 1
    num_slots: int = 12
    slot_duration_seconds: int = 12
    num_attestors: int = 8
    attestation_data_size: int = 128
    signature_size: int = 96
    attestation_validation_delay_ms: int = 5
    attestation_validation_std_dev_ms: int = 0
    validation_batch_window_ms: int = 5
    per_attestation_validation_us: int = 100
    publish_delay_mean_ms: int = 0
    stop_time_minutes: float = 30.0
    log_level: Literal["debug", "info", "warn", "error"] = "info"
    bandwidth_log_frequency_ms: int = 100
    disable_ihave_gossip: bool = False

    # Partial messages
    publish_interval_ms: int = 20
    max_peers_per_attestation: int = 0  # 0 = D*2
    divergent_attestor_fraction: float = 0.01

    # Partial-messages path (lists of attestor IDs + ephemeral iwant)
    use_partial_messages: bool = False

    # Partial-priority path: size-capped, least-forwarded-first forwarding.
    # An alternative to the default partial push, over the same partial-messages
    # extension. max_attestations_per_message caps attestations per outgoing
    # data message (0 = default 30).
    partial_priority: bool = False
    max_attestations_per_message: int = 30
    # Partial-priority only: piggyback our validated bitmap onto the first data
    # message to each mesh peer per tick so peers stop forwarding us duplicates.
    send_available_with_data: bool = False

    # att_propagation path: a native libp2p protocol (no gossipsub) with three
    # per-topic streams (push / bitmap / control). Mutually exclusive with
    # use_partial_messages and partial_priority. N reuses
    # max_attestations_per_message. All tunables below are optional; 0 means the
    # Go side applies the spec default.
    att_propagation: bool = False
    attprop_push_dlow: int = 0
    attprop_push_d: int = 0
    attprop_push_dhigh: int = 0
    attprop_bitmap_dlow: int = 0
    attprop_bitmap_d: int = 0
    attprop_bitmap_dhigh: int = 0
    attprop_send_budget_b: int = 0
    attprop_max_peers_per_att: int = 0
    attprop_tick_interval_ms: int = 0
    attprop_bitmap_floor_interval_ms: int = 0
    attprop_heartbeat_interval_ms: int = 0
    attprop_prune_backoff_seconds: int = 0

    @model_validator(mode="after")
    def _check_mode_exclusion(self) -> "AttestationSimConfig":
        if self.att_propagation and (self.use_partial_messages or self.partial_priority):
            raise ValueError(
                "att_propagation is mutually exclusive with use_partial_messages and "
                "partial_priority"
            )
        return self


class AttestationConfig(BaseModel):
    """Root configuration for an attestation run."""

    simulation: AttestationSimConfig


def load_attestation_config(path: Path) -> AttestationConfig:
    """Load and validate attestation configuration from YAML file."""
    import yaml

    with open(path) as f:
        data = yaml.safe_load(f)
    return AttestationConfig(**data)


class AttestationSimParams(BaseModel):
    """Simulation parameters for a single run within an experiment (no topology)."""

    gossipsub_params: GossipsubParams = GossipsubParams()
    # Optional per-tier mesh degree; see AttestationSimConfig.
    supernode_d: GossipsubParams | None = None
    homenode_d: GossipsubParams | None = None
    num_topics: int = 1
    num_slots: int = 12
    slot_duration_seconds: int = 12
    num_attestors: int = 8
    attestation_data_size: int = 128
    signature_size: int = 96
    attestation_validation_delay_ms: int = 5
    attestation_validation_std_dev_ms: int = 0
    validation_batch_window_ms: int = 5
    per_attestation_validation_us: int = 100
    publish_delay_mean_ms: int = 0
    stop_time_minutes: float = 30.0
    log_level: Literal["debug", "info", "warn", "error"] = "info"
    bandwidth_log_frequency_ms: int = 100
    disable_ihave_gossip: bool = False

    # Partial messages
    publish_interval_ms: int = 20
    max_peers_per_attestation: int = 0
    divergent_attestor_fraction: float = 0.01

    # Partial-messages path (lists of attestor IDs + ephemeral iwant)
    use_partial_messages: bool = False

    # Partial-priority path: size-capped, least-forwarded-first forwarding.
    # An alternative to the default partial push, over the same partial-messages
    # extension. max_attestations_per_message caps attestations per outgoing
    # data message (0 = default 30).
    partial_priority: bool = False
    max_attestations_per_message: int = 30
    # Partial-priority only: piggyback our validated bitmap onto the first data
    # message to each mesh peer per tick so peers stop forwarding us duplicates.
    send_available_with_data: bool = False

    # att_propagation path; see AttestationSimConfig. N reuses
    # max_attestations_per_message; all tunables optional (0 = Go spec default).
    att_propagation: bool = False
    attprop_push_dlow: int = 0
    attprop_push_d: int = 0
    attprop_push_dhigh: int = 0
    attprop_bitmap_dlow: int = 0
    attprop_bitmap_d: int = 0
    attprop_bitmap_dhigh: int = 0
    attprop_send_budget_b: int = 0
    attprop_max_peers_per_att: int = 0
    attprop_tick_interval_ms: int = 0
    attprop_bitmap_floor_interval_ms: int = 0
    attprop_heartbeat_interval_ms: int = 0
    attprop_prune_backoff_seconds: int = 0

    @model_validator(mode="after")
    def _check_mode_exclusion(self) -> "AttestationSimParams":
        if self.att_propagation and (self.use_partial_messages or self.partial_priority):
            raise ValueError(
                "att_propagation is mutually exclusive with use_partial_messages and "
                "partial_priority"
            )
        return self


class AttestationExperimentConfig(BaseModel):
    """Experiment config: shared topology with multiple simulation variations."""

    topology: TopologyConfig
    simulations: list[AttestationSimParams]


def load_attestation_experiment(path: Path) -> AttestationExperimentConfig:
    """Load and validate attestation experiment configuration from YAML file."""
    import yaml

    with open(path) as f:
        data = yaml.safe_load(f)
    return AttestationExperimentConfig(**data)


def get_run_name(alg: AlgorithmConfig) -> str:
    """Generate a compact run name from algorithm config."""
    parts = [alg.name]

    if alg.name in ("RS", "RS-ChunkLen"):
        parts.append(f"d{alg.data_shards}")
        parts.append(f"p{alg.parity_shards}")

    if alg.name in ("RLNC", "RLNC-ChunkLen"):
        parts.append(f"nc{alg.num_chunks}")

    if alg.name in ("RS-ChunkLen", "RLNC-ChunkLen") and alg.chunk_len is not None:
        parts.append(f"cl{alg.chunk_len}")

    if alg.name != "gossipsub":
        parts.append(f"bm{int(alg.enable_bitmaps)}")
        parts.append(f"t{alg.bitmap_threshold}")

    return "-".join(parts)
