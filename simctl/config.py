"""Configuration schema and validation."""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel


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
    num_messages_per_attestor: int = 1
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
    num_messages_per_attestor: int = 1
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
