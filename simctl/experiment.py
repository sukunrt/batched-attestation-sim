"""Experiment runner for attestation simulations."""

import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from simctl import config, manifest, runner, topology


@dataclass
class RunResult:
    """Result of a single simulation run."""

    run_dir: Path
    exit_code: int


@dataclass
class ExperimentResult:
    """Result of an experiment with all simulation runs."""

    experiment_dir: Path
    runs: list[RunResult]

    @property
    def all_successful(self) -> bool:
        return all(r.exit_code == 0 for r in self.runs)


def _generate_topology(
    exp: config.AttestationExperimentConfig,
    num_topics: int,
) -> topology.Topology:
    """Generate topology from experiment config."""
    topo_cfg = exp.topology
    total_fanout = topo_cfg.fanout_nodes_per_topic * num_topics
    if topo_cfg.type == "random":
        return topology.generate_random_topology(
            num_nodes=topo_cfg.num_nodes,
            degree=topo_cfg.degree,
            seed=topo_cfg.seed,
            super_node_fraction=topo_cfg.super_node_fraction,
            fanout_nodes=total_fanout,
            fanout_node_mesh_peers=topo_cfg.fanout_node_mesh_peers,
            min_latency_ms=topo_cfg.min_node_to_node_latency_ms,
        )
    return topology.generate_ring_topology(
        num_nodes=topo_cfg.num_nodes,
        seed=topo_cfg.seed,
        super_node_fraction=topo_cfg.super_node_fraction,
        min_latency_ms=topo_cfg.min_node_to_node_latency_ms,
    )


def _sim_params_to_config(
    params: config.AttestationSimParams,
    exp: config.AttestationExperimentConfig,
) -> config.AttestationConfig:
    """Convert experiment sim params to a full AttestationConfig."""
    return config.AttestationConfig(
        simulation=config.AttestationSimConfig(
            topology=exp.topology,
            gossipsub_params=params.gossipsub_params,
            supernode_d=params.supernode_d,
            homenode_d=params.homenode_d,
            num_topics=params.num_topics,
            num_slots=params.num_slots,
            slot_duration_seconds=params.slot_duration_seconds,
            num_attestors=params.num_attestors,
            attestation_data_size=params.attestation_data_size,
            signature_size=params.signature_size,
            attestation_validation_delay_ms=params.attestation_validation_delay_ms,
            attestation_validation_std_dev_ms=params.attestation_validation_std_dev_ms,
            publish_delay_mean_ms=params.publish_delay_mean_ms,
            stop_time_minutes=params.stop_time_minutes,
            log_level=params.log_level,
            bandwidth_log_frequency_ms=params.bandwidth_log_frequency_ms,
            disable_ihave_gossip=params.disable_ihave_gossip,
            publish_interval_ms=params.publish_interval_ms,
            max_peers_per_attestation=params.max_peers_per_attestation,
            divergent_attestor_fraction=params.divergent_attestor_fraction,
            validation_batch_window_ms=params.validation_batch_window_ms,
            per_attestation_validation_us=params.per_attestation_validation_us,
            use_partial_messages=params.use_partial_messages,
            partial_priority=params.partial_priority,
            max_attestations_per_message=params.max_attestations_per_message,
            send_available_with_data=params.send_available_with_data,
            att_propagation=params.att_propagation,
            disable_bitmap_sends=params.disable_bitmap_sends,
            enable_push_mesh_bitmap=params.enable_push_mesh_bitmap,
            attprop_push_dlow=params.attprop_push_dlow,
            attprop_push_d=params.attprop_push_d,
            attprop_push_dhigh=params.attprop_push_dhigh,
            attprop_bitmap_dlow=params.attprop_bitmap_dlow,
            attprop_bitmap_d=params.attprop_bitmap_d,
            attprop_bitmap_dhigh=params.attprop_bitmap_dhigh,
            attprop_send_budget_b=params.attprop_send_budget_b,
            attprop_max_peers_per_att=params.attprop_max_peers_per_att,
            attprop_tick_interval_ms=params.attprop_tick_interval_ms,
            attprop_bitmap_floor_interval_ms=params.attprop_bitmap_floor_interval_ms,
            attprop_heartbeat_interval_ms=params.attprop_heartbeat_interval_ms,
            attprop_prune_backoff_seconds=params.attprop_prune_backoff_seconds,
        )
    )


def _create_experiment_dir(base_dir: Path) -> Path:
    """Create a unique experiment directory."""
    for _ in range(20):
        now = datetime.now()
        timestamp = manifest.format_dir_timestamp(now)
        suffix = manifest.random_suffix()
        candidate = base_dir / f"exp-{timestamp}-{suffix}"
        try:
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        except FileExistsError:
            continue
    raise RuntimeError("Failed to create unique experiment directory after 20 attempts")


def run_experiment(
    experiment_path: Path,
    output_dir: Path,
) -> ExperimentResult:
    """Run all simulation variations in an experiment.

    Creates an experiment directory containing:
    - experiment.yaml: Copy of the original config
    - topology.json: Generated once, shared by all runs
    - runs/: Per-simulation run directories
    """
    started_at = manifest.utcnow_iso()
    git_sha = manifest.try_get_git_sha(runner.get_simlab_root())
    exp = config.load_attestation_experiment(experiment_path)

    # Create experiment directory
    exp_dir = _create_experiment_dir(output_dir)

    manifest_path = exp_dir / "manifest.json"
    manifest_data: dict[str, object] = {
        "schema_version": 1,
        "kind": "experiment",
        "status": "running",
        "started_at": started_at,
        "command": manifest.get_command_argv(),
        "cwd": str(Path.cwd()),
        "git_sha": git_sha,
        "paths": {
            "experiment_dir": str(exp_dir.resolve()),
            "experiment_input": str(experiment_path),
            "experiment_input_resolved": str(experiment_path.resolve()),
            "experiment_copy": str((exp_dir / "experiment.yaml").resolve()),
        },
        "params": exp.model_dump(),
        "runs": [],
    }
    manifest.write_json_atomic(manifest_path, manifest_data)

    # Copy experiment config
    shutil.copy(experiment_path, exp_dir / "experiment.yaml")

    # Generate topology once (num_topics from first simulation; all must agree since topology is shared)
    num_topics = exp.simulations[0].num_topics
    topo = _generate_topology(exp, num_topics)
    topo.save(exp_dir / "topology.json")

    # Build Go binary once
    binary_path = exp_dir / "attestation"
    runner.build_attestation(binary_path)

    sims = exp.simulations
    print(f"Experiment directory: {exp_dir}")
    print(f"Running {len(sims)} simulation variations...")

    # Run each simulation
    runs_dir = exp_dir / "runs"
    results: list[RunResult] = []
    for i, sim_params in enumerate(sims, 1):
        print(f"\n[{i}/{len(sims)}] Running simulation {i}...")

        sim_config = _sim_params_to_config(sim_params, exp)

        run_dir, result = runner.run_simulation(
            config=sim_config,
            output_dir=runs_dir,
            topology=topo,
            binary_src=binary_path,
        )

        results.append(
            RunResult(
                run_dir=run_dir,
                exit_code=result.returncode,
            )
        )

        run_summary = {
            "run_dir": str(run_dir.relative_to(exp_dir)),
            "exit_code": result.returncode,
        }
        runs_list = list(manifest_data.get("runs", []))
        runs_list.append(run_summary)
        manifest_data["runs"] = runs_list
        manifest.write_json_atomic(manifest_path, manifest_data)

    # Print summary
    print(f"\n{'=' * 50}")
    print("Experiment completed!")
    print(f"Results directory: {exp_dir}")
    print("\nRun summary:")
    for r in results:
        status = "OK" if r.exit_code == 0 else f"FAILED (exit code {r.exit_code})"
        print(f"  {r.run_dir.name}: {status}")

    manifest_data["status"] = "completed"
    manifest_data["completed_at"] = manifest.utcnow_iso()
    manifest.write_json_atomic(manifest_path, manifest_data)

    return ExperimentResult(experiment_dir=exp_dir, runs=results)
