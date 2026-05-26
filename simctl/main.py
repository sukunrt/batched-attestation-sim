"""CLI entry point."""

from pathlib import Path

import click

from . import config, experiment, remote, runner, topology


@click.group()
def cli() -> None:
    """Simulation control CLI for eth-ec-broadcast."""
    pass


@cli.command(name="topology")
@click.option("-n", "--num-nodes", type=int, required=True, help="Number of nodes")
@click.option(
    "-d",
    "--degree",
    type=int,
    required=True,
    help="Graph degree (connections per node)",
)
@click.option(
    "-t",
    "--type",
    "topo_type",
    default="random",
    type=click.Choice(["random", "ring"]),
    help="Topology type",
)
@click.option("-s", "--seed", default=42, help="Random seed for reproducibility")
@click.option("--super-node-fraction", default=0.0, help="Fraction of super nodes")
@click.option("-o", "--output-file", default="topology.json", help="Output file path")
def gen_topology(
    num_nodes: int,
    degree: int,
    topo_type: str,
    seed: int,
    super_node_fraction: float,
    output_file: str,
) -> None:
    """Generate a network topology."""
    if topo_type == "random":
        topo = topology.generate_random_topology(
            num_nodes=num_nodes,
            degree=degree,
            seed=seed,
            super_node_fraction=super_node_fraction,
        )
    else:
        topo = topology.generate_ring_topology(
            num_nodes=num_nodes,
            seed=seed,
            super_node_fraction=super_node_fraction,
        )

    topo.save(Path(output_file))
    click.echo(f"Topology saved to {output_file}")


@cli.command(name="attestations")
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to attestation config YAML file",
)
@click.option(
    "--output-dir",
    default="./runs/attestation",
    help="Output folder for simulation results",
)
@click.option(
    "--remote",
    "remote_host",
    default=None,
    help="Run on remote host (user@hostname)",
)
@click.option("--dry-run", is_flag=True, help="Print remote commands without executing")
def attestations(
    config_path: str, output_dir: str, remote_host: str | None, dry_run: bool
) -> None:
    """Run a gossipsub attestation simulation using Shadow."""
    if remote_host:
        simlab_root = runner.get_simlab_root()
        r = remote.Runner(dry_run=dry_run)

        click.echo(f"Syncing to {remote_host}...")
        sync_res, remote_cwd = r.sync_to_remote(remote_host, simlab_root)
        if sync_res.returncode != 0:
            raise SystemExit(sync_res.returncode)

        click.echo(f"Running on {remote_host}:{remote_cwd}...")
        exit_code = r.run_remote_simctl(
            host=remote_host,
            cwd=remote_cwd,
            simctl_args=[
                "attestations",
                "--config",
                config_path,
                "--output-dir",
                output_dir,
            ],
        )
        tar_rc = r.tar_and_cleanup(remote_host, remote_cwd, output_dir)
        rc = exit_code or tar_rc
        if rc != 0:
            raise SystemExit(rc)
    else:
        cfg = config.load_attestation_config(Path(config_path))
        _, result = runner.run_simulation(cfg, Path(output_dir))
        if result.returncode != 0:
            raise SystemExit(result.returncode)


@cli.command(name="experiment")
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to experiment config YAML file",
)
@click.option(
    "--output-dir",
    default="./runs/experiments",
    help="Output folder for experiment results",
)
@click.option(
    "--remote",
    "remote_host",
    default=None,
    help="Run on remote host (user@hostname)",
)
@click.option("--dry-run", is_flag=True, help="Print remote commands without executing")
def run_experiment(
    config_path: str, output_dir: str, remote_host: str | None, dry_run: bool
) -> None:
    """Run an attestation experiment with multiple simulation variations."""
    if remote_host:
        simlab_root = runner.get_simlab_root()
        r = remote.Runner(dry_run=dry_run)

        click.echo(f"Syncing to {remote_host}...")
        sync_res, remote_cwd = r.sync_to_remote(remote_host, simlab_root)
        if sync_res.returncode != 0:
            raise SystemExit(sync_res.returncode)

        click.echo(f"Running experiment on {remote_host}:{remote_cwd}...")
        exit_code = r.run_remote_simctl(
            host=remote_host,
            cwd=remote_cwd,
            simctl_args=[
                "experiment",
                "--config",
                config_path,
                "--output-dir",
                output_dir,
            ],
        )
        tar_rc = r.tar_and_cleanup(remote_host, remote_cwd, output_dir)
        rc = exit_code or tar_rc
        if rc != 0:
            raise SystemExit(rc)
    else:
        result = experiment.run_experiment(Path(config_path), Path(output_dir))
        if not result.all_successful:
            raise SystemExit(1)


if __name__ == "__main__":
    cli()
