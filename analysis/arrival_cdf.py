#!/usr/bin/env python3
"""Overlay attestation arrival-delay CDFs: mainnet reference slots vs our sim.

Picks one sim node (preferably a supernode, optionally in a given country), one
committee/topic, and one slot, and plots the CDF of its per-attestation arrival
delays (sim `latency_ms` = received_time - ExpectedPublishAt) against one or more
mainnet arrival-delay CSVs. The two delays are treated as directly comparable
(both "delay since the attestation was supposed to be broadcast").

Mainnet CSVs may be either:
  - single column `arrivalDelayMs`, or
  - two columns `run,arrivalDelayMs` (the same slot observed over several runs /
    vantage points) — runs are pooled per slot.
Negative delays are dropped.

Usage:
    uv run python analysis/arrival_cdf.py <exp_dir> --csv slot-*.csv \
        [--country germany] [--node N] [--committee C] [--slot S] \
        [--out plot.png] [--linear] [--seed 42]

`<exp_dir>` is an experiment dir (topology.json + runs/) with a classic and a
partial run, or a single attestations run dir. If --node is omitted a random
supernode is chosen (restricted to --country when given).
"""

import argparse
import json
import random
import re
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import yaml

CLASSIC_PAT = re.compile(
    r'\breceived\b.*?\bslot=(\d+)\b.*?\bcommittee_index=(\d+)\b.*?\blatency_ms=(-?\d+)')
PARTIAL_PAT = re.compile(
    r'\bpartial_received\b.*?\bslot=(\d+)\b.*?\bcommittee_index=(\d+)\b.*?\bposition=(\d+)\b.*?\blatency_ms=(-?\d+)')


def load_csv(path: Path) -> np.ndarray:
    """Load arrival delays (ms), pooling runs for 2-col files, dropping negatives."""
    lines = path.read_text().splitlines()
    header, rows = lines[0], lines[1:]
    if header.startswith("run,"):
        vals = [float(ln.split(",")[1]) for ln in rows if ln.strip()]
    else:
        vals = [float(x) for x in rows if x.strip()]
    return np.array(sorted(v for v in vals if v >= 0))


def pick_node(exp_dir: Path, country: str | None, rng: random.Random) -> int:
    topo = json.loads((exp_dir / "topology.json").read_text())
    fanout = set(topo.get("fanout_nodes", []))
    pool = [n["num"] for n in topo["nodes"]
            if n["num"] not in fanout and n["upload_bw_mbps"] >= 1024
            and (country is None or country.lower() in n["country"].lower())]
    if not pool:
        raise SystemExit(f"no supernode matches country={country!r}")
    return rng.choice(sorted(pool))


def run_mode(run_dir: Path) -> str:
    sim = yaml.safe_load((run_dir / "config.yaml").read_text())["simulation"]
    if sim.get("partial_priority"):
        return "partial-priority"
    return "partial" if sim.get("use_partial_messages") else "classic"


def node_latencies(run_dir: Path, node: int, committee: int, slot: int) -> np.ndarray:
    mode = run_mode(run_dir)
    sf = next(Path(f"{run_dir}/shadow.data/hosts/node{node}").glob("*.stderr"), None)
    if sf is None:
        return np.array([])
    # partial-priority emits the same partial_received lines as partial.
    is_partial = mode in ("partial", "partial-priority")
    pat = PARTIAL_PAT if is_partial else CLASSIC_PAT
    lat_idx = 4 if is_partial else 3
    out = [int(m.group(lat_idx)) for ln in open(sf, errors="replace")
           if (m := pat.search(ln)) and int(m.group(1)) == slot and int(m.group(2)) == committee]
    return np.array(sorted(out))


def cdf_xy(a: np.ndarray):
    return a, np.arange(1, len(a) + 1) / len(a)


def stats(a: np.ndarray) -> str:
    return (f"n={len(a)} p50={np.percentile(a,50):.0f} "
            f"p95={np.percentile(a,95):.0f} p99={np.percentile(a,99):.0f} max={a.max():.0f}")


def slot_label(path: Path) -> str:
    m = re.search(r'slot-(\d+)', path.name)
    return m.group(1) if m else path.stem


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("exp_dir", type=Path)
    ap.add_argument("--csv", type=Path, nargs="+", required=True)
    ap.add_argument("--node", type=int, default=None)
    ap.add_argument("--country", default=None)
    ap.add_argument("--committee", type=int, default=0)
    ap.add_argument("--slot", type=int, default=1)
    ap.add_argument("--out", type=Path, default=Path("arrival_cdf.png"))
    ap.add_argument("--linear", action="store_true", help="linear x-axis (default log)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    node = args.node if args.node is not None else pick_node(args.exp_dir, args.country, rng)
    topo = json.loads((args.exp_dir / "topology.json").read_text())
    node_country = next(n["country"] for n in topo["nodes"] if n["num"] == node)

    runs_dir = args.exp_dir / "runs"
    runs = sorted(runs_dir.iterdir()) if runs_dir.is_dir() else [args.exp_dir]

    plt.figure(figsize=(10, 6.5))

    # Mainnet reference slots — colored family.
    csvs = sorted(args.csv)
    cmap = plt.cm.viridis(np.linspace(0, 0.85, len(csvs)))
    for path, col in zip(csvs, cmap):
        a = load_csv(path)
        print(f"mainnet {slot_label(path)}: {stats(a)}")
        x, y = cdf_xy(a)
        plt.plot(x, y, color=col, lw=1.4, alpha=0.9,
                 label=f"mainnet slot {slot_label(path)} (n={len(a)})")

    # Our sim — bold black, solid=classic, dashed=partial, dotted=partial-priority.
    styles = {"classic": dict(color="black", lw=2.6, ls="-"),
              "partial": dict(color="black", lw=2.6, ls="--"),
              "partial-priority": dict(color="black", lw=2.6, ls=":")}
    for rd in runs:
        mode = run_mode(rd)
        lat = node_latencies(rd, node, args.committee, args.slot)
        if len(lat) == 0:
            continue
        print(f"sim {mode:7}: {stats(lat)}")
        x, y = cdf_xy(lat)
        plt.plot(x, y, label=f"sim {mode} (n={len(lat)})", **styles.get(mode, {}))

    if not args.linear:
        plt.xscale("log")
    plt.xlabel("attestation arrival delay (ms)" + ("" if args.linear else "  [log scale]"))
    plt.ylabel("CDF")
    plt.title(f"Attestation arrival delay: mainnet slots vs sim\n"
              f"sim node {node} (super, {node_country}), committee {args.committee}, slot {args.slot}")
    plt.grid(True, alpha=0.3, which="both")
    plt.legend(fontsize=8, loc="lower right")
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.savefig(args.out, dpi=130)
    print(f"saved {args.out}")


if __name__ == "__main__":
    main()
