#!/usr/bin/env python3
"""Plot the attestation arrival-latency CDF from a single node's perspective.

Picks one node and one topic and plots the CDF of per-attestation
time-to-receive (latency_ms) for the chosen slot, overlaying the classic and
partial runs of an experiment so the two wire paths can be compared from the
same vantage point.

Usage:
    uv run python analysis/plot_arrival_latency_cdf.py <exp_dir> \
        [--node N] [--topic T] [--slot S] [--out path.png]
"""
import argparse
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import yaml

# Classic app-level receive: "received node=N from=F slot=S committee_index=C latency_ms=L"
CLASSIC_PAT = re.compile(
    r"\breceived node=\d+.*?\bslot=(\d+)\b.*?\bcommittee_index=(\d+)\b.*?\blatency_ms=(\d+)"
)
# Partial app-level receive: "partial_received node=N slot=S committee_index=C position=P att_digest=H latency_ms=L"
PARTIAL_PAT = re.compile(
    r"\bpartial_received node=\d+.*?\bslot=(\d+)\b.*?\bcommittee_index=(\d+)\b.*?\bposition=\d+.*?\blatency_ms=(\d+)"
)


def run_mode(run_dir: Path) -> str:
    sim = yaml.safe_load((run_dir / "config.yaml").read_text())["simulation"]
    if sim.get("partial_priority"):
        return "partial-priority"
    return "partial" if sim.get("use_partial_messages") else "classic"


def latencies(stderr: Path, pat: re.Pattern, topic: int, slot: int) -> list[int]:
    out = []
    with open(stderr) as f:
        for line in f:
            m = pat.search(line)
            if m and int(m.group(1)) == slot and int(m.group(2)) == topic:
                out.append(int(m.group(3)))
    return out


def run_latencies(run_dir: Path, node: int, topic: int, slot: int) -> list[int] | None:
    """Sorted arrival latencies for one node/topic/slot, parsed with the
    classic or partial pattern depending on the run's mode. partial-priority
    emits the same partial_received lines as partial."""
    pat = PARTIAL_PAT if run_mode(run_dir) in ("partial", "partial-priority") else CLASSIC_PAT
    stderr = next((run_dir / "shadow.data" / "hosts" / f"node{node}").glob("*.stderr"), None)
    if stderr is None:
        print(f"no stderr for node {node} in {run_dir.name}")
        return None
    return sorted(latencies(stderr, pat, topic, slot))


# Stable colors for the auto classic/partial/partial-priority overlay; extra
# runs cycle the palette.
MODE_COLORS = {"classic": "#c0392b", "partial": "#2471a3", "partial-priority": "#27ae60"}
PALETTE = ["#c0392b", "#2471a3", "#27ae60", "#8e44ad", "#d35400", "#16a085"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("exp_dir", type=Path)
    ap.add_argument("--node", type=int, default=3)
    ap.add_argument("--topic", type=int, default=0)
    ap.add_argument("--slot", type=int, default=2)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--runs", type=Path, nargs="+", default=None,
                    help="explicit run dirs to overlay (default: auto-discover under exp_dir/runs)")
    ap.add_argument("--labels", nargs="+", default=None,
                    help="labels for --runs, in order (default: run mode / dir name)")
    args = ap.parse_args()

    if args.runs:
        run_dirs = args.runs
        labels = args.labels or [d.name for d in run_dirs]
        if len(labels) != len(run_dirs):
            ap.error("--labels must match --runs in count")
    else:
        run_dirs = [d for d in sorted((args.exp_dir / "runs").iterdir())
                    if (d / "config.yaml").exists()]
        labels = [run_mode(d) for d in run_dirs]

    fig, ax = plt.subplots(figsize=(8, 5))
    for i, (run_dir, label) in enumerate(zip(run_dirs, labels)):
        lat = run_latencies(run_dir, args.node, args.topic, args.slot)
        if not lat:
            continue
        color = MODE_COLORS.get(label, PALETTE[i % len(PALETTE)])
        n = len(lat)
        ys = [(j + 1) / n for j in range(n)]
        p50 = lat[int(0.50 * (n - 1))]
        p95 = lat[int(0.95 * (n - 1))]
        ax.step(lat, ys, where="post", color=color, lw=2,
                label=f"{label}  (p50={p50} ms, p95={p95} ms)")
        ax.axvline(p95, color=color, ls=":", lw=1, alpha=0.6)
        print(f"{label}: {n} arrivals on topic {args.topic}, slot {args.slot}")

    ax.set_xlabel("attestation arrival latency (ms)")
    ax.set_ylabel("cumulative fraction of attestations")
    ax.set_ylim(0, 1.02)
    ax.set_xlim(left=0)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower right")
    ax.set_title(
        f"Attestation arrival latency — super node {args.node}, topic {args.topic}, slot {args.slot}\n"
        f"degree-30, 500-attestor committee (dotted = p95)"
    )
    fig.tight_layout()

    out = args.out or Path("graphs") / f"arrival_latency_node{args.node}_topic{args.topic}_slot{args.slot}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
