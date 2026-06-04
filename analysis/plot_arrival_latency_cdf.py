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

# Classic app-level receive: "received node=N from=F slot=S committee_index=C msg_index=I latency_ms=L"
CLASSIC_PAT = re.compile(
    r"\breceived node=\d+.*?\bslot=(\d+)\b.*?\bcommittee_index=(\d+)\b.*?\blatency_ms=(\d+)"
)
# Partial app-level receive: "partial_received node=N slot=S committee_index=C position=P att_digest=H latency_ms=L"
PARTIAL_PAT = re.compile(
    r"\bpartial_received node=\d+.*?\bslot=(\d+)\b.*?\bcommittee_index=(\d+)\b.*?\bposition=\d+.*?\blatency_ms=(\d+)"
)


def run_mode(run_dir: Path) -> str:
    cfg = yaml.safe_load((run_dir / "config.yaml").read_text())
    return "partial" if cfg["simulation"].get("use_partial_messages") else "classic"


def latencies(stderr: Path, pat: re.Pattern, topic: int, slot: int) -> list[int]:
    out = []
    with open(stderr) as f:
        for line in f:
            m = pat.search(line)
            if m and int(m.group(1)) == slot and int(m.group(2)) == topic:
                out.append(int(m.group(3)))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("exp_dir", type=Path)
    ap.add_argument("--node", type=int, default=3)
    ap.add_argument("--topic", type=int, default=0)
    ap.add_argument("--slot", type=int, default=2)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    series = {}
    for run_dir in sorted((args.exp_dir / "runs").iterdir()):
        if not (run_dir / "config.yaml").exists():
            continue
        mode = run_mode(run_dir)
        pat = PARTIAL_PAT if mode == "partial" else CLASSIC_PAT
        stderr = next(
            (run_dir / "shadow.data" / "hosts" / f"node{args.node}").glob("*.stderr"),
            None,
        )
        if stderr is None:
            print(f"no stderr for node {args.node} in {run_dir.name}")
            continue
        series[mode] = sorted(latencies(stderr, pat, args.topic, args.slot))
        print(f"{mode}: {len(series[mode])} arrivals on topic {args.topic}, slot {args.slot}")

    colors = {"classic": "#c0392b", "partial": "#2471a3"}
    fig, ax = plt.subplots(figsize=(8, 5))
    for mode in ("classic", "partial"):
        lat = series.get(mode)
        if not lat:
            continue
        n = len(lat)
        ys = [(i + 1) / n for i in range(n)]
        p50 = lat[int(0.50 * (n - 1))]
        p95 = lat[int(0.95 * (n - 1))]
        ax.step(lat, ys, where="post", color=colors[mode], lw=2,
                label=f"{mode}  (p50={p50} ms, p95={p95} ms)")
        ax.axvline(p95, color=colors[mode], ls=":", lw=1, alpha=0.6)

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
