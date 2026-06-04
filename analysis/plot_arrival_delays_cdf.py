#!/usr/bin/env python3
"""Overlay attestation arrival-delay CDFs from one-column `arrivalDelayMs` CSVs.

Each CSV has a single `arrivalDelayMs` column (one row per attestation), as
exported under mainnet-data/. Pass one or more `label=path.csv` series; they are
drawn as step CDFs on a shared axis for comparison (e.g. mainnet vs sim).

Usage:
    uv run python analysis/plot_arrival_delays_cdf.py \
        mainnet=mainnet-data/slot-13969227-arrival-delays.csv \
        classic=mainnet-data/sim-node173-germany-classic-slot2-val10-arrival-delays.csv \
        partial=mainnet-data/sim-node173-germany-partial-slot2-val10-arrival-delays.csv \
        --out graphs/arrival_cdf_mainnet_vs_classic_partial.png
"""
import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Stable colors per known label; anything else cycles through extras.
COLORS = {"mainnet": "#222222", "classic": "#c0392b", "partial": "#2471a3"}
EXTRA = ["#27ae60", "#8e44ad", "#d68910"]


def read_delays(path: Path) -> list[float]:
    out = []
    with open(path, newline="") as f:
        r = csv.reader(f)
        header = next(r, None)  # skip "arrivalDelayMs"
        for row in r:
            if not row:
                continue
            try:
                out.append(float(row[0]))
            except ValueError:
                continue
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("series", nargs="+", help="label=path.csv")
    ap.add_argument("--out", type=Path, default=Path("graphs/arrival_cdf.png"))
    ap.add_argument("--title", default="Attestation arrival-delay CDF — mainnet vs sim")
    ap.add_argument("--xmax", type=float, default=None, help="clip x-axis at this ms value")
    args = ap.parse_args()

    fig, ax = plt.subplots(figsize=(8, 5))
    extra_i = 0
    for spec in args.series:
        label, _, path = spec.partition("=")
        delays = sorted(read_delays(Path(path)))
        if not delays:
            print(f"warning: no data in {path}")
            continue
        n = len(delays)
        ys = [(i + 1) / n for i in range(n)]
        p50 = delays[int(0.50 * (n - 1))]
        p95 = delays[int(0.95 * (n - 1))]
        if label in COLORS:
            color = COLORS[label]
        else:
            color = EXTRA[extra_i % len(EXTRA)]
            extra_i += 1
        ax.step(delays, ys, where="post", color=color, lw=2,
                label=f"{label}  (n={n}, p50={p50:.0f}, p95={p95:.0f} ms)")
        ax.axvline(p95, color=color, ls=":", lw=1, alpha=0.5)
        print(f"{label}: n={n} p50={p50:.0f} p95={p95:.0f} ms  ({path})")

    ax.set_xlabel("attestation arrival delay (ms)")
    ax.set_ylabel("cumulative fraction of attestations")
    ax.set_ylim(0, 1.02)
    if args.xmax is not None:
        ax.set_xlim(left=min(0, ax.get_xlim()[0]), right=args.xmax)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower right")
    ax.set_title(args.title)
    fig.tight_layout()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=150)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
