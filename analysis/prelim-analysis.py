#!/usr/bin/env python3
"""Preliminary analysis for attestation runs (classic + partial-message).

Shows:
  - time-to-receive-95% of attestations across (node, slot, committee), p25..p99.
  - Received wire composition for a few randomly picked mesh nodes (super vs
    regular): total bytes plus the split into attestation_data / signature /
    control bytes, the count of attestations received, and the share of
    attestation-related bytes that is attestation_data. This is what exposes why
    partial wins: classic re-ships the full AttestationData for every attestation
    and every duplicate, so its att_data share is high; partial ships the shared
    AttestationData once per bucket, so its bytes are mostly signatures + small
    control bitmaps.

Usage:
    uv run python analysis/prelim-analysis.py <dir> [--num-samples N]

`<dir>` can be either:
  - an experiment directory (contains topology.json + runs/), or
  - a single attestations run directory (contains topology.json + shadow.data/).
"""

import argparse
import json
import random
import re
from collections import defaultdict
from pathlib import Path

import numpy as np
import yaml

# Classic-mode receive (from SlogTracer.OnReceive, simple-format slog):
# "<ts> INFO received node=N from=F slot=S committee_index=C msg_index=I latency_ms=L"
RECEIVED_PAT = re.compile(
    r'\breceived\b.*?\b(?:from|idx)=(\d+)\b.*?\bslot=(\d+)\b.*?\bcommittee_index=(\d+)\b.*?\blatency_ms=(\d+)'
)
# Partial-mode receive (from SlogTracer.OnPartialReceive):
# "<ts> INFO partial_received node=N slot=S committee_index=C position=P att_digest=H latency_ms=L"
PARTIAL_RECEIVED_PAT = re.compile(
    r'\bpartial_received\b.*?\bslot=(\d+)\b.*?\bcommittee_index=(\d+)\b.*?\bposition=(\d+)\b.*?\blatency_ms=(\d+)'
)
# Bandwidth: n.logger uses the slog text handler with AddSource, which emits
# "time=... level=INFO source=... msg=bandwidth node=N sentbps=X ..." — so the
# pattern doesn't anchor on "INFO bandwidth".
BW_PAT = re.compile(
    r'\bbandwidth\b.*?\bnode=(\d+).*?\bsentbps=(\d+).*?\breceivedbps=(\d+).*?\bsentBytesTotal=(\d+).*?\breceivedBytesTotal=(\d+)'
)
# Wire-level attestation accounting (from rpc_tracer.go), received side. Lines
# are slog text format: "time=... level=INFO msg=<key> ... <field>=N ...".
#
# Both modes now log att_count / att_data_bytes / sig_bytes per received RPC, so
# we can split received bytes into attestation payload (data vs signature) and
# control:
#   - classic: topic_message_received carries one Attestation (att_count=1).
#   - partial: partial_received carries a batched envelope (att_count = #sigs)
#     plus metadata_bytes (the parts-metadata control blob).
# Control bytes received:
#   - classic = gossipsub IHAVE (topic_ihave_received, ihave_size) + IWANT
#     (rpc_received, iwant_size).
#   - partial = parts metadata (partial_received, metadata_bytes).
# Note: the SlogTracer's app-level "partial_received" lifecycle event also
# appears in the same stderr but has slot=/position=/latency_ms= rather than
# att_count=/metadata_bytes=, so it won't collide with the wire patterns below.
CLASSIC_RECV_PAT = re.compile(
    r'msg=topic_message_received\b.*?\batt_count=(\d+)\b.*?\batt_data_bytes=(\d+)\b.*?\bsig_bytes=(\d+)'
)
PARTIAL_DATA_RECV_PAT = re.compile(
    r'msg=partial_received\b.*?\batt_count=(\d+)\b.*?\batt_data_bytes=(\d+)\b.*?\bsig_bytes=(\d+)'
)
IHAVE_RECV_PAT = re.compile(r'msg=topic_ihave_received\b.*?\bihave_size=(\d+)')
IWANT_RECV_PAT = re.compile(r'msg=rpc_received\b.*?\biwant_size=(\d+)')
PARTIAL_MD_RECV_PAT = re.compile(r'msg=partial_received\b.*?\bmetadata_bytes=(\d+)')


def load_topology(exp_dir: Path) -> dict:
    topo = json.loads((exp_dir / "topology.json").read_text())
    fanout = set(topo.get("fanout_nodes", []))
    super_mesh = {n["num"] for n in topo["nodes"]
                  if n["num"] not in fanout and n["upload_bw_mbps"] >= 1024}
    regular_mesh = {n["num"] for n in topo["nodes"]
                    if n["num"] not in fanout and n["upload_bw_mbps"] < 1024}
    return {"fanout": fanout, "super_mesh": super_mesh, "regular_mesh": regular_mesh}


def parse_node_stderr(stderr_path: Path, parse_bw: bool = False):
    """Return (received_records, bw_stats).

    received_records: list of dicts with slot, committee, latency_ms.
    bw_stats: dict with totals plus the received wire composition
              (att_data_recv / sig_recv / att_recv and the control counters), or
              None if parse_bw is False / no bandwidth lines seen.
    """
    records = []
    sent_total = None
    recv_total = None
    peak_sent_bps = 0
    peak_recv_bps = 0
    att_data_recv = 0   # attestation_data bytes received (deduped per bucket in partial)
    sig_recv = 0        # signature bytes received
    att_recv = 0        # attestations received (instances on the wire, with dups)
    ihave_recv = 0
    iwant_recv = 0
    md_recv = 0
    with open(stderr_path) as f:
        for line in f:
            m = RECEIVED_PAT.search(line)
            if m:
                records.append({
                    "slot": int(m.group(2)),
                    "committee": int(m.group(3)),
                    "latency_ms": int(m.group(4)),
                })
                continue
            m = PARTIAL_RECEIVED_PAT.search(line)
            if m:
                records.append({
                    "slot": int(m.group(1)),
                    "committee": int(m.group(2)),
                    "latency_ms": int(m.group(4)),
                })
                continue
            if parse_bw:
                mb = BW_PAT.search(line)
                if mb:
                    sent_total = int(mb.group(4))
                    recv_total = int(mb.group(5))
                    peak_sent_bps = max(peak_sent_bps, int(mb.group(2)))
                    peak_recv_bps = max(peak_recv_bps, int(mb.group(3)))
                    continue
                gm = CLASSIC_RECV_PAT.search(line)
                if gm:
                    att_recv += int(gm.group(1))
                    att_data_recv += int(gm.group(2))
                    sig_recv += int(gm.group(3))
                    continue
                gm = PARTIAL_DATA_RECV_PAT.search(line)
                if gm:
                    att_recv += int(gm.group(1))
                    att_data_recv += int(gm.group(2))
                    sig_recv += int(gm.group(3))
                    # partial_received also carries the control (parts metadata).
                    mdm = PARTIAL_MD_RECV_PAT.search(line)
                    if mdm:
                        md_recv += int(mdm.group(1))
                    continue
                gm = IHAVE_RECV_PAT.search(line)
                if gm:
                    ihave_recv += int(gm.group(1))
                    continue
                gm = IWANT_RECV_PAT.search(line)
                if gm:
                    iwant_recv += int(gm.group(1))
                    continue
    bw = None
    if sent_total is not None:
        bw = {
            "sent_total": sent_total,
            "recv_total": recv_total,
            "peak_sent_bps": peak_sent_bps,
            "peak_recv_bps": peak_recv_bps,
            "att_data_recv": att_data_recv,
            "sig_recv": sig_recv,
            "att_recv": att_recv,
            "ihave_recv": ihave_recv,
            "iwant_recv": iwant_recv,
            "md_recv": md_recv,
        }
    return records, bw


def analyze_run(run_dir: Path, topo: dict, num_samples: int = 10,
                rng: random.Random | None = None):
    hosts = run_dir / "shadow.data" / "hosts"
    mesh_ids = topo["super_mesh"] | topo["regular_mesh"]

    cfg = yaml.safe_load((run_dir / "config.yaml").read_text())
    mode = "partial" if cfg["simulation"].get("use_partial_messages") else "classic"

    # Decide which nodes to parse bandwidth for (spot check — avoid scanning all stderrs).
    rng = rng or random.Random(42)
    super_pool = sorted(topo["super_mesh"])
    reg_pool = sorted(topo["regular_mesh"])
    super_pick = set(rng.sample(super_pool, min(num_samples, len(super_pool))) if super_pool else [])
    reg_pick = set(rng.sample(reg_pool, min(num_samples, len(reg_pool))) if reg_pool else [])
    bw_sample = super_pick | reg_pick

    # per-node (slot, committee) -> list of latencies
    per_node_sc_lats: dict[int, dict[tuple[int, int], list[int]]] = defaultdict(lambda: defaultdict(list))
    node_bw: dict[int, dict] = {}

    for nd in hosts.iterdir():
        nn = int(nd.name.replace("node", ""))
        sf = next(nd.glob("*.stderr"), None)
        if not sf:
            continue
        recs, bw = parse_node_stderr(sf, parse_bw=(nn in bw_sample))
        if bw is not None:
            node_bw[nn] = bw
        if nn not in mesh_ids:
            continue  # only mesh nodes have receive stats of interest
        for r in recs:
            per_node_sc_lats[nn][(r["slot"], r["committee"])].append(r["latency_ms"])

    # Time-to-95% per (node, slot, committee).
    t95 = []
    for nn, sc_lats in per_node_sc_lats.items():
        for (_slot, _committee), lats in sc_lats.items():
            total = len(lats)
            if total == 0:
                continue
            target = int(np.ceil(0.95 * total))
            sl = sorted(lats)
            t95.append(sl[target - 1])

    if not t95:
        return None
    arr = np.array(t95)
    super_sorted = sorted(super_pick & node_bw.keys())
    reg_sorted = sorted(reg_pick & node_bw.keys())

    def class_bw(nodes: list[int]) -> dict:
        keys = ["sent_total", "recv_total", "att_data_recv", "sig_recv",
                "att_recv", "ihave_recv", "iwant_recv", "md_recv"]
        if not nodes:
            d = {k: 0.0 for k in keys}
        else:
            d = {k: sum(node_bw[n][k] for n in nodes) / len(nodes) for k in keys}
        # Control received = IHAVE+IWANT in classic, parts metadata in partial.
        if mode == "partial":
            d["control_recv"] = d["md_recv"]
        else:
            d["control_recv"] = d["ihave_recv"] + d["iwant_recv"]
        return d

    return {
        "mode": mode,
        "num_topics": int(cfg["simulation"].get("num_topics", 1)),
        "t95": arr,
        "super_bw": class_bw(super_sorted),
        "regular_bw": class_bw(reg_sorted),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("exp_dir", type=Path, help="experiment dir (with topology.json + runs/) or single attestations run dir (with topology.json + shadow.data/)")
    ap.add_argument("--num-samples", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    topo = load_topology(args.exp_dir)
    rng = random.Random(args.seed)

    # Detect layout: experiment dir has `runs/`, single run dir has `shadow.data/`.
    runs_dir = args.exp_dir / "runs"
    if runs_dir.is_dir():
        runs = sorted(runs_dir.iterdir())
    elif (args.exp_dir / "shadow.data").is_dir():
        runs = [args.exp_dir]
    else:
        raise SystemExit(f"{args.exp_dir} has neither runs/ nor shadow.data/")

    results = {}
    for rd in runs:
        res = analyze_run(rd, topo, num_samples=args.num_samples, rng=rng)
        if res is not None:
            results[res["mode"]] = res

    if "classic" in results and "partial" in results:
        print_comparison(results["classic"], results["partial"])


def print_table(title: str, headers: list[str], rows: list[list[str]]):
    """Print a simple aligned ASCII table."""
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    def fmt(cells: list[str]) -> str:
        out = ["|"]
        for i, c in enumerate(cells):
            # right-align numeric-ish cells, left-align first column
            align = "<" if i == 0 else ">"
            out.append(f" {c:{align}{widths[i]}} |")
        return "".join(out)
    print(title)
    print(sep)
    print(fmt(headers))
    print(sep)
    for r in rows:
        print(fmt(r))
    print(sep)
    print()


def pct_delta(c: float, p: float) -> str:
    if c == 0:
        return "n/a"
    return f"{(p - c) / c * 100:+.1f}%"


def data_share(d: dict) -> float:
    """Percent of attestation-related received bytes (att_data + sig + control)
    that is attestation_data. High for classic (full data re-shipped per
    attestation and per duplicate); low for partial (data deduped per bucket)."""
    denom = d["att_data_recv"] + d["sig_recv"] + d["control_recv"]
    return (d["att_data_recv"] / denom * 100) if denom else 0.0


def print_comparison(classic: dict, partial: dict):
    # Latency
    pcts = [25, 50, 95, 99]
    c_vals = [np.percentile(classic["t95"], p) for p in pcts]
    p_vals = [np.percentile(partial["t95"], p) for p in pcts]
    headers = ["mode"] + [f"p{p}" for p in pcts]
    rows = [
        ["classic"] + [f"{v:.0f}" for v in c_vals],
        ["partial"] + [f"{v:.0f}" for v in p_vals],
        ["delta"]   + [pct_delta(c, p) for c, p in zip(c_vals, p_vals)],
    ]
    print_table("Latency: time-to-receive-95% (ms)", headers, rows)

    nt_c = classic["num_topics"]
    nt_p = partial["num_topics"]
    # (header, bw-dict key, unit divisor)
    cols = [
        ("sent (MB)",     "sent_total",    1e6),
        ("recv (MB)",     "recv_total",    1e6),
        ("att_data (KB)", "att_data_recv", 1e3),
        ("sig (KB)",      "sig_recv",      1e3),
        ("control (KB)",  "control_recv",  1e3),
        ("att recv",      "att_recv",      1.0),
    ]
    for label, key in [("super", "super_bw"), ("regular", "regular_bw")]:
        cb = classic[key]
        pb = partial[key]
        headers = ["mode"] + [h for h, _, _ in cols] + ["att_data %"]
        c_row = ["classic"] + [f"{cb[k]/nt_c/unit:.2f}" for _, k, unit in cols] + [f"{data_share(cb):.1f}%"]
        p_row = ["partial"] + [f"{pb[k]/nt_p/unit:.2f}" for _, k, unit in cols] + [f"{data_share(pb):.1f}%"]
        d_row = ["delta"]   + [pct_delta(cb[k]/nt_c, pb[k]/nt_p) for _, k, _ in cols] + [f"{data_share(pb) - data_share(cb):+.1f}pp"]
        print_table(
            f"Received wire composition — {label} (mean per sampled mesh node, per topic; assumes equal usage across {nt_c} topics)",
            headers, [c_row, p_row, d_row],
        )


if __name__ == "__main__":
    main()
