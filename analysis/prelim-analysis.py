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

All numbers are scoped to the LAST slot only: per node we start counting at the
`msg="starting slot" slot=<num_slots>` line and stop at the matching
`msg="slot complete"` line, discarding everything emitted after the slot-ends
line (the post-slot re-advertisement / drain tail). Latency records carry
`slot=` and are additionally filtered to the last slot; bandwidth is the
cumulative delta across the window. This avoids mixing slot-1 warmup and the
trailing drain into the per-slot picture.

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
# "<ts> INFO received node=N from=F slot=S committee_index=C latency_ms=L"
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
# Slot lifecycle (node.go: n.logger.Info("starting slot"/"slot complete", "slot", N)).
# slog text format quotes the message: msg="starting slot" ... slot=N.
SLOT_START_PAT = re.compile(r'msg="starting slot".*?\bslot=(\d+)')
SLOT_END_PAT = re.compile(r'msg="slot complete".*?\bslot=(\d+)')


def load_topology(exp_dir: Path) -> dict:
    topo = json.loads((exp_dir / "topology.json").read_text())
    fanout = set(topo.get("fanout_nodes", []))
    super_mesh = {n["num"] for n in topo["nodes"]
                  if n["num"] not in fanout and n["upload_bw_mbps"] >= 1024}
    regular_mesh = {n["num"] for n in topo["nodes"]
                    if n["num"] not in fanout and n["upload_bw_mbps"] < 1024}
    return {"fanout": fanout, "super_mesh": super_mesh, "regular_mesh": regular_mesh}


def parse_node_stderr(stderr_path: Path, last_slot: int, parse_bw: bool = False):
    """Return (received_records, bw_stats), scoped to the LAST slot only.

    We stream the stderr in order and only accumulate between this node's
    `msg="starting slot" slot=last_slot` line and its matching
    `msg="slot complete"` line, breaking at that line so everything emitted
    after the slot ends (the post-slot re-advertisement / drain tail) is
    discarded. Latency records carry `slot=` and are additionally filtered to
    `last_slot`. Bandwidth totals are the cumulative delta across the window
    (value at slot end minus value at slot start), so they reflect bytes moved
    during the last slot rather than the whole run.

    received_records: list of dicts with slot, committee, latency_ms.
    bw_stats: dict with last-slot totals plus the received wire composition
              (att_data_recv / sig_recv / att_recv and the control counters), or
              None if parse_bw is False / no bandwidth lines seen.
    """
    records = []
    sent_base = recv_base = None   # cumulative totals just before the last slot
    sent_top = recv_top = None     # cumulative totals at/within the last slot
    peak_sent_bps = 0
    peak_recv_bps = 0
    att_data_recv = 0   # attestation_data bytes received (deduped per bucket in partial)
    sig_recv = 0        # signature bytes received
    att_recv = 0        # attestations received (instances on the wire, with dups)
    ihave_recv = 0
    iwant_recv = 0
    md_recv = 0
    in_window = False
    with open(stderr_path) as f:
        for line in f:
            sm = SLOT_START_PAT.search(line)
            if sm:
                if int(sm.group(1)) == last_slot:
                    in_window = True
                continue
            em = SLOT_END_PAT.search(line)
            if em:
                if int(em.group(1)) == last_slot:
                    break  # discard everything after the slot-ends line
                continue
            if not in_window:
                # Before the last slot: keep the running cumulative bandwidth so
                # we can subtract it off once the window starts.
                if parse_bw:
                    mb = BW_PAT.search(line)
                    if mb:
                        sent_base = int(mb.group(4))
                        recv_base = int(mb.group(5))
                continue
            # ---- within the last-slot window ----
            m = RECEIVED_PAT.search(line)
            if m:
                if int(m.group(2)) == last_slot:
                    records.append({
                        "slot": int(m.group(2)),
                        "committee": int(m.group(3)),
                        "latency_ms": int(m.group(4)),
                    })
                continue
            m = PARTIAL_RECEIVED_PAT.search(line)
            if m:
                if int(m.group(1)) == last_slot:
                    records.append({
                        "slot": int(m.group(1)),
                        "committee": int(m.group(2)),
                        "latency_ms": int(m.group(4)),
                    })
                continue
            if parse_bw:
                mb = BW_PAT.search(line)
                if mb:
                    sent_top = int(mb.group(4))
                    recv_top = int(mb.group(5))
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
    if parse_bw and (sent_top is not None or sent_base is not None):
        base_s = sent_base or 0
        base_r = recv_base or 0
        top_s = sent_top if sent_top is not None else base_s
        top_r = recv_top if recv_top is not None else base_r
        bw = {
            "sent_total": max(0, top_s - base_s),
            "recv_total": max(0, top_r - base_r),
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
    sim = cfg["simulation"]
    mode = "partial" if sim.get("use_partial_messages") else "classic"
    tier = "tiered" if (sim.get("supernode_d") or sim.get("homenode_d")) else "uniform"
    last_slot = int(sim.get("num_slots", 1))

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
        recs, bw = parse_node_stderr(sf, last_slot, parse_bw=(nn in bw_sample))
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
        "tier": tier,
        "num_topics": int(sim.get("num_topics", 1)),
        "last_slot": last_slot,
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
            results[(res["mode"], res["tier"])] = res

    modes = sorted({m for m, _t in results})
    tiers = {t for _m, t in results}

    if "tiered" in tiers and "uniform" in tiers:
        # tiered-D vs uniform-D within each mode (uniform is the baseline).
        for mode in modes:
            base = results.get((mode, "uniform"))
            tiered = results.get((mode, "tiered"))
            if base and tiered:
                print(f"=== {mode}: uniform-D vs tiered-D ===\n")
                print_comparison(base, f"{mode}/uniform", tiered, f"{mode}/tiered")
    else:
        # Single-tier experiment: fall back to classic-vs-partial.
        tier = next(iter(tiers), None)
        classic = results.get(("classic", tier))
        partial = results.get(("partial", tier))
        if classic and partial:
            print_comparison(classic, "classic", partial, "partial")


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


def print_comparison(a: dict, label_a: str, b: dict, label_b: str):
    """Compare two runs. `delta` is how b differs from a (the baseline)."""
    # Latency
    pcts = [25, 50, 95, 99]
    a_vals = [np.percentile(a["t95"], p) for p in pcts]
    b_vals = [np.percentile(b["t95"], p) for p in pcts]
    headers = ["variant"] + [f"p{p}" for p in pcts]
    rows = [
        [label_a] + [f"{v:.0f}" for v in a_vals],
        [label_b] + [f"{v:.0f}" for v in b_vals],
        ["delta"] + [pct_delta(x, y) for x, y in zip(a_vals, b_vals)],
    ]
    ls = a.get("last_slot", b.get("last_slot"))
    print_table(f"Latency: time-to-receive-95% (ms) — last slot ({ls}) only", headers, rows)

    nt_a = a["num_topics"]
    nt_b = b["num_topics"]
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
        ab = a[key]
        bb = b[key]
        headers = ["variant"] + [h for h, _, _ in cols] + ["att_data %"]
        a_row = [label_a] + [f"{ab[k]/nt_a/unit:.2f}" for _, k, unit in cols] + [f"{data_share(ab):.1f}%"]
        b_row = [label_b] + [f"{bb[k]/nt_b/unit:.2f}" for _, k, unit in cols] + [f"{data_share(bb):.1f}%"]
        d_row = ["delta"] + [pct_delta(ab[k]/nt_a, bb[k]/nt_b) for _, k, _ in cols] + [f"{data_share(bb) - data_share(ab):+.1f}pp"]
        print_table(
            f"Received wire composition — {label} (last slot ({ls}) only; mean per sampled mesh node, per topic; assumes equal usage across {nt_a} topics)",
            headers, [a_row, b_row, d_row],
        )


if __name__ == "__main__":
    main()
