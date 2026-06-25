#!/usr/bin/env python3
"""Send-side chunk-size histogram for one run dir (the run-... dir that holds
shadow.data/). Works for both att_propagation (attprop_send_data positions=N)
and partial-priority (partial_send_tick total_positions_sent=N).

Usage:
  uv run python analysis/send_size_histogram.py <run_dir> [--node N] [--peer P]

  <run_dir>  a single run dir, e.g. runs/<exp>/runs/run-...-n1500-...
  --node N   restrict to node N (default: all mesh nodes, sampled)
  --peer P   restrict to sends to peer-id substring P
  --max-nodes K  cap how many node files to scan when --node is omitted (default 20)
"""
import argparse
import re
from collections import Counter
from pathlib import Path

# (log key, size field) per mode.
PATTERNS = [
    ("attprop_send_data", re.compile(r"\bpositions=(\d+)")),
    ("partial_send_tick", re.compile(r"\btotal_positions_sent=(\d+)")),
]


SLOT_RE = re.compile(r"\bslot=(\d+)")


def sizes_from_file(path: Path, peer: str | None, slot: int | None) -> Counter:
    c = Counter()
    with path.open() as f:
        for line in f:
            if peer and peer not in line:
                continue
            for key, pat in PATTERNS:
                if key in line:
                    if slot is not None:
                        sm = SLOT_RE.search(line)
                        if not sm or int(sm.group(1)) != slot:
                            break
                    m = pat.search(line)
                    if m:
                        c[int(m.group(1))] += 1
                    break
    return c


def last_slot_in_file(path: Path) -> int | None:
    last = None
    with path.open() as f:
        for line in f:
            if any(k in line for k, _ in PATTERNS):
                sm = SLOT_RE.search(line)
                if sm:
                    s = int(sm.group(1))
                    if last is None or s > last:
                        last = s
    return last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", type=Path)
    ap.add_argument("--node", type=int, default=None)
    ap.add_argument("--peer", default=None)
    ap.add_argument("--max-nodes", type=int, default=20)
    ap.add_argument("--slot", type=int, default=None,
                    help="restrict to this slot (default: last slot seen); --all-slots to disable")
    ap.add_argument("--all-slots", action="store_true", help="count every slot, not just the last")
    args = ap.parse_args()

    hosts = args.run_dir / "shadow.data" / "hosts"
    if args.node is not None:
        files = [hosts / f"node{args.node}" / "attestation.1000.stderr"]
    else:
        files = sorted(hosts.glob("node*/attestation.1000.stderr"))[: args.max_nodes]

    files = [fp for fp in files if fp.exists()]
    slot = None
    if not args.all_slots:
        slot = args.slot
        if slot is None:
            slots = [s for s in (last_slot_in_file(fp) for fp in files) if s is not None]
            slot = max(slots) if slots else None

    hist = Counter()
    for fp in files:
        hist += sizes_from_file(fp, args.peer, slot)

    if not hist:
        raise SystemExit("no send lines found (attprop_send_data / partial_send_tick)")

    total = sum(hist.values())
    pos = sum(k * v for k, v in hist.items())
    biggest = max(hist)
    ones = hist.get(1, 0)
    full = hist.get(biggest, 0)
    where = f"node{args.node}" if args.node is not None else f"{len(files)} nodes"
    label = f"{where}  slot={'all' if slot is None else slot}"
    print(f"{label}  sends={total}  positions={pos}  mean={pos/total:.1f}  "
          f"singletons={ones} ({100*ones/total:.0f}%)  full({biggest})={full}")
    for k in range(1, biggest + 1):
        if hist.get(k):
            bar = "#" * round(40 * hist[k] / max(hist.values()))
            print(f"  {k:3d}: {hist[k]:6d} {bar}")


if __name__ == "__main__":
    main()
