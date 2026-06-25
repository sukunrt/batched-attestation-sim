#!/usr/bin/env python3
"""Last-slot bandwidth profile for specific nodes in a single run dir.

For each node prints: bytes sent/recv during the last slot, peak sent/recv bps,
and the received wire split (att_data / sig / control) + attestations received.
Works for classic / partial / partial-priority / att_propagation.

Usage: uv run python analysis/node_bw_profile.py <run_dir> --nodes 0 2
"""
import argparse
import re
from pathlib import Path

BW = re.compile(r"\bbandwidth\b.*?\bsentbps=(\d+).*?\breceivedbps=(\d+)"
                r".*?\bsentBytesTotal=(\d+).*?\breceivedBytesTotal=(\d+)")
START = re.compile(r'msg="starting slot".*?\bslot=(\d+)')
END = re.compile(r'msg="slot complete".*?\bslot=(\d+)')
DATA = re.compile(r"\b(?:topic_message_received|partial_received|attprop_data_received)\b"
                  r".*?\batt_count=(\d+)\b.*?\batt_data_bytes=(\d+)\b.*?\bsig_bytes=(\d+)")
MD = re.compile(r"msg=partial_received\b.*?\bmetadata_bytes=(\d+)")
IHAVE = re.compile(r"msg=rpc_received\b.*?\bihave_size=(\d+)")
IWANT = re.compile(r"msg=rpc_received\b.*?\biwant_size=(\d+)")
ATTPROP_MD = re.compile(r"attprop_metadata_received\b.*?\bbytes=(\d+)")
ATTPROP_CTRL = re.compile(r"attprop_control_received\b.*?\bbytes=(\d+)")
SENDKEYS = (("attprop_send_data", re.compile(r"\bpositions=(\d+)")),
            ("partial_send_tick", re.compile(r"\btotal_positions_sent=(\d+)")))


def profile(path: Path, last_slot: int) -> dict:
    d = dict(sent=0, recv=0, peak_sbps=0, peak_rbps=0, att_data=0, sig=0,
             att_recv=0, ihave=0, iwant=0, md=0, ctrl=0, sends=0, send_pos=0,
             send_peers=set())
    sbase = rbase = stop = rtop = None
    inw = False
    with path.open() as f:
        for line in f:
            m = START.search(line)
            if m:
                if int(m.group(1)) == last_slot:
                    inw = True
                continue
            m = END.search(line)
            if m:
                if int(m.group(1)) == last_slot:
                    break
                continue
            mb = BW.search(line)
            if mb and not inw:
                sbase, rbase = int(mb.group(3)), int(mb.group(4))
                continue
            if not inw:
                continue
            if mb:
                stop, rtop = int(mb.group(3)), int(mb.group(4))
                d["peak_sbps"] = max(d["peak_sbps"], int(mb.group(1)))
                d["peak_rbps"] = max(d["peak_rbps"], int(mb.group(2)))
                continue
            g = DATA.search(line)
            if g:
                d["att_recv"] += int(g.group(1)); d["att_data"] += int(g.group(2)); d["sig"] += int(g.group(3))
                mm = MD.search(line)
                if mm:
                    d["md"] += int(mm.group(1))
                continue
            for pat, key in ((IHAVE, "ihave"), (IWANT, "iwant"),
                             (ATTPROP_MD, "md"), (ATTPROP_CTRL, "ctrl")):
                g = pat.search(line)
                if g:
                    d[key] += int(g.group(1)); break
            for sk, pat in SENDKEYS:
                if sk in line:
                    g = pat.search(line)
                    if g:
                        d["sends"] += 1; d["send_pos"] += int(g.group(1))
                    pm = re.search(r"peer=([A-Za-z0-9]+)", line)
                    if pm:
                        d["send_peers"].add(pm.group(1))
                    break
    d["sent"] = (stop - sbase) if (stop is not None and sbase is not None) else 0
    d["recv"] = (rtop - rbase) if (rtop is not None and rbase is not None) else 0
    d["control"] = d["md"] + d["ctrl"] + d["ihave"] + d["iwant"]
    return d


def last_slot_in(path: Path) -> int:
    last = 1
    with path.open() as f:
        for line in f:
            m = START.search(line)
            if m:
                last = max(last, int(m.group(1)))
    return last


def find_topology(run_dir: Path):
    for p in [run_dir, *run_dir.parents]:
        cand = p / "topology.json"
        if cand.exists():
            return __import__("json").load(open(cand))
    return None


def row(label, ds):
    """Average a list of profile dicts into one printed row."""
    n = len(ds)
    def avg(k):
        return sum(d[k] for d in ds) / n if n else 0
    print(f"{label:>12} {avg('sent')/1e3:>9.1f} {avg('recv')/1e3:>9.1f} "
          f"{avg('peak_sbps')*8/1e6:>10.2f} {avg('peak_rbps')*8/1e6:>10.2f} "
          f"{avg('att_data')/1e3:>11.1f} {avg('sig')/1e3:>8.1f} {avg('control')/1e3:>8.1f} "
          f"{avg('att_recv'):>9.0f} {avg('sends'):>6.0f} {avg('send_pos'):>9.0f} "
          f"{sum(len(d['send_peers']) for d in ds)/n if n else 0:>6.1f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", type=Path)
    ap.add_argument("--nodes", type=int, nargs="+")
    ap.add_argument("--by-tier", action="store_true",
                    help="average across all super and all regular MESH nodes")
    ap.add_argument("--sample", type=int, default=0,
                    help="with --by-tier, profile at most N nodes per tier (0=all)")
    args = ap.parse_args()
    hosts = args.run_dir / "shadow.data" / "hosts"
    hdr = f"{'who':>12} {'sent_KB':>9} {'recv_KB':>9} {'peakSmbps':>10} {'peakRmbps':>10} " \
          f"{'att_data_KB':>11} {'sig_KB':>8} {'ctrl_KB':>8} {'att_recv':>9} " \
          f"{'sends':>6} {'send_pos':>9} {'peers':>6}"

    if args.by_tier:
        topo = find_topology(args.run_dir)
        fan = set(topo["fanout_nodes"])
        sup = [n["num"] for n in topo["nodes"] if n["num"] not in fan and n["upload_bw_mbps"] >= 1024]
        reg = [n["num"] for n in topo["nodes"] if n["num"] not in fan and n["upload_bw_mbps"] < 1024]
        if args.sample:
            sup, reg = sup[: args.sample], reg[: args.sample]
        ls = last_slot_in(hosts / f"node{(sup or reg)[0]}" / "attestation.1000.stderr")
        print(f"run={args.run_dir.name}  last_slot={ls}  super_n={len(sup)} regular_n={len(reg)}")
        print(hdr)
        for label, nums in (("super", sup), ("regular", reg)):
            ds = [profile(hosts / f"node{n}" / "attestation.1000.stderr", ls)
                  for n in nums if (hosts / f"node{n}" / "attestation.1000.stderr").exists()]
            row(f"{label}(n={len(ds)})", ds)
        return

    ls = last_slot_in(hosts / f"node{args.nodes[0]}" / "attestation.1000.stderr")
    print(f"run={args.run_dir.name}  last_slot={ls}")
    print(hdr)
    for n in args.nodes:
        ds = [profile(hosts / f"node{n}" / "attestation.1000.stderr", ls)]
        row(f"node{n}", ds)


if __name__ == "__main__":
    main()
