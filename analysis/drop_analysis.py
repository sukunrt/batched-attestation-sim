#!/usr/bin/env python3
"""Aggregate DropTracer `drop_summary` lines across mesh nodes of one run.

For each mesh node (node0..node<mesh-1>) take the LAST drop_summary line
(cumulative totals) and sum across nodes. Reports, for classic forwards:

  attempted fanout = (sent_publish_msgs + drop_publish_msgs) / deliver_msgs
                     -- what gossipsub's rpcs() actually computed (~mesh-1)
  wire    fanout   = sent_publish_msgs / deliver_msgs
                     -- what survived the size-32 per-peer queue
  drop rate        = drop_publish_msgs / attempted

and the partial-mode analogues (sent/drop partial RPCs).
"""
import sys
import glob
import os
import re
import argparse

FIELDS = [
    "sent_publish_rpc", "sent_publish_msgs", "sent_partial_rpc", "sent_control_rpc",
    "drop_publish_rpc", "drop_publish_msgs", "drop_partial_rpc", "drop_control_rpc",
    "recv_publish_msgs", "recv_partial_rpc", "dup_msgs", "deliver_msgs",
    "rej_throttle", "rej_queue_full", "rej_ignored", "rej_failed",
]

kv_re = re.compile(r"(\w+)=([0-9]+)\b")


def parse_last_summary(path):
    last = None
    with open(path, "r", errors="replace") as f:
        for line in f:
            if "msg=drop_summary" in line:
                last = line
    if last is None:
        return None
    d = {k: int(v) for k, v in kv_re.findall(last)}
    return {k: d.get(k, 0) for k in FIELDS}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--mesh", type=int, default=500, help="mesh node count (node0..node<mesh-1>)")
    args = ap.parse_args()

    hosts = os.path.join(args.run_dir, "shadow.data", "hosts")
    tot = {k: 0 for k in FIELDS}
    n_nodes = 0
    per_node_attempt = []
    per_node_wire = []
    per_node_droprate = []
    per_node_dup = []

    for i in range(args.mesh):
        path = os.path.join(hosts, f"node{i}", "attestation.1000.stderr")
        if not os.path.exists(path):
            continue
        s = parse_last_summary(path)
        if s is None:
            continue
        n_nodes += 1
        for k in FIELDS:
            tot[k] += s[k]
        deliver = s["deliver_msgs"]
        attempt = s["sent_publish_msgs"] + s["drop_publish_msgs"]
        if deliver > 0 and attempt > 0:
            per_node_attempt.append(attempt / deliver)
            per_node_wire.append(s["sent_publish_msgs"] / deliver)
            per_node_droprate.append(s["drop_publish_msgs"] / attempt)
        if deliver > 0 and s["recv_publish_msgs"] > 0:
            per_node_dup.append(s["recv_publish_msgs"] / deliver)

    def mean(xs):
        return sum(xs) / len(xs) if xs else 0.0

    classic = tot["sent_publish_msgs"] + tot["drop_publish_msgs"] > 0
    partial = tot["sent_partial_rpc"] + tot["drop_partial_rpc"] > 0

    print(f"run_dir: {args.run_dir}")
    print(f"mesh nodes with data: {n_nodes}")
    print(f"mode: {'CLASSIC' if classic else ''}{'PARTIAL' if partial else ''}")
    print()

    if classic:
        attempt = tot["sent_publish_msgs"] + tot["drop_publish_msgs"]
        print("== CLASSIC publish forwards ==")
        print(f"  deliver_msgs (unique forwarded) : {tot['deliver_msgs']}")
        print(f"  forwards attempted (rpcs yield)  : {attempt}")
        print(f"  forwards queued/sent (wire)      : {tot['sent_publish_msgs']}")
        print(f"  forwards DROPPED (queue full)    : {tot['drop_publish_msgs']}")
        print(f"  --> attempted fanout / msg       : {attempt / tot['deliver_msgs']:.2f}")
        print(f"  --> wire fanout / msg            : {tot['sent_publish_msgs'] / tot['deliver_msgs']:.2f}")
        print(f"  --> DROP RATE                    : {100 * tot['drop_publish_msgs'] / attempt:.1f}%")
        print(f"  recv_publish_msgs                : {tot['recv_publish_msgs']}")
        print(f"  --> dup (recv/deliver)           : {tot['recv_publish_msgs'] / tot['deliver_msgs']:.2f}")
        print(f"  per-node attempted fanout mean   : {mean(per_node_attempt):.2f}  (min {min(per_node_attempt):.2f} max {max(per_node_attempt):.2f})")
        print(f"  per-node wire fanout mean        : {mean(per_node_wire):.2f}")
        print(f"  per-node drop rate mean          : {100*mean(per_node_droprate):.1f}%  (min {100*min(per_node_droprate):.1f}% max {100*max(per_node_droprate):.1f}%)")
        print(f"  per-node dup mean                : {mean(per_node_dup):.2f}")

    if partial:
        psent = tot["sent_partial_rpc"]
        pdrop = tot["drop_partial_rpc"]
        denom = psent + pdrop
        print("== PARTIAL message RPCs ==")
        print(f"  partial RPCs queued/sent (wire)  : {psent}")
        print(f"  partial RPCs DROPPED (queue full): {pdrop}")
        print(f"  --> DROP RATE                    : {100 * pdrop / denom if denom else 0:.2f}%")
        print(f"  recv_partial_rpc                 : {tot['recv_partial_rpc']}")

    print()
    print("== validation-pipeline rejections (mechanism B check) ==")
    print(f"  rej_throttle={tot['rej_throttle']} rej_queue_full={tot['rej_queue_full']} "
          f"rej_ignored={tot['rej_ignored']} rej_failed={tot['rej_failed']}")
    print(f"  drop_control_rpc={tot['drop_control_rpc']}")


if __name__ == "__main__":
    main()
