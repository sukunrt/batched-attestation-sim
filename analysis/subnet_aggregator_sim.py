"""Toy simulation: how many subnets does a beacon node aggregate in?

Steps (per the request):
  1. Assign 1M validators to 10000 beacon nodes round-robin.
  2. Assign each beacon node randomly to one of 80 subnets (its subscription).
  3. Assign each validator randomly to one of the same 80 subnets.
  4. For each subnet, pick 16 of its validators as aggregators.
  5. Count, per beacon node, how many distinct subnets it aggregates in
     (a node aggregates in a subnet if any validator it owns is an
     aggregator there). This is usually more than the one subnet it
     subscribed to in step 2.
"""

import argparse
import random
import statistics
from collections import defaultdict

NUM_VALIDATORS = 1_000_000
NUM_NODES = 10_000
NUM_SUBNETS = 80
AGGREGATORS_PER_SUBNET = 16


def simulate(seed: int) -> list[int]:
    rng = random.Random(seed)

    # 1. round-robin: validator v is owned by node v % NUM_NODES.
    #    (No need to store this; it's just modulo.)

    # 2. each beacon node subscribes to one random subnet.
    node_subnet = [rng.randrange(NUM_SUBNETS) for _ in range(NUM_NODES)]

    # 3. each validator picks a random subnet; collect validators per subnet.
    validators_in_subnet: list[list[int]] = [[] for _ in range(NUM_SUBNETS)]
    for v in range(NUM_VALIDATORS):
        validators_in_subnet[rng.randrange(NUM_SUBNETS)].append(v)

    # 4 + 5. pick aggregators per subnet; each node participates in its
    #        subscribed subnet plus every subnet it aggregates in.
    subnets_per_node: list[set[int]] = [{s} for s in node_subnet]
    for subnet, validators in enumerate(validators_in_subnet):
        k = min(AGGREGATORS_PER_SUBNET, len(validators))
        for v in rng.sample(validators, k):
            subnets_per_node[v % NUM_NODES].add(subnet)

    return [len(s) for s in subnets_per_node]


def report(counts: list[int]) -> None:
    n = len(counts)
    dist = defaultdict(int)
    for c in counts:
        dist[c] += 1

    print(f"beacon nodes:               {n}")
    print(f"validators:                 {NUM_VALIDATORS}")
    print(f"subnets:                    {NUM_SUBNETS}")
    print(f"aggregators per subnet:     {AGGREGATORS_PER_SUBNET}")
    print(f"total aggregator slots:     {NUM_SUBNETS * AGGREGATORS_PER_SUBNET} "
          f"(= {NUM_SUBNETS} * {AGGREGATORS_PER_SUBNET})")
    print()
    print(f"subnets per node (subscribed + aggregated):")
    print(f"  mean:   {statistics.mean(counts):.3f}")
    print(f"  median: {statistics.median(counts)}")
    print(f"  min:    {min(counts)}")
    print(f"  max:    {max(counts)}")
    print()
    print("  distribution (subnets per node -> #nodes):")
    for c in sorted(dist):
        bar = "#" * (dist[c] * 60 // n)
        print(f"    {c:2d}: {dist[c]:6d}  {bar}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seed", type=int, default=0, help="RNG seed")
    args = ap.parse_args()
    report(simulate(args.seed))


if __name__ == "__main__":
    main()
