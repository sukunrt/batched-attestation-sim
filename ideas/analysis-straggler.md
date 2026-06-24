# Propagation tapering and the straggler tail

Why the per-message propagation curve `N(t)` (number of nodes holding a given
attestation by time `t`) climbs exponentially and then **tapers** instead of
shooting straight to 100%.

## Run analysed

Experiment `exp-20260624-095659-183-f85fe0`
(`configs/experiment_classic_partial_priority_500_f2000_t2.yaml`), full attestor
load:

- 500 mesh (relay) nodes, degree 30, 20% super
- 2 topics, 2000 fanout attestors per topic, 2000 attestors/topic/slot
- D=8, publish interval 20 ms, `max_peers_per_attestation` → `D*2` = 16
- classic vs partial vs partial-priority

Measured on topic 0, slot 3 (warm slot). The nodes that log arrivals are the
**500-node relay mesh** — the fanout attestors are senders, so `N(t)` here means
"mesh nodes holding the message." `latency_ms` is wire receipt for
partial/partial-priority and post-validation delivery for classic.

## Exponential phase

Fitting `N(t) = N0 · e^{kt}` to the 5%→50% rise (R² ≈ 0.96):

| variant          | k (per s) | k (per ms) | doubling | 50% reach | 90% reach |
|------------------|-----------|------------|----------|-----------|-----------|
| classic          | 7.2       | 0.0072     | 96 ms    | 515 ms    | 1015 ms   |
| partial          | 11.8      | 0.0118     | 59 ms    | 345 ms    | 625 ms    |
| partial-priority | 11.9      | 0.0119     | 58 ms    | 350 ms    | 620 ms    |

Both partial modes grow ~64% faster than classic; partial-priority ≈ partial.
The per-message-averaged `k` matches the aggregate-arrival `k` exactly, so the
rate is well defined.

## Why it tapers (the core fact)

`e^{kt}` is only the small-`N` approximation of the real dynamics

```
dN/dt = k · N · (1 − N/M)
```

The instantaneous spread rate is **(holders) × (fraction still needing it)**.
Those two factors fight each other: early on almost everyone still needs it, so
the rate tracks the holder count and grows exponentially; but as the holder
fraction rises, each forward increasingly lands on a node that **already holds
the message** — a wasted, duplicate send — so the new-delivery rate falls back
to zero. The exponential rise and the taper are the two halves of the *same*
S-curve (logistic); the product `N·(1−N/M)` peaks at `N = M/2`.

It cannot snap straight to 100% because, near the top, **no node knows which
few are still missing it**. Holders forward to random peers, so the last
stragglers are reached only by luck — many wasted sends per useful one. A curve
that climbs and stops sharply would need a coordinated broadcast tree (zero
wasted sends); gossip trades that efficiency for simplicity and churn-robustness,
and pays for it with the slow tail.

This is the same thing as the duplicate-receive overhead measured separately:
those extra copies *are* the wasted forwards that flatten the curve.

## It is steeper than a perfect epidemic (fat tail)

A clean logistic is a straight line in logit space (`ln(N/(M−N))` vs `t`, slope
`k`). Measured slopes split the curve in half:

| variant          | k bottom half | k top half | ratio top/bottom |
|------------------|---------------|------------|------------------|
| classic          | 9.4/s         | 4.3/s      | 0.46             |
| partial          | 15.1/s        | 8.0/s      | 0.53             |
| partial-priority | 15.7/s        | 8.2/s      | 0.52             |

The **top half spreads about half as fast as the bottom half** (a perfect
logistic would give ≈ 1.0). Two effects stack:

1. **Intrinsic gossip mop-up.** A single message reaches half the mesh in
   ~355 ms, then takes another ~390 ms to reach *all* of it — about as long
   again for a much smaller set of nodes. That is the "find the last stragglers
   by luck" effect, unavoidable in gossip.

2. **Throughput ceiling at full load (hypothesis).** At 2000 attestors/topic
   every mesh node receives and relays ~2000 messages (×2 topics) in under a
   second. Once its send/receive queue saturates, messages arrive at a roughly
   steady *rate* rather than exponentially, bending the second half down on top
   of effect (1).

The slow tail is **systemic, not a few unlucky nodes**: the slowest node
finishes at 1237 ms vs the median node at 884 ms (~1.4×) — everyone is
throttled, not a handful.

Per-node / per-message straggler stats (partial-priority):

- per-node time to receive its messages: 50% @ med 341 ms, 90% @ 604 ms,
  99% @ 775 ms, 100% @ 884 ms (max node 1237 ms)
- per-message reach over the mesh: 50% @ med 355 ms, 90% @ 522 ms,
  100% @ 745 ms (slowest message 1237 ms)

## Open question / how to confirm effect (2)

The throughput-ceiling explanation is inferred from the curve shape, not proven.
Clean test: rerun at a much lighter attestor load (e.g. 500/topic) and re-measure
the logit top/bottom ratio.

- ratio moves back toward 1.0 at light load ⇒ the fat tail is throughput
  saturation.
- ratio stays ~0.5 ⇒ it is pure gossip mop-up, independent of load.

## Artifacts

- run dir: `runs/exp-priority-500-f2000-t2-20260624-132639/exp-20260624-095659-183-f85fe0`
- exponential-fit plot: `graphs/propagation_fit_topic0_slot3.png`
- logit-space plot (downward bend = fat tail): `graphs/propagation_logit_topic0_slot3.png`
