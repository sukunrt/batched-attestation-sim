# att_propagation — current state

## TL;DR
The att_propagation mode is implemented and committed (8 atomic commits). The
multi-node mesh test was hanging; the real cause was a **lifecycle ordering bug**
(not the eventloop send, not bidirectional streams). It's fixed and the 4-node
mesh test passes in 0.04s with full propagation. Two tests still fail (one a
diagnostic-scaffold double-start, one a genuine bidirectional-forward gap to
investigate). All deadlock-fix + test work is **uncommitted working-copy**.

## What the hang actually was (resolved)
- Symptom: `TestAttProp32MeshPropagation` (32/8/4 nodes) timed out under synctest.
- Disproven theories: eventloop blocking on `s.work <- frame` (instrumented:
  `enqueue_start == enqueue_done`, it never blocked); bidirectional QUIC streams
  (a wrong turn — QUIC streams are bidirectional by design).
- **Real cause:** `runAttProp` created its own `context.WithCancel(Background())`
  and `cancel()`ed the eventloop at the END of `Run`. The test then called
  `ValidatedCount`, which posts a `funcEvent` to the now-dead eventloop and blocks
  forever on the reply. Propagation itself always completed (16/16 at 4 nodes,
  64/64 at 8 nodes) — the hang was purely the post-shutdown read.
- **Fix applied** (in `node.go`): start the eventloop in `startAttProp` under the
  node's lifecycle `ctx` (so it outlives `Run`); `runAttProp` no longer makes/
  cancels its own ctx, it just drives the publish schedule + drain;
  `context.AfterFunc(ctx, n.verifier.Stop)` stops the verifier on ctx cancel.
  `ValidatedCount` works unchanged. No new channel, no `markStopped`.

## Test status (`cd cmd/attestation`, always use `-timeout`)
PASS:
- `TestAttProp32MeshPropagation` (currently numNodes=4) — 4/4 full, 0.03s
- `TestAttProp32FanoutInjection` — 0.30s
- `TestAttPropStartNoGossipsub`, `TestAttPropFanoutToMesh`, `TestAttPropDiagN`

FAIL (2):
1. `TestAttPropProbeConnect` (`node/zz_probe_test.go`) — panics
   `index out of range [-1]`. Cause: it does its own
   `go nodes[i].attProp.Run(ctx)` (line 38) AND `startAttProp` now also starts the
   eventloop → two eventloops drain one `events` chan and race the holder-count
   index negative. Fix: delete line 38, or delete this diagnostic test (`zz_`
   scaffolding, now redundant with the real passing tests).
2. `TestMeshForwardBidirectional` (`node/attprop/mesh_forward_test.go`) —
   opener validated 1, expected 2. The acceptor→opener direction isn't delivering/
   validating its position. NOT yet root-caused. Suspect: the role-forcing race
   (`onPeerUp` sets role back to `roleConnected`) or the acceptor's
   `openSendStreams` set not ready when the test ticks. Unaffected by the node.go
   lifecycle fix (it drives Managers directly via the harness).
   - To debug: `ATTPROP_LOG=1 go test -v -count=1 -timeout 30s -run TestMeshForwardBidirectional ./node/attprop/`
     (intgCfg routes slog to stderr when ATTPROP_LOG set). NOTE: last attempt
     produced only 3 lines of output — log surfacing under synctest needs sorting
     out first.

## Stream design (current working copy — the user's openSendStreams rework)
- Each peer opens its OWN three send-streams to the other (push/bitmap/control);
  readers only read the peer's streams. One writer + one reader per QUIC stream.
- Lower-peerID dials the connection (`connectPeer`); both sides open their send
  set (the acceptor's via `inboundHandler` → `go openSendStreams`, guarded by
  `markSendStreamsOpening`). `onPeerUp` registers the three senders and sets the
  peer to `roleConnected`.

## Uncommitted working-copy files
- `cmd/attestation/node/node.go` — startAttProp/runAttProp lifecycle fix.
- `cmd/attestation/node/attprop/{wire.go, eventloop.go, attprop.go}` —
  openSendStreams rework; eventloop has NO markStopped/stopped (reverted).
- `cmd/attestation/node/attprop_sim_test.go` — mesh (numNodes=4) + fanout tests.
- `cmd/attestation/node/zz_probe_test.go`, `zz_diag_test.go` — diagnostics.
- `cmd/attestation/node/attprop/mesh_forward_test.go` — failing bidi test.
- `cmd/attestation/node/attprop/testhooks.go` — `ValidatedCount` accessor.
- `ideas/new-algo-spec.md`, `ideas/new-algo.md` — pre-existing (do not touch).

## Next steps
1. Fix `TestAttPropProbeConnect` (drop the double `go Run`, or delete zz_ tests).
2. Root-cause + fix `TestMeshForwardBidirectional` (acceptor→opener = 1/2).
3. Get logs surfacing under synctest (ATTPROP_LOG path) so the above is debuggable.
4. Add to `AGENTS.md`: ALWAYS run `go test` with an explicit `-timeout` (≤60s for
   synctest tests) so a deadlock fails fast.
5. Scale the mesh test back to 8 then 32 once green; confirm fast.
6. Commit the deadlock fix + tests atomically (nothing since the 8 base commits).
7. NOT YET DONE — requested algorithm change: stop forwarding an attestation to
   **bitmap-mesh** peers once ≥1/2 of the bitmap mesh already holds it (the push
   mesh still gets every message); the mesh handles the rest from there.
8. Measurement (task 5): local `simctl attestations` + `simctl experiment`
   (att_propagation vs partial_priority, shared topology), then the remote
   500-mesh / 2000-attestor / 2-topic run on `ethp2p` and the p90–p99.9 tail
   comparison.

## Base commits (green at commit time)
verify pkg extraction · AttPropControl proto · attprop skeleton+msgio substrate ·
mesh state machine · holder-count scarcity index · send eventloop+bitmap+fanout ·
Node/config/main plumbing · simctl/analysis/docs plumbing.
