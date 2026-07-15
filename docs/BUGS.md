# Bugs found by testing — with reproductions

Every bug below was caught by this repo's own test infrastructure before it
could have shipped, each by a different layer of the testing stack. That is
the point of building the stack: each layer catches the class of bug the
layers below it cannot see.

---

## Bug 1 — Snapshot-transfer wedge (deterministic simulation, seed 6)

**Layer that caught it:** deterministic simulator, full-cluster convergence
check. **Reproduce:** `cargo run --release -p sim --bin simulate -- --start 6
--seeds 1` (with the fix reverted). Seed 6 is in `crates/sim/regressions/seeds.txt`
and replays in CI forever.

**Symptom:** at end of run, node 3's state machine had converged to a stale
prefix — frozen at exactly its snapshot index — while the rest of the
cluster kept committing. The convergence assertion (all state machines
byte-identical after the network heals) failed.

**Root cause:** when a follower lags behind the leader's compacted log, the
leader sends `InstallSnapshot` and sets a `pending_snapshot` flag for that
follower, which suppresses all further `AppendEntries` (you cannot append
to a log the follower doesn't have yet). The flag was cleared only by the
`InstallSnapshotResp`. If the network dropped **either** the snapshot or
the response, the flag stayed set forever: the leader stopped sending that
follower *anything* — including heartbeats — and there was no retry path.
The follower stayed wedged until leadership changed, which in a stable
cluster is never.

**Fix:** an election-timeout-based retransmit in `send_append`
(`crates/raft/src/node.rs`): if `pending_snapshot` has been set for longer
than an election timeout with no response, re-send the snapshot.

**Why only simulation caught it:** the wedge needs (a) a follower lagging
past the leader's compaction horizon, (b) an InstallSnapshot in flight,
(c) that specific message dropped, then (d) a long quiet period where
nothing else perturbs the cluster. Unit tests don't generate (a)–(d)
together; a real chaos test might hit it once and never again. The
simulator hit it on the 7th random schedule it tried and replays it
on demand.

---

## Bug 2 — Restarted followers starved by a full pipeline window (raft scenario tests)

**Layer:** raft scenario test battery (crash-recovery scenario).

**Symptom:** a follower that crashed and restarted never caught up; the
leader had stopped sending it anything, permanently.

**Root cause:** replication uses optimistic pipelining with a bounded
in-flight window per follower. The crashed follower lost the in-flight
AppendEntries (volatile state), so their acks never arrived, so the
leader's window for that follower stayed "full" — and a full window
suppressed all sends, including heartbeats. Flow control designed to
protect a slow follower permanently silenced a recovered one.

**Fix:** heartbeat-fallback (empty AppendEntries always allowed regardless
of window) plus lost-ack window decay: in-flight slots time out after an
election timeout so the window drains even when acks are lost.

---

## Bug 3 — Block-boundary read missed the newest version (storage property tests)

**Layer:** storage engine property tests — randomized ops checked against
an in-memory model DB.

**Symptom:** after enough writes to spill multi-version keys across SSTable
block boundaries, a `get` returned an older version of a key than the model
said it should.

**Root cause:** SSTable blocks are keyed by `(user_key ASC, seq DESC)`. The
sparse-index binary search located the block whose `first_key` matched the
lookup key exactly — but when a key's versions straddled a block boundary,
the *newest* versions could live at the tail of the *previous* block, and
the search skipped them.

**Fix:** the block-selection search backs up one block when the target key
equals the located block's first key. The property test that caught it
(6 seeds × 3000 ops with flushes and multi-level compactions) runs in CI.

---

## Bug 4 — Windows append-mode WAL handle broke torn-tail truncation (crash tests)

**Layer:** storage crash-injection tests (kill the process mid-write,
reopen, verify).

**Symptom:** on Windows, recovery from a torn WAL tail failed with a
permission error when truncating the file in place.

**Root cause:** the WAL file was opened with `FILE_APPEND_DATA` but not
`FILE_WRITE_DATA`; on Windows, `SetEndOfFile` (what `set_len` uses) needs
the latter. Purely a platform semantics difference — the same code was
fine on Linux.

**Fix:** open the WAL read-write and seek to end, rather than using
append mode.

---

## Bug 5 — Client session-ID collision serialized two clients (live load test)

**Layer:** the YCSB load generator against a real 3-node cluster.

**Symptom:** a 16-client run reported exactly 1250 errors = exactly one
worker's share of 20,000 ops. One entire client's operations all failed
with `Stale`.

**Root cause:** `KvClient::connect` derived its session ID from
`SystemTime` nanos + pid. Windows reports time in 100 ns units, and the
bench creates 16 clients back-to-back — two got the same timestamp, hence
the same session ID. The state machine's dedup table then treated the two
clients as one session: whichever client's seq fell behind got every
operation rejected as a stale duplicate. Silent in small tests; determinate
at load.

**Fix:** mix a process-wide atomic counter into the session ID. (The
deeper lesson is documented in `kvsm`: dedup identity must be unique per
logical client, and "random enough" needs an actual uniqueness guarantee.)

---

## Bug 6 — Metrics endpoint RST the scraper (live smoke test)

**Layer:** live cluster smoke test (curl against the Prometheus endpoint).

**Symptom:** `curl` received an empty body, exit 0, no error.

**Root cause:** the hand-rolled HTTP responder wrote its response and
closed the socket without ever reading the request. Closing a socket with
unread inbound data sends RST, not FIN — the client's buffered response
data was discarded. Classic TCP footgun.

**Fix:** read (and discard) the request bytes before responding.

---

## What this list demonstrates

| Bug | Would unit tests have caught it? | What actually caught it |
|-----|----------------------------------|-------------------------|
| Snapshot wedge | No — needs 4 rare conditions to align | Simulation, seed 6, replayable |
| Pipeline starvation | Only with a crash-restart scenario test | Scenario battery |
| Block boundary | Only with randomized multi-version data | Property tests vs. model |
| WAL truncation | Only on Windows, only mid-crash | Crash injection |
| Session collision | No — needs concurrent same-tick clients | Load generator |
| Metrics RST | No — needs a real TCP peer | Live smoke test |

The two consensus bugs (1, 2) share a shape worth naming in interviews:
**"the mechanism that suppresses sends must always have a timeout-based
escape hatch."** Both were flow-control features working as designed, and
both designs were wrong in the same way — a suppressed channel with no
retry is a permanent wedge the first time the network eats a message.
