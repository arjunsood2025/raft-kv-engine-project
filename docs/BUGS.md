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

## Bug 7 — `kvctl` panicked when its reader hung up (failover script)

**Layer:** the chaos failover script (`chaos/kill-leader.sh`) driving a live
3-node cluster. **Reproduce:** with the leader on the *first* status line and
any later node down, `kvctl --cluster ... status | grep -m1 " Leader "` —
`kvctl` exits 101. 15/15 before the fix, 0/15 after.

**Symptom:** `chaos/kill-leader.sh 10` aborted mid-run with exit 101 (a Rust
panic) after 3–8 rounds had already printed correct measurements. Each rerun
died at a *different* round.

**Root cause:** the script locates the leader with `kvctl status | grep -m1
" Leader "`. `grep -m1` exits at its first match, closing the pipe, while
`kvctl` is still printing one line per node. `println!` panics when a write
to stdout fails, so the closed pipe killed the writer with exit 101. Under
`set -o pipefail` that panic became the pipeline's status, and `set -e`
aborted the script.

The raciness is the interesting part: when every node answers in one burst,
`kvctl` finishes writing before `grep` is scheduled and nothing breaks. It
only loses the race when a *later* node is slow to answer — which is exactly
the cluster's state in the seconds after a kill and restart. The bug was
therefore invisible except while measuring failover, and it corrupted the
measurement it was hiding inside of.

**Fix:** `kvctl` now treats `BrokenPipe` on stdout as a normal early exit
(`emit` in `crates/client/src/bin/kvctl.rs`) — the standard CLI contract that
makes `ls | head` work. The same latent bug affected `kvctl scan | head`.
Independently, `kill-leader.sh` captures status into a variable before
matching, so `kvctl` is never in a pipe with an early-exiting reader.

**Why it matters beyond the script:** a CLI that panics when piped into
`head`, `grep -m1`, or a closed terminal is broken for interactive use, not
just for this script. It surfaced only because the failover harness pipes
`kvctl` under `pipefail` — a reminder that *the test harness is production
code for the person running it*.

---

## Bug 8 — A committed entry was lost when a snapshot described a log prefix (deterministic simulation, seed 19519)

**Layer that caught it:** deterministic simulator, the **state-machine
safety** invariant — the strongest assertion in the stack: *the same log
index, once applied, must hold the same entry on every node, forever.*
**Reproduce:** `cargo run --release -p sim --bin simulate -- --start 19519
--seeds 1` (with the fix reverted). Seed 19519 is in
`crates/sim/regressions/seeds.txt` and replays in CI forever.

**Symptom:** the simulator flagged that index 422 was committed by the
leader, applied, and then — after a later election — a *different* entry
occupied index 422 on the node that won. A committed entry had been
overwritten. This is the one thing Raft's Leader Completeness property
exists to make impossible, so the violation meant a real safety bug, not a
liveness or convergence issue.

**Root cause:** an `InstallSnapshot` whose `last_index` the follower
*already had in its log* — i.e. the snapshot covered a **prefix** of the
follower's log, not a suffix beyond it. The follower handled it by calling
`reset_to_snapshot()`, which does `self.entries.clear()` — it wipes the
**entire** log and re-seats it at the snapshot point. That is correct when
the snapshot is *ahead* of everything the follower has, but wrong when the
follower's log extends *past* the snapshot: those trailing entries were
**acked-but-uncommitted**, and the leader was already counting this
follower's ack toward the commit majority. Clearing them silently retracted
an acknowledgement the leader had already believed and acted on.

The failure then unfolds in three moves:

1. Follower appends entries up to index 422, fsyncs, and **acks** them. The
   leader now has a majority for 422 (itself + this follower).
2. Before the leader advances its commit index, the follower falls behind on
   an *earlier* index, the leader compacts, and sends an `InstallSnapshot`
   whose `last_index < 422`. The follower clears its whole log — index 422
   is gone from disk — but the ack for 422 is already counted.
3. The leader commits 422 and applies it. Index 422 now survives on the
   leader only. That leader loses power; an election picks a node without
   422 (permitted — its log is not behind on any *committed* index it knows
   of), and the new leader overwrites 422 with its own entry.

A committed entry ended up surviving on zero nodes. The bug is not in the
election rules or the commit rule; each did exactly what Raft says. It is in
the unstated assumption that receiving a snapshot can only ever *add*
information — when a snapshot describes a prefix, honoring it by truncation
*destroys* durable, acked state.

**Fix:** `handle_install_snapshot` (`crates/raft/src/node.rs`) now detects
the prefix case before touching the log. If the follower already holds an
entry at `snapshot.last_index` whose term matches `snapshot.last_term`, the
snapshot is a prefix of a log the follower can already reconstruct: it keeps
its log intact, advances only its commit index to the snapshot point, and
acks. The whole-log reset now runs only when the snapshot is genuinely ahead
of the follower's log — the case it was written for.

```rust
// §7: a snapshot whose last entry we already hold describes a PREFIX
// of our log. Resetting to it would drop the suffix — entries we have
// already persisted and ACKED, which the leader is counting toward its
// commit majority. Keep the suffix; adopt only the snapshot's commit point.
if self.log.term(snapshot.last_index) == Some(snapshot.last_term) {
    self.advance_commit_to(snapshot.last_index);
    // ...ack and return without clearing the log
}
```

**Why only simulation caught it:** the bug needs five things true at the same
virtual instant — (a) a follower whose log extends *past* a point the leader
is about to snapshot, (b) those trailing entries acked but not yet committed,
(c) the leader counting that ack for its majority, (d) compaction firing so an
`InstallSnapshot` (not an `AppendEntries`) is what reaches the follower, and
(e) an election after the commit but before the follower re-converges. No unit
test constructs that interleaving; no chaos run is likely to hit it twice. On
the buggy code, a sweep of 20,000 seeds reaches the violation on **exactly one
seed — 19519**; every other seed passes. That rarity is the whole point: the
bug is a single needle in a schedule space so large that only randomized
sweeping at scale finds it. After the fix, the same 20,000-seed sweep — 419M
events, 17.3M client operations — is clean. This is the
bug the entire deterministic-simulation apparatus was built to catch: a
genuine consensus safety violation, found by machine, reduced to a
single reproducing integer.

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
| `kvctl` SIGPIPE panic | No — needs a slow peer *and* an early-exiting reader | Failover script on a live cluster |
| Committed-entry loss | No — needs 5 rare conditions at one instant | Simulation, seed 19519, replayable |

Two of the consensus bugs (1, 2) share a shape worth naming in interviews:
**"the mechanism that suppresses sends must always have a timeout-based
escape hatch."** Both were flow-control features working as designed, and
both designs were wrong in the same way — a suppressed channel with no
retry is a permanent wedge the first time the network eats a message.

Bug 8 is the deepest of the set and the one that most justifies the whole
simulator: not a wedge or a stall but a genuine **state-machine safety**
violation — a committed entry overwritten by a later election — caused by a
snapshot handler that assumed snapshots only ever move a follower *forward*.
Bugs 1, 2, and 7 degrade liveness; bug 8 broke correctness, silently, and
only the strongest invariant over the longest schedules could see it.
