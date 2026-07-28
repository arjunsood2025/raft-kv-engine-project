# raft-kv

A linearizable, replicated key-value database built from scratch in Rust,
covering the storage engine, consensus, networking, and the test infrastructure
needed to prove it correct: a FoundationDB-style deterministic simulator that has executed
**20,000 randomized failure schedules** (≈420M simulated events, ≈17.3M
client operations), every one checked for linearizability. Exactly one of
those 20,000 schedules (seed 19519) exposed a real committed-entry-loss bug
([docs/BUGS.md](docs/BUGS.md)); the rest run clean.

No database crates, no consensus crates, no RPC frameworks. The dependency
list is: `tokio`, `serde`/`bincode`, `crc32fast`, `tempfile` (tests only).

![Layered architecture of one node: clients talk to the tokio server over
length-prefixed bincode on TCP; below the server sit the kvsm replicated state
machine, the sans-IO raft consensus core, and the LSM storage engine. Deployed
as three Raft-replicated peers.](docs/img/architecture.svg)

<details>
<summary>Same architecture as plain text</summary>

```
┌─────────────────────────────────────────────────────────────────┐
│  clients (kvctl CLI · loadgen · library)                        │
│  leader routing · retries w/ jitter · sessions (dedup-safe)     │
└──────────────────────────────┬──────────────────────────────────┘
                               │ length-prefixed bincode / TCP
┌──────────────────────────────┴──────────────────────────────────┐
│  server: tokio host — single-owner event loop, group commit     │
│  consistency modes: ReadIndex · leader lease · stale            │
├─────────────────────────────────────────────────────────────────┤
│  kvsm: replicated state machine — client sessions, idempotent   │
│  apply (at-least-once delivery + dedup = exactly-once effect)   │
├─────────────────────────────────────────────────────────────────┤
│  raft (sans-IO): pre-vote elections · pipelined replication ·   │
│  snapshots + InstallSnapshot · membership changes · ReadIndex   │
├─────────────────────────────────────────────────────────────────┤
│  storage: LSM engine — WAL (CRC, torn-write recovery) ·         │
│  memtable · SSTables (bloom filters, block checksums) ·         │
│  leveled compaction · MVCC snapshots                            │
└─────────────────────────────────────────────────────────────────┘
        ▲ production: tokio + real disks   ▲ tests: deterministic
        │ (crates/server)                  │ simulator (crates/sim)
```

</details>

The raft core is **sans-IO**: a pure state machine with no sockets, clocks,
or threads. The same code runs under the production tokio host and under a
single-threaded simulator that owns every scheduling decision, which is
what makes the next section possible.

## Deterministic simulation testing

`crates/sim` runs the whole cluster (nodes, disks, network, and clients) on
virtual time, driven by one seeded PRNG. Every run injects: message drops,
duplication, reordering, symmetric and asymmetric partitions, crash/restart
with loss of volatile state, and log-compaction pressure that forces
snapshot transfers. During the run it continuously asserts election safety
(≤1 leader per term), state-machine safety (same entry applied at same
index everywhere), and log matching. At the end it checks full-cluster
convergence and runs a from-scratch **Wing & Gong linearizability checker**
over the complete client history.

![The simulator loop: a single seed drives a PRNG that feeds a virtual-time
event queue; each popped event steps the sans-IO raft core, whose outputs
(persist-before-send) pass through a simulated network and disks that inject
faults and enqueue new events. Election safety, state-machine safety, and log
matching are asserted every step; the Wing & Gong linearizability check runs at
the end.](docs/img/sim-loop.svg)

A run is a pure function of its seed. A failure prints the seed; the seed
replays the bug exactly, down to the virtual microsecond:

```
cargo run --release -p sim --bin simulate -- --seeds 1000        # sweep
cargo run --release -p sim --bin simulate -- --start 6 --seeds 1 # replay
```

Throughput: ~5,300 seeds/minute on the bench above (each seed ≈21k
simulated events). Every seed that ever exposed a bug lives in
`crates/sim/regressions/seeds.txt` and replays in CI.

**[docs/BUGS.md](docs/BUGS.md)** documents eight real bugs this stack caught,
with root causes and reproductions, including a **state-machine safety
violation** (seed 19519): an `InstallSnapshot` describing a log *prefix*
truncated a follower's acked entries, and a committed entry was later
overwritten by a legitimate election. It needed five rare conditions to
align at one instant and would survive any amount of conventional testing.

## Measured performance

3-node cluster on one machine: AMD Ryzen 5 2600 (6C/12T), 48 GB, Samsung
970 EVO Plus NVMe, Windows 11. Localhost TCP, 100-byte values, zipfian keys
(θ=0.99), 20 ms ticks, fsync-per-commit on the raft log. All three nodes
*and* the load generator share those six cores, so these are conservative.
Each read/mixed workload below is a **median of 3 measured runs**, each
preceded by a discarded warm-up pass; the load row is a median of the
from-scratch 100k loads run across the suite (a cold insert has no warm-up to
discard). The data directory is wiped and the cluster restarted between runs.
A cold store measures SSTable traversal, a warm one the page cache, and an
un-wiped store accumulated compaction debt, so the protocol controls all
three. Run-to-run spread was a few percent on most workloads (tightest on the
100%-read rows, ≤2%); workload B was the noisiest at ~15%. Reproduce with
`chaos/local-cluster.sh start 20` + `loadgen` (exact commands under *Quick
start*).

| Workload (YCSB) | Consistency | Throughput | p50 | p99 | p999 |
|---|---|---|---|---|---|
| Load (100% insert, 16 conns) | — | 1,318 ops/s | 10.7 ms | 18.6 ms | 345 ms |
| A (50% read / 50% update, 32 conns) | linearizable reads | 2,308 ops/s | 11.0 ms (reads) | 20.6 ms | 756 ms |
| B (95% read / 5% update, 32 conns) | leader-lease reads | 16,187 ops/s | 0.69 ms (reads) | 2.73 ms | 4.0 ms |
| C (100% read, 32 conns) | stale reads | 13,317 ops/s | 2.35 ms | 3.20 ms | 4.36 ms |

That table does **not** price consistency, and it is worth saying so: every
row changes the workload *and* the consistency mode together. B's reads
(p50 0.69 ms) beat C's (p50 2.35 ms) despite B being the stricter mode.
B's 5% updates keep the zipfian-hot keys resident in the memtable, so its
reads are served from memory while C's walk the SSTables. That is a workload
effect wearing a consistency costume, and it is the trap in every YCSB table
laid out this way.

The comparison that *does* price consistency holds the workload fixed
(100% read, 60k ops, 32 conns) and varies only `--consistency`:

| Consistency | Throughput | p50 | What it costs |
|---|---|---|---|
| stale | 13,517 ops/s | 2.34 ms | 1 RTT |
| lease | 13,057 ops/s | 2.42 ms | 1 RTT + a local lease check |
| linearizable | 9,043 ops/s | 3.49 ms | quorum RTT |

**Linearizable reads cost ~1.5×**, which is the ReadIndex quorum round-trip,
and it is the real price of strict reads. **Stale and lease are
indistinguishable** (~3% apart, inside run-to-run variance); that is a
consequence of the client, not a fluke, as explained below. Writes to stale reads
span ~10× (1,318 → 13,517 ops/s), and that gap is fsync plus replication.
Measuring it honestly was the point.

![The price of linearizable reads: with the workload held fixed at 100% read,
stale and lease reads land at ~13,000–13,500 ops/s while linearizable reads
cost ~1.5× more at 9,043 ops/s.](docs/img/consistency-price.png)

Group commit, in which the event loop drains every queued proposal before
fsyncing once (`crates/server/src/core.rs`), is what makes the write path
affordable. A grouped-vs-ungrouped speedup ratio is **not** quoted here: it
would need a second build with group commit disabled, which I have not run,
so there is no honest number to report for it.

### Stale reads do not spread load

`KvClient` starts at `addrs[0]` and only moves when a node answers
`NotLeader`. A stale read never triggers that answer, because every replica will
serve one, so **every client pins to node 1 for its entire life** and stale
reads never fan out across the cluster. Measured: workload C against all
three nodes gives 13,317 ops/s; against node 1 alone, 13,268 ops/s, the same
number. It was already only using node 1.

So "1 RTT to any node" in the table below describes the protocol, not this
client's behaviour, and it is why stale and lease reads cost the same here:
they traverse an identical path, and the lease check is local. Fixing it
(round-robin the stale-read target across replicas) is the first item under
*What I would build next*.

### Failover, measured honestly

Under a 32-connection lease-read workload, `kill -9` on the leader:

![Failover under load: throughput holds at ~16k ops/s, drops to zero for about
two seconds after the leader is killed at t≈8.5s while the cluster elects a new
leader and clients re-route, then fully recovers to ~18k ops/s by t=12s, with 0 of
300,000 operations lost.](docs/img/failover.png)

Client-observed failover over 10 consecutive leader kills
(`chaos/kill-leader.sh 10 20`): **min 2,103 ms / median 2,141 ms / max
2,204 ms**. Decomposition (20 ms ticks → 200–400 ms election timeout): the
election itself is sub-second; the ~2.1 s a client actually waits is
dominated by leader *discovery*: surviving followers keep hinting the dead
leader until the new leader's first heartbeat, so clients ping-pong with
backoff. Server recovery and client convergence are different quantities;
most systems only quote the first.

Full throughput recovery takes ~3.5 s after the kill, with ~2 s at zero.
Writes in flight during the handover are delayed, not dropped: **0 of 300,000
ops failed**, and the update tail stayed tight (p999 = 99 ms). The handful of
writes caught mid-election simply waited out the ~2 s outage.

Both measurements above use the chaos harness's client-retry settings (short
backoff, up to 60 attempts: `RAFTKV_BACKOFF_*` / `RAFTKV_MAX_ATTEMPTS` in
`chaos/kill-leader.sh`), so the client rides out the election rather than
giving up. "Failover" here therefore measures the cluster's recovery time, not
client give-up behaviour; with default retries an op could error instead of
waiting.

## Consistency guarantees, precisely

| Mode | Guarantee | Cost | Anomalies permitted |
|---|---|---|---|
| Writes / CAS | Linearizable | quorum + 2 fsyncs | none |
| `linearizable` read | Linearizable (ReadIndex: heartbeat-quorum confirms leadership, serve at ≥ confirmed commit index) | quorum RTT, no fsync, no log entry | none, under any clock behavior |
| `lease` read | Linearizable **iff clock drift is bounded** (heartbeat-quorum lease) | 1 RTT to leader | stale read if a paused/drifted leader serves after its lease truly expired |
| `stale` read | Committed-but-possibly-old data (never uncommitted) | 1 RTT to any node (but see *Stale reads do not spread load*; this client always asks node 1) | arbitrary staleness; no read-your-writes |

Retried writes are safe: every client session carries a sequence number and
the replicated state machine keeps a per-session dedup table (which rides in
snapshots), so a retry after timeout is answered from cache, never
re-executed. This is at-least-once delivery plus idempotent apply, the honest
construction of "exactly-once" (true exactly-once *delivery* does not exist
in an asynchronous network).

## Layout

```
crates/storage    LSM engine: wal, memtable, sstable (+bloom), manifest,
                  merge iterators, leveled compaction, MVCC snapshots
crates/raft       sans-IO consensus core + scenario test battery
crates/kvsm       replicated KV state machine w/ sessions
crates/proto      wire types + length-prefixed bincode framing
crates/server     tokio host: kvd binary, raft-log persistence on the LSM
                  engine, group commit, consistency modes, Prometheus metrics
crates/client     smart client library + kvctl CLI
crates/bench      YCSB-style workloads (zipfian, A–F) + loadgen binary
crates/sim        deterministic simulator, WGL checker, seed regressions
chaos/            local-cluster/kill-leader scripts, docker-compose + netem
docs/BUGS.md      eight bugs, root causes, reproducing seeds
```

## Quick start

```bash
cargo test --workspace                 # 47 tests incl. 30-seed sim sweep
cargo build --release

chaos/local-cluster.sh start           # 3 nodes on localhost
target/release/kvctl --cluster 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003 \
    put hello world
target/release/kvctl --cluster ... get hello --consistency lease
chaos/kill-leader.sh 5                 # failover distribution, 5 kills
```

## Design tradeoffs (the short list)

- **fsync before ack, always, on the raft log.** Losing an acked entry or
  re-casting a vote breaks safety; the WAL batches an entire event-loop
  drain into one fsync (group commit) to make it affordable.
- **State-machine Db never fsyncs.** It is derived state, rebuilt from the
  raft snapshot + log replay at restart. Durability tax paid once, not twice.
- **Message loss over backpressure.** Outbound peer queues drop when full;
  raft's retransmission is the flow control. A dead peer must never stall
  the event loop.
- **Length-prefixed bincode instead of gRPC.** This keeps the wire format
  from-scratch and the build `cargo build`-only; the RPC semantics are
  shaped so tonic could be swapped in at the proto crate boundary.
- **Single-server membership changes** (not joint consensus): one
  uncommitted config change at a time, config effective at append; the
  simpler protocol whose edge cases are actually testable. Joint consensus
  is the multi-change generalization.

## What I would build next

Spread stale reads across replicas (today every client pins to `addrs[0]`,
so the one mode that *could* scale horizontally doesn't; the fix is a
per-client starting offset plus rotation on the stale path); multi-group
sharding with a shard-map config service and migration cutover; transactions
(2PC over raft groups, then percolator-style); lease-based clocks (bound the
lease-read anomaly with measured clock error); io_uring on the storage path;
coverage-guided schedule exploration in the simulator.
