# raft-kv

A linearizable, replicated key-value database built from scratch in Rust —
storage engine, consensus, networking, and the test infrastructure to prove
it correct: a FoundationDB-style deterministic simulator that has executed
**11,000+ randomized failure schedules** (≈230M simulated events, ≈9.5M
client operations), every one checked for linearizability.

No database crates, no consensus crates, no RPC frameworks. The dependency
list is: `tokio`, `serde`/`bincode`, `crc32fast`, `tempfile` (tests only).

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

The raft core is **sans-IO**: a pure state machine with no sockets, clocks,
or threads. The same code runs under the production tokio host and under a
single-threaded simulator that owns every scheduling decision — which is
what makes the next section possible.

## Deterministic simulation testing

`crates/sim` runs the whole cluster — nodes, disks, network, clients — on
virtual time, driven by one seeded PRNG. Every run injects: message drops,
duplication, reordering, symmetric and asymmetric partitions, crash/restart
with loss of volatile state, and log-compaction pressure that forces
snapshot transfers. During the run it continuously asserts election safety
(≤1 leader per term), state-machine safety (same entry applied at same
index everywhere), and log matching. At the end it checks full-cluster
convergence and runs a from-scratch **Wing & Gong linearizability checker**
over the complete client history.

A run is a pure function of its seed. A failure prints the seed; the seed
replays the bug exactly, down to the virtual microsecond:

```
cargo run --release -p sim --bin simulate -- --seeds 1000        # sweep
cargo run --release -p sim --bin simulate -- --start 6 --seeds 1 # replay
```

Throughput: ~2,300 seeds/minute on a laptop (each seed ≈21k simulated
events). Every seed that ever exposed a bug lives in
`crates/sim/regressions/seeds.txt` and replays in CI.

**[docs/BUGS.md](docs/BUGS.md)** documents six real bugs this stack caught,
with root causes and reproductions — including a snapshot-transfer wedge
(seed 6) that required four rare conditions to align and would likely have
survived any amount of conventional testing.

## Measured performance

3-node cluster, one machine (localhost TCP, NVMe, Windows 11), 100-byte
values, zipfian keys (θ=0.99), fsync-per-commit on the raft log. Numbers
are medians of the runs recorded in PROGRESS.md; reproduce with
`chaos/local-cluster.sh start` + `loadgen`.

| Workload (YCSB) | Consistency | Throughput | p50 | p99 | p999 |
|---|---|---|---|---|---|
| Load (100% insert, 16 conns) | — | 1,548 ops/s | 9.5 ms | 27.7 ms | 113 ms |
| A (50% read / 50% update, 32 conns) | linearizable reads | 3,031 ops/s | 9.9 ms | 18 ms | 98 ms |
| B (95% read / 5% update, 32 conns) | leader-lease reads | 16,757 ops/s | 0.94 ms (reads) | 3.4 ms | 4.5 ms |
| C (100% read, 32 conns) | stale reads | 52,720 ops/s | 0.55 ms | 1.4 ms | 1.8 ms |

The ~30× spread between workload A and C is the price list of consistency:
a write costs two fsyncs plus a quorum round-trip; a ReadIndex read costs a
quorum round-trip; a lease read costs one RTT to the leader; a stale read
costs one RTT to any replica. Writes went from **455 → 1,548 ops/s (3.4×)**
by adding group commit — the event loop drains all queued proposals before
fsyncing once (`crates/server/src/core.rs`).

### Failover, measured honestly

Under a 32-connection lease-read workload, `kill -9` on the leader:

```
t=4s  20,494 ops/s      ← steady state
t=5s   1,576            ← leader killed
t=6s…t=9s    0          ← election + client re-routing
t=10s  4,690            ← new leader serving
t=13s 23,047 ops/s      ← fully recovered; 0 of 300,000 ops lost or failed
```

Decomposition (20 ms ticks → 200–400 ms election timeout): a new leader is
**elected and serving in < 900 ms**; the client-observed gap (~2.1 s
median over 10 kills) is dominated by leader *discovery* — surviving
followers keep hinting the dead leader until the new leader's first
heartbeat, so clients ping-pong with backoff. Server recovery and client
convergence are different quantities; most systems only quote the first.

## Consistency guarantees, precisely

| Mode | Guarantee | Cost | Anomalies permitted |
|---|---|---|---|
| Writes / CAS | Linearizable | quorum + 2 fsyncs | none |
| `linearizable` read | Linearizable (ReadIndex: heartbeat-quorum confirms leadership, serve at ≥ confirmed commit index) | quorum RTT, no fsync, no log entry | none, under any clock behavior |
| `lease` read | Linearizable **iff clock drift is bounded** (heartbeat-quorum lease) | 1 RTT to leader | stale read if a paused/drifted leader serves after its lease truly expired |
| `stale` read | Committed-but-possibly-old data (never uncommitted) | 1 RTT to any node | arbitrary staleness; no read-your-writes |

Retried writes are safe: every client session carries a sequence number and
the replicated state machine keeps a per-session dedup table (which rides in
snapshots), so a retry after timeout is answered from cache, never
re-executed. This is at-least-once delivery + idempotent apply — the honest
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
docs/BUGS.md      six bugs, root causes, reproducing seeds
GUIDE.md          full write-up: design walkthrough + interview depth
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

- **fsync before ack, always, on the raft log** — losing an acked entry or
  re-casting a vote breaks safety; the WAL batches an entire event-loop
  drain into one fsync (group commit) to make it affordable.
- **State-machine Db never fsyncs** — it is derived state, rebuilt from the
  raft snapshot + log replay at restart. Durability tax paid once, not twice.
- **Message loss over backpressure** — outbound peer queues drop when full;
  raft's retransmission is the flow control. A dead peer must never stall
  the event loop.
- **Length-prefixed bincode instead of gRPC** — keeps the wire format
  from-scratch and the build `cargo build`-only; the RPC semantics are
  shaped so tonic could be swapped in at the proto crate boundary.
- **Single-server membership changes** (not joint consensus) — one
  uncommitted config change at a time, config effective at append; the
  simpler protocol whose edge cases are actually testable. Joint consensus
  is the multi-change generalization.

## What I would build next

Multi-group sharding with a shard-map config service and migration cutover;
transactions (2PC over raft groups, then percolator-style); lease-based
clocks (bound the lease-read anomaly with measured clock error); io_uring
on the storage path; coverage-guided schedule exploration in the simulator.
