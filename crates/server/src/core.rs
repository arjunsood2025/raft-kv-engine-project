//! The single-owner event loop.
//!
//! One task owns the `RaftNode`, the `LogStore`, and the `StateMachine`.
//! Everything else (peer sockets, client connections, the tick timer) talks
//! to it over an mpsc channel. This buys the same property the simulator
//! verifies: consensus state is mutated sequentially, so there are no data
//! races to reason about — the concurrency lives entirely at the network
//! edge.
//!
//! Output-handling order (the host contract, identical to `sim::cluster`):
//! 1. fsync hard state / appended entries / installed snapshot (LogStore);
//! 2. apply committed entries, answering waiting client writes;
//! 3. complete ReadIndex reads whose index has been applied;
//! 4. only then hand messages to the network.
//!
//! Note the fsync happens inline on this task. That is a deliberate
//! simplification: it serializes commit latency behind disk latency, which
//! is the honest cost of durability; a production system overlaps fsyncs
//! with message processing via a dedicated persistence thread (etcd) or
//! group commit. The batch already amortizes: one Output can carry many
//! entries and they hit the WAL as one fsynced write.

use crate::backend::DbBackend;
use crate::logstore::LogStore;
use crate::metrics::Metrics;
use kvsm::{Command, Op, OpResult, StateMachine};
use proto::{Consistency, Request, Response};
use raft::{Config, EntryPayload, Index, Message, NodeId, Persisted, ProposeError, RaftNode};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub enum Event {
    Tick,
    Peer(Message),
    Client(Request, oneshot::Sender<Response>),
}

/// How long a pending write/read may wait for commit/apply before the
/// client is told to retry, in ticks (100 ms each by default).
const PENDING_TIMEOUT_TICKS: u64 = 50;

struct PendingWrite {
    tx: oneshot::Sender<Response>,
    deadline: u64,
}

struct PendingRead {
    req: ReadReq,
    tx: oneshot::Sender<Response>,
    deadline: u64,
}

enum ReadReq {
    Get(Vec<u8>),
    Scan {
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: usize,
    },
}

pub struct Core {
    raft: RaftNode,
    log: LogStore,
    sm: StateMachine<DbBackend>,
    applied: Index,
    applied_since_snap: u64,
    snapshot_every: u64,
    peer_tx: HashMap<NodeId, mpsc::Sender<Message>>,
    pending_writes: HashMap<(u64, u64), PendingWrite>,
    pending_reads: HashMap<u64, PendingRead>,
    /// ReadIndex rounds that completed but whose index isn't applied yet.
    ready_reads: Vec<(u64, Index)>,
    next_read_id: u64,
    tick: u64,
    metrics: Arc<Metrics>,
}

impl Core {
    /// Open the two Dbs under `data_dir` and reconstruct node state:
    /// raft state comes straight off the log store; the state machine is
    /// rebuilt from the latest raft snapshot (sessions ride inside it) and
    /// the log suffix re-applies as it re-commits.
    pub fn open(
        data_dir: &Path,
        cfg: Config,
        initial_voters: Vec<NodeId>,
        snapshot_every: u64,
        peer_tx: HashMap<NodeId, mpsc::Sender<Message>>,
        metrics: Arc<Metrics>,
    ) -> storage::Result<Core> {
        let (log, persisted): (LogStore, Persisted) = LogStore::open(&data_dir.join("raft"))?;
        let mut sm = StateMachine::new(DbBackend::open(&data_dir.join("sm"))?);
        let mut applied = 0;
        match &persisted.snapshot {
            Some(snap) => {
                sm.restore(&snap.data);
                applied = snap.last_index;
            }
            None => {
                // Ensure a clean slate even if a previous run left SM data
                // (the SM Db is unsynced derived state; see backend.rs).
                sm.restore(&StateMachine::new(kvsm::MemBackend::default()).snapshot());
            }
        }
        let raft = RaftNode::new(cfg, initial_voters, persisted);
        Ok(Core {
            raft,
            log,
            sm,
            applied,
            applied_since_snap: 0,
            snapshot_every,
            peer_tx,
            pending_writes: HashMap::new(),
            pending_reads: HashMap::new(),
            ready_reads: Vec::new(),
            next_read_id: 1,
            tick: 0,
            metrics,
        })
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<Event>) {
        while let Some(ev) = rx.recv().await {
            self.handle_event(ev);
            // Group commit: drain everything already queued before touching
            // the disk, so all their raft output shares one fsynced batch.
            // Measured on the load benchmark (16 clients, localhost): one
            // fsync per proposal = 455 ops/sec; grouped = see PROGRESS.md.
            // The budget bounds worst-case latency for the first request in
            // the batch.
            let mut budget = 512;
            while budget > 0 {
                match rx.try_recv() {
                    Ok(ev) => {
                        self.handle_event(ev);
                        budget -= 1;
                    }
                    Err(_) => break,
                }
            }
            self.process_output().await;
        }
    }

    fn handle_event(&mut self, ev: Event) {
        match ev {
            Event::Tick => {
                self.tick += 1;
                self.raft.tick();
                self.sweep_timeouts();
                self.stamp_gauges();
            }
            Event::Peer(m) => self.raft.step(m),
            Event::Client(req, tx) => self.handle_client(req, tx),
        }
    }

    // ------------------------------------------------------------- clients

    fn handle_client(&mut self, req: Request, tx: oneshot::Sender<Response>) {
        match req {
            Request::Put {
                session_id,
                seq,
                key,
                value,
            } => self.propose_write(session_id, seq, Op::Put { key, value }, tx),
            Request::Delete {
                session_id,
                seq,
                key,
            } => self.propose_write(session_id, seq, Op::Delete { key }, tx),
            Request::Cas {
                session_id,
                seq,
                key,
                expect,
                new,
            } => self.propose_write(session_id, seq, Op::Cas { key, expect, new }, tx),
            Request::Get { key, consistency } => {
                self.handle_read(ReadReq::Get(key), consistency, tx)
            }
            Request::Scan {
                start,
                end,
                limit,
                consistency,
            } => self.handle_read(
                ReadReq::Scan {
                    start,
                    end,
                    limit: limit as usize,
                },
                consistency,
                tx,
            ),
            Request::Status => {
                let _ = tx.send(Response::Status {
                    id: self.raft.id(),
                    role: format!("{:?}", self.raft.role()),
                    term: self.raft.term(),
                    leader: self.raft.leader_hint(),
                    commit: self.raft.commit_index(),
                    applied: self.applied,
                    last_log_index: self.raft.log().last_index(),
                    voters: self.raft.voters(),
                });
            }
        }
    }

    fn propose_write(
        &mut self,
        session_id: u64,
        seq: u64,
        op: Op,
        tx: oneshot::Sender<Response>,
    ) {
        if session_id == 0 {
            let _ = tx.send(Response::Err("session_id must be nonzero".into()));
            return;
        }
        let cmd = Command {
            session_id,
            seq,
            op,
        };
        match self.raft.propose(cmd.encode()) {
            Ok(_) => {
                Metrics::inc(&self.metrics.proposals_total);
                self.pending_writes.insert(
                    (session_id, seq),
                    PendingWrite {
                        tx,
                        deadline: self.tick + PENDING_TIMEOUT_TICKS,
                    },
                );
            }
            Err(e) => {
                let _ = tx.send(self.propose_err(e));
            }
        }
    }

    fn handle_read(&mut self, req: ReadReq, consistency: Consistency, tx: oneshot::Sender<Response>) {
        match consistency {
            Consistency::Stale => {
                Metrics::inc(&self.metrics.reads_stale_total);
                let _ = tx.send(self.serve_read(&req));
            }
            Consistency::LeaderLease => {
                if !self.raft.is_leader() {
                    Metrics::inc(&self.metrics.not_leader_total);
                    let _ = tx.send(Response::NotLeader {
                        hint: self.raft.leader_hint(),
                    });
                } else if self.raft.lease_valid() {
                    Metrics::inc(&self.metrics.reads_lease_total);
                    let _ = tx.send(self.serve_read(&req));
                } else {
                    Metrics::inc(&self.metrics.retries_total);
                    let _ = tx.send(Response::Retry {
                        reason: "leader lease not established".into(),
                    });
                }
            }
            Consistency::Linearizable => {
                let rid = self.next_read_id;
                self.next_read_id += 1;
                match self.raft.read_index(rid) {
                    Ok(()) => {
                        Metrics::inc(&self.metrics.reads_linearizable_total);
                        self.pending_reads.insert(
                            rid,
                            PendingRead {
                                req,
                                tx,
                                deadline: self.tick + PENDING_TIMEOUT_TICKS,
                            },
                        );
                    }
                    Err(e) => {
                        let _ = tx.send(self.propose_err(e));
                    }
                }
            }
        }
    }

    fn serve_read(&self, req: &ReadReq) -> Response {
        match req {
            ReadReq::Get(key) => Response::Value(self.sm.read(key)),
            ReadReq::Scan { start, end, limit } => {
                Response::Kvs(self.sm.read_range(start, end.as_deref(), *limit))
            }
        }
    }

    fn propose_err(&mut self, e: ProposeError) -> Response {
        match e {
            ProposeError::NotLeader(hint) => {
                Metrics::inc(&self.metrics.not_leader_total);
                Response::NotLeader { hint }
            }
            ProposeError::NotReady => {
                Metrics::inc(&self.metrics.retries_total);
                Response::Retry {
                    reason: "leader not ready (no commit in current term yet)".into(),
                }
            }
            ProposeError::ConfChangeInFlight => Response::Err("conf change in flight".into()),
        }
    }

    fn sweep_timeouts(&mut self) {
        let now = self.tick;
        let metrics = &self.metrics;
        self.pending_writes.retain(|_, w| {
            if w.deadline <= now {
                Metrics::inc(&metrics.retries_total);
                // Taking tx by value needs removal; retain gives &mut, so
                // signal by closing: dropping the sender wakes the waiter
                // with an error the connection task maps to Retry.
                false
            } else {
                true
            }
        });
        self.pending_reads.retain(|_, r| r.deadline > now);
        self.ready_reads.retain(|(rid, _)| {
            // Drop ready reads whose waiter timed out.
            self.pending_reads.contains_key(rid)
        });
    }

    fn stamp_gauges(&self) {
        Metrics::set(&self.metrics.term, self.raft.term());
        Metrics::set(&self.metrics.commit_index, self.raft.commit_index());
        Metrics::set(&self.metrics.applied_index, self.applied);
        Metrics::set(&self.metrics.is_leader, self.raft.is_leader() as u64);
    }

    // -------------------------------------------------------------- output

    async fn process_output(&mut self) {
        loop {
            let out = self.raft.take_output();
            if out.is_empty() {
                return;
            }

            // (1) Durability, before anything leaves this node.
            if out.hard_state.is_some() || !out.append.is_empty() || out.snapshot.is_some() {
                self.log
                    .persist(out.hard_state.as_ref(), &out.append, out.snapshot.as_ref())
                    .expect("raft log persistence failed — cannot continue safely");
            }
            if let Some(snap) = &out.snapshot {
                // Leader-installed snapshot: reset the state machine.
                self.sm.restore(&snap.data);
                self.applied = snap.last_index;
                self.applied_since_snap = 0;
            }

            // (2) Apply committed entries; answer waiting writes.
            for e in &out.committed {
                if let EntryPayload::Normal(data) = &e.payload {
                    let cmd = Command::decode(data).expect("committed command decodes");
                    let result = self.sm.apply(&cmd);
                    Metrics::inc(&self.metrics.applied_total);
                    if let Some(w) = self.pending_writes.remove(&(cmd.session_id, cmd.seq)) {
                        let _ = w.tx.send(op_result_response(result));
                    }
                }
                self.applied = e.index;
                self.applied_since_snap += 1;
            }

            // Log compaction once enough entries have applied.
            if self.applied_since_snap >= self.snapshot_every {
                let data = self.sm.snapshot();
                if let Some(snap) = self.raft.compact(self.applied, data) {
                    self.log.compact_to(&snap).expect("compaction persistence");
                    Metrics::inc(&self.metrics.snapshots_taken_total);
                }
                self.applied_since_snap = 0;
            }

            // (3) ReadIndex rounds that completed; serve once applied.
            self.ready_reads.extend(out.read_states.iter().copied());
            let applied = self.applied;
            let mut still_waiting = Vec::new();
            for (rid, idx) in std::mem::take(&mut self.ready_reads) {
                if idx <= applied {
                    if let Some(r) = self.pending_reads.remove(&rid) {
                        let _ = r.tx.send(self.serve_read(&r.req));
                    }
                } else {
                    still_waiting.push((rid, idx));
                }
            }
            self.ready_reads = still_waiting;

            // (4) Messages last. try_send: a slow/dead peer drops messages
            // instead of backpressuring consensus — raft is built for loss.
            for m in out.messages {
                if let Some(tx) = self.peer_tx.get(&m.to) {
                    let _ = tx.try_send(m);
                }
            }
        }
    }
}

fn op_result_response(r: OpResult) -> Response {
    match r {
        OpResult::Ok => Response::Ok,
        OpResult::Value(v) => Response::Value(v),
        OpResult::Cas { success, actual } => Response::Cas { success, actual },
        OpResult::Stale => Response::Err("stale sequence number".into()),
    }
}
