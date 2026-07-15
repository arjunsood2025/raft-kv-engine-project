//! The deterministic cluster simulator.
//!
//! One `run()` executes an entire multi-node Raft cluster — nodes, disks,
//! network, clients, and faults — inside a single thread on virtual time.
//! Every scheduling decision, message delay, fault, and client choice is
//! drawn from one seeded PRNG in a fixed order, so a given seed always
//! produces the exact same run. A failure prints its seed; re-running that
//! seed replays the bug instruction-for-instruction.
//!
//! Determinism rules enforced throughout:
//!   - all iteration is over `BTreeMap`/`BTreeSet`/`Vec` (std `HashMap`
//!     iteration order is randomized per process and would break replay);
//!   - the event queue breaks virtual-time ties with a monotone sequence
//!     number, so equal-time events pop in insertion order;
//!   - the raft cores' internal randomized timeouts are seeded from the run
//!     seed.
//!
//! What is checked, continuously and at the end of every run:
//!   - **Election safety**: at most one leader per term.
//!   - **State-machine safety**: every node applies the same entry at the
//!     same index (compared against a global applied-entry table).
//!   - **Log matching**: entries with the same index and term have the same
//!     payload across all live nodes (sampled every few thousand events).
//!   - **Durability contract**: hard state / entries / snapshots are written
//!     to the simulated disk before any message from that output batch is
//!     sent (structurally enforced by `process_output`).
//!   - **Convergence**: after faults stop and the network quiesces, all
//!     nodes hold identical state machines at identical applied indexes.
//!   - **Linearizability**: the full client history passes the WGL checker.

use crate::history::{decode_val, encode_val, HOp, HRecord, HRes};
use crate::rng::Rng;
use crate::wgl;
use kvsm::{Command, MemBackend, Op, OpResult, StateMachine};
use raft::{
    Config, Entry, EntryPayload, Index, Message, NodeId, Persisted, ProposeError, RaftNode,
    Term,
};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, BTreeMap, BTreeSet};

// ---------------------------------------------------------------- config

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: u64,
    pub clients: u64,
    pub keys: u8,
    /// Virtual milliseconds of total run (chaos + quiet tail).
    pub run_ms: u64,
    /// Final portion of the run with faults disabled so the cluster heals.
    pub quiet_ms: u64,
    pub msg_drop: f64,
    pub msg_dup: f64,
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
    /// Per fault-tick probability of starting a partition / crashing a node.
    pub partition_prob: f64,
    pub crash_prob: f64,
    /// Compact the raft log after this many applied entries.
    pub snapshot_every: u64,
}

impl SimConfig {
    /// Derive a full fault profile from a seed, so a seed sweep explores
    /// calm networks, lossy networks, partition-heavy runs, crash-heavy
    /// runs, and combinations — while staying a pure function of the seed.
    pub fn from_seed(seed: u64) -> SimConfig {
        let mut r = Rng::new(seed ^ 0xC0FF_EE00_D15E_A5E5);
        SimConfig {
            seed,
            nodes: *r.pick(&[3, 3, 3, 5]),
            clients: 3,
            keys: 4,
            run_ms: 20_000,
            quiet_ms: 8_000,
            msg_drop: *r.pick(&[0.0, 0.02, 0.10, 0.20]),
            msg_dup: *r.pick(&[0.0, 0.05]),
            min_delay_ms: 1,
            max_delay_ms: r.range(3, 25),
            partition_prob: *r.pick(&[0.0, 0.15, 0.30]),
            crash_prob: *r.pick(&[0.0, 0.10, 0.20]),
            snapshot_every: 60,
        }
    }
}

// ---------------------------------------------------------------- events

#[derive(Debug)]
enum Event {
    NodeTick(NodeId),
    RaftMsg(Message),
    ClientReq { client: usize, node: NodeId, req: CReq },
    ClientResp { client: usize, resp: CResp },
    ClientWake(usize),
    ClientTimeout { client: usize, invoke: u64 },
    FaultTick,
    HealPartition,
    Restart(NodeId),
}

#[derive(Debug, Clone)]
enum CReq {
    Write { seq: u64, key: u8, val: u64 },
    Cas { seq: u64, key: u8, expect: Option<u64>, new: u64 },
    Read { op_ts: u64, key: u8 },
}

#[derive(Debug, Clone)]
enum CResp {
    /// NotLeader or leader-not-ready; op_ref = seq (writes) / op_ts (reads).
    Retry { op_ref: u64, hint: Option<NodeId> },
    WriteDone { seq: u64, result: OpResult },
    ReadDone { op_ts: u64, value: Option<u64> },
}

struct QueuedEvent {
    time: u64,
    seq: u64,
    ev: Event,
}
impl PartialEq for QueuedEvent {
    fn eq(&self, o: &Self) -> bool {
        self.time == o.time && self.seq == o.seq
    }
}
impl Eq for QueuedEvent {}
impl PartialOrd for QueuedEvent {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for QueuedEvent {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.time, self.seq).cmp(&(o.time, o.seq))
    }
}

// ---------------------------------------------------------------- actors

struct SimNode {
    id: NodeId,
    up: bool,
    raft: Option<RaftNode>,
    sm: StateMachine<MemBackend>,
    applied: Index,
    applied_since_snap: u64,
    next_read_id: u64,
    /// read_id -> (client, op_ts, key); volatile, lost on crash.
    pending_reads: BTreeMap<u64, (usize, u64, u8)>,
    /// ReadIndex rounds that completed but wait for applied >= idx.
    ready_reads: Vec<(u64, Index)>,
    /// (session, seq) proposals this node owes a response for; volatile.
    waiting_writes: BTreeSet<(u64, u64)>,
    /// The simulated durable disk. Survives crashes; this is ALL that does.
    disk: Persisted,
    restarts: u64,
}

#[derive(Clone, Debug)]
enum OpKind {
    Read { key: u8 },
    Write { key: u8, val: u64, seq: u64 },
    Cas { key: u8, expect: Option<u64>, new: u64, seq: u64 },
}

struct CurOp {
    hist_idx: usize,
    invoke: u64,
    kind: OpKind,
}

struct SimClient {
    /// Session id (also the client's identity in the history).
    id: u64,
    next_seq: u64,
    val_counter: u64,
    leader_hint: Option<NodeId>,
    /// Client's last observed value per key, used to build CAS expectations.
    last_seen: BTreeMap<u8, Option<u64>>,
    cur: Option<CurOp>,
}

// ---------------------------------------------------------------- results

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Stats {
    pub events: u64,
    pub msgs_sent: u64,
    pub msgs_dropped: u64,
    pub msgs_duplicated: u64,
    pub partitions: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub ops_completed: u64,
    pub ops_pending: u64,
    pub max_term: Term,
    pub final_applied: Index,
    pub snapshots_taken: u64,
}

pub struct RunReport {
    pub stats: Stats,
    pub history: Vec<HRecord>,
    /// Converged state machine bytes (identical on every node by assertion).
    pub final_state: Vec<u8>,
}

// ---------------------------------------------------------------- sim

struct Sim {
    cfg: SimConfig,
    rng: Rng,
    now: u64,
    event_seq: u64,
    heap: BinaryHeap<Reverse<QueuedEvent>>,
    nodes: Vec<SimNode>,
    clients: Vec<SimClient>,
    /// Directed blocked links (from, to) — asymmetric partitions supported.
    blocked: BTreeSet<(NodeId, NodeId)>,
    history: Vec<HRecord>,
    /// Global monotone stamp for history invoke/return points.
    stamp: u64,
    /// term -> leader observed at that term (election safety).
    leaders_by_term: BTreeMap<Term, NodeId>,
    /// index -> entry first applied there by any node (state-machine safety).
    applied_global: BTreeMap<Index, Entry>,
    stats: Stats,
    fault_end: u64,
    stop_new_ops: u64,
    voters: Vec<NodeId>,
}

pub fn run(cfg: &SimConfig) -> RunReport {
    let mut sim = Sim::new(cfg.clone());
    sim.boot();
    sim.event_loop();
    sim.finalize()
}

impl Sim {
    fn new(cfg: SimConfig) -> Sim {
        let voters: Vec<NodeId> = (1..=cfg.nodes).collect();
        let fault_end = cfg.run_ms - cfg.quiet_ms;
        let stop_new_ops = cfg.run_ms - 3_000;
        Sim {
            rng: Rng::new(cfg.seed),
            now: 0,
            event_seq: 0,
            heap: BinaryHeap::new(),
            nodes: Vec::new(),
            clients: Vec::new(),
            blocked: BTreeSet::new(),
            history: Vec::new(),
            stamp: 0,
            leaders_by_term: BTreeMap::new(),
            applied_global: BTreeMap::new(),
            stats: Stats::default(),
            fault_end,
            stop_new_ops,
            voters,
            cfg,
        }
    }

    fn stamp(&mut self) -> u64 {
        self.stamp += 1;
        self.stamp
    }

    fn push(&mut self, time: u64, ev: Event) {
        self.event_seq += 1;
        self.heap.push(Reverse(QueuedEvent { time, seq: self.event_seq, ev }));
    }

    fn node_cfg(&self, id: NodeId, restarts: u64) -> Config {
        // Election timeout 10-20 ticks at ~10ms/tick ≈ 100-200ms; heartbeat
        // every ~20ms. Message delays up to cfg.max_delay_ms stress this.
        Config::new(
            id,
            self.cfg
                .seed
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(id * 1_000_003 + restarts * 7),
        )
    }

    fn boot(&mut self) {
        for id in 1..=self.cfg.nodes {
            let cfg = self.node_cfg(id, 0);
            let raft = RaftNode::new(cfg, self.voters.clone(), Persisted::default());
            self.nodes.push(SimNode {
                id,
                up: true,
                raft: Some(raft),
                sm: StateMachine::new(MemBackend::default()),
                applied: 0,
                applied_since_snap: 0,
                next_read_id: 1,
                pending_reads: BTreeMap::new(),
                ready_reads: Vec::new(),
                waiting_writes: BTreeSet::new(),
                disk: Persisted::default(),
                restarts: 0,
            });
            let first_tick = self.rng.range(1, 12);
            self.push(first_tick, Event::NodeTick(id));
        }
        for c in 0..self.cfg.clients {
            self.clients.push(SimClient {
                id: 100 + c + 1, // session ids; 0 is reserved for "no session"
                next_seq: 0,
                val_counter: 0,
                leader_hint: None,
                last_seen: BTreeMap::new(),
                cur: None,
            });
            let wake = self.rng.range(50, 300); // let the first election settle
            self.push(wake, Event::ClientWake(c as usize));
        }
        let ft = self.rng.range(300, 800);
        self.push(ft, Event::FaultTick);
    }

    // ------------------------------------------------------------ loop

    fn event_loop(&mut self) {
        // Phase 1 (0..run_ms): full activity. Phase 2 (run_ms..run_ms+drain):
        // ticks and message delivery only — no clients, no faults — so every
        // in-flight commit reaches every node before convergence is checked.
        let drain_end = self.cfg.run_ms + 2_000;
        while let Some(Reverse(qe)) = self.heap.pop() {
            if qe.time > drain_end {
                break;
            }
            self.now = qe.time;
            self.stats.events += 1;
            assert!(
                self.stats.events < 3_000_000,
                "event budget exceeded (livelock?) seed={}",
                self.cfg.seed
            );
            let draining = qe.time > self.cfg.run_ms;
            match qe.ev {
                Event::NodeTick(id) => self.on_node_tick(id),
                Event::RaftMsg(m) => self.on_raft_msg(m),
                Event::Restart(id) => self.on_restart(id),
                Event::HealPartition => self.blocked.clear(),
                Event::FaultTick if !draining => self.on_fault_tick(),
                Event::ClientReq { client, node, req } if !draining => {
                    self.on_client_req(client, node, req)
                }
                Event::ClientResp { client, resp } if !draining => {
                    self.on_client_resp(client, resp)
                }
                Event::ClientWake(c) if !draining => self.on_client_wake(c),
                Event::ClientTimeout { client, invoke } if !draining => {
                    self.on_client_timeout(client, invoke)
                }
                _ => {} // client/fault events ignored during drain
            }
            if self.stats.events % 2_000 == 0 {
                self.check_log_matching();
            }
        }
    }

    fn on_node_tick(&mut self, id: NodeId) {
        let i = (id - 1) as usize;
        if self.nodes[i].up {
            self.nodes[i].raft.as_mut().unwrap().tick();
            self.process_output(i);
        }
        let next = self.now + self.rng.range(8, 12); // per-node clock skew
        self.push(next, Event::NodeTick(id));
    }

    fn on_raft_msg(&mut self, m: Message) {
        // Partitions and crashes are checked at delivery time.
        if self.blocked.contains(&(m.from, m.to)) {
            self.stats.msgs_dropped += 1;
            return;
        }
        let i = (m.to - 1) as usize;
        if !self.nodes[i].up {
            self.stats.msgs_dropped += 1;
            return;
        }
        self.nodes[i].raft.as_mut().unwrap().step(m);
        self.process_output(i);
    }

    // ------------------------------------------------------------ output

    /// Drain a node's raft output honoring the host contract IN ORDER:
    /// (1) persist hard state, appended entries, and snapshots to the
    /// simulated disk; (2) apply committed entries; (3) serve ready reads;
    /// (4) only then hand messages to the (faulty) network. A crash between
    /// any of these steps loses exactly what a real crash would lose.
    fn process_output(&mut self, i: usize) {
        let out = self.nodes[i].raft.as_mut().unwrap().take_output();

        // (1) Durable state first.
        {
            let n = &mut self.nodes[i];
            if let Some(hs) = &out.hard_state {
                n.disk.hard_state = hs.clone();
            }
            if let Some(snap) = &out.snapshot {
                // InstallSnapshot from the leader: replaces log and state.
                n.disk.snapshot = Some(snap.clone());
                n.disk.entries.clear();
                n.sm.restore(&snap.data);
                n.applied = snap.last_index;
                n.applied_since_snap = 0;
            }
            for e in &out.append {
                // Truncate-from-then-append semantics.
                if let Some(pos) = n.disk.entries.iter().position(|x| x.index >= e.index) {
                    n.disk.entries.truncate(pos);
                }
                n.disk.entries.push(e.clone());
            }
        }

        // (2) Apply committed entries; answer waiting client writes.
        let mut responses: Vec<(usize, CResp)> = Vec::new();
        for e in &out.committed {
            // State-machine safety: every node must apply the same entry at
            // the same index, across crashes, elections, and truncations.
            match self.applied_global.get(&e.index) {
                Some(prev) => assert_eq!(
                    prev, e,
                    "STATE MACHINE SAFETY VIOLATION at index {} seed={}",
                    e.index, self.cfg.seed
                ),
                None => {
                    self.applied_global.insert(e.index, e.clone());
                }
            }
            let n = &mut self.nodes[i];
            if let EntryPayload::Normal(data) = &e.payload {
                let cmd = Command::decode(data).expect("committed command decodes");
                let result = n.sm.apply(&cmd);
                if cmd.session_id != 0 && n.waiting_writes.remove(&(cmd.session_id, cmd.seq)) {
                    let client = (cmd.session_id - 101) as usize;
                    responses.push((client, CResp::WriteDone { seq: cmd.seq, result }));
                }
            }
            n.applied = e.index;
            n.applied_since_snap += 1;
        }

        // Log compaction: fold the applied prefix into a snapshot.
        if self.nodes[i].applied_since_snap >= self.cfg.snapshot_every {
            let n = &mut self.nodes[i];
            let data = n.sm.snapshot();
            if let Some(snap) = n.raft.as_mut().unwrap().compact(n.applied, data) {
                n.disk.snapshot = Some(snap.clone());
                n.disk.entries.retain(|e| e.index > snap.last_index);
                self.stats.snapshots_taken += 1;
            }
            n.applied_since_snap = 0;
        }

        // (3) ReadIndex rounds that completed; serve once applied >= idx.
        {
            let n = &mut self.nodes[i];
            for (rid, idx) in &out.read_states {
                n.ready_reads.push((*rid, *idx));
            }
            let applied = n.applied;
            let mut still_waiting = Vec::new();
            for (rid, idx) in n.ready_reads.drain(..) {
                if idx <= applied {
                    if let Some((client, op_ts, key)) = n.pending_reads.remove(&rid) {
                        let value = n.sm.read(&[key]).map(|b| decode_val(&b));
                        responses.push((client, CResp::ReadDone { op_ts, value }));
                    }
                } else {
                    still_waiting.push((rid, idx));
                }
            }
            n.ready_reads = still_waiting;
        }

        for (client, resp) in responses {
            self.send_client_resp(client, resp);
        }

        // (4) Messages last — durable state above is already "on disk".
        for m in out.messages {
            self.send_raft_msg(m);
        }

        // Election safety: at most one leader per term, ever.
        let raft = self.nodes[i].raft.as_ref().unwrap();
        self.stats.max_term = self.stats.max_term.max(raft.term());
        if raft.is_leader() {
            let (t, id) = (raft.term(), raft.id());
            match self.leaders_by_term.get(&t) {
                Some(&other) => assert_eq!(
                    other, id,
                    "ELECTION SAFETY VIOLATION: two leaders in term {} seed={}",
                    t, self.cfg.seed
                ),
                None => {
                    self.leaders_by_term.insert(t, id);
                }
            }
        }
    }

    fn send_raft_msg(&mut self, m: Message) {
        self.stats.msgs_sent += 1;
        if self.rng.chance(self.cfg.msg_drop) {
            self.stats.msgs_dropped += 1;
            return;
        }
        let copies = if self.rng.chance(self.cfg.msg_dup) {
            self.stats.msgs_duplicated += 1;
            2
        } else {
            1
        };
        for _ in 0..copies {
            let t = self.now + self.rng.range(self.cfg.min_delay_ms, self.cfg.max_delay_ms);
            self.push(t, Event::RaftMsg(m.clone()));
        }
    }

    fn send_client_resp(&mut self, client: usize, resp: CResp) {
        if self.rng.chance(self.cfg.msg_drop / 2.0) {
            return; // client will time out and retry
        }
        let t = self.now + self.rng.range(1, 5);
        self.push(t, Event::ClientResp { client, resp });
    }

    // ------------------------------------------------------------ clients

    fn on_client_wake(&mut self, c: usize) {
        if self.clients[c].cur.is_some() || self.now >= self.stop_new_ops {
            return;
        }
        let key = self.rng.below(self.cfg.keys as u64) as u8;
        let roll = self.rng.below(10);
        let client = &mut self.clients[c];
        let kind = if roll < 4 {
            OpKind::Read { key }
        } else if roll < 8 {
            client.next_seq += 1;
            client.val_counter += 1;
            OpKind::Write {
                key,
                val: (client.id << 40) | client.val_counter,
                seq: client.next_seq,
            }
        } else {
            client.next_seq += 1;
            client.val_counter += 1;
            let expect = client.last_seen.get(&key).copied().unwrap_or(None);
            OpKind::Cas {
                key,
                expect,
                new: (client.id << 40) | client.val_counter,
                seq: client.next_seq,
            }
        };
        let invoke = self.stamp();
        let client = &self.clients[c];
        let op = match &kind {
            OpKind::Read { key } => HOp::Read { key: *key },
            OpKind::Write { key, val, .. } => HOp::Write { key: *key, val: *val },
            OpKind::Cas { key, expect, new, .. } => {
                HOp::Cas { key: *key, expect: *expect, new: *new }
            }
        };
        self.history.push(HRecord {
            client: client.id,
            invoke,
            ret: None,
            op,
            result: None,
        });
        let hist_idx = self.history.len() - 1;
        self.clients[c].cur = Some(CurOp { hist_idx, invoke, kind });
        self.send_request(c, None);
    }

    /// Send (or resend) the client's current op to `target` (None = leader
    /// hint if known, else random). Schedules a retry timeout.
    fn send_request(&mut self, c: usize, target: Option<NodeId>) {
        let cur = match &self.clients[c].cur {
            Some(cur) => cur,
            None => return,
        };
        let invoke = cur.invoke;
        let req = match &cur.kind {
            OpKind::Read { key } => CReq::Read { op_ts: invoke, key: *key },
            OpKind::Write { key, val, seq } => CReq::Write { seq: *seq, key: *key, val: *val },
            OpKind::Cas { key, expect, new, seq } => {
                CReq::Cas { seq: *seq, key: *key, expect: *expect, new: *new }
            }
        };
        let node = target
            .or(self.clients[c].leader_hint)
            .unwrap_or_else(|| self.rng.range(1, self.cfg.nodes));
        if !self.rng.chance(self.cfg.msg_drop / 2.0) {
            let t = self.now + self.rng.range(1, 5);
            self.push(t, Event::ClientReq { client: c, node, req });
        }
        let timeout = self.now + self.rng.range(150, 300);
        self.push(timeout, Event::ClientTimeout { client: c, invoke });
    }

    fn on_client_timeout(&mut self, c: usize, invoke: u64) {
        let waiting = matches!(&self.clients[c].cur, Some(cur) if cur.invoke == invoke);
        if !waiting {
            return;
        }
        // Whoever we asked isn't answering — forget the hint, try anyone.
        self.clients[c].leader_hint = None;
        let node = self.rng.range(1, self.cfg.nodes);
        self.send_request(c, Some(node));
    }

    fn on_client_req(&mut self, c: usize, node: NodeId, req: CReq) {
        let i = (node - 1) as usize;
        if !self.nodes[i].up {
            return; // client times out
        }
        let session = self.clients[c].id;
        let resp = match req {
            CReq::Write { seq, key, val } => {
                let cmd = Command {
                    session_id: session,
                    seq,
                    op: Op::Put { key: vec![key], value: encode_val(val) },
                };
                self.try_propose(i, c, session, seq, cmd)
            }
            CReq::Cas { seq, key, expect, new } => {
                let cmd = Command {
                    session_id: session,
                    seq,
                    op: Op::Cas {
                        key: vec![key],
                        expect: expect.map(encode_val),
                        new: Some(encode_val(new)),
                    },
                };
                self.try_propose(i, c, session, seq, cmd)
            }
            CReq::Read { op_ts, key } => {
                let n = &mut self.nodes[i];
                let rid = n.next_read_id;
                n.next_read_id += 1;
                match n.raft.as_mut().unwrap().read_index(rid) {
                    Ok(()) => {
                        n.pending_reads.insert(rid, (c, op_ts, key));
                        self.process_output(i);
                        None
                    }
                    Err(ProposeError::NotLeader(hint)) => {
                        Some(CResp::Retry { op_ref: op_ts, hint })
                    }
                    Err(_) => Some(CResp::Retry { op_ref: op_ts, hint: Some(node) }),
                }
            }
        };
        if let Some(resp) = resp {
            self.send_client_resp(c, resp);
        }
    }

    fn try_propose(
        &mut self,
        i: usize,
        _c: usize,
        session: u64,
        seq: u64,
        cmd: Command,
    ) -> Option<CResp> {
        let n = &mut self.nodes[i];
        match n.raft.as_mut().unwrap().propose(cmd.encode()) {
            Ok(_) => {
                n.waiting_writes.insert((session, seq));
                self.process_output(i);
                None
            }
            Err(ProposeError::NotLeader(hint)) => Some(CResp::Retry { op_ref: seq, hint }),
            Err(_) => Some(CResp::Retry { op_ref: seq, hint: Some(n.id) }),
        }
    }

    fn on_client_resp(&mut self, c: usize, resp: CResp) {
        let cur = match &self.clients[c].cur {
            Some(cur) => cur,
            None => return, // late response for an op we already finished
        };
        match resp {
            CResp::Retry { op_ref, hint } => {
                let matches_cur = match &cur.kind {
                    OpKind::Read { .. } => op_ref == cur.invoke,
                    OpKind::Write { seq, .. } | OpKind::Cas { seq, .. } => op_ref == *seq,
                };
                if matches_cur {
                    self.clients[c].leader_hint = hint;
                    // Damped retry: during elections there is no leader and
                    // instant retries would spin.
                    let node = hint.unwrap_or_else(|| self.rng.range(1, self.cfg.nodes));
                    let t = self.now + self.rng.range(10, 40);
                    let cur = self.clients[c].cur.as_ref().unwrap();
                    let req = match &cur.kind {
                        OpKind::Read { key } => CReq::Read { op_ts: cur.invoke, key: *key },
                        OpKind::Write { key, val, seq } => {
                            CReq::Write { seq: *seq, key: *key, val: *val }
                        }
                        OpKind::Cas { key, expect, new, seq } => {
                            CReq::Cas { seq: *seq, key: *key, expect: *expect, new: *new }
                        }
                    };
                    self.push(t, Event::ClientReq { client: c, node, req });
                }
            }
            CResp::WriteDone { seq, result } => {
                let (accept, key_update) = match &cur.kind {
                    OpKind::Write { key, val, seq: s } if *s == seq => {
                        (Some(HRes::WriteOk), Some((*key, Some(*val))))
                    }
                    OpKind::Cas { key, new, seq: s, .. } if *s == seq => match &result {
                        OpResult::Cas { success, actual } => (
                            Some(HRes::CasOk { success: *success }),
                            Some((
                                *key,
                                if *success {
                                    Some(*new)
                                } else {
                                    actual.as_ref().map(|b| decode_val(b))
                                },
                            )),
                        ),
                        _ => (None, None), // Stale/other: ignore, keep waiting
                    },
                    _ => (None, None),
                };
                if let Some(res) = accept {
                    self.complete_op(c, res);
                    if let Some((key, v)) = key_update {
                        self.clients[c].last_seen.insert(key, v);
                    }
                }
            }
            CResp::ReadDone { op_ts, value } => {
                if let OpKind::Read { key } = cur.kind {
                    if op_ts == cur.invoke {
                        self.complete_op(c, HRes::ReadOk(value));
                        self.clients[c].last_seen.insert(key, value);
                    }
                }
            }
        }
    }

    fn complete_op(&mut self, c: usize, res: HRes) {
        let ret = self.stamp();
        let cur = self.clients[c].cur.take().unwrap();
        self.history[cur.hist_idx].ret = Some(ret);
        self.history[cur.hist_idx].result = Some(res);
        self.stats.ops_completed += 1;
        let wake = self.now + self.rng.range(5, 30);
        self.push(wake, Event::ClientWake(c));
    }

    // ------------------------------------------------------------ faults

    fn on_fault_tick(&mut self) {
        if self.now >= self.fault_end {
            // Chaos window over: heal everything so the cluster can converge.
            self.blocked.clear();
            for id in 1..=self.cfg.nodes {
                if !self.nodes[(id - 1) as usize].up {
                    let t = self.now + self.rng.range(10, 50);
                    self.push(t, Event::Restart(id));
                }
            }
            return; // no more fault ticks
        }
        if self.rng.chance(self.cfg.partition_prob) {
            self.start_partition();
        }
        if self.rng.chance(self.cfg.crash_prob) {
            let id = self.rng.range(1, self.cfg.nodes);
            self.crash(id);
        }
        let t = self.now + self.rng.range(300, 800);
        self.push(t, Event::FaultTick);
    }

    fn start_partition(&mut self) {
        self.stats.partitions += 1;
        // Random split: shuffle ids, cut at a random point.
        let mut ids: Vec<NodeId> = (1..=self.cfg.nodes).collect();
        for i in (1..ids.len()).rev() {
            let j = self.rng.below((i + 1) as u64) as usize;
            ids.swap(i, j);
        }
        let cut = self.rng.range(1, self.cfg.nodes - 1) as usize;
        let (a, b) = ids.split_at(cut);
        let asymmetric = self.rng.chance(0.3);
        for &x in a {
            for &y in b {
                self.blocked.insert((x, y));
                if !asymmetric {
                    self.blocked.insert((y, x));
                }
            }
        }
        let heal = self.now + self.rng.range(500, 3_000);
        self.push(heal, Event::HealPartition);
    }

    fn crash(&mut self, id: NodeId) {
        let i = (id - 1) as usize;
        if !self.nodes[i].up {
            return;
        }
        self.stats.crashes += 1;
        let n = &mut self.nodes[i];
        // Everything volatile dies: raft in-memory state, the state machine,
        // pending reads, response bookkeeping. Only `disk` survives.
        n.up = false;
        n.raft = None;
        n.sm = StateMachine::new(MemBackend::default());
        n.applied = 0;
        n.applied_since_snap = 0;
        n.pending_reads.clear();
        n.ready_reads.clear();
        n.waiting_writes.clear();
        let t = self.now + self.rng.range(300, 4_000);
        self.push(t, Event::Restart(id));
    }

    fn on_restart(&mut self, id: NodeId) {
        let i = (id - 1) as usize;
        if self.nodes[i].up {
            return;
        }
        self.stats.restarts += 1;
        let restarts = self.nodes[i].restarts + 1;
        let cfg = self.node_cfg(id, restarts);
        let disk = self.nodes[i].disk.clone();
        let raft = RaftNode::new(cfg, self.voters.clone(), disk);
        let n = &mut self.nodes[i];
        n.restarts = restarts;
        n.raft = Some(raft);
        // Recover the state machine exactly as the real server does: restore
        // the last durable snapshot, then let raft re-deliver committed
        // entries above it (deterministic re-apply; sessions dedup retries).
        if let Some(snap) = &n.disk.snapshot {
            n.sm.restore(&snap.data);
            n.applied = snap.last_index;
        }
        n.up = true;
        self.process_output(i);
    }

    // ------------------------------------------------------------ checks

    /// Log matching property: if two logs contain an entry with the same
    /// index and term, the entries are identical.
    fn check_log_matching(&self) {
        let mut seen: BTreeMap<(Index, Term), &Entry> = BTreeMap::new();
        for n in &self.nodes {
            let raft = match &n.raft {
                Some(r) if n.up => r,
                _ => continue,
            };
            let log = raft.log();
            for idx in log.first_index()..=log.last_index() {
                if let Some(e) = log.get(idx) {
                    match seen.get(&(idx, e.term)) {
                        Some(prev) => assert_eq!(
                            *prev, e,
                            "LOG MATCHING VIOLATION at index {} term {} seed={}",
                            idx, e.term, self.cfg.seed
                        ),
                        None => {
                            seen.insert((idx, e.term), e);
                        }
                    }
                }
            }
        }
    }

    fn finalize(mut self) -> RunReport {
        self.check_log_matching();

        // Convergence: every node is up, applied the same prefix, and holds
        // byte-identical state (kv contents AND session dedup tables).
        for n in &self.nodes {
            assert!(n.up, "node {} still down after heal seed={}", n.id, self.cfg.seed);
        }
        let max_applied = self.nodes.iter().map(|n| n.applied).max().unwrap();
        for n in &self.nodes {
            assert_eq!(
                n.applied, max_applied,
                "node {} failed to converge (applied {} vs {}) seed={}",
                n.id, n.applied, max_applied, self.cfg.seed
            );
        }
        let reference = self.nodes[0].sm.snapshot();
        for n in &self.nodes[1..] {
            assert_eq!(
                n.sm.snapshot(),
                reference,
                "node {} diverged from node 1 at applied={} seed={}",
                n.id,
                max_applied,
                self.cfg.seed
            );
        }
        assert!(
            self.nodes.iter().any(|n| n.raft.as_ref().unwrap().is_leader()),
            "no leader after quiet period seed={}",
            self.cfg.seed
        );

        // The centerpiece: is the entire client history linearizable?
        if let Err(e) = wgl::check_history(&self.history) {
            panic!(
                "LINEARIZABILITY VIOLATION seed={}: {}",
                self.cfg.seed, e
            );
        }

        self.stats.ops_pending =
            self.history.iter().filter(|r| r.ret.is_none()).count() as u64;
        self.stats.final_applied = max_applied;
        RunReport {
            stats: self.stats,
            history: self.history,
            final_state: reference,
        }
    }
}
