//! The Raft node state machine. See lib.rs for the host contract.

use crate::log::RaftLog;
use crate::types::*;
// BTreeMap, not HashMap: iteration order feeds message emission order, and
// deterministic simulation requires identical behavior for identical seeds.
// std HashMap's per-process SipHash keys would break exact replay.
use std::collections::BTreeMap;

/// Leader's view of one follower.
#[derive(Debug, Clone)]
struct Progress {
    /// Next log index to send.
    next: Index,
    /// Highest index known replicated.
    matched: Index,
    /// Optimistically pipelined AppendEntries not yet acked.
    inflight: usize,
    /// A snapshot transfer is outstanding; don't spam more.
    pending_snapshot: bool,
    /// Local tick when the outstanding snapshot was sent, for retry: if the
    /// InstallSnapshot (or its response) is lost, waiting forever would
    /// permanently wedge this follower — heartbeats also stop while
    /// `pending_snapshot` holds. (Found by simulation: seed 6.)
    snapshot_sent_tick: u64,
    /// Highest ReadIndex round this follower has echoed this term.
    acked_read_ctx: u64,
    /// Local tick at which we last heard from this follower (leader lease).
    last_resp_tick: u64,
}

impl Progress {
    fn new(next: Index) -> Self {
        Progress {
            next,
            matched: 0,
            inflight: 0,
            pending_snapshot: false,
            snapshot_sent_tick: 0,
            acked_read_ctx: 0,
            last_resp_tick: 0,
        }
    }
}

pub struct RaftNode {
    cfg: Config,
    log: RaftLog,
    hs: HardState,
    role: Role,
    leader_id: Option<NodeId>,
    commit: Index,
    /// Latest local snapshot — retained so InstallSnapshot can be served.
    snapshot: Option<Snapshot>,
    /// Voter set at the log base (snapshot boundary); conf-change entries in
    /// the log are layered on top (Raft uses the LATEST conf in the log,
    /// committed or not).
    voters_base: Vec<NodeId>,

    election_elapsed: u32,
    randomized_timeout: u32,
    heartbeat_elapsed: u32,
    tick_counter: u64,

    /// Votes received this (pre-)election round.
    votes: BTreeMap<NodeId, bool>,
    prs: BTreeMap<NodeId, Progress>,

    read_ctx_counter: u64,
    /// (ctx, request_id, commit index at request time).
    pending_reads: Vec<(u64, u64, Index)>,

    out: Output,
    rng: u64,
}

impl RaftNode {
    /// `initial_voters` seeds the config for a brand-new cluster; ignored if
    /// `persisted` carries a snapshot (whose voter set wins).
    pub fn new(cfg: Config, initial_voters: Vec<NodeId>, persisted: Persisted) -> Self {
        let (snap_index, snap_term, voters_base, snapshot) = match &persisted.snapshot {
            Some(s) => (s.last_index, s.last_term, s.voters.clone(), persisted.snapshot.clone()),
            None => (0, 0, initial_voters, None),
        };
        let log = RaftLog::new(snap_index, snap_term, persisted.entries);
        let seed = cfg.seed ^ cfg.id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut node = RaftNode {
            cfg,
            log,
            hs: persisted.hard_state,
            role: Role::Follower,
            leader_id: None,
            // Volatile: commit is rediscovered from the leader (or by
            // becoming one); it never regresses past the snapshot base.
            commit: snap_index,
            snapshot,
            voters_base,
            election_elapsed: 0,
            randomized_timeout: 0,
            heartbeat_elapsed: 0,
            tick_counter: 0,
            votes: BTreeMap::new(),
            prs: BTreeMap::new(),
            read_ctx_counter: 0,
            pending_reads: Vec::new(),
            out: Output::default(),
            rng: seed | 1,
        };
        node.reset_election_timer();
        node
    }

    // ------------------------------------------------------------ getters

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.hs.term
    }
    pub fn commit_index(&self) -> Index {
        self.commit
    }
    pub fn leader_hint(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn log(&self) -> &RaftLog {
        &self.log
    }
    pub fn hard_state(&self) -> &HardState {
        &self.hs
    }

    /// Effective voter set: base config + every conf change entry in the log
    /// (uncommitted ones included, per the Raft dissertation §4.1).
    pub fn voters(&self) -> Vec<NodeId> {
        self.voters_at(self.log.last_index())
    }

    fn voters_at(&self, idx: Index) -> Vec<NodeId> {
        let mut v = self.voters_base.clone();
        for e in &self.log.entries {
            if e.index > idx {
                break;
            }
            if let EntryPayload::ConfChange(cc) = &e.payload {
                match cc {
                    ConfChange::AddNode(id) => {
                        if !v.contains(id) {
                            v.push(*id);
                        }
                    }
                    ConfChange::RemoveNode(id) => v.retain(|x| x != id),
                }
            }
        }
        v
    }

    fn quorum(&self) -> usize {
        self.voters().len() / 2 + 1
    }

    /// True while the heartbeat-quorum leader lease holds: a majority
    /// responded within the minimum election timeout, so no other node can
    /// have been elected yet (assuming bounded clock drift — the documented
    /// caveat of lease reads vs ReadIndex).
    pub fn lease_valid(&self) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        let voters = self.voters();
        let mut ticks: Vec<u64> = voters
            .iter()
            .map(|v| {
                if *v == self.cfg.id {
                    self.tick_counter
                } else {
                    self.prs.get(v).map(|p| p.last_resp_tick).unwrap_or(0)
                }
            })
            .collect();
        ticks.sort_unstable_by(|a, b| b.cmp(a));
        let q = self.quorum();
        if ticks.len() < q {
            return false;
        }
        let quorum_tick = ticks[q - 1];
        self.tick_counter.saturating_sub(quorum_tick) < self.cfg.election_tick_min as u64
    }

    pub fn take_output(&mut self) -> Output {
        std::mem::take(&mut self.out)
    }

    // -------------------------------------------------------------- RNG

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn reset_election_timer(&mut self) {
        let span = (self.cfg.election_tick_max - self.cfg.election_tick_min).max(1) as u64;
        self.randomized_timeout =
            self.cfg.election_tick_min + (self.next_rand() % span) as u32;
        self.election_elapsed = 0;
    }

    // ------------------------------------------------------------- ticks

    pub fn tick(&mut self) {
        self.tick_counter += 1;
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.cfg.heartbeat_tick {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append();
                }
            }
            _ => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.randomized_timeout {
                    self.reset_election_timer();
                    // A node outside the voter set never campaigns.
                    if self.voters().contains(&self.cfg.id) {
                        if self.cfg.pre_vote {
                            self.campaign_pre();
                        } else {
                            self.campaign();
                        }
                    }
                }
            }
        }
    }

    // --------------------------------------------------------- elections

    fn campaign_pre(&mut self) {
        self.role = Role::PreCandidate;
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        if self.count_granted() >= self.quorum() {
            self.campaign();
            return;
        }
        let (lli, llt) = (self.log.last_index(), self.log.last_term());
        for peer in self.voters() {
            if peer == self.cfg.id {
                continue;
            }
            self.out.messages.push(Message {
                from: self.cfg.id,
                to: peer,
                term: self.hs.term + 1, // prospective term, NOT persisted
                body: MessageBody::PreVote {
                    last_log_index: lli,
                    last_log_term: llt,
                },
            });
        }
    }

    fn campaign(&mut self) {
        self.set_term(self.hs.term + 1);
        self.set_vote(Some(self.cfg.id));
        self.role = Role::Candidate;
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        if self.count_granted() >= self.quorum() {
            self.become_leader();
            return;
        }
        let (lli, llt) = (self.log.last_index(), self.log.last_term());
        for peer in self.voters() {
            if peer == self.cfg.id {
                continue;
            }
            self.out.messages.push(Message {
                from: self.cfg.id,
                to: peer,
                term: self.hs.term,
                body: MessageBody::RequestVote {
                    last_log_index: lli,
                    last_log_term: llt,
                },
            });
        }
    }

    fn count_granted(&self) -> usize {
        let voters = self.voters();
        self.votes
            .iter()
            .filter(|(id, g)| **g && voters.contains(id))
            .count()
    }

    fn log_up_to_date(&self, last_log_index: Index, last_log_term: Term) -> bool {
        (last_log_term, last_log_index) >= (self.log.last_term(), self.log.last_index())
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.hs.term {
            self.set_term(term);
            self.set_vote(None);
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.prs.clear();
        self.pending_reads.clear();
        self.reset_election_timer();
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.cfg.id);
        self.heartbeat_elapsed = 0;
        self.prs.clear();
        self.pending_reads.clear();
        self.read_ctx_counter = 0;
        let next = self.log.last_index() + 1;
        for peer in self.voters() {
            if peer != self.cfg.id {
                self.prs.insert(peer, Progress::new(next));
            }
        }
        // Commit an entry from our own term ASAP — required both for safety
        // of commit-counting (Figure 8) and to enable ReadIndex.
        self.append_local(EntryPayload::Noop);
        self.broadcast_append();
    }

    // -------------------------------------------------------- proposals

    pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader_id));
        }
        Ok(self.append_local(EntryPayload::Normal(data)))
    }

    pub fn propose_conf_change(&mut self, cc: ConfChange) -> Result<Index, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader_id));
        }
        // Single-server change rule: one at a time. Any conf entry above the
        // commit index is still in flight.
        let in_flight = self
            .log
            .entries
            .iter()
            .any(|e| e.index > self.commit && matches!(e.payload, EntryPayload::ConfChange(_)));
        if in_flight {
            return Err(ProposeError::ConfChangeInFlight);
        }
        let idx = self.append_local(EntryPayload::ConfChange(cc));
        // The new config takes effect the moment it is appended.
        self.sync_progress_with_voters();
        Ok(idx)
    }

    fn sync_progress_with_voters(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let next = self.log.last_index() + 1;
        let voters = self.voters();
        for peer in &voters {
            if *peer != self.cfg.id {
                self.prs.entry(*peer).or_insert_with(|| Progress::new(next));
            }
        }
        self.prs.retain(|id, _| voters.contains(id));
    }

    fn append_local(&mut self, payload: EntryPayload) -> Index {
        let idx = self.log.last_index() + 1;
        let e = Entry {
            term: self.hs.term,
            index: idx,
            payload,
        };
        self.log.push(e.clone());
        self.out.append.push(e);
        self.maybe_advance_commit();
        self.broadcast_append();
        idx
    }

    /// Linearizable read: capture commit index, confirm leadership with a
    /// heartbeat quorum round, then the host serves the read once the state
    /// machine has applied up to that index.
    pub fn read_index(&mut self, request_id: u64) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader_id));
        }
        // The leader's commit index is only known-current once it has
        // committed an entry in its own term.
        if self.log.term(self.commit) != Some(self.hs.term) {
            return Err(ProposeError::NotReady);
        }
        self.read_ctx_counter += 1;
        self.pending_reads
            .push((self.read_ctx_counter, request_id, self.commit));
        if self.quorum() == 1 {
            self.release_reads(self.read_ctx_counter);
        } else {
            self.broadcast_append(); // carry the new ctx now, not next tick
        }
        Ok(())
    }

    fn release_reads(&mut self, up_to_ctx: u64) {
        let mut released: Vec<(u64, Index)> = Vec::new();
        self.pending_reads.retain(|(ctx, id, idx)| {
            if *ctx <= up_to_ctx {
                released.push((*id, *idx));
                false
            } else {
                true
            }
        });
        self.out.read_states.extend(released);
    }

    fn process_read_acks(&mut self) {
        if self.pending_reads.is_empty() {
            return;
        }
        let voters = self.voters();
        let mut acks: Vec<u64> = voters
            .iter()
            .map(|v| {
                if *v == self.cfg.id {
                    self.read_ctx_counter
                } else {
                    self.prs.get(v).map(|p| p.acked_read_ctx).unwrap_or(0)
                }
            })
            .collect();
        acks.sort_unstable_by(|a, b| b.cmp(a));
        let q = self.quorum();
        if acks.len() >= q {
            let quorum_ctx = acks[q - 1];
            if quorum_ctx > 0 {
                self.release_reads(quorum_ctx);
            }
        }
    }

    // ------------------------------------------------------- replication

    fn broadcast_append(&mut self) {
        self.sync_progress_with_voters();
        let peers: Vec<NodeId> = self.prs.keys().copied().collect();
        for peer in peers {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let read_ctx = self.read_ctx_counter;
        let snap_index = self.log.snap_index;
        let tick_now = self.tick_counter;
        let retry_after = self.cfg.election_tick_min as u64;
        let pr = match self.prs.get_mut(&peer) {
            Some(p) => p,
            None => return,
        };
        if pr.pending_snapshot {
            // The transfer (or its ack) may have been lost. Waiting forever
            // would wedge this follower permanently — while pending, we send
            // it nothing, not even heartbeats. Retry after an election
            // timeout's worth of silence. (Bug found by simulation, seed 6:
            // dropped InstallSnapshotResp froze a follower at its snapshot
            // index for the rest of the run.)
            if tick_now.saturating_sub(pr.snapshot_sent_tick) < retry_after {
                return;
            }
            pr.pending_snapshot = false;
        }
        if pr.next <= snap_index {
            // Follower is behind our log base — ship the snapshot.
            if let Some(snap) = self.snapshot.clone() {
                pr.pending_snapshot = true;
                pr.snapshot_sent_tick = tick_now;
                self.out.messages.push(Message {
                    from: self.cfg.id,
                    to: peer,
                    term: self.hs.term,
                    body: MessageBody::InstallSnapshot { snapshot: snap },
                });
            }
            return;
        }
        let prev = pr.next - 1;
        let prev_term = match self.log.term(prev) {
            Some(t) => t,
            None => return, // race after compaction; snapshot path next round
        };
        let mut entries = self.log.slice(pr.next, self.cfg.max_batch_entries);
        if !entries.is_empty() && pr.inflight >= self.cfg.max_inflight {
            // Pipeline window full. We must still heartbeat — a follower that
            // lost our inflight traffic (crash/drops) would otherwise never
            // hear from us again. Decay one window slot so lost acks can't
            // wedge the pipeline permanently; a rejection resets it exactly.
            entries.clear();
            pr.inflight = pr.inflight.saturating_sub(1);
        }
        let n = entries.len() as u64;
        if n > 0 {
            // Optimistic pipelining: assume success, keep streaming. A
            // rejection resets `next` via the conflict hint.
            pr.next += n;
            pr.inflight += 1;
        }
        self.out.messages.push(Message {
            from: self.cfg.id,
            to: peer,
            term: self.hs.term,
            body: MessageBody::AppendEntries {
                prev_log_index: prev,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit,
                read_ctx,
            },
        });
    }

    fn maybe_advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let voters = self.voters();
        let mut matches: Vec<Index> = voters
            .iter()
            .map(|v| {
                if *v == self.cfg.id {
                    self.log.last_index()
                } else {
                    self.prs.get(v).map(|p| p.matched).unwrap_or(0)
                }
            })
            .collect();
        matches.sort_unstable_by(|a, b| b.cmp(a));
        let q = self.quorum();
        if matches.len() < q {
            return;
        }
        let candidate = matches[q - 1];
        // §5.4.2: only count replication for entries of the CURRENT term.
        if candidate > self.commit && self.log.term(candidate) == Some(self.hs.term) {
            self.advance_commit_to(candidate);
        }
    }

    fn advance_commit_to(&mut self, new_commit: Index) {
        debug_assert!(new_commit >= self.commit, "commit index must not regress");
        let old = self.commit;
        self.commit = new_commit;
        let mut stepped_down = false;
        for idx in old + 1..=new_commit {
            if let Some(e) = self.log.get(idx) {
                self.out.committed.push(e.clone());
                if let EntryPayload::ConfChange(ConfChange::RemoveNode(id)) = &e.payload {
                    if *id == self.cfg.id && self.role == Role::Leader {
                        stepped_down = true;
                    }
                }
            }
        }
        if stepped_down {
            // A leader that removed itself steps down once the removal
            // commits (dissertation §4.2.2).
            let term = self.hs.term;
            self.become_follower(term, None);
        }
    }

    // ------------------------------------------------------------- step

    pub fn step(&mut self, msg: Message) {
        match &msg.body {
            MessageBody::PreVote { .. } => return self.handle_pre_vote(msg),
            MessageBody::PreVoteResp { .. } => return self.handle_pre_vote_resp(msg),
            _ => {}
        }

        if msg.term > self.hs.term {
            let leader = match msg.body {
                MessageBody::AppendEntries { .. } | MessageBody::InstallSnapshot { .. } => {
                    Some(msg.from)
                }
                _ => None,
            };
            self.become_follower(msg.term, leader);
        } else if msg.term < self.hs.term {
            // Answer stale senders so they learn the current term.
            match msg.body {
                MessageBody::AppendEntries { read_ctx, .. } => {
                    self.out.messages.push(Message {
                        from: self.cfg.id,
                        to: msg.from,
                        term: self.hs.term,
                        body: MessageBody::AppendEntriesResp {
                            success: false,
                            match_index: 0,
                            conflict_index: 0,
                            read_ctx,
                        },
                    });
                }
                MessageBody::RequestVote { .. } => {
                    self.out.messages.push(Message {
                        from: self.cfg.id,
                        to: msg.from,
                        term: self.hs.term,
                        body: MessageBody::RequestVoteResp { granted: false },
                    });
                }
                _ => {}
            }
            return;
        }

        match msg.body {
            MessageBody::RequestVote {
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(msg.from, last_log_index, last_log_term),
            MessageBody::RequestVoteResp { granted } => {
                self.handle_request_vote_resp(msg.from, granted)
            }
            MessageBody::AppendEntries {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_ctx,
            } => self.handle_append_entries(
                msg.from,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_ctx,
            ),
            MessageBody::AppendEntriesResp {
                success,
                match_index,
                conflict_index,
                read_ctx,
            } => self.handle_append_resp(msg.from, success, match_index, conflict_index, read_ctx),
            MessageBody::InstallSnapshot { snapshot } => {
                self.handle_install_snapshot(msg.from, snapshot)
            }
            MessageBody::InstallSnapshotResp { last_index } => {
                self.handle_install_snapshot_resp(msg.from, last_index)
            }
            MessageBody::PreVote { .. } | MessageBody::PreVoteResp { .. } => unreachable!(),
        }
    }

    fn handle_pre_vote(&mut self, msg: Message) {
        let (last_log_index, last_log_term) = match msg.body {
            MessageBody::PreVote {
                last_log_index,
                last_log_term,
            } => (last_log_index, last_log_term),
            _ => unreachable!(),
        };
        // Grant iff the candidate could actually win: higher prospective
        // term, up-to-date log, AND we haven't heard from a live leader
        // recently. The last condition is what stops a rejoining partitioned
        // node from disrupting a healthy cluster.
        let leader_alive = self.leader_id.is_some()
            && self.election_elapsed < self.cfg.election_tick_min;
        let granted = msg.term > self.hs.term
            && self.log_up_to_date(last_log_index, last_log_term)
            && !leader_alive;
        self.out.messages.push(Message {
            from: self.cfg.id,
            to: msg.from,
            // Echo the candidate's prospective term so it can match the round.
            term: if granted { msg.term } else { self.hs.term },
            body: MessageBody::PreVoteResp { granted },
        });
    }

    fn handle_pre_vote_resp(&mut self, msg: Message) {
        let granted = match msg.body {
            MessageBody::PreVoteResp { granted } => granted,
            _ => unreachable!(),
        };
        if !granted {
            if msg.term > self.hs.term {
                self.become_follower(msg.term, None);
            }
            return;
        }
        if self.role != Role::PreCandidate || msg.term != self.hs.term + 1 {
            return;
        }
        self.votes.insert(msg.from, true);
        if self.count_granted() >= self.quorum() {
            self.campaign();
        }
    }

    fn handle_request_vote(&mut self, from: NodeId, lli: Index, llt: Term) {
        let can_vote = self.hs.voted_for.is_none() || self.hs.voted_for == Some(from);
        let granted = can_vote && self.log_up_to_date(lli, llt);
        if granted {
            self.set_vote(Some(from));
            self.reset_election_timer();
        }
        self.out.messages.push(Message {
            from: self.cfg.id,
            to: from,
            term: self.hs.term,
            body: MessageBody::RequestVoteResp { granted },
        });
    }

    fn handle_request_vote_resp(&mut self, from: NodeId, granted: bool) {
        if self.role != Role::Candidate {
            return;
        }
        self.votes.insert(from, granted);
        if self.count_granted() >= self.quorum() {
            self.become_leader();
        }
    }

    fn handle_append_entries(
        &mut self,
        from: NodeId,
        prev_log_index: Index,
        prev_log_term: Term,
        mut entries: Vec<Entry>,
        leader_commit: Index,
        read_ctx: u64,
    ) {
        // Equal term + AppendEntries ⇒ `from` is the legitimate leader.
        self.role = Role::Follower;
        self.leader_id = Some(from);
        self.election_elapsed = 0;

        let mut prev_idx = prev_log_index;
        let mut prev_term = prev_log_term;

        // Entries at or below our snapshot base are already committed &
        // applied here; skip them.
        if prev_idx < self.log.snap_index {
            let covered = (self.log.snap_index - prev_idx) as usize;
            if covered >= entries.len() {
                self.reply_append(from, true, self.log.snap_index, 0, read_ctx);
                return;
            }
            entries.drain(..covered);
            prev_idx = self.log.snap_index;
            prev_term = self.log.snap_term;
        }

        match self.log.term(prev_idx) {
            None => {
                // Log too short: tell the leader where our log ends.
                let conflict = self.log.last_index() + 1;
                self.reply_append(from, false, 0, conflict, read_ctx);
            }
            Some(t) if t != prev_term => {
                // Conflicting term at prev: hint the first index of that
                // term so the leader skips the whole term.
                let mut conflict = prev_idx;
                while conflict > self.log.first_index()
                    && self.log.term(conflict - 1) == Some(t)
                {
                    conflict -= 1;
                }
                self.reply_append(from, false, 0, conflict, read_ctx);
            }
            Some(_) => {
                // Find the first genuinely new entry; truncate on conflict.
                let mut to_append: Vec<Entry> = Vec::new();
                for (i, e) in entries.iter().enumerate() {
                    match self.log.term(e.index) {
                        Some(t) if t == e.term => continue, // duplicate
                        Some(_) => {
                            // EXECUTABLE INVARIANT: a committed entry can
                            // never conflict (Log Matching + Leader
                            // Completeness). If it does, consensus is broken.
                            assert!(
                                e.index > self.commit,
                                "raft safety violation: leader {from} tried to overwrite \
                                 committed entry {} on node {}",
                                e.index,
                                self.cfg.id
                            );
                            self.log.truncate_from(e.index);
                            to_append = entries[i..].to_vec();
                            break;
                        }
                        None => {
                            to_append = entries[i..].to_vec();
                            break;
                        }
                    }
                }
                for e in &to_append {
                    self.log.push(e.clone());
                }
                self.out.append.extend(to_append);

                let new_last = prev_idx + entries.len() as u64;
                // Commit only what we've VERIFIED matches the leader.
                let commit_target = leader_commit.min(new_last);
                if commit_target > self.commit {
                    self.advance_commit_to(commit_target);
                }
                self.reply_append(from, true, new_last, 0, read_ctx);
            }
        }
    }

    fn reply_append(
        &mut self,
        to: NodeId,
        success: bool,
        match_index: Index,
        conflict_index: Index,
        read_ctx: u64,
    ) {
        self.out.messages.push(Message {
            from: self.cfg.id,
            to,
            term: self.hs.term,
            body: MessageBody::AppendEntriesResp {
                success,
                match_index,
                conflict_index,
                read_ctx,
            },
        });
    }

    fn handle_append_resp(
        &mut self,
        from: NodeId,
        success: bool,
        match_index: Index,
        conflict_index: Index,
        read_ctx: u64,
    ) {
        if self.role != Role::Leader {
            return;
        }
        let now = self.tick_counter;
        let last_index = self.log.last_index();
        let pr = match self.prs.get_mut(&from) {
            Some(p) => p,
            None => return,
        };
        pr.last_resp_tick = now;
        pr.acked_read_ctx = pr.acked_read_ctx.max(read_ctx);
        if success {
            if match_index > pr.matched {
                pr.matched = match_index;
            }
            pr.next = pr.next.max(match_index + 1);
            pr.inflight = pr.inflight.saturating_sub(1);
            self.maybe_advance_commit();
            self.process_read_acks();
            // Keep the pipeline full if the follower is still behind.
            if self.prs.get(&from).map(|p| p.next <= last_index).unwrap_or(false) {
                self.send_append(from);
            }
        } else {
            // Rejected: fall back. conflict_index == 0 means "stale term
            // probe" — already handled by the generic term bump, or ignore.
            if conflict_index > 0 {
                pr.next = conflict_index.clamp(1, last_index + 1);
                pr.inflight = 0;
                self.send_append(from);
            }
        }
    }

    fn handle_install_snapshot(&mut self, from: NodeId, snapshot: Snapshot) {
        self.role = Role::Follower;
        self.leader_id = Some(from);
        self.election_elapsed = 0;

        if snapshot.last_index <= self.commit {
            // Stale snapshot; just tell the leader where we are.
            self.out.messages.push(Message {
                from: self.cfg.id,
                to: from,
                term: self.hs.term,
                body: MessageBody::InstallSnapshotResp {
                    last_index: self.commit,
                },
            });
            return;
        }
        // §7: a snapshot whose last entry we already hold describes a PREFIX
        // of our log. Resetting to it would drop the suffix — entries we have
        // already persisted and ACKED, which the leader is counting toward its
        // commit majority. It would then commit an entry that survives on no
        // one, and a later election can legitimately overwrite it. Keep the
        // suffix; adopt only the snapshot's commit point.
        // (Found by simulation: seed 19519.)
        if self.log.term(snapshot.last_index) == Some(snapshot.last_term) {
            self.advance_commit_to(snapshot.last_index);
            self.out.messages.push(Message {
                from: self.cfg.id,
                to: from,
                term: self.hs.term,
                body: MessageBody::InstallSnapshotResp {
                    // Only claim what the snapshot proves we match; our
                    // suffix above it is not known to agree with the leader.
                    last_index: snapshot.last_index,
                },
            });
            return;
        }
        self.log
            .reset_to_snapshot(snapshot.last_index, snapshot.last_term);
        self.commit = snapshot.last_index;
        self.voters_base = snapshot.voters.clone();
        self.snapshot = Some(snapshot.clone());
        let last_index = snapshot.last_index;
        self.out.snapshot = Some(snapshot);
        self.out.messages.push(Message {
            from: self.cfg.id,
            to: from,
            term: self.hs.term,
            body: MessageBody::InstallSnapshotResp { last_index },
        });
    }

    fn handle_install_snapshot_resp(&mut self, from: NodeId, last_index: Index) {
        if self.role != Role::Leader {
            return;
        }
        let now = self.tick_counter;
        if let Some(pr) = self.prs.get_mut(&from) {
            pr.pending_snapshot = false;
            pr.last_resp_tick = now;
            if last_index > pr.matched {
                pr.matched = last_index;
            }
            pr.next = pr.next.max(last_index + 1);
            pr.inflight = 0;
            self.maybe_advance_commit();
            self.send_append(from);
        }
    }

    // --------------------------------------------------------- snapshots

    /// Local log compaction: the host has serialized its state machine at
    /// `applied` and hands it to us. Returns the snapshot the host must
    /// persist. Entries <= applied are dropped from the in-memory log.
    pub fn compact(&mut self, applied: Index, sm_data: Vec<u8>) -> Option<Snapshot> {
        if applied <= self.log.snap_index || applied > self.commit {
            return None;
        }
        let term = self.log.term(applied)?;
        let voters = self.voters_at(applied);
        // Fold conf changes up to `applied` into the base before dropping
        // the entries that encode them.
        self.voters_base = voters.clone();
        self.log.compact(applied, term);
        let snap = Snapshot {
            last_index: applied,
            last_term: term,
            voters,
            data: sm_data,
        };
        self.snapshot = Some(snap.clone());
        Some(snap)
    }

    // -------------------------------------------------------- hard state

    fn set_term(&mut self, term: Term) {
        if self.hs.term != term {
            self.hs.term = term;
            self.out.hard_state = Some(self.hs.clone());
        }
    }

    fn set_vote(&mut self, v: Option<NodeId>) {
        if self.hs.voted_for != v {
            self.hs.voted_for = v;
            self.out.hard_state = Some(self.hs.clone());
        }
    }
}
