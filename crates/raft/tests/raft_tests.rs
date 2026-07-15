//! Raft behavior tests on a tiny synchronous in-memory cluster.
//! The heavy randomized fault testing lives in the `sim` crate; these tests
//! stage specific scenarios (partitions, divergence, snapshots, membership).

use raft::*;
use std::collections::{HashMap, HashSet, VecDeque};

struct Harness {
    nodes: HashMap<NodeId, RaftNode>,
    persisted: HashMap<NodeId, Persisted>,
    /// Global applied-command record for state-machine safety: index →
    /// (term, payload). Every node applying that index must agree.
    applied_global: HashMap<Index, Entry>,
    applied_count: HashMap<NodeId, u64>,
    read_states: HashMap<NodeId, Vec<(u64, Index)>>,
    queue: VecDeque<Message>,
    blocked: HashSet<(NodeId, NodeId)>,
    initial_voters: Vec<NodeId>,
}

impl Harness {
    fn new(n: u64) -> Self {
        let voters: Vec<NodeId> = (1..=n).collect();
        let mut h = Harness {
            nodes: HashMap::new(),
            persisted: HashMap::new(),
            applied_global: HashMap::new(),
            applied_count: HashMap::new(),
            read_states: HashMap::new(),
            queue: VecDeque::new(),
            blocked: HashSet::new(),
            initial_voters: voters.clone(),
        };
        for id in 1..=n {
            h.nodes.insert(
                id,
                RaftNode::new(Config::new(id, 42), voters.clone(), Persisted::default()),
            );
            h.persisted.insert(id, Persisted::default());
        }
        h
    }

    fn add_fresh_node(&mut self, id: NodeId, bootstrap_voters: Vec<NodeId>) {
        self.nodes.insert(
            id,
            RaftNode::new(Config::new(id, 42), bootstrap_voters, Persisted::default()),
        );
        self.persisted.insert(id, Persisted::default());
    }

    fn drain(&mut self, id: NodeId) {
        let out = match self.nodes.get_mut(&id) {
            Some(n) => n.take_output(),
            None => return,
        };
        // Host contract: persist BEFORE sending.
        let p = self.persisted.get_mut(&id).unwrap();
        if let Some(hs) = &out.hard_state {
            p.hard_state = hs.clone();
        }
        if let Some(s) = &out.snapshot {
            p.entries.retain(|e| e.index > s.last_index);
            p.snapshot = Some(s.clone());
        }
        for e in &out.append {
            if let Some(pos) = p.entries.iter().position(|x| x.index >= e.index) {
                p.entries.truncate(pos);
            }
            p.entries.push(e.clone());
        }
        for e in &out.committed {
            // State machine safety: all nodes apply the same entry at the
            // same index.
            let prev = self.applied_global.insert(e.index, e.clone());
            if let Some(prev) = prev {
                assert_eq!(
                    prev, *e,
                    "state machine safety violated at index {}",
                    e.index
                );
            }
            *self.applied_count.entry(id).or_insert(0) += 1;
        }
        self.read_states
            .entry(id)
            .or_default()
            .extend(out.read_states.iter().copied());
        for m in out.messages {
            self.queue.push_back(m);
        }
    }

    fn process(&mut self) {
        let mut guard = 0;
        while let Some(m) = self.queue.pop_front() {
            guard += 1;
            assert!(guard < 100_000, "message storm — probable livelock");
            if self.blocked.contains(&(m.from, m.to)) || !self.nodes.contains_key(&m.to) {
                continue;
            }
            let to = m.to;
            self.nodes.get_mut(&to).unwrap().step(m);
            self.drain(to);
        }
    }

    fn tick_all(&mut self) {
        let mut ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            self.nodes.get_mut(&id).unwrap().tick();
            self.drain(id);
        }
        self.process();
        self.check_election_safety();
    }

    fn check_election_safety(&self) {
        // At most one leader per term.
        let mut by_term: HashMap<Term, NodeId> = HashMap::new();
        for (id, n) in &self.nodes {
            if n.is_leader() {
                if let Some(other) = by_term.insert(n.term(), *id) {
                    panic!(
                        "election safety violated: {other} and {id} both lead term {}",
                        n.term()
                    );
                }
            }
        }
    }

    fn current_leader(&self) -> Option<NodeId> {
        // The leader at the highest term (a stale leader may linger inside a
        // partition — that's legal Raft).
        self.nodes
            .iter()
            .filter(|(_, n)| n.is_leader())
            .max_by_key(|(_, n)| n.term())
            .map(|(id, _)| *id)
    }

    fn tick_until_leader(&mut self, max: usize) -> NodeId {
        for _ in 0..max {
            self.tick_all();
            if let Some(l) = self.current_leader() {
                return l;
            }
        }
        panic!("no leader elected after {max} ticks");
    }

    fn propose(&mut self, id: NodeId, data: &[u8]) -> Index {
        let idx = self
            .nodes
            .get_mut(&id)
            .unwrap()
            .propose(data.to_vec())
            .expect("propose on non-leader");
        self.drain(id);
        self.process();
        idx
    }

    fn partition_both_ways(&mut self, a: NodeId, others: &[NodeId]) {
        for o in others {
            self.blocked.insert((a, *o));
            self.blocked.insert((*o, a));
        }
    }

    fn heal_all(&mut self) {
        self.blocked.clear();
    }

    fn crash(&mut self, id: NodeId) {
        self.nodes.remove(&id);
    }

    fn restart(&mut self, id: NodeId) {
        let p = self.persisted.get(&id).cloned().unwrap_or_default();
        self.nodes.insert(
            id,
            RaftNode::new(Config::new(id, 42), self.initial_voters.clone(), p),
        );
    }

    fn logs_converged(&self) -> bool {
        let mut reference: Option<(Index, &RaftNode)> = None;
        for n in self.nodes.values() {
            match reference {
                None => reference = Some((n.log().last_index(), n)),
                Some((li, r)) => {
                    if n.log().last_index() != li {
                        return false;
                    }
                    for idx in n.log().first_index()..=n.log().last_index() {
                        if r.log().term(idx).is_some() && n.log().term(idx) != r.log().term(idx) {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }
}

#[test]
fn elects_exactly_one_leader() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);
    assert_eq!(
        h.nodes.values().filter(|n| n.is_leader()).count(),
        1,
        "exactly one leader"
    );
    assert!(h.nodes[&leader].is_leader());
}

#[test]
fn replicates_and_commits_to_all() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);
    let idx = h.propose(leader, b"cmd-1");
    for _ in 0..10 {
        h.tick_all();
    }
    for (id, n) in &h.nodes {
        assert!(
            n.commit_index() >= idx,
            "node {id} commit {} < {idx}",
            n.commit_index()
        );
    }
    assert_eq!(
        h.applied_global.get(&idx).map(|e| &e.payload),
        Some(&EntryPayload::Normal(b"cmd-1".to_vec()))
    );
}

#[test]
fn reelects_after_leader_partition_and_old_leader_steps_down() {
    let mut h = Harness::new(3);
    let old_leader = h.tick_until_leader(200);
    let others: Vec<NodeId> = h.nodes.keys().copied().filter(|i| *i != old_leader).collect();
    h.partition_both_ways(old_leader, &others);

    // Majority side elects a new leader at a higher term.
    let mut new_leader = None;
    for _ in 0..300 {
        h.tick_all();
        if let Some(l) = h.current_leader() {
            if l != old_leader && h.nodes[&l].term() > h.nodes[&old_leader].term() {
                new_leader = Some(l);
                break;
            }
        }
    }
    let new_leader = new_leader.expect("majority failed to elect");
    let idx = h.propose(new_leader, b"after-partition");
    for _ in 0..10 {
        h.tick_all();
    }
    assert!(h.nodes[&new_leader].commit_index() >= idx);

    h.heal_all();
    for _ in 0..30 {
        h.tick_all();
    }
    assert!(
        !h.nodes[&old_leader].is_leader(),
        "old leader must step down after heal"
    );
    assert!(h.nodes[&old_leader].commit_index() >= idx);
    assert!(h.logs_converged());
}

#[test]
fn follower_crash_recovers_and_catches_up() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);
    let dead = *h.nodes.keys().find(|i| **i != leader).unwrap();
    h.crash(dead);

    let mut last = 0;
    for i in 0..20 {
        last = h.propose(leader, format!("op{i}").as_bytes());
        h.tick_all();
    }
    assert!(h.nodes[&leader].commit_index() >= last, "2/3 must still commit");

    h.restart(dead);
    for _ in 0..50 {
        h.tick_all();
    }
    assert!(
        h.nodes[&dead].commit_index() >= last,
        "restarted follower must catch up (commit {} < {last})",
        h.nodes[&dead].commit_index()
    );
    assert!(h.logs_converged());
}

#[test]
fn divergent_uncommitted_entries_are_truncated() {
    let mut h = Harness::new(3);
    let old_leader = h.tick_until_leader(200);
    let others: Vec<NodeId> = h.nodes.keys().copied().filter(|i| *i != old_leader).collect();

    // Isolate the leader, then feed it proposals that can never commit.
    h.partition_both_ways(old_leader, &others);
    h.nodes
        .get_mut(&old_leader)
        .unwrap()
        .propose(b"doomed-1".to_vec())
        .unwrap();
    h.nodes
        .get_mut(&old_leader)
        .unwrap()
        .propose(b"doomed-2".to_vec())
        .unwrap();
    h.drain(old_leader);
    h.process();
    let doomed_last = h.nodes[&old_leader].log().last_index();

    // Majority elects a new leader and commits different entries.
    let mut new_leader = None;
    for _ in 0..300 {
        h.tick_all();
        if let Some(l) = h.current_leader() {
            if l != old_leader {
                new_leader = Some(l);
                break;
            }
        }
    }
    let new_leader = new_leader.unwrap();
    let idx = h.propose(new_leader, b"the-truth");
    for _ in 0..10 {
        h.tick_all();
    }

    h.heal_all();
    for _ in 0..50 {
        h.tick_all();
    }
    // Old leader's doomed entries must be gone, replaced by the new
    // leader's log; committed data intact everywhere.
    let ol = &h.nodes[&old_leader];
    assert!(ol.commit_index() >= idx);
    for i in ol.log().first_index()..=ol.log().last_index() {
        if let Some(e) = ol.log().get(i) {
            assert_ne!(
                e.payload,
                EntryPayload::Normal(b"doomed-1".to_vec()),
                "uncommitted divergent entry survived repair"
            );
        }
    }
    let _ = doomed_last;
    assert!(h.logs_converged());
}

#[test]
fn lagging_follower_receives_snapshot() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);
    let dead = *h.nodes.keys().find(|i| **i != leader).unwrap();
    h.crash(dead);

    for i in 0..30 {
        h.propose(leader, format!("op{i}").as_bytes());
        h.tick_all();
    }
    // Leader compacts its log at the commit point.
    let commit = h.nodes[&leader].commit_index();
    let snap = h
        .nodes
        .get_mut(&leader)
        .unwrap()
        .compact(commit, b"sm-state".to_vec())
        .expect("compaction");
    // Host persists its own snapshot.
    let p = h.persisted.get_mut(&leader).unwrap();
    p.entries.retain(|e| e.index > snap.last_index);
    p.snapshot = Some(snap);
    assert!(h.nodes[&leader].log().first_index() > 1);

    h.restart(dead);
    for _ in 0..100 {
        h.tick_all();
    }
    let n = &h.nodes[&dead];
    assert!(
        n.commit_index() >= commit,
        "snapshot-installed follower at commit {} < {commit}",
        n.commit_index()
    );
    assert!(
        h.persisted[&dead].snapshot.is_some(),
        "follower must have persisted the installed snapshot"
    );
    assert!(h.logs_converged());
}

#[test]
fn membership_add_then_remove_node() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);

    // Add node 4.
    h.add_fresh_node(4, vec![1, 2, 3]);
    h.nodes
        .get_mut(&leader)
        .unwrap()
        .propose_conf_change(ConfChange::AddNode(4))
        .unwrap();
    h.drain(leader);
    h.process();
    for _ in 0..50 {
        h.tick_all();
    }
    assert!(h.nodes[&leader].voters().contains(&4));
    let idx = h.propose(leader, b"with-four");
    for _ in 0..10 {
        h.tick_all();
    }
    assert!(h.nodes[&4].commit_index() >= idx, "new node participates");

    // Only one conf change in flight at a time.
    {
        let l = h.nodes.get_mut(&leader).unwrap();
        l.propose_conf_change(ConfChange::RemoveNode(4)).unwrap();
        assert_eq!(
            l.propose_conf_change(ConfChange::AddNode(9)),
            Err(ProposeError::ConfChangeInFlight)
        );
    }
    h.drain(leader);
    h.process();
    for _ in 0..50 {
        h.tick_all();
    }
    assert!(!h.nodes[&leader].voters().contains(&4));

    // Cluster of three still commits.
    let idx = h.propose(leader, b"back-to-three");
    for _ in 0..10 {
        h.tick_all();
    }
    assert!(h.nodes[&leader].commit_index() >= idx);
}

#[test]
fn prevote_prevents_term_inflation_from_partitioned_node() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);
    let stable_term = h.nodes[&leader].term();
    let isolated = *h.nodes.keys().find(|i| **i != leader).unwrap();
    let others: Vec<NodeId> = h.nodes.keys().copied().filter(|i| *i != isolated).collect();

    h.partition_both_ways(isolated, &others);
    for _ in 0..200 {
        h.tick_all();
    }
    // Without pre-vote the isolated node's term would have exploded.
    assert_eq!(
        h.nodes[&isolated].term(),
        stable_term,
        "pre-vote must stop term inflation while partitioned"
    );

    h.heal_all();
    for _ in 0..30 {
        h.tick_all();
    }
    // The rejoin must NOT depose the healthy leader.
    assert_eq!(h.current_leader(), Some(leader), "leader deposed by rejoin");
    assert_eq!(h.nodes[&leader].term(), stable_term);
}

#[test]
fn read_index_completes_after_quorum_round() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);
    h.propose(leader, b"x");
    for _ in 0..10 {
        h.tick_all();
    }
    let commit = h.nodes[&leader].commit_index();
    h.nodes.get_mut(&leader).unwrap().read_index(77).unwrap();
    h.drain(leader);
    h.process();
    for _ in 0..5 {
        h.tick_all();
    }
    let rs = &h.read_states[&leader];
    let got = rs.iter().find(|(id, _)| *id == 77).expect("read released");
    assert!(got.1 >= commit);
}

#[test]
fn read_index_rejected_on_follower() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);
    let follower = *h.nodes.keys().find(|i| **i != leader).unwrap();
    let err = h.nodes.get_mut(&follower).unwrap().read_index(1);
    assert!(matches!(err, Err(ProposeError::NotLeader(_))));
}

#[test]
fn leader_lease_holds_with_quorum_and_lapses_when_isolated() {
    let mut h = Harness::new(3);
    let leader = h.tick_until_leader(200);
    for _ in 0..10 {
        h.tick_all();
    }
    assert!(h.nodes[&leader].lease_valid(), "healthy leader holds lease");

    let others: Vec<NodeId> = h.nodes.keys().copied().filter(|i| *i != leader).collect();
    h.partition_both_ways(leader, &others);
    // Tick only the leader far enough for the lease to lapse but keep the
    // others frozen so no new election muddies the assertion.
    for _ in 0..30 {
        h.nodes.get_mut(&leader).unwrap().tick();
        h.drain(leader);
        h.process();
    }
    assert!(
        !h.nodes[&leader].lease_valid(),
        "isolated leader must lose its lease"
    );
}
