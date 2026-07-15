//! Wire and state types shared by the Raft core and its hosts.

use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ConfChange {
    AddNode(NodeId),
    RemoveNode(NodeId),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum EntryPayload {
    /// Appended by a new leader to commit an entry from its own term
    /// immediately (the Figure 8 safety fix in practice).
    Noop,
    /// Opaque state-machine command.
    Normal(Vec<u8>),
    ConfChange(ConfChange),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    pub payload: EntryPayload,
}

/// The state that MUST be fsynced before any message reflecting it leaves
/// the node. Losing term/voted_for allows double-voting; losing log entries
/// allows committed-entry loss.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub last_index: Index,
    pub last_term: Term,
    /// Voter set as of last_index (conf changes up to that point applied).
    pub voters: Vec<NodeId>,
    /// Serialized state machine.
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    /// Sender's term — except PreVote/PreVoteResp, where it is the term the
    /// candidate WOULD campaign at (current + 1) without having bumped it.
    pub term: Term,
    pub body: MessageBody,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MessageBody {
    PreVote {
        last_log_index: Index,
        last_log_term: Term,
    },
    PreVoteResp {
        granted: bool,
    },
    RequestVote {
        last_log_index: Index,
        last_log_term: Term,
    },
    RequestVoteResp {
        granted: bool,
    },
    /// Doubles as heartbeat when `entries` is empty. `read_ctx` piggybacks
    /// the ReadIndex quorum round (0 = none).
    AppendEntries {
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
        read_ctx: u64,
    },
    AppendEntriesResp {
        success: bool,
        /// On success: highest index known replicated on the follower.
        match_index: Index,
        /// On failure: hint for the leader to jump next_index back to
        /// (first index of the conflicting term — skips whole terms instead
        /// of decrementing one at a time).
        conflict_index: Index,
        read_ctx: u64,
    },
    InstallSnapshot {
        snapshot: Snapshot,
    },
    InstallSnapshotResp {
        last_index: Index,
    },
}

/// Everything a `tick()`/`step()` produced. Host contract:
/// 1. persist `hard_state`, `append`, `snapshot` durably (fsync);
/// 2. only then send `messages`;
/// 3. apply `committed` to the state machine in order;
/// 4. serve each read in `read_states` once applied index >= its index.
#[derive(Default, Debug)]
pub struct Output {
    pub messages: Vec<Message>,
    pub committed: Vec<Entry>,
    pub hard_state: Option<HardState>,
    /// Entries to persist. Each entry REPLACES anything previously stored at
    /// its index (i.e. truncate-from-then-append).
    pub append: Vec<Entry>,
    /// A snapshot installed from the leader: persist it, drop the log prefix,
    /// and reset the state machine to its contents.
    pub snapshot: Option<Snapshot>,
    /// (request_id, read_index) pairs whose quorum round completed.
    pub read_states: Vec<(u64, Index)>,
}

impl Output {
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
            && self.committed.is_empty()
            && self.hard_state.is_none()
            && self.append.is_empty()
            && self.snapshot.is_none()
            && self.read_states.is_empty()
    }
}

/// Durable node state handed back at restart.
#[derive(Clone, Debug, Default)]
pub struct Persisted {
    pub hard_state: HardState,
    pub entries: Vec<Entry>,
    pub snapshot: Option<Snapshot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeError {
    /// Not the leader; hint is the last known leader if any.
    NotLeader(Option<NodeId>),
    /// A configuration change is already in flight (single-server change
    /// rule: at most one uncommitted conf change).
    ConfChangeInFlight,
    /// Leader hasn't committed an entry in its own term yet (ReadIndex must
    /// wait for the no-op to commit).
    NotReady,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    /// Election timeout is randomized in [election_tick_min, election_tick_max).
    pub election_tick_min: u32,
    pub election_tick_max: u32,
    pub heartbeat_tick: u32,
    pub max_batch_entries: usize,
    /// Max optimistically-pipelined AppendEntries per follower.
    pub max_inflight: usize,
    pub pre_vote: bool,
    /// Seeds the node-local RNG for randomized timeouts. Hosts MUST derive
    /// this deterministically in simulation.
    pub seed: u64,
}

impl Config {
    pub fn new(id: NodeId, seed: u64) -> Self {
        Config {
            id,
            election_tick_min: 10,
            election_tick_max: 20,
            heartbeat_tick: 2,
            max_batch_entries: 64,
            max_inflight: 10,
            pre_vote: true,
            seed,
        }
    }
}
