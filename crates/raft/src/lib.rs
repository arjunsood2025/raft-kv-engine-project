//! Sans-IO Raft consensus core.
//!
//! [`RaftNode`] is a pure state machine: it never touches sockets, disks,
//! threads, or clocks. The host drives it with [`RaftNode::tick`] (logical
//! time) and [`RaftNode::step`] (incoming messages), and drains
//! [`RaftNode::take_output`] — which spells out the host's obligations:
//! persist hard state / log entries / snapshots BEFORE sending the messages.
//!
//! Implemented: pre-vote elections with randomized timeouts, log replication
//! with batching + optimistic pipelining, conflict-index fast log repair,
//! snapshot compaction + InstallSnapshot for lagging followers,
//! single-server membership changes, ReadIndex linearizable reads, and a
//! heartbeat-quorum leader lease.
//!
//! The same core runs under the production tokio host (`server`) and the
//! deterministic simulator (`sim`) — that is the point of sans-IO.

mod log;
mod node;
mod types;

pub use log::RaftLog;
pub use node::RaftNode;
pub use types::{
    Config, ConfChange, Entry, EntryPayload, HardState, Index, Message, MessageBody, NodeId,
    Output, Persisted, ProposeError, Role, Snapshot, Term,
};
