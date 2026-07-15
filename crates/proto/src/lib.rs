//! Wire protocol: message types + framing shared by servers and clients.
//!
//! # Why length-prefixed bincode instead of gRPC
//! The design doc suggests tonic/gRPC. This project deliberately uses a
//! hand-rolled framing layer instead: a 4-byte little-endian length prefix
//! followed by a bincode-encoded message, over plain TCP. Two reasons:
//! (1) it keeps the entire wire format inspectable and from-scratch, in the
//! same spirit as the storage engine and consensus core; (2) it removes the
//! protoc toolchain dependency, so the repo builds with `cargo build` alone.
//! The RPC *semantics* (typed request/response enums, one in-flight request
//! per connection from the client, a message stream between peers) are the
//! same shape a gRPC service would have, and swapping tonic in later would
//! only touch this crate and the accept loops.
//!
//! Frames are capped at 64 MiB — a corrupt or malicious length prefix must
//! not make the server allocate unbounded memory.

use raft::{Index, Message, NodeId, Term};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Hard cap on a single frame. Snapshots are the largest legitimate frames;
/// at this project's scale they are far below this.
pub const MAX_FRAME: u32 = 64 << 20;

// ---------------------------------------------------------------- framing

/// Write one length-prefixed bincode frame.
pub async fn write_frame<W, T>(w: &mut W, msg: &T) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let body = bincode::serialize(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let len = body.len() as u32;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds MAX_FRAME",
        ));
    }
    w.write_all(&len.to_le_bytes()).await?;
    w.write_all(&body).await?;
    w.flush().await
}

/// Read one length-prefixed bincode frame. Returns `Ok(None)` on clean EOF
/// (peer closed between frames).
pub async fn read_frame<R, T>(r: &mut R) -> std::io::Result<Option<T>>
where
    R: AsyncReadExt + Unpin,
    T: DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "incoming frame exceeds MAX_FRAME",
        ));
    }
    let mut body = vec![0u8; len as usize];
    r.read_exact(&mut body).await?;
    let msg = bincode::deserialize(&body)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(msg))
}

// ---------------------------------------------------- peer (raft) protocol

/// Server↔server frames. A peer connection opens with `Hello` so the
/// acceptor knows who is talking, then carries raft messages. Connections
/// are unidirectional pipes (each node dials every peer); responses flow
/// back over the responder's own outbound connection, mirroring how the
/// raft core addresses messages by NodeId rather than by connection.
#[derive(Serialize, Deserialize, Debug)]
pub enum PeerFrame {
    Hello { from: NodeId },
    Msg(Message),
}

// ---------------------------------------------------------- client protocol

/// Read consistency modes, weakest to strongest guarantees explained:
///
/// - `Stale`: served immediately from the local replica's applied state —
///   any node, no quorum round. May return arbitrarily old (but committed)
///   data; never returns uncommitted data.
/// - `LeaderLease`: served by the leader from local state while it holds a
///   heartbeat-quorum lease. Linearizable *if clocks are sane* (the lease
///   assumes bounded clock drift); one network hop cheaper than ReadIndex.
/// - `Linearizable`: ReadIndex — the leader confirms leadership with a
///   heartbeat quorum round, then serves at >= the confirmed commit index.
///   Linearizable with no clock assumptions.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    Linearizable,
    LeaderLease,
    Stale,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Request {
    Put {
        session_id: u64,
        seq: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        session_id: u64,
        seq: u64,
        key: Vec<u8>,
    },
    Cas {
        session_id: u64,
        seq: u64,
        key: Vec<u8>,
        expect: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    },
    Get {
        key: Vec<u8>,
        consistency: Consistency,
    },
    Scan {
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        consistency: Consistency,
    },
    /// Node status for CLI/debugging and leader discovery.
    Status,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Response {
    Ok,
    Value(Option<Vec<u8>>),
    Cas {
        success: bool,
        actual: Option<Vec<u8>>,
    },
    Kvs(Vec<(Vec<u8>, Vec<u8>)>),
    /// This node is not the leader; `hint` is its best guess at who is.
    NotLeader { hint: Option<NodeId> },
    /// Transient condition (election in progress, lease lapsed, commit
    /// timeout). Safe to retry — writes carry session/seq so a retry that
    /// races the original is deduplicated by the state machine.
    Retry { reason: String },
    Status {
        id: NodeId,
        role: String,
        term: Term,
        leader: Option<NodeId>,
        commit: Index,
        applied: Index,
        last_log_index: Index,
        voters: Vec<NodeId>,
    },
    Err(String),
}
