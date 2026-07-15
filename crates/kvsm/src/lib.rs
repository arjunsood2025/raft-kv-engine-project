//! The replicated key-value state machine applied from the Raft log.
//!
//! # Exactly-once, honestly
//! Raft gives at-least-once delivery to the state machine: a client that
//! times out retries, and its command may be applied after the original also
//! commits. True "exactly-once delivery" does not exist in an asynchronous
//! network; what we implement is **at-least-once delivery + idempotent
//! apply**: every client owns a session, every request carries a
//! monotonically increasing sequence number, and the state machine keeps a
//! per-session dedup table of (last applied seq, last result). A duplicate
//! is answered from the table without re-executing. Because the table is
//! part of the replicated state (it rides in snapshots too), every replica
//! makes the same dedup decision — that is what makes retried writes safe
//! and linearizable.
//!
//! The backend is a trait: the simulator plugs in an in-memory map, the
//! server plugs in the LSM engine.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    /// Compare-and-swap: apply `new` iff current value == `expect`
    /// (None = expect key absent / delete on success).
    Cas {
        key: Vec<u8>,
        expect: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    },
    /// Read-through-the-log (used when a caller wants linearizability
    /// without ReadIndex; normal reads bypass the log).
    Get { key: Vec<u8> },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Command {
    /// 0 = no session (internal ops, no dedup).
    pub session_id: u64,
    /// Client-assigned, strictly increasing per session.
    pub seq: u64,
    pub op: Op,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum OpResult {
    Ok,
    Value(Option<Vec<u8>>),
    Cas {
        success: bool,
        /// Value observed at apply time (what CAS compared against).
        actual: Option<Vec<u8>>,
    },
    /// seq older than the session's last applied seq — the client has
    /// already moved on; there is nothing meaningful to return.
    Stale,
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("command serialization is infallible")
    }
    pub fn decode(data: &[u8]) -> Option<Command> {
        bincode::deserialize(data).ok()
    }
}

/// Storage abstraction under the state machine.
pub trait Backend {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    /// None value = delete.
    fn set(&mut self, key: Vec<u8>, value: Option<Vec<u8>>);
    fn scan(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)>;
    /// Full contents, for snapshotting. Fine at this project's scale; a
    /// production system streams snapshots instead.
    fn dump(&self) -> Vec<(Vec<u8>, Vec<u8>)>;
    fn clear_and_load(&mut self, kvs: Vec<(Vec<u8>, Vec<u8>)>);
}

#[derive(Default, Clone)]
pub struct MemBackend {
    map: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Backend for MemBackend {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).cloned()
    }
    fn set(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        match value {
            Some(v) => {
                self.map.insert(key, v);
            }
            None => {
                self.map.remove(&key);
            }
        }
    }
    fn scan(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.map
            .range(start.to_vec()..)
            .take_while(|(k, _)| end.map_or(true, |e| k.as_slice() < e))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    fn dump(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
    fn clear_and_load(&mut self, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
        self.map = kvs.into_iter().collect();
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SessionState {
    pub last_seq: u64,
    pub last_result: OpResult,
}

pub struct StateMachine<B: Backend> {
    backend: B,
    sessions: HashMap<u64, SessionState>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotPayload {
    kvs: Vec<(Vec<u8>, Vec<u8>)>,
    sessions: Vec<(u64, SessionState)>,
}

impl<B: Backend> StateMachine<B> {
    pub fn new(backend: B) -> Self {
        StateMachine {
            backend,
            sessions: HashMap::new(),
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Apply one committed command. Deterministic: every replica applying
    /// the same log prefix reaches the same state and returns the same
    /// results — this is checked continuously in simulation.
    pub fn apply(&mut self, cmd: &Command) -> OpResult {
        if cmd.session_id != 0 {
            if let Some(s) = self.sessions.get(&cmd.session_id) {
                if cmd.seq == s.last_seq {
                    return s.last_result.clone(); // duplicate: cached answer
                }
                if cmd.seq < s.last_seq {
                    return OpResult::Stale;
                }
            }
        }
        let result = self.execute(&cmd.op);
        if cmd.session_id != 0 {
            self.sessions.insert(
                cmd.session_id,
                SessionState {
                    last_seq: cmd.seq,
                    last_result: result.clone(),
                },
            );
        }
        result
    }

    fn execute(&mut self, op: &Op) -> OpResult {
        match op {
            Op::Put { key, value } => {
                self.backend.set(key.clone(), Some(value.clone()));
                OpResult::Ok
            }
            Op::Delete { key } => {
                self.backend.set(key.clone(), None);
                OpResult::Ok
            }
            Op::Cas { key, expect, new } => {
                let actual = self.backend.get(key);
                if actual == *expect {
                    self.backend.set(key.clone(), new.clone());
                    OpResult::Cas {
                        success: true,
                        actual,
                    }
                } else {
                    OpResult::Cas {
                        success: false,
                        actual,
                    }
                }
            }
            Op::Get { key } => OpResult::Value(self.backend.get(key)),
        }
    }

    /// Local read — the host is responsible for the consistency protocol
    /// around it (ReadIndex / lease / stale mode).
    pub fn read(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.backend.get(key)
    }

    pub fn read_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.backend.scan(start, end, limit)
    }

    /// Serialize the FULL state (kv + session table). Sessions must ride in
    /// snapshots or a snapshot-restored replica would re-apply duplicates.
    pub fn snapshot(&self) -> Vec<u8> {
        let mut sessions: Vec<(u64, SessionState)> = self
            .sessions
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        sessions.sort_by_key(|(id, _)| *id); // deterministic bytes
        let payload = SnapshotPayload {
            kvs: self.backend.dump(),
            sessions,
        };
        bincode::serialize(&payload).expect("snapshot serialization")
    }

    pub fn restore(&mut self, data: &[u8]) {
        let payload: SnapshotPayload =
            bincode::deserialize(data).expect("snapshot payload decode");
        self.backend.clear_and_load(payload.kvs);
        self.sessions = payload.sessions.into_iter().collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(session: u64, seq: u64, k: &[u8], v: &[u8]) -> Command {
        Command {
            session_id: session,
            seq,
            op: Op::Put {
                key: k.to_vec(),
                value: v.to_vec(),
            },
        }
    }

    #[test]
    fn duplicate_command_applies_once() {
        let mut sm = StateMachine::new(MemBackend::default());
        let cas = Command {
            session_id: 1,
            seq: 1,
            op: Op::Cas {
                key: b"k".to_vec(),
                expect: None,
                new: Some(b"v1".to_vec()),
            },
        };
        let r1 = sm.apply(&cas);
        assert_eq!(
            r1,
            OpResult::Cas {
                success: true,
                actual: None
            }
        );
        // Retry (duplicate delivery): must return the CACHED success, not
        // re-execute (a re-executed CAS would now fail since k exists).
        let r2 = sm.apply(&cas);
        assert_eq!(r1, r2, "duplicate must be answered from the dedup table");
        assert_eq!(sm.read(b"k"), Some(b"v1".to_vec()));
    }

    #[test]
    fn old_seq_is_stale() {
        let mut sm = StateMachine::new(MemBackend::default());
        sm.apply(&put(1, 5, b"a", b"x"));
        assert_eq!(sm.apply(&put(1, 3, b"a", b"y")), OpResult::Stale);
        assert_eq!(sm.read(b"a"), Some(b"x".to_vec()));
    }

    #[test]
    fn cas_failure_reports_actual() {
        let mut sm = StateMachine::new(MemBackend::default());
        sm.apply(&put(1, 1, b"k", b"v1"));
        let r = sm.apply(&Command {
            session_id: 1,
            seq: 2,
            op: Op::Cas {
                key: b"k".to_vec(),
                expect: Some(b"wrong".to_vec()),
                new: Some(b"v2".to_vec()),
            },
        });
        assert_eq!(
            r,
            OpResult::Cas {
                success: false,
                actual: Some(b"v1".to_vec())
            }
        );
        assert_eq!(sm.read(b"k"), Some(b"v1".to_vec()));
    }

    #[test]
    fn snapshot_roundtrip_preserves_sessions_and_data() {
        let mut sm = StateMachine::new(MemBackend::default());
        sm.apply(&put(7, 42, b"key1", b"val1"));
        sm.apply(&put(8, 9, b"key2", b"val2"));
        let snap = sm.snapshot();

        let mut sm2 = StateMachine::new(MemBackend::default());
        sm2.restore(&snap);
        assert_eq!(sm2.read(b"key1"), Some(b"val1".to_vec()));
        // Dedup table must survive the snapshot: a duplicate of session 7
        // seq 42 must be recognized.
        let r = sm2.apply(&put(7, 42, b"key1", b"DIFFERENT"));
        assert_eq!(r, OpResult::Ok, "cached result");
        assert_eq!(
            sm2.read(b"key1"),
            Some(b"val1".to_vec()),
            "duplicate must not re-execute after snapshot restore"
        );
    }

    #[test]
    fn scan_range() {
        let mut sm = StateMachine::new(MemBackend::default());
        for i in 0..10u8 {
            sm.apply(&put(1, i as u64 + 1, &[b'k', b'0' + i], &[i]));
        }
        let r = sm.read_range(b"k2", Some(b"k5"), 100);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0, b"k2".to_vec());
    }
}
