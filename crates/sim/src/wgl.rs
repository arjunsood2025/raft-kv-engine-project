//! Wing & Gong linearizability checker, specialized per key.
//!
//! Every simulated run records a complete client history (invocations and
//! responses with global timestamps). This module decides whether that
//! history is linearizable against a register with read / write / CAS
//! semantics: does there exist a total order of the operations, consistent
//! with real time (if op A returned before op B was invoked, A must come
//! first), under which every response is exactly what a single sequential
//! register would have returned?
//!
//! The search is the classic WGL algorithm: depth-first over "which op
//! linearizes next", with memoization on (set of linearized ops, current
//! register value). Operations on different keys commute, so we check each
//! key independently — this turns one exponential search over N ops into
//! K small searches, which is the standard practical trick (porcupine does
//! the same when given a partition function).
//!
//! Pending operations (invoked, never returned before the run ended) may
//! have taken effect or not — the checker must allow both. Pending reads
//! have no effect on state and no observable result, so they are dropped
//! up front; pending writes/CAS are optional candidates in the search.

use crate::history::{HOp, HRecord, HRes};
use std::collections::HashSet;

/// One operation on a single key, prepared for checking.
#[derive(Clone, Debug)]
pub struct POp {
    pub invoke: u64,
    /// None = pending (no response before run end).
    pub ret: Option<u64>,
    pub kind: PKind,
    /// Which client issued it (only used in violation reports).
    pub client: u64,
}

#[derive(Clone, Debug)]
pub enum PKind {
    /// Completed read observing this value (None = key absent).
    Read { result: Option<u64> },
    Write { val: u64 },
    Cas {
        expect: Option<u64>,
        new: u64,
        /// None = pending (outcome unknown).
        success: Option<bool>,
    },
}

/// If `kind` is applied to a register holding `value`, does its recorded
/// response hold up, and what does the register hold afterwards?
fn try_apply(kind: &PKind, value: Option<u64>) -> Option<Option<u64>> {
    match kind {
        PKind::Read { result } => {
            if *result == value {
                Some(value)
            } else {
                None
            }
        }
        PKind::Write { val } => Some(Some(*val)),
        PKind::Cas { expect, new, success } => {
            let would_succeed = value == *expect;
            match success {
                Some(s) if *s != would_succeed => None,
                _ => Some(if would_succeed { Some(*new) } else { value }),
            }
        }
    }
}

const EXPANSION_BUDGET: u64 = 5_000_000;

/// Check one key's operations (any order; sorted internally).
pub fn check_key(ops: &[POp]) -> Result<(), String> {
    let mut ops: Vec<POp> = ops.to_vec();
    ops.sort_by_key(|o| o.invoke);
    let n = ops.len();
    if n == 0 {
        return Ok(());
    }
    if n > 4096 {
        return Err(format!("too many ops for checker: {}", n));
    }
    let words = (n + 63) / 64;
    let all_completed_mask: Vec<u64> = {
        let mut m = vec![0u64; words];
        for (i, o) in ops.iter().enumerate() {
            if o.ret.is_some() {
                m[i / 64] |= 1 << (i % 64);
            }
        }
        m
    };
    let done = |mask: &[u64]| -> bool {
        mask.iter()
            .zip(&all_completed_mask)
            .all(|(m, c)| m & c == *c)
    };

    let mut memo: HashSet<(Vec<u64>, Option<u64>)> = HashSet::new();
    let mut stack: Vec<(Vec<u64>, Option<u64>)> = vec![(vec![0u64; words], None)];
    let mut expansions: u64 = 0;

    while let Some((mask, value)) = stack.pop() {
        if done(&mask) {
            return Ok(());
        }
        if !memo.insert((mask.clone(), value)) {
            continue;
        }
        expansions += 1;
        if expansions > EXPANSION_BUDGET {
            return Err(format!(
                "checker expansion budget exceeded ({} states, {} ops)",
                expansions, n
            ));
        }
        // An op may linearize next only if no *completed*, not-yet-linearized
        // op returned before it was invoked (real-time order constraint).
        let mut min_ret = u64::MAX;
        for (i, o) in ops.iter().enumerate() {
            if mask[i / 64] & (1 << (i % 64)) != 0 {
                continue;
            }
            if let Some(r) = o.ret {
                min_ret = min_ret.min(r);
            }
        }
        for (i, o) in ops.iter().enumerate() {
            if mask[i / 64] & (1 << (i % 64)) != 0 {
                continue;
            }
            if o.invoke > min_ret {
                continue;
            }
            if let Some(new_value) = try_apply(&o.kind, value) {
                let mut m2 = mask.clone();
                m2[i / 64] |= 1 << (i % 64);
                stack.push((m2, new_value));
            }
        }
    }

    let mut report = String::from("history NOT linearizable; ops (sorted by invoke):\n");
    for o in &ops {
        report.push_str(&format!(
            "  client={} invoke={} ret={:?} {:?}\n",
            o.client, o.invoke, o.ret, o.kind
        ));
    }
    Err(report)
}

/// Check a full multi-key history by partitioning per key.
pub fn check_history(history: &[HRecord]) -> Result<(), String> {
    let mut keys: Vec<u8> = history.iter().map(|r| r.op.key()).collect();
    keys.sort_unstable();
    keys.dedup();
    for key in keys {
        let mut ops: Vec<POp> = Vec::new();
        for r in history.iter().filter(|r| r.op.key() == key) {
            let kind = match (&r.op, &r.result) {
                (HOp::Read { .. }, None) => continue, // pending read: no effect
                (HOp::Read { .. }, Some(HRes::ReadOk(v))) => PKind::Read { result: *v },
                (HOp::Write { val, .. }, _) => PKind::Write { val: *val },
                (HOp::Cas { expect, new, .. }, res) => PKind::Cas {
                    expect: *expect,
                    new: *new,
                    success: match res {
                        Some(HRes::CasOk { success }) => Some(*success),
                        None => None,
                        other => return Err(format!("malformed CAS result {:?}", other)),
                    },
                },
                (op, res) => return Err(format!("malformed record {:?} / {:?}", op, res)),
            };
            ops.push(POp {
                invoke: r.invoke,
                ret: r.ret,
                kind,
                client: r.client,
            });
        }
        check_key(&ops).map_err(|e| format!("key {}: {}", key, e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(invoke: u64, ret: u64, result: Option<u64>) -> POp {
        POp { invoke, ret: Some(ret), kind: PKind::Read { result }, client: 0 }
    }
    fn write(invoke: u64, ret: u64, val: u64) -> POp {
        POp { invoke, ret: Some(ret), kind: PKind::Write { val }, client: 0 }
    }

    #[test]
    fn sequential_ops_ok() {
        let ops = vec![
            write(1, 2, 10),
            read(3, 4, Some(10)),
            write(5, 6, 20),
            read(7, 8, Some(20)),
        ];
        assert!(check_key(&ops).is_ok());
    }

    #[test]
    fn stale_read_is_a_violation() {
        // w(10) completes, then w(20) completes, then a read sees 10: the
        // second write's effect vanished — not linearizable.
        let ops = vec![write(1, 2, 10), write(3, 4, 20), read(5, 6, Some(10))];
        assert!(check_key(&ops).is_err());
    }

    #[test]
    fn concurrent_read_may_see_either_value() {
        // Read overlaps the write: both old and new value are legal.
        let w = write(2, 6, 20);
        let setup = write(0, 1, 10);
        assert!(check_key(&[setup.clone(), w.clone(), read(3, 5, Some(10))]).is_ok());
        assert!(check_key(&[setup, w, read(3, 5, Some(20))]).is_ok());
    }

    #[test]
    fn read_from_the_future_is_a_violation() {
        // Read returns a value whose write hadn't been invoked yet.
        let ops = vec![read(1, 2, Some(99)), write(3, 4, 99)];
        assert!(check_key(&ops).is_err());
    }

    #[test]
    fn pending_write_may_or_may_not_apply() {
        let pending = POp {
            invoke: 1,
            ret: None,
            kind: PKind::Write { val: 7 },
            client: 0,
        };
        // Later read sees it: fine (it took effect).
        assert!(check_key(&[pending.clone(), read(2, 3, Some(7))]).is_ok());
        // Later read never sees it: also fine (it never took effect).
        assert!(check_key(&[pending.clone(), read(2, 3, None)]).is_ok());
        // But it cannot half-apply: seen then unseen is a violation.
        assert!(check_key(&[pending, read(2, 3, Some(7)), read(4, 5, None)]).is_err());
    }

    #[test]
    fn cas_success_must_match_register_state() {
        let setup = write(0, 1, 10);
        let cas_ok = POp {
            invoke: 2,
            ret: Some(3),
            kind: PKind::Cas { expect: Some(10), new: 20, success: Some(true) },
            client: 0,
        };
        assert!(check_key(&[setup.clone(), cas_ok.clone(), read(4, 5, Some(20))]).is_ok());
        // CAS claimed success against expect=99 which never held: violation.
        let cas_lie = POp {
            invoke: 2,
            ret: Some(3),
            kind: PKind::Cas { expect: Some(99), new: 20, success: Some(true) },
            client: 0,
        };
        assert!(check_key(&[setup, cas_lie]).is_err());
    }

    #[test]
    fn two_concurrent_cas_only_one_wins() {
        // Both CAS from None: linearizable only if exactly one succeeds.
        let setup_free = read(0, 1, None);
        let c1 = POp {
            invoke: 2, ret: Some(10),
            kind: PKind::Cas { expect: None, new: 1, success: Some(true) }, client: 1,
        };
        let c2 = POp {
            invoke: 3, ret: Some(9),
            kind: PKind::Cas { expect: None, new: 2, success: Some(true) }, client: 2,
        };
        assert!(check_key(&[setup_free, c1, c2]).is_err(), "both cannot win");
    }
}
