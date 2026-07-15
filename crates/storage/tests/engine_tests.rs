//! Engine-level correctness tests, including randomized model checking and
//! crash-injection durability tests.

use storage::{Db, Options, SyncPolicy};

fn small_opts() -> Options {
    // Tiny thresholds so a few thousand ops exercise flush + multi-level
    // compaction paths hard.
    Options {
        memtable_bytes: 2048,
        sync: SyncPolicy::Always,
        l0_trigger: 2,
        level_base_bytes: 8192,
        level_multiplier: 4,
        target_table_bytes: 4096,
        block_bytes: 256,
        bits_per_key: 10,
    }
}

/// xorshift64* — deterministic per-seed randomness without a dependency.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn basic_ops_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = Db::open(dir.path(), small_opts()).unwrap();
        db.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
        db.put(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        db.delete(b"foo".to_vec()).unwrap();
        assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(db.get(b"foo").unwrap(), None);
    }
    // Reopen: WAL replay must restore state (no flush ever happened).
    let db = Db::open(dir.path(), small_opts()).unwrap();
    assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
    assert_eq!(db.get(b"foo").unwrap(), None);
}

#[test]
fn randomized_against_model() {
    for seed in 1..=6u64 {
        randomized_run(seed, 3000);
    }
}

fn randomized_run(seed: u64, ops: usize) {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = Rng::new(seed);
    let mut model: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = Default::default();
    // sync: Never here — this test checks read/flush/compaction correctness,
    // not durability (acked_writes_survive_crash covers that), and fsync per
    // op makes the model run ~50x slower.
    let mut opts = small_opts();
    opts.sync = SyncPolicy::Never;
    let mut db = Some(Db::open(dir.path(), opts.clone()).unwrap());

    for i in 0..ops {
        let key = format!("key{:03}", rng.below(50)).into_bytes();
        match rng.below(10) {
            0..=6 => {
                let val = format!("val-{seed}-{i}-{}", rng.next()).into_bytes();
                db.as_mut().unwrap().put(key.clone(), val.clone()).unwrap();
                model.insert(key, val);
            }
            7 | 8 => {
                db.as_mut().unwrap().delete(key.clone()).unwrap();
                model.remove(&key);
            }
            _ => {
                // Simulated process restart: drop without flushing.
                drop(db.take());
                db = Some(Db::open(dir.path(), opts.clone()).unwrap());
            }
        }
        if i % 97 == 0 {
            let d = db.as_ref().unwrap();
            for k in 0..50 {
                let key = format!("key{k:03}").into_bytes();
                assert_eq!(
                    d.get(&key).unwrap(),
                    model.get(&key).cloned(),
                    "seed {seed} op {i} key {k}"
                );
            }
        }
    }

    let d = db.as_ref().unwrap();
    let scanned = d.scan(b"", None, usize::MAX).unwrap();
    let expected: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(scanned, expected, "seed {seed} final scan");
    // The run must actually have exercised compaction to mean anything.
    let counts = d.level_table_counts();
    assert!(
        counts.len() > 1,
        "seed {seed}: compaction never ran, levels = {counts:?}"
    );
}

#[test]
fn acked_writes_survive_crash() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..500u32 {
            // sync = Always means every put is acked-durable when it returns.
            db.put(
                format!("k{i:04}").into_bytes(),
                format!("v{i}").into_bytes(),
            )
            .unwrap();
        }
        // No flush, no clean shutdown: the Db is dropped mid-flight,
        // equivalent to kill -9 for WAL purposes.
    }
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..500u32 {
        assert_eq!(
            db.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "acked write {i} lost after crash"
        );
    }
}

#[test]
fn torn_wal_tail_loses_only_the_torn_batch() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..50u32 {
            db.put(format!("k{i:02}").into_bytes(), b"v".to_vec()).unwrap();
        }
    }
    // Simulate a torn final write: chop bytes off the WAL tail.
    let wal_path = dir.path().join("wal");
    let data = std::fs::read(&wal_path).unwrap();
    std::fs::write(&wal_path, &data[..data.len() - 5]).unwrap();

    let db = Db::open(dir.path(), small_opts()).unwrap();
    // Everything except possibly the last batch must survive; nothing may
    // be half-applied or corrupt.
    for i in 0..49u32 {
        let v = db.get(format!("k{i:02}").as_bytes()).unwrap();
        assert!(
            v == Some(b"v".to_vec()) || v.is_none(),
            "key {i} corrupt after torn tail"
        );
    }
    // All fully-durable earlier batches must be intact (only the final frame
    // was damaged).
    for i in 0..49u32 {
        assert_eq!(
            db.get(format!("k{i:02}").as_bytes()).unwrap(),
            Some(b"v".to_vec()),
            "fully-synced write {i} lost"
        );
    }
}

#[test]
fn snapshot_reads_survive_flush_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), small_opts()).unwrap();
    db.put(b"pinned".to_vec(), b"original".to_vec()).unwrap();
    let snap = db.snapshot();

    // Churn hard enough to force flushes and compactions that would GC the
    // old version if the snapshot were not pinning it.
    for i in 0..2000u32 {
        db.put(b"pinned".to_vec(), format!("overwrite-{i}").into_bytes())
            .unwrap();
        db.put(
            format!("filler{:03}", i % 200).into_bytes(),
            vec![b'x'; 64],
        )
        .unwrap();
    }
    assert!(db.level_table_counts().len() > 1, "compaction never ran");

    assert_eq!(
        db.get_at(b"pinned", snap.seq).unwrap(),
        Some(b"original".to_vec()),
        "snapshot read broken by compaction GC"
    );
    assert_eq!(
        db.get(b"pinned").unwrap(),
        Some(b"overwrite-1999".to_vec())
    );
    drop(snap);
}

#[test]
fn scans_respect_range_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..100u32 {
        db.put(format!("k{i:03}").into_bytes(), format!("v{i}").into_bytes())
            .unwrap();
    }
    let r = db.scan(b"k010", Some(b"k020"), usize::MAX).unwrap();
    assert_eq!(r.len(), 10);
    assert_eq!(r[0].0, b"k010".to_vec());
    assert_eq!(r[9].0, b"k019".to_vec());

    let r = db.scan(b"k050", None, 5).unwrap();
    assert_eq!(r.len(), 5);
    assert_eq!(r[0].0, b"k050".to_vec());
}
