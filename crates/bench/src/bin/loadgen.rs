//! loadgen — YCSB-style load generator.
//!
//! ```text
//! # load 100k keys, then run workload B with 32 client connections:
//! loadgen --cluster 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003 \
//!         --phase load --keys 100000 --clients 16
//! loadgen --cluster ... --phase run --workload b --ops 100000 \
//!         --keys 100000 --clients 32 --consistency lease [--timeline]
//! ```
//!
//! Each client task owns one connection and one session (so the server sees
//! `--clients` concurrent request streams — this is the concurrency knob).
//! Reports throughput and p50/p95/p99/p999 per op kind. `--timeline` prints
//! ops completed per second — that is the plot to run during a leader kill
//! to see the failover dip.

use bench::{key_of, scramble, splitmix, Latencies, OpKind, Workload, Zipf};
use client::{ClientConfig, KvClient};
use proto::Consistency;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Args {
    cluster: Vec<String>,
    phase: String,
    workload: Workload,
    ops: u64,
    keys: u64,
    clients: u64,
    value_bytes: usize,
    consistency: Consistency,
    timeline: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        cluster: vec![],
        phase: "run".into(),
        workload: Workload::B,
        ops: 10_000,
        keys: 10_000,
        clients: 8,
        value_bytes: 100,
        consistency: Consistency::Linearizable,
        timeline: false,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].as_str();
        if flag == "--timeline" {
            a.timeline = true;
            i += 1;
            continue;
        }
        let val = argv.get(i + 1).unwrap_or_else(|| {
            eprintln!("{flag} requires a value");
            std::process::exit(2);
        });
        match flag {
            "--cluster" => a.cluster = val.split(',').map(|s| s.to_string()).collect(),
            "--phase" => a.phase = val.clone(),
            "--workload" => {
                a.workload = Workload::parse(val).unwrap_or_else(|| {
                    eprintln!("workload must be a..f");
                    std::process::exit(2);
                })
            }
            "--ops" => a.ops = val.parse().expect("--ops"),
            "--keys" => a.keys = val.parse().expect("--keys"),
            "--clients" => a.clients = val.parse().expect("--clients"),
            "--value-bytes" => a.value_bytes = val.parse().expect("--value-bytes"),
            "--consistency" => {
                a.consistency = match val.as_str() {
                    "linearizable" => Consistency::Linearizable,
                    "lease" => Consistency::LeaderLease,
                    "stale" => Consistency::Stale,
                    _ => {
                        eprintln!("consistency: linearizable|lease|stale");
                        std::process::exit(2);
                    }
                }
            }
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
        i += 2;
    }
    if a.cluster.is_empty() {
        eprintln!("--cluster is required");
        std::process::exit(2);
    }
    a
}

fn make_value(rng: &mut u64, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    while v.len() < len {
        v.extend_from_slice(&splitmix(rng).to_le_bytes());
    }
    v.truncate(len);
    v
}

struct WorkerOut {
    per_kind: Vec<(OpKind, Latencies)>,
    errors: u64,
}

#[tokio::main]
async fn main() {
    let args = Arc::new(parse_args());
    let done = Arc::new(AtomicU64::new(0));
    // Workload D/E inserts append fresh keys after the loaded keyspace.
    let insert_cursor = Arc::new(AtomicU64::new(args.keys));

    if args.timeline {
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            let mut last = 0u64;
            let mut sec = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                sec += 1;
                let now = done.load(Ordering::Relaxed);
                println!("t={sec}s ops/sec={}", now - last);
                last = now;
            }
        });
    }

    let start = Instant::now();
    let mut handles = Vec::new();
    for w in 0..args.clients {
        let args = Arc::clone(&args);
        let done = Arc::clone(&done);
        let insert_cursor = Arc::clone(&insert_cursor);
        handles.push(tokio::spawn(async move {
            worker(w, args, done, insert_cursor).await
        }));
    }

    let mut per_kind: Vec<(OpKind, Latencies)> = Vec::new();
    let mut errors = 0u64;
    for h in handles {
        let out = h.await.expect("worker panicked");
        errors += out.errors;
        for (kind, lat) in out.per_kind {
            match per_kind.iter_mut().find(|(k, _)| *k == kind) {
                Some((_, agg)) => agg.merge(lat),
                None => per_kind.push((kind, lat)),
            }
        }
    }
    let elapsed = start.elapsed();

    let total: usize = per_kind.iter().map(|(_, l)| l.micros.len()).sum();
    println!(
        "\nphase={} workload={:?} clients={} consistency={:?}",
        args.phase, args.workload, args.clients, args.consistency
    );
    println!(
        "{} ops in {:.2}s = {:.0} ops/sec ({} errors)",
        total,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64(),
        errors
    );
    println!(
        "{:<8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "op", "count", "mean_us", "p50_us", "p95_us", "p99_us", "p999_us"
    );
    for (kind, lat) in per_kind.iter_mut() {
        lat.finalize();
        println!(
            "{:<8} {:>9} {:>9.0} {:>9} {:>9} {:>9} {:>9}",
            format!("{kind:?}"),
            lat.micros.len(),
            lat.mean(),
            lat.percentile(0.50),
            lat.percentile(0.95),
            lat.percentile(0.99),
            lat.percentile(0.999),
        );
    }
}

async fn worker(
    w: u64,
    args: Arc<Args>,
    done: Arc<AtomicU64>,
    insert_cursor: Arc<AtomicU64>,
) -> WorkerOut {
    let mut kv = KvClient::connect(ClientConfig::new(args.cluster.clone()));
    let mut rng = 0xBEEF ^ (w << 17) ^ kv.session_id();
    let zipf = Zipf::new(args.keys.max(1));
    let mut out = WorkerOut {
        per_kind: Vec::new(),
        errors: 0,
    };
    let mut record = |out: &mut WorkerOut, kind: OpKind, us: u64| {
        match out.per_kind.iter_mut().find(|(k, _)| *k == kind) {
            Some((_, l)) => l.record(us),
            None => {
                let mut l = Latencies::default();
                l.record(us);
                out.per_kind.push((kind, l));
            }
        }
    };

    if args.phase == "load" {
        // Partition [0, keys) across workers; sequential inserts.
        let per = args.keys / args.clients + 1;
        let lo = w * per;
        let hi = ((w + 1) * per).min(args.keys);
        for i in lo..hi {
            let value = make_value(&mut rng, args.value_bytes);
            let t0 = Instant::now();
            match kv.put(key_of(i), value).await {
                Ok(()) => record(&mut out, OpKind::Insert, t0.elapsed().as_micros() as u64),
                Err(_) => out.errors += 1,
            }
            done.fetch_add(1, Ordering::Relaxed);
        }
        return out;
    }

    let my_ops = args.ops / args.clients;
    for _ in 0..my_ops {
        let kind = args.workload.choose(&mut rng);
        let key_idx = scramble(zipf.next(&mut rng), args.keys.max(1));
        let t0 = Instant::now();
        let ok = match kind {
            OpKind::Read => {
                let key = if args.workload == Workload::D {
                    // read-latest: uniform over the most recent 1000 inserts
                    let hi = insert_cursor.load(Ordering::Relaxed);
                    let lo = hi.saturating_sub(1000);
                    key_of(lo + (splitmix(&mut rng) % (hi - lo).max(1)))
                } else {
                    key_of(key_idx)
                };
                kv.get(key, args.consistency).await.is_ok()
            }
            OpKind::Update => kv
                .put(key_of(key_idx), make_value(&mut rng, args.value_bytes))
                .await
                .is_ok(),
            OpKind::Insert => {
                let i = insert_cursor.fetch_add(1, Ordering::Relaxed);
                kv.put(key_of(i), make_value(&mut rng, args.value_bytes))
                    .await
                    .is_ok()
            }
            OpKind::Scan => {
                let len = 1 + (splitmix(&mut rng) % 100) as u32;
                kv.scan(key_of(key_idx), None, len, args.consistency)
                    .await
                    .is_ok()
            }
            OpKind::Rmw => {
                // Read-modify-write via CAS; a lost race counts as done
                // (YCSB counts the op, not the outcome).
                let key = key_of(key_idx);
                match kv.get(key.clone(), args.consistency).await {
                    Ok(cur) => kv
                        .cas(key, cur, Some(make_value(&mut rng, args.value_bytes)))
                        .await
                        .is_ok(),
                    Err(_) => false,
                }
            }
        };
        let us = t0.elapsed().as_micros() as u64;
        if ok {
            record(&mut out, kind, us);
        } else {
            out.errors += 1;
        }
        done.fetch_add(1, Ordering::Relaxed);
    }
    out
}
