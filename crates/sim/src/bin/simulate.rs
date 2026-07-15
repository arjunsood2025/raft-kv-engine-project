//! Seed-sweep runner: executes many simulated cluster runs and reports any
//! failing seeds. A failing seed reproduces its bug exactly:
//!
//!     cargo run --release -p sim --bin simulate -- --start <seed> --seeds 1
//!
//! Usage: simulate [--seeds N] [--start S] [--verbose]

use std::panic;

fn main() {
    let mut seeds: u64 = 100;
    let mut start: u64 = 0;
    let mut verbose = false;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seeds" => {
                seeds = args[i + 1].parse().expect("--seeds N");
                i += 2;
            }
            "--start" => {
                start = args[i + 1].parse().expect("--start S");
                i += 2;
            }
            "--verbose" => {
                verbose = true;
                i += 1;
            }
            other => panic!("unknown arg {other}"),
        }
    }

    let t0 = std::time::Instant::now();
    let mut failures: Vec<(u64, String)> = Vec::new();
    let mut total_ops: u64 = 0;
    let mut total_events: u64 = 0;

    for seed in start..start + seeds {
        let result = panic::catch_unwind(|| {
            let cfg = sim::SimConfig::from_seed(seed);
            sim::run(&cfg)
        });
        match result {
            Ok(report) => {
                total_ops += report.stats.ops_completed;
                total_events += report.stats.events;
                if verbose {
                    println!(
                        "seed {:>6} ok: {} events, {} ops, {} crashes, {} partitions, term {}, applied {}",
                        seed,
                        report.stats.events,
                        report.stats.ops_completed,
                        report.stats.crashes,
                        report.stats.partitions,
                        report.stats.max_term,
                        report.stats.final_applied
                    );
                }
            }
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "<non-string panic>".to_string());
                eprintln!("seed {seed} FAILED: {msg}");
                failures.push((seed, msg));
            }
        }
    }

    let dt = t0.elapsed();
    println!(
        "\n{} seeds in {:.1}s ({:.0} seeds/min) — {} events, {} client ops total",
        seeds,
        dt.as_secs_f64(),
        seeds as f64 / dt.as_secs_f64() * 60.0,
        total_events,
        total_ops
    );
    if failures.is_empty() {
        println!("all seeds passed");
    } else {
        println!("{} FAILING SEEDS:", failures.len());
        for (seed, msg) in &failures {
            let first = msg.lines().next().unwrap_or("");
            println!("  seed {seed}: {first}");
        }
        println!("\nreplay with: cargo run --release -p sim --bin simulate -- --start <seed> --seeds 1");
        std::process::exit(1);
    }
}
