//! Simulation test battery. The heavy seed sweeps run via the `simulate`
//! binary in release mode; these tests keep CI honest with a smaller sweep,
//! a strict determinism check, and the regression seed corpus.

use sim::{run, SimConfig};

#[test]
fn seed_sweep_passes() {
    // Each seed derives its own fault profile (drop rate, partitions,
    // crashes). 30 seeds ≈ 10 virtual minutes of cluster time under chaos,
    // each run ending in a full linearizability + convergence check.
    for seed in 0..30 {
        let cfg = SimConfig::from_seed(seed);
        let report = run(&cfg);
        assert!(report.stats.ops_completed > 0, "seed {seed} completed no ops");
    }
}

#[test]
fn same_seed_is_bit_identical() {
    // THE deterministic-simulation property: a run is a pure function of
    // its seed. If this ever fails, replay-debugging is broken and every
    // other guarantee is built on sand.
    let cfg = SimConfig::from_seed(12345);
    let a = run(&cfg);
    let b = run(&cfg);
    assert_eq!(a.stats, b.stats, "stats diverged between identical seeds");
    assert_eq!(a.history, b.history, "histories diverged between identical seeds");
    assert_eq!(a.final_state, b.final_state, "final state diverged");
}

#[test]
fn regression_seeds_pass() {
    // Every seed that ever exposed a bug lives in regressions/seeds.txt
    // forever. A reintroduced bug fails here with its original seed.
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/regressions/seeds.txt");
    let text = std::fs::read_to_string(path).expect("read regression corpus");
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let seed: u64 = line
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .expect("seed line");
        let cfg = SimConfig::from_seed(seed);
        run(&cfg);
    }
}
