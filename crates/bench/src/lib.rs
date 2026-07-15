//! YCSB-style workload machinery: key distributions, workload mixes, and
//! latency recording. The load generator binary (`loadgen`) drives these
//! against a live cluster through the smart client.

/// splitmix64 — same generator family the simulator uses.
pub fn splitmix(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

pub fn rand_f64(state: &mut u64) -> f64 {
    (splitmix(state) >> 11) as f64 / (1u64 << 53) as f64
}

/// YCSB's zipfian generator (Gray et al., "Quickly Generating
/// Billion-Record Synthetic Databases"). theta=0.99, the YCSB default:
/// hot keys get most of the traffic, which is what stresses contention.
pub struct Zipf {
    n: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipf {
    pub fn new(n: u64) -> Zipf {
        let theta = 0.99;
        let zetan = Self::zeta(n, theta);
        let zeta2 = Self::zeta(2, theta);
        Zipf {
            n,
            theta,
            alpha: 1.0 / (1.0 - theta),
            zetan,
            eta: (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan),
        }
    }

    fn zeta(n: u64, theta: f64) -> f64 {
        (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
    }

    /// Draw a key index in [0, n). Hot indexes are the small ones; callers
    /// hash the index into the keyspace so hot keys spread across nodes'
    /// sort order (YCSB does the same).
    pub fn next(&self, rng: &mut u64) -> u64 {
        let u = rand_f64(rng);
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        ((self.n as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64
    }
}

/// Scramble a zipf rank into the key space (fnv-ish) so hotness isn't
/// correlated with sort order.
pub fn scramble(rank: u64, n: u64) -> u64 {
    let mut x = rank.wrapping_mul(0xC6A4A7935BD1E995);
    x ^= x >> 33;
    x = x.wrapping_mul(0xFF51AFD7ED558CCD);
    x % n
}

pub fn key_of(i: u64) -> Vec<u8> {
    format!("user{i:012}").into_bytes()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OpKind {
    Read,
    Update,
    Insert,
    Scan,
    Rmw,
}

/// The six standard YCSB core workloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    /// A: 50% read / 50% update — update heavy.
    A,
    /// B: 95% read / 5% update — read heavy.
    B,
    /// C: 100% read.
    C,
    /// D: 95% read of recent keys / 5% insert — read latest.
    D,
    /// E: 95% short scans / 5% insert.
    E,
    /// F: 50% read / 50% read-modify-write (CAS).
    F,
}

impl Workload {
    pub fn parse(s: &str) -> Option<Workload> {
        match s.to_ascii_lowercase().as_str() {
            "a" => Some(Workload::A),
            "b" => Some(Workload::B),
            "c" => Some(Workload::C),
            "d" => Some(Workload::D),
            "e" => Some(Workload::E),
            "f" => Some(Workload::F),
            _ => None,
        }
    }

    pub fn choose(&self, rng: &mut u64) -> OpKind {
        let r = rand_f64(rng);
        match self {
            Workload::A => {
                if r < 0.5 {
                    OpKind::Read
                } else {
                    OpKind::Update
                }
            }
            Workload::B => {
                if r < 0.95 {
                    OpKind::Read
                } else {
                    OpKind::Update
                }
            }
            Workload::C => OpKind::Read,
            Workload::D => {
                if r < 0.95 {
                    OpKind::Read
                } else {
                    OpKind::Insert
                }
            }
            Workload::E => {
                if r < 0.95 {
                    OpKind::Scan
                } else {
                    OpKind::Insert
                }
            }
            Workload::F => {
                if r < 0.5 {
                    OpKind::Read
                } else {
                    OpKind::Rmw
                }
            }
        }
    }
}

/// Latency sink: microsecond samples, percentile report by sort. A sorted
/// vec is exact (unlike a fixed-bucket histogram) and cheap at bench scale.
#[derive(Default)]
pub struct Latencies {
    pub micros: Vec<u64>,
}

impl Latencies {
    pub fn record(&mut self, us: u64) {
        self.micros.push(us);
    }

    pub fn merge(&mut self, other: Latencies) {
        self.micros.extend(other.micros);
    }

    pub fn percentile(&self, q: f64) -> u64 {
        if self.micros.is_empty() {
            return 0;
        }
        let idx = ((self.micros.len() - 1) as f64 * q).round() as usize;
        self.micros[idx]
    }

    /// Sort once before reading percentiles.
    pub fn finalize(&mut self) {
        self.micros.sort_unstable();
    }

    pub fn mean(&self) -> f64 {
        if self.micros.is_empty() {
            return 0.0;
        }
        self.micros.iter().sum::<u64>() as f64 / self.micros.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zipf_skews_towards_hot_ranks() {
        let z = Zipf::new(10_000);
        let mut rng = 42u64;
        let mut hits_top10 = 0;
        let total = 100_000;
        for _ in 0..total {
            if z.next(&mut rng) < 10 {
                hits_top10 += 1;
            }
        }
        // With theta=0.99 over 10k keys, the top-10 ranks draw a large
        // constant fraction of traffic (empirically ~54% here); uniform
        // would give 0.1%.
        assert!(
            hits_top10 > total / 4,
            "zipf not skewed: top-10 got {hits_top10}/{total}"
        );
    }

    #[test]
    fn zipf_stays_in_range() {
        let z = Zipf::new(1000);
        let mut rng = 7u64;
        for _ in 0..10_000 {
            assert!(z.next(&mut rng) < 1000);
        }
    }

    #[test]
    fn percentiles() {
        let mut l = Latencies::default();
        for i in (1..=100).rev() {
            l.record(i);
        }
        l.finalize();
        assert_eq!(l.percentile(0.5), 51);
        assert_eq!(l.percentile(0.99), 99);
        assert_eq!(l.percentile(1.0), 100);
    }
}
