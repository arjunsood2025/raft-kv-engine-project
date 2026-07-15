//! Smart client: leader routing, retries with backoff + jitter, and the
//! session/sequence discipline that makes retried writes safe.
//!
//! # Why retries are safe
//! Every write carries `(session_id, seq)`. If a write times out, the client
//! retries **the same seq** — possibly against a new leader. If the original
//! actually committed, the state machine's dedup table answers the retry
//! from cache instead of re-executing (see `kvsm`). Only after a definitive
//! response does the client advance `seq`. This converts "at-least-once
//! delivery" into "exactly-once apply" without pretending the network can
//! deliver exactly once.
//!
//! # Leader routing
//! By convention `addrs[i]` is the client address of node `i+1`, so a
//! `NotLeader { hint }` response lets the client jump straight to the
//! leader. With no hint (mid-election) it round-robins with backoff.

use proto::{read_frame, write_frame, Consistency, Request, Response};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Debug)]
pub enum ClientError {
    /// Retries exhausted (cluster unreachable or no leader for too long).
    Unavailable(String),
    /// The server rejected the request definitively.
    Server(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Unavailable(s) => write!(f, "unavailable: {s}"),
            ClientError::Server(s) => write!(f, "server error: {s}"),
        }
    }
}
impl std::error::Error for ClientError {}

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Client addresses ordered by node id (addrs[0] = node 1).
    pub addrs: Vec<String>,
    pub request_timeout: Duration,
    pub max_attempts: u32,
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
}

impl ClientConfig {
    pub fn new(addrs: Vec<String>) -> Self {
        // Env overrides exist for failover experiments (see chaos/):
        // aggressive retries find the new leader faster at the cost of
        // retry-storm pressure during the outage.
        let env_ms = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        ClientConfig {
            addrs,
            request_timeout: Duration::from_secs(8),
            max_attempts: env_ms("RAFTKV_MAX_ATTEMPTS", 12) as u32,
            backoff_base: Duration::from_millis(env_ms("RAFTKV_BACKOFF_BASE_MS", 50)),
            backoff_cap: Duration::from_millis(env_ms("RAFTKV_BACKOFF_CAP_MS", 2000)),
        }
    }
}

pub struct KvClient {
    cfg: ClientConfig,
    session_id: u64,
    seq: u64,
    /// Index into `addrs` we currently believe is the leader.
    target: usize,
    conn: Option<TcpStream>,
    rng: u64,
}

impl KvClient {
    /// `session_id` must be unique among concurrently-live clients; the
    /// convenience constructor derives one from time + pid + a process-wide
    /// counter. The counter matters: two clients created in the same clock
    /// tick (Windows reports time in 100ns units and batches of clients are
    /// created back-to-back) would otherwise collide, and a session
    /// collision silently serializes two clients through one dedup slot —
    /// every op of the "loser" is answered `Stale`. Found live by the load
    /// generator: exactly one worker's share of ops failed.
    pub fn connect(cfg: ClientConfig) -> KvClient {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let session_id =
            splitmix(nanos ^ ((std::process::id() as u64) << 32) ^ (n << 48) ^ n) | 1;
        KvClient::with_session(cfg, session_id)
    }

    pub fn with_session(cfg: ClientConfig, session_id: u64) -> KvClient {
        assert!(session_id != 0, "session 0 is reserved");
        KvClient {
            rng: splitmix(session_id),
            cfg,
            session_id,
            seq: 0,
            target: 0,
            conn: None,
        }
    }

    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    // ------------------------------------------------------------- api

    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.seq += 1;
        let (session_id, seq) = (self.session_id, self.seq);
        match self
            .call(Request::Put {
                session_id,
                seq,
                key,
                value,
            })
            .await?
        {
            Response::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.seq += 1;
        let (session_id, seq) = (self.session_id, self.seq);
        match self
            .call(Request::Delete {
                session_id,
                seq,
                key,
            })
            .await?
        {
            Response::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// Returns (success, actual value at apply time).
    pub async fn cas(
        &mut self,
        key: Vec<u8>,
        expect: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    ) -> Result<(bool, Option<Vec<u8>>)> {
        self.seq += 1;
        let (session_id, seq) = (self.session_id, self.seq);
        match self
            .call(Request::Cas {
                session_id,
                seq,
                key,
                expect,
                new,
            })
            .await?
        {
            Response::Cas { success, actual } => Ok((success, actual)),
            other => Err(unexpected(other)),
        }
    }

    pub async fn get(&mut self, key: Vec<u8>, consistency: Consistency) -> Result<Option<Vec<u8>>> {
        match self.call(Request::Get { key, consistency }).await? {
            Response::Value(v) => Ok(v),
            other => Err(unexpected(other)),
        }
    }

    pub async fn scan(
        &mut self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
        consistency: Consistency,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self
            .call(Request::Scan {
                start,
                end,
                limit,
                consistency,
            })
            .await?
        {
            Response::Kvs(kvs) => Ok(kvs),
            other => Err(unexpected(other)),
        }
    }

    /// Status of one specific node (no leader routing).
    pub async fn status_of(&mut self, addr: &str) -> Result<Response> {
        let mut s = TcpStream::connect(addr)
            .await
            .map_err(|e| ClientError::Unavailable(format!("{addr}: {e}")))?;
        write_frame(&mut s, &Request::Status)
            .await
            .map_err(|e| ClientError::Unavailable(e.to_string()))?;
        match read_frame::<_, Response>(&mut s).await {
            Ok(Some(r)) => Ok(r),
            Ok(None) => Err(ClientError::Unavailable("connection closed".into())),
            Err(e) => Err(ClientError::Unavailable(e.to_string())),
        }
    }

    // -------------------------------------------------------- retry core

    /// Send one request with leader routing + retry. Stale reads are served
    /// by whichever node we hit; everything else chases the leader.
    async fn call(&mut self, req: Request) -> Result<Response> {
        let mut last_err = String::new();
        for attempt in 0..self.cfg.max_attempts {
            if attempt > 0 {
                self.backoff(attempt).await;
            }
            match self.try_once(&req).await {
                Ok(Response::NotLeader { hint }) => {
                    self.conn = None;
                    match hint {
                        // node ids are 1-based; addrs is 0-based.
                        Some(id) if (id as usize) <= self.cfg.addrs.len() && id >= 1 => {
                            self.target = (id - 1) as usize;
                        }
                        _ => self.next_target(),
                    }
                    last_err = "not leader".into();
                }
                Ok(Response::Retry { reason }) => {
                    // Same node, transient (election / lease / commit wait).
                    last_err = reason;
                }
                Ok(Response::Err(e)) => return Err(ClientError::Server(e)),
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    self.conn = None;
                    self.next_target();
                    last_err = e;
                }
            }
        }
        Err(ClientError::Unavailable(format!(
            "no definitive response after {} attempts (last: {last_err})",
            self.cfg.max_attempts
        )))
    }

    async fn try_once(&mut self, req: &Request) -> std::result::Result<Response, String> {
        let fut = async {
            if self.conn.is_none() {
                let addr = &self.cfg.addrs[self.target];
                let s = TcpStream::connect(addr)
                    .await
                    .map_err(|e| format!("connect {addr}: {e}"))?;
                let _ = s.set_nodelay(true);
                self.conn = Some(s);
            }
            let s = self.conn.as_mut().unwrap();
            write_frame(s, req).await.map_err(|e| e.to_string())?;
            match read_frame::<_, Response>(s).await {
                Ok(Some(r)) => Ok(r),
                Ok(None) => Err("connection closed".into()),
                Err(e) => Err(e.to_string()),
            }
        };
        match timeout(self.cfg.request_timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err("request timed out".into()),
        }
    }

    fn next_target(&mut self) {
        self.target = (self.target + 1) % self.cfg.addrs.len();
    }

    /// Exponential backoff with full jitter (sleep a uniform fraction of the
    /// capped exponential window — avoids retry stampedes after failover).
    async fn backoff(&mut self, attempt: u32) {
        let exp = self
            .cfg
            .backoff_base
            .saturating_mul(1u32 << attempt.min(6))
            .min(self.cfg.backoff_cap);
        self.rng = splitmix(self.rng);
        let frac = (self.rng >> 11) as f64 / (1u64 << 53) as f64;
        tokio::time::sleep(exp.mul_f64(frac.max(0.1))).await;
    }
}

fn unexpected(r: Response) -> ClientError {
    ClientError::Server(format!("unexpected response: {r:?}"))
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}
