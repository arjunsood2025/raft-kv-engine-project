//! Hand-rolled Prometheus text-format endpoint. No HTTP crate: we accept a
//! TCP connection, ignore the request bytes, and write one HTTP/1.0 response
//! with the metrics body — which is all a Prometheus scraper needs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

#[derive(Default)]
pub struct Metrics {
    pub proposals_total: AtomicU64,
    pub applied_total: AtomicU64,
    pub reads_linearizable_total: AtomicU64,
    pub reads_lease_total: AtomicU64,
    pub reads_stale_total: AtomicU64,
    pub not_leader_total: AtomicU64,
    pub retries_total: AtomicU64,
    /// Gauges, stamped by the core each tick.
    pub term: AtomicU64,
    pub commit_index: AtomicU64,
    pub applied_index: AtomicU64,
    pub is_leader: AtomicU64,
    pub snapshots_taken_total: AtomicU64,
}

impl Metrics {
    pub fn inc(field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }
    pub fn set(field: &AtomicU64, v: u64) {
        field.store(v, Ordering::Relaxed);
    }

    fn render(&self, node_id: u64) -> String {
        let l = format!("{{node=\"{node_id}\"}}");
        let g = |v: &AtomicU64| v.load(Ordering::Relaxed);
        format!(
            "# TYPE raftkv_proposals_total counter\n\
             raftkv_proposals_total{l} {}\n\
             # TYPE raftkv_applied_total counter\n\
             raftkv_applied_total{l} {}\n\
             # TYPE raftkv_reads_total counter\n\
             raftkv_reads_total{{node=\"{node_id}\",mode=\"linearizable\"}} {}\n\
             raftkv_reads_total{{node=\"{node_id}\",mode=\"lease\"}} {}\n\
             raftkv_reads_total{{node=\"{node_id}\",mode=\"stale\"}} {}\n\
             # TYPE raftkv_not_leader_total counter\n\
             raftkv_not_leader_total{l} {}\n\
             # TYPE raftkv_retries_total counter\n\
             raftkv_retries_total{l} {}\n\
             # TYPE raftkv_term gauge\n\
             raftkv_term{l} {}\n\
             # TYPE raftkv_commit_index gauge\n\
             raftkv_commit_index{l} {}\n\
             # TYPE raftkv_applied_index gauge\n\
             raftkv_applied_index{l} {}\n\
             # TYPE raftkv_apply_lag gauge\n\
             raftkv_apply_lag{l} {}\n\
             # TYPE raftkv_is_leader gauge\n\
             raftkv_is_leader{l} {}\n\
             # TYPE raftkv_snapshots_taken_total counter\n\
             raftkv_snapshots_taken_total{l} {}\n",
            g(&self.proposals_total),
            g(&self.applied_total),
            g(&self.reads_linearizable_total),
            g(&self.reads_lease_total),
            g(&self.reads_stale_total),
            g(&self.not_leader_total),
            g(&self.retries_total),
            g(&self.term),
            g(&self.commit_index),
            g(&self.applied_index),
            g(&self.commit_index).saturating_sub(g(&self.applied_index)),
            g(&self.is_leader),
            g(&self.snapshots_taken_total),
        )
    }
}

/// Serve `GET /metrics` forever. Spawned as its own task.
pub async fn serve(addr: String, node_id: u64, metrics: Arc<Metrics>) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[metrics] bind {addr} failed: {e}");
            return;
        }
    };
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        // Read (and discard) the request before responding; replying first
        // can RST the connection while the client is still sending.
        let mut buf = [0u8; 1024];
        use tokio::io::AsyncReadExt;
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            sock.read(&mut buf),
        )
        .await;
        let body = metrics.render(node_id);
        let resp = format!(
            "HTTP/1.0 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(resp.as_bytes()).await;
        let _ = sock.shutdown().await;
    }
}
