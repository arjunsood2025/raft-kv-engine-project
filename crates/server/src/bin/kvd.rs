//! kvd — one raft-kv node.
//!
//! ```text
//! kvd --id 1 \
//!     --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//!     --client-listen 127.0.0.1:6001 \
//!     --data-dir ./data/node1 \
//!     [--metrics 127.0.0.1:9101] [--snapshot-every 8192] [--tick-ms 100]
//! ```
//!
//! `--peers` lists the RAFT addresses of every voter including this node;
//! this node binds its own entry. Flags are hand-parsed on purpose — the
//! binary has no dependencies beyond the workspace crates.

use raft::Config;
use server::core::{Core, Event};
use server::metrics::{self, Metrics};
use server::net;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

struct Args {
    id: u64,
    peers: HashMap<u64, String>,
    client_listen: String,
    data_dir: String,
    metrics: Option<String>,
    snapshot_every: u64,
    tick_ms: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut id = None;
    let mut peers = HashMap::new();
    let mut client_listen = None;
    let mut data_dir = None;
    let mut metrics = None;
    let mut snapshot_every = 8192u64;
    let mut tick_ms = 100u64;

    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].as_str();
        let val = argv
            .get(i + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag {
            "--id" => id = Some(val.parse().map_err(|_| "--id must be a number")?),
            "--peers" => {
                for part in val.split(',') {
                    let (pid, addr) = part
                        .split_once('=')
                        .ok_or("--peers format: 1=host:port,2=host:port")?;
                    peers.insert(
                        pid.parse::<u64>().map_err(|_| "peer id must be a number")?,
                        addr.to_string(),
                    );
                }
            }
            "--client-listen" => client_listen = Some(val.clone()),
            "--data-dir" => data_dir = Some(val.clone()),
            "--metrics" => metrics = Some(val.clone()),
            "--snapshot-every" => {
                snapshot_every = val.parse().map_err(|_| "--snapshot-every must be a number")?
            }
            "--tick-ms" => tick_ms = val.parse().map_err(|_| "--tick-ms must be a number")?,
            other => return Err(format!("unknown flag {other}")),
        }
        i += 2;
    }
    Ok(Args {
        id: id.ok_or("--id is required")?,
        peers: if peers.is_empty() {
            return Err("--peers is required".into());
        } else {
            peers
        },
        client_listen: client_listen.ok_or("--client-listen is required")?,
        data_dir: data_dir.ok_or("--data-dir is required")?,
        metrics,
        snapshot_every,
        tick_ms,
    })
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("kvd: {e}");
            std::process::exit(2);
        }
    };
    if !args.peers.contains_key(&args.id) {
        eprintln!("kvd: --peers must include this node's own id/address");
        std::process::exit(2);
    }

    let metrics_state = Arc::new(Metrics::default());
    let (core_tx, core_rx) = mpsc::channel::<Event>(4096);

    // Outbound pipes to peers; inbound peer + client listeners.
    let peer_tx = net::spawn_peer_senders(args.id, &args.peers);
    let my_raft_addr = args.peers[&args.id].clone();
    tokio::spawn(net::peer_accept_loop(my_raft_addr, core_tx.clone()));
    tokio::spawn(net::client_accept_loop(
        args.client_listen.clone(),
        core_tx.clone(),
    ));
    if let Some(addr) = args.metrics.clone() {
        tokio::spawn(metrics::serve(addr, args.id, Arc::clone(&metrics_state)));
    }

    // Tick pump. Raft config: election timeout 10–20 ticks, heartbeat every
    // 2 — at 100 ms/tick that is a 1–2 s election timeout, 200 ms heartbeat.
    let tick_tx = core_tx.clone();
    let tick_ms = args.tick_ms;
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(std::time::Duration::from_millis(tick_ms));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            iv.tick().await;
            if tick_tx.send(Event::Tick).await.is_err() {
                return;
            }
        }
    });

    let mut voters: Vec<u64> = args.peers.keys().copied().collect();
    voters.sort();
    // Production entropy is fine here; only the SIMULATOR needs derived
    // seeds. The seed only randomizes election timeouts.
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64
        ^ (args.id << 32);

    let core = Core::open(
        std::path::Path::new(&args.data_dir),
        Config::new(args.id, seed),
        voters,
        args.snapshot_every,
        peer_tx,
        metrics_state,
    )
    .unwrap_or_else(|e| panic!("open data dir {}: {e:?}", args.data_dir));

    eprintln!(
        "[kvd {}] raft={} clients={} data={}",
        args.id, args.peers[&args.id], args.client_listen, args.data_dir
    );
    core.run(core_rx).await;
}
