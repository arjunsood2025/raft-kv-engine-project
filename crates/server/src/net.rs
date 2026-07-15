//! Networking edge: peer dial/accept loops and client connection handling.
//!
//! Topology: every node dials every peer and keeps one outbound pipe open;
//! raft responses travel over the responder's own outbound pipe, not back
//! down the inbound one. That matches the raft core's model (messages are
//! addressed by NodeId, not by connection) and makes reconnect trivial —
//! there is no connection state to reconcile, and message loss during a
//! reconnect is something consensus already tolerates by design.

use crate::core::Event;
use proto::{read_frame, write_frame, PeerFrame, Request};
use raft::{Message, NodeId};
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Instant};

/// Per-peer outbound queue depth. Full queue = drop (raft tolerates loss);
/// backpressuring the core on a dead peer would stall consensus.
const PEER_QUEUE: usize = 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
/// Minimum spacing between reconnect attempts to a down peer.
const RECONNECT_COOLDOWN: Duration = Duration::from_millis(500);

/// Spawn the outbound pipe to one peer; returns the sender the core uses.
pub fn spawn_peer_sender(my_id: NodeId, peer_addr: String) -> mpsc::Sender<Message> {
    let (tx, mut rx) = mpsc::channel::<Message>(PEER_QUEUE);
    tokio::spawn(async move {
        let mut conn: Option<TcpStream> = None;
        let mut last_attempt: Option<Instant> = None;
        while let Some(msg) = rx.recv().await {
            if conn.is_none() {
                // Rate-limit dialing so a dead peer costs one attempt per
                // cooldown, not one per heartbeat.
                if last_attempt.map_or(true, |t| t.elapsed() >= RECONNECT_COOLDOWN) {
                    last_attempt = Some(Instant::now());
                    if let Ok(Ok(mut s)) =
                        timeout(CONNECT_TIMEOUT, TcpStream::connect(&peer_addr)).await
                    {
                        let _ = s.set_nodelay(true);
                        if write_frame(&mut s, &PeerFrame::Hello { from: my_id })
                            .await
                            .is_ok()
                        {
                            conn = Some(s);
                        }
                    }
                }
            }
            if let Some(s) = conn.as_mut() {
                if write_frame(s, &PeerFrame::Msg(msg)).await.is_err() {
                    conn = None; // message dropped; raft will retransmit
                }
            }
            // No connection: message silently dropped — correct for raft.
        }
    });
    tx
}

/// Accept inbound peer connections and pump their messages into the core.
pub async fn peer_accept_loop(listen: String, core_tx: mpsc::Sender<Event>) {
    let listener = TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|e| panic!("bind peer listener {listen}: {e}"));
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        let _ = sock.set_nodelay(true);
        let core_tx = core_tx.clone();
        tokio::spawn(async move {
            // First frame must be Hello (identifies the peer; useful in logs).
            let from = match read_frame::<_, PeerFrame>(&mut sock).await {
                Ok(Some(PeerFrame::Hello { from })) => from,
                _ => return,
            };
            loop {
                match read_frame::<_, PeerFrame>(&mut sock).await {
                    Ok(Some(PeerFrame::Msg(m))) => {
                        if core_tx.send(Event::Peer(m)).await.is_err() {
                            return; // core shut down
                        }
                    }
                    Ok(Some(PeerFrame::Hello { .. })) => continue,
                    Ok(None) | Err(_) => {
                        // Peer went away; it will redial us.
                        let _ = from;
                        return;
                    }
                }
            }
        });
    }
}

/// Accept client connections. One request in flight per connection: read a
/// Request, ask the core, write the Response. Clients pipeline by opening
/// multiple connections (the bench client does exactly that).
pub async fn client_accept_loop(listen: String, core_tx: mpsc::Sender<Event>) {
    let listener = TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|e| panic!("bind client listener {listen}: {e}"));
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        let _ = sock.set_nodelay(true);
        let core_tx = core_tx.clone();
        tokio::spawn(async move {
            loop {
                let req = match read_frame::<_, Request>(&mut sock).await {
                    Ok(Some(r)) => r,
                    Ok(None) | Err(_) => return,
                };
                let (tx, rx) = oneshot::channel();
                if core_tx.send(Event::Client(req, tx)).await.is_err() {
                    return;
                }
                // A dropped sender (commit timeout sweep) surfaces as Err —
                // tell the client to retry; its session/seq makes that safe.
                let resp = rx.await.unwrap_or(proto::Response::Retry {
                    reason: "request timed out waiting for commit".into(),
                });
                if write_frame(&mut sock, &resp).await.is_err() {
                    return;
                }
            }
        });
    }
}

/// Build the outbound pipes for every peer except ourselves.
pub fn spawn_peer_senders(
    my_id: NodeId,
    peers: &HashMap<NodeId, String>,
) -> HashMap<NodeId, mpsc::Sender<Message>> {
    peers
        .iter()
        .filter(|(id, _)| **id != my_id)
        .map(|(id, addr)| (*id, spawn_peer_sender(my_id, addr.clone())))
        .collect()
}
