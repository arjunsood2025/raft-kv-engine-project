//! kvctl — command-line client.
//!
//! ```text
//! kvctl --cluster 127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003 <command>
//!
//! commands:
//!   put <key> <value>
//!   get <key> [--consistency linearizable|lease|stale]
//!   del <key>
//!   cas <key> <expect|-> <new|->      ('-' = absent/delete)
//!   scan <start> [end] [--limit N] [--consistency ...]
//!   status                            (asks every node)
//! ```

use client::{ClientConfig, KvClient};
use proto::{Consistency, Response};

fn parse_consistency(args: &mut Vec<String>) -> Consistency {
    if let Some(pos) = args.iter().position(|a| a == "--consistency") {
        if pos + 1 < args.len() {
            let v = args.remove(pos + 1);
            args.remove(pos);
            return match v.as_str() {
                "linearizable" => Consistency::Linearizable,
                "lease" => Consistency::LeaderLease,
                "stale" => Consistency::Stale,
                other => {
                    eprintln!("unknown consistency {other}");
                    std::process::exit(2);
                }
            };
        }
    }
    Consistency::Linearizable
}

fn parse_limit(args: &mut Vec<String>) -> u32 {
    if let Some(pos) = args.iter().position(|a| a == "--limit") {
        if pos + 1 < args.len() {
            let v = args.remove(pos + 1);
            args.remove(pos);
            return v.parse().unwrap_or_else(|_| {
                eprintln!("--limit must be a number");
                std::process::exit(2);
            });
        }
    }
    1000
}

fn opt_bytes(s: &str) -> Option<Vec<u8>> {
    if s == "-" {
        None
    } else {
        Some(s.as_bytes().to_vec())
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: kvctl --cluster host:port,host:port,... <put|get|del|cas|scan|status> [args]"
    );
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();

    let cluster = match args.iter().position(|a| a == "--cluster") {
        Some(pos) if pos + 1 < args.len() => {
            let v = args.remove(pos + 1);
            args.remove(pos);
            v.split(',').map(|s| s.to_string()).collect::<Vec<_>>()
        }
        _ => usage(),
    };
    let consistency = parse_consistency(&mut args);
    let limit = parse_limit(&mut args);
    if args.is_empty() {
        usage();
    }

    let cfg = ClientConfig::new(cluster.clone());
    let mut kv = KvClient::connect(cfg);

    let cmd = args.remove(0);
    let result: Result<(), Box<dyn std::error::Error>> = match (cmd.as_str(), args.as_slice()) {
        ("put", [k, v]) => kv
            .put(k.as_bytes().to_vec(), v.as_bytes().to_vec())
            .await
            .map(|_| println!("OK"))
            .map_err(Into::into),
        ("get", [k]) => kv
            .get(k.as_bytes().to_vec(), consistency)
            .await
            .map(|v| match v {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => println!("(nil)"),
            })
            .map_err(Into::into),
        ("del", [k]) => kv
            .delete(k.as_bytes().to_vec())
            .await
            .map(|_| println!("OK"))
            .map_err(Into::into),
        ("cas", [k, expect, new]) => kv
            .cas(k.as_bytes().to_vec(), opt_bytes(expect), opt_bytes(new))
            .await
            .map(|(ok, actual)| {
                println!(
                    "{} (actual: {})",
                    if ok { "SWAPPED" } else { "FAILED" },
                    actual
                        .map(|a| String::from_utf8_lossy(&a).into_owned())
                        .unwrap_or_else(|| "(nil)".into())
                )
            })
            .map_err(Into::into),
        ("scan", rest) if !rest.is_empty() => {
            let start = rest[0].as_bytes().to_vec();
            let end = rest.get(1).map(|e| e.as_bytes().to_vec());
            kv.scan(start, end, limit, consistency)
                .await
                .map(|kvs| {
                    for (k, v) in &kvs {
                        println!(
                            "{}\t{}",
                            String::from_utf8_lossy(k),
                            String::from_utf8_lossy(v)
                        );
                    }
                    println!("({} keys)", kvs.len());
                })
                .map_err(Into::into)
        }
        ("status", []) => {
            for (i, addr) in cluster.iter().enumerate() {
                match kv.status_of(addr).await {
                    Ok(Response::Status {
                        id,
                        role,
                        term,
                        leader,
                        commit,
                        applied,
                        last_log_index,
                        voters,
                    }) => println!(
                        "node {id} @ {addr}: {role} term={term} leader={leader:?} \
                         commit={commit} applied={applied} last_log={last_log_index} voters={voters:?}"
                    ),
                    Ok(other) => println!("node {} @ {addr}: unexpected {other:?}", i + 1),
                    Err(e) => println!("node {} @ {addr}: DOWN ({e})", i + 1),
                }
            }
            Ok(())
        }
        _ => usage(),
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
