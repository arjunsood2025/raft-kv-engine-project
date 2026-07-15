#!/usr/bin/env bash
# Start/stop/status for a local 3-node cluster (works in git-bash on
# Windows and any Linux shell). Data lives in ./data/, logs in ./data/*.log.
#
#   chaos/local-cluster.sh start [tick_ms]
#   chaos/local-cluster.sh stop
#   chaos/local-cluster.sh status
set -euo pipefail
cd "$(dirname "$0")/.."

PEERS="1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
CLUSTER="127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003"
KVD=./target/release/kvd
KVCTL=./target/release/kvctl
[ -f "$KVD.exe" ] && KVD="$KVD.exe" && KVCTL="$KVCTL.exe"

case "${1:-}" in
  start)
    TICK="${2:-100}"
    mkdir -p data
    : > data/pids.txt
    for i in 1 2 3; do
      "$KVD" --id "$i" --peers "$PEERS" \
        --client-listen "127.0.0.1:600$i" \
        --data-dir "data/node$i" \
        --metrics "127.0.0.1:910$i" \
        --tick-ms "$TICK" > "data/node$i.log" 2>&1 &
      echo "node$i $!" >> data/pids.txt
      echo "started node$i (pid $!)"
    done
    sleep 3
    "$KVCTL" --cluster "$CLUSTER" status
    ;;
  stop)
    while read -r _ pid; do kill -9 "$pid" 2>/dev/null || true; done < data/pids.txt
    echo "stopped"
    ;;
  status)
    "$KVCTL" --cluster "$CLUSTER" status
    ;;
  *)
    echo "usage: $0 start [tick_ms] | stop | status" >&2
    exit 2
    ;;
esac
