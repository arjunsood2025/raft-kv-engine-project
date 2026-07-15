#!/usr/bin/env bash
# Kill the current leader (kill -9 — no clean shutdown), measure how long
# until the next write commits, then restart the node. Repeats N times and
# prints the distribution. Run against a cluster started by local-cluster.sh.
#
#   chaos/kill-leader.sh [rounds] [tick_ms]
set -euo pipefail
cd "$(dirname "$0")/.."

ROUNDS="${1:-5}"
TICK="${2:-100}"
PEERS="1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
CLUSTER="127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003"
KVD=./target/release/kvd
KVCTL=./target/release/kvctl
[ -f "$KVD.exe" ] && KVD="$KVD.exe" && KVCTL="$KVCTL.exe"

# Aggressive client retries: we are measuring the cluster, not the backoff.
export RAFTKV_BACKOFF_BASE_MS=10 RAFTKV_BACKOFF_CAP_MS=200 RAFTKV_MAX_ATTEMPTS=60

times=()
for round in $(seq 1 "$ROUNDS"); do
  LEADER=$("$KVCTL" --cluster "$CLUSTER" status 2>/dev/null \
    | grep -m1 " Leader " | sed 's/node \([0-9]*\).*/\1/')
  PID=$(awk -v n="node$LEADER" '$1==n {print $2}' data/pids.txt | tail -1)
  kill -9 "$PID" 2>/dev/null || true
  START=$(date +%s%N)
  "$KVCTL" --cluster "$CLUSTER" put "chaos-$round" ok > /dev/null 2>&1
  END=$(date +%s%N)
  MS=$(( (END-START)/1000000 ))
  times+=("$MS")
  echo "round $round: killed leader node$LEADER, next write committed in ${MS} ms"
  "$KVD" --id "$LEADER" --peers "$PEERS" \
    --client-listen "127.0.0.1:600$LEADER" \
    --data-dir "data/node$LEADER" --tick-ms "$TICK" \
    >> "data/node$LEADER.log" 2>&1 &
  echo "node$LEADER $!" >> data/pids.txt
  sleep 2
done

printf '%s\n' "${times[@]}" | sort -n | awk '
  { a[NR]=$1 } END {
    printf "min=%dms median=%dms max=%dms (n=%d)\n", a[1], a[int((NR+1)/2)], a[NR], NR
  }'
