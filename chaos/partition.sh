#!/usr/bin/env bash
# Partition / heal / degrade a node in the docker-compose cluster using
# tc-netem inside the container (requires NET_ADMIN, granted in compose).
#
#   chaos/partition.sh kv1              # drop 100% of kv1's traffic
#   chaos/partition.sh --heal kv1       # remove all shaping
#   chaos/partition.sh --slow kv1 200   # add 200ms +/-50ms delay
#   chaos/partition.sh --lossy kv1 20   # drop 20% of packets
set -euo pipefail

heal() { docker exec "chaos-$1-1" tc qdisc del dev eth0 root 2>/dev/null || true; }

case "${1:-}" in
  --heal)  heal "$2"; echo "$2 healed" ;;
  --slow)  heal "$2"; docker exec "chaos-$2-1" tc qdisc add dev eth0 root netem delay "${3:-200}ms" 50ms
           echo "$2 delayed ${3:-200}ms" ;;
  --lossy) heal "$2"; docker exec "chaos-$2-1" tc qdisc add dev eth0 root netem loss "${3:-20}%"
           echo "$2 dropping ${3:-20}%" ;;
  "")      echo "usage: $0 [--heal|--slow|--lossy] <kv1|kv2|kv3> [param]" >&2; exit 2 ;;
  *)       heal "$1"; docker exec "chaos-$1-1" tc qdisc add dev eth0 root netem loss 100%
           echo "$1 partitioned (100% loss)" ;;
esac
