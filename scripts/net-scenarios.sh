#!/usr/bin/env bash
# Apply real network conditions to the tailnet link and measure the library.
#
#   scripts/net-scenarios.sh <remote-host> <remote-ip> [entries] [cert-name]
#
# Expects a node already serving on <remote-ip>:7778. Off loopback it must use
# TLS; pass the name on its certificate as [cert-name].
#
# Shapes egress on both ends' tailscale0 with netem, so both directions cost
# what the profile says. Always clears the qdisc, including on failure.
set -uo pipefail

HOST="${1:?remote host}"
IP="${2:?remote ip}"
N="${3:-50}"
IFACE=tailscale0
PORT=7778
TARGET="$IP:$PORT${4:+=$4}"

qdisc() { # remote|<local>  add|del  profile
  local where="$1" action="$2" profile="$3"
  local cmd
  if [ "$action" = del ]; then
    cmd="sudo tc qdisc del dev $IFACE root 2>/dev/null; true"
  else
    cmd="sudo tc qdisc replace dev $IFACE root netem $profile"
  fi
  case "$where" in
    remote) ssh -n -o ConnectTimeout=10 "$HOST" "$cmd" >/dev/null 2>&1 ;;
    local)  bash -c "$cmd" >/dev/null 2>&1 ;;
  esac
}

clear_all() { qdisc remote del x; qdisc local del x; }
trap clear_all EXIT INT TERM

printf '%-12s %10s %10s %12s %12s\n' profile "append p50" "append p95" "appends/s" "end-to-end"

# Each profile: netem args for both ends.
run_profile() {
  local name="$1" spec="$2"
  clear_all
  if [ -n "$spec" ]; then
    qdisc remote add "$spec"
    qdisc local add "$spec"
  fi
  sleep 0.5
  local out
  out=$(timeout 240 ./target/release/examples/scenarios "$TARGET" "$N" "$name" 2>/dev/null | tail -1)
  if [ -z "$out" ]; then
    printf '%-12s %10s %10s %12s %12s\n' "$name" "-" "-" "-" "-"
    return
  fi
  IFS=, read -r label p50 p95 rate e2e <<<"$out"
  printf '%-12s %8s ms %8s ms %10s/s %10s ms\n' "$name" "$p50" "$p95" "$rate" "$e2e"
  clear_all
}

run_profile clean      ""
run_profile lossy-lan  "delay 2ms rate 1000mbit loss 5%"
run_profile lte        "delay 40ms 15ms rate 30mbit loss 1%"
run_profile dsl        "delay 25ms 10ms rate 8mbit loss 0.2%"
run_profile 3g         "delay 120ms 40ms rate 2mbit loss 2%"
run_profile satellite  "delay 400ms 50ms rate 3mbit loss 0.5%"
run_profile dialup     "delay 300ms 100ms rate 64kbit loss 3%"
