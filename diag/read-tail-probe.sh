#!/usr/bin/env bash
# Per-target read-latency probe — run INSIDE a pg container via docker exec.
# Opens ONE psql session against HOST:PORT and times N balance-shaped
# SELECTs over random account_numbers (ACC_0000001..ACC_0100000), printing
# one "Time: X ms" line per query. MODE=select1 swaps the row fetch for
# SELECT 1 to isolate transport/proxy cost from index/heap access.
#
# Usage: read-tail-probe.sh HOST PORT N [MODE]
set -euo pipefail
HOST="$1"; PORT="$2"; N="${3:-200}"; MODE="${4:-row}"
PW="${PGPASSWORD:-CHANGEME}"

{
  echo "\\timing on"
  for i in $(seq 1 "$N"); do
    if [ "$MODE" = "select1" ]; then
      echo "SELECT 1;"
    else
      n=$(( (RANDOM * 32768 + RANDOM) % 100000 + 1 ))
      printf "SELECT account_number, balance, status FROM users WHERE account_number = 'ACC_%07d' AND status = 'active';\n" "$n"
    fi
  done
} > /tmp/probe.sql

PGPASSWORD="$PW" psql -h "$HOST" -p "$PORT" -U peakload_user -d peakload_db \
  -q -At -f /tmp/probe.sql 2>&1 | grep '^Time:'
