#!/usr/bin/env bash
# Chaos test: prove at-least-once delivery survives a hard crash.
#
# Submits N events to a deliberately slow sink so most stay queued, hard-kills
# the running hookline (kill -9) mid-flight, restarts it against the same
# storage, and checks how many distinct events were ultimately delivered.
#
# With -wal-dir (or a database) the queue is durable, so recovery delivers every
# event: unique == N. With the in-memory backend the queued work is lost on the
# crash, so unique << N. That contrast is the point.
#
# Usage:
#   loadtest/chaos.sh                 # in-memory backend (expect loss)
#   loadtest/chaos.sh -wal-dir /tmp/hlwal   # WAL backend (expect zero loss)
set -euo pipefail
cd "$(dirname "$0")/.."

N=${N:-5000}
go build -o bin/sink.exe ./loadtest/sink
go build -o bin/loadgen.exe ./loadtest/loadgen
go build -o bin/hookline.exe ./cmd/hookline

./bin/sink.exe -addr :9100 -delay 40ms >/tmp/hl-sink.log 2>&1 &
SINK=$!
trap 'kill $SINK 2>/dev/null || true' EXIT

./bin/hookline.exe -addr :8080 -api-key dev-key -concurrency 8 "$@" >/tmp/hl-a.log 2>&1 &
HL=$!
sleep 1.2

./bin/loadgen.exe -url http://localhost:8080 -api-key dev-key \
  -target http://localhost:9100/hook -n "$N" -c 40 >/dev/null
sleep 1.0

echo "hard-killing hookline (kill -9) with work still queued..."
kill -9 "$HL" 2>/dev/null || true
wait "$HL" 2>/dev/null || true

echo "restarting against the same storage..."
./bin/hookline.exe -addr :8080 -api-key dev-key -concurrency 64 "$@" >/tmp/hl-b.log 2>&1 &
HL2=$!

# Wait for delivery to settle, then report.
sleep 8
UNIQUE=$(curl -s localhost:9100/stats | sed -E 's/.*"unique":([0-9]+).*/\1/')
kill "$HL2" 2>/dev/null || true
echo "RESULT: delivered ${UNIQUE}/${N} distinct events after crash recovery"
