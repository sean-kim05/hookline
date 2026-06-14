# Load & chaos tests

Two harnesses, both runnable without external tools (a Go load generator and a
plain `bash` chaos script), plus a k6 script for those who have k6.

## Components

- **`sink/`** — a minimal delivery target. Counts deliveries and the set of
  unique event IDs (`GET /stats`), supports an artificial `-delay` (to keep work
  queued) and `-fail-until` (to force retries).
- **`loadgen/`** — submits N events with C concurrent workers and reports
  ingestion throughput and latency percentiles. Go equivalent of the k6 script.
- **`k6/ingest.js`** — k6 ingestion load test (`k6 run loadtest/k6/ingest.js`).
- **`chaos.sh`** — hard-kills hookline mid-flight and reports how many distinct
  events survived to delivery.

## Running the load test

```bash
go build -o bin/sink.exe ./loadtest/sink
go build -o bin/hookline.exe ./cmd/hookline
go build -o bin/loadgen.exe ./loadtest/loadgen

./bin/sink.exe -addr :9100 &
./bin/hookline.exe -addr :8080 -api-key dev-key -concurrency 64 &
./bin/loadgen.exe -url http://localhost:8080 -api-key dev-key \
  -target http://localhost:9100/hook -n 20000 -c 50
```

## Running the chaos test

```bash
loadtest/chaos.sh                      # in-memory backend (work is lost on crash)
loadtest/chaos.sh -wal-dir /tmp/hlwal  # WAL backend (zero loss on crash)
```

## Measured results

Measured on a Windows 11 dev box (single node, localhost sink), in-memory queue
unless noted. These are the numbers behind the project's claims — reproduce them
with the commands above; they are not pre-committed targets.

| Metric | Result |
| --- | --- |
| Ingestion throughput | ~40,000 events/sec accepted (single node) |
| Ingestion latency | p50 ~1 ms, p95 ~3 ms, p99 ~4 ms |
| End-to-end delivery | ~5,900 events/sec at concurrency 64 (ingest → sign → POST → ack) |
| Crash recovery (WAL) | 5,000 / 5,000 distinct events delivered across a `kill -9` |
| Crash recovery (in-memory) | 68 / 5,000 — the rest lost with the process |

The chaos contrast is the headline: under a hard kill mid-flight, the
WAL-backed queue lost **zero** events while the in-memory backend lost ~99% of
the still-queued work.
