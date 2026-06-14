# Hookline

A self-hostable webhook delivery platform built on a **from-scratch persistent
queue**. No Redis. No RabbitMQ. No Kafka.

Producers POST events; Hookline guarantees **at-least-once delivery** to
consumer endpoints, with the operational machinery a real webhook service needs:

- **From-scratch durable queue** — a write-ahead, segmented log with CRC framing,
  crash recovery, and compaction, behind the same interface as the in-memory and
  PostgreSQL backends.
- **At-least-once delivery** with lease-based work claiming and **fencing tokens**
  (no double-delivery races when a lease expires).
- **HMAC-signed payloads** (Stripe-style `timestamp.body` signatures) with a
  **per-endpoint signing secret** minted by the endpoint registry.
- **Exponential backoff with full jitter**, **dead-letter queue**, and **replay**.
- **Per-endpoint rate limiting and circuit breakers**.
- **Idempotency keys** for consumer-side deduplication.
- **Prometheus metrics + Grafana dashboard**, structured logs, graceful shutdown.
- **Operator dashboard** (Next.js) to watch deliveries, inspect/replay the DLQ,
  and register endpoints.

## Status

✅ **Feature-complete.** All ten roadmap milestones are implemented and tested.
See [docs/DESIGN.md](docs/DESIGN.md) for the architecture and
[docs/WRITEUP.md](docs/WRITEUP.md) for the engineering narrative and measured
results.

## Quickstart

The fastest way to see the whole thing — service on PostgreSQL, Prometheus, and a
pre-provisioned Grafana dashboard:

```sh
docker compose up -d --build
# API:        http://localhost:8080
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000  (admin/admin)
```

Or run it locally with zero dependencies (in-memory backend):

```sh
# Terminal 1 — a consumer endpoint that verifies the signature
go run ./examples/receiver -secret whsec_dev

# Terminal 2 — the service
go run ./cmd/hookline -api-key dev-key -secret whsec_dev
```

Submit an event:

```sh
curl -X POST localhost:8080/v1/events \
  -H "Authorization: Bearer dev-key" \
  -H "Content-Type: application/json" \
  -d '{"endpoint":"http://localhost:9099/hook","payload":{"hello":"world"}}'
```

### Storage backends

| Flag | Backend | Durable |
| --- | --- | --- |
| *(default)* | in-memory | no |
| `-wal-dir DIR` | from-scratch WAL queue | queue only |
| `-database-url DSN` | PostgreSQL (queue, audit, registry, DLQ) | fully |

### API

| Method & path | Purpose |
| --- | --- |
| `POST /v1/events` | submit an event for delivery |
| `POST /v1/endpoints` | register an endpoint (returns its signing secret once) |
| `GET /v1/endpoints` | list endpoints (never returns secrets) |
| `GET /v1/deliveries` | recent delivery attempts (filter by `event_id`, `outcome`) |
| `GET /v1/dlq` | dead-letter queue |
| `POST /v1/dlq/{id}/replay` | re-enqueue a dead-lettered event |
| `GET /healthz` | health check (unauthenticated) |
| `GET /metrics` | Prometheus metrics (unauthenticated) |

## Measured results

Single node, localhost, in-memory queue unless noted — reproduce with the
harness in [`loadtest/`](loadtest/README.md):

- **~40,000 events/sec** ingestion throughput (p50 ~1 ms, p95 ~3 ms, p99 ~4 ms).
- **~5,900 events/sec** end-to-end delivery at concurrency 64.
- **Zero loss across a `kill -9`**: the WAL backend delivered **5,000/5,000**
  distinct events after a hard crash mid-flight; the in-memory backend delivered
  only 68.

## Development

```sh
# Fast path: in-memory + WAL backends (Postgres tests skip without a database).
go test ./...

# Full suite incl. the Postgres backend, against a disposable database:
docker compose -f docker-compose.test.yml up -d
HOOKLINE_TEST_DATABASE_URL=postgres://hookline:hookline@localhost:5432/hookline go test ./...
docker compose -f docker-compose.test.yml down -v

# Dashboard
cd dashboard && npm install && npm run dev   # http://localhost:3001
```

CI runs the whole Go suite with the race detector against a Postgres service
container, and builds + Playwright-tests the dashboard, on every push.

## Architecture (short version)

Everything is built against a small `Queue` interface
(`Enqueue` / `Lease` / `Ack` / `Nack`), with three backends:

1. **In-memory** — reference implementation for tests and local dev.
2. **PostgreSQL** — `SELECT ... FOR UPDATE SKIP LOCKED`, the easy durable option.
3. **Custom WAL** — segmented append-only log with CRC framing, crash recovery,
   and compaction; the reason this project exists.

One shared conformance suite (16 cases incl. a model-based randomized property
test and a concurrent no-double-claim stress test) runs unmodified against all
three, so delivery semantics are provably identical. The design decisions — why
at-least-once, why fencing tokens, why the queue and the audit log live in
different stores — are written up in [docs/DESIGN.md](docs/DESIGN.md).
