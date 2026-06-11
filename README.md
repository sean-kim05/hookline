# Hookline

A self-hostable webhook delivery platform built on a from-scratch persistent
queue. No Redis. No RabbitMQ.

Producers POST events; Hookline guarantees **at-least-once delivery** to
consumer endpoints with:

- HMAC-signed payloads (Stripe-style `timestamp.body` signatures)
- Exponential backoff with full jitter
- Idempotency keys for consumer-side deduplication
- Lease-based work claiming with **fencing tokens** (no double-delivery races)
- Dead-letter queues and manual replay
- Prometheus/Grafana observability

## Status

🚧 **Week 4** — the delivery worker: leases ready messages, signs and POSTs
each to its endpoint, and acks on success, reschedules with exponential
backoff + full jitter on failure, or dead-letters once attempts are exhausted.

Earlier: the `Queue` interface with in-memory and PostgreSQL
(`SELECT ... FOR UPDATE SKIP LOCKED`) backends, both passing one shared
conformance suite (16 cases incl. a model-based randomized property test). See
[docs/DESIGN.md](docs/DESIGN.md) for the architecture and roadmap.

## Development

```sh
# Fast path: in-memory backend only (Postgres tests skip without a database).
go test ./...

# Full suite, including the Postgres backend, against a disposable database:
docker compose -f docker-compose.test.yml up -d
HOOKLINE_TEST_DATABASE_URL=postgres://hookline:hookline@localhost:5432/hookline go test ./...
docker compose -f docker-compose.test.yml down -v
```

CI runs the whole suite with the race detector (`go test -race`) against a
Postgres service container on every push.

## Architecture (short version)

Everything is built against a small `Queue` interface
(`Enqueue` / `Lease` / `Ack` / `Nack`), with three backends planned:

1. **In-memory** — reference implementation for tests and local dev ✅
2. **PostgreSQL** — `SELECT ... FOR UPDATE SKIP LOCKED`, the MVP backend
3. **Custom WAL** — segmented append-only log with consumer offsets and
   crash recovery; the reason this project exists

One shared conformance suite guarantees identical delivery semantics across
all three. The interesting design decisions (why at-least-once, why fencing
tokens, why the queue and the audit log live in different stores) are written
up in [docs/DESIGN.md](docs/DESIGN.md).
