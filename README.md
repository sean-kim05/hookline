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

🚧 **Week 1** — queue core abstractions, in-memory reference implementation,
and the test suite that all storage backends must pass. See
[docs/DESIGN.md](docs/DESIGN.md) for the architecture and roadmap.

## Development

```sh
go test -race ./...
```

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
