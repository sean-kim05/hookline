# Hookline Design

**Status:** Week 1 draft — core abstractions and delivery semantics.
**Author:** Sean Kim

Hookline is a self-hostable webhook delivery service: producers POST events,
Hookline guarantees **at-least-once** delivery to consumer endpoints with
HMAC-signed payloads, exponential backoff with jitter, idempotency keys,
dead-letter queues, and a replay UI. The durable queue at its core is
hand-built — no Redis, no RabbitMQ.

## 1. Delivery semantics: at-least-once, on purpose

Exactly-once delivery over HTTP to arbitrary third-party endpoints is not
achievable: an endpoint can process a request and crash before responding, and
the sender cannot distinguish that from a failure. Hookline therefore promises:

- **At-least-once:** an acknowledged event is never lost. It may be delivered
  more than once.
- **Idempotency keys:** every delivery carries the event's idempotency key in a
  header so consumers can deduplicate. This is the industry-standard contract
  (Stripe, Svix, GitHub webhooks all work this way).

Everything in the system is designed backward from this contract.

## 2. The Queue interface is the keystone

The headline component is a from-scratch WAL-backed persistent queue. It is
also the highest-risk component. So Hookline is built against a small `Queue`
interface (`internal/queue`) with three planned implementations:

| Implementation | Purpose | Status |
|---|---|---|
| `MemoryQueue` | Reference implementation; used by tests and local dev | Done (week 1) |
| `PostgresQueue` | MVP backend — `SELECT ... FOR UPDATE SKIP LOCKED` | Done (week 2-3) |
| `WALQueue` | The headline: segmented append-only log + offsets | Planned (week 5-7) |

All implementations must pass one shared conformance test suite. If the WAL
engine slips, the product still ships on Postgres and the WAL becomes a
follow-up — a failed component, not a failed project.

### Queue contract

- `Enqueue(event) -> id` — durably accept an event.
- `Lease(n, leaseFor) -> []Lease` — atomically claim up to `n` ready messages.
  A lease is exclusive and time-bounded; if the worker dies, the lease lapses
  and the message becomes available again (this is what makes delivery
  at-least-once rather than at-most-once).
- `Ack(id, token)` — delivery succeeded; remove the message.
- `Nack(id, token, retryAfter)` — delivery failed; make the message ready
  again after a backoff delay.

### Fencing tokens

Every lease carries a monotonically increasing **fencing token**, and `Ack` /
`Nack` are rejected with `ErrStaleLease` unless the token matches the
message's current lease.

Why: consider worker A leasing message M, stalling (GC pause, network
partition, slow endpoint) past its lease expiry. M is re-leased to worker B.
Without fencing, A's late `Ack` could delete M while B is mid-delivery, or
A's late `Nack` could clobber B's retry schedule. With fencing, A's token is
stale and its calls are rejected. The worker treats `ErrStaleLease` as "my
work no longer counts" and moves on.

This is the same fencing-token pattern used in distributed lock services
(Chubby/ZooKeeper lineage), applied per-message.

## 3. Retry policy

- Exponential backoff with full jitter: `delay = rand(0, base * 2^attempt)`,
  capped at a maximum delay. Jitter prevents synchronized retry storms when an
  endpoint comes back after an outage.
- After a configurable max attempts, the message moves to a **dead-letter
  queue** instead of being dropped. DLQ'd events are visible in the dashboard
  and can be replayed manually.

## 4. Storage split: queue vs. audit log

Two different jobs, two different stores:

- **The queue** (custom WAL, eventually) holds only *pending* work. Its needs
  are write-path latency and crash-recovery invariants, not queryability.
- **The delivery audit log** lives in PostgreSQL: every attempt, response code,
  and latency, with proper migrations, time-range indexes, and a retention
  policy. Its needs are queryability (dashboard, replay, debugging), not
  ingest speed.

## 5. Security

- Producers authenticate with API keys.
- Deliveries are signed: `X-Hookline-Signature: hmac-sha256(secret, timestamp + "." + body)`,
  with the timestamp echoed in a header to allow replay-attack windows to be
  enforced consumer-side. Same scheme as Stripe's webhook signatures.

## 6. Observability

Prometheus metrics from day one of the worker (week 4+): queue depth, delivery
attempts/outcomes, retry rate, per-endpoint p99 latency. Grafana dashboard
runs publicly alongside the hosted instance, so every performance number
quoted about Hookline is attributable to a live graph.

## 7. Testing strategy

- **Conformance suite** (`internal/queue/queuetest`) shared by all `Queue`
  implementations (lease exclusivity, fencing, retry timing, FIFO-ish ordering)
  using an injected clock — no sleeps, no flaky timing. The in-memory and
  Postgres backends pass it unmodified; the WAL engine will too. Done.
- **Model-based property test** (part of the conformance suite): an independent
  in-memory model of the queue contract is driven through long random op
  sequences (enqueue/lease/ack/nack/advance-time, including deliberate
  stale-token probes) in lockstep with the implementation under test, asserting
  they agree after every step. Fixed seeds keep failures reproducible. This is
  how we get "no message is ever leased to two live leases simultaneously" and
  "attempt counts and stale-lease rejections match the contract" without
  enumerating interleavings by hand. Done.
- **Concurrent claim test**: 8 goroutines hammer `Lease` over 100 messages and
  assert no message is handed to two live leases — the race-detector workout
  for `FOR UPDATE SKIP LOCKED` and the in-memory mutex. Done.
- **Chaos harness in CI** (week 7): kill -9 the broker mid-write under load,
  restart, and assert zero acknowledged-event loss — repeated 100+ times per
  CI run. toxiproxy injects network faults between workers and endpoints.
- **Race detector** (`go test -race`) on every CI run from week 1.

## 8. Explicitly out of scope

Multi-node clustering/replication, exactly-once semantics, per-tenant
multi-region routing, and any LLM/AI features. Hookline is a single-node
delivery platform done rigorously.
