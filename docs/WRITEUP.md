# Hookline — engineering write-up

Hookline is a self-hostable webhook delivery platform built on a from-scratch
durable queue. This document is the narrative behind the code: the design bets,
how correctness is proven, and the measured results. The architecture rationale
lives in [DESIGN.md](DESIGN.md); this is the "what got built and how well it
works" companion.

## What it does

Producers `POST` events to an authenticated API. Hookline durably enqueues each
one and a pool of delivery workers sign and `POST` it to the destination
endpoint, retrying with backoff until it succeeds or exhausts its attempts and
lands in a dead-letter queue an operator can inspect and replay. Every attempt
is recorded; deliveries are HMAC-signed with a per-endpoint secret; endpoints
are protected by rate limiting and circuit breakers.

## Architecture

```
producer ──POST /v1/events──▶  API  ──Enqueue──▶  Queue  ◀──Lease/Ack/Nack──  delivery workers ──signed POST──▶ endpoint
                                │                  (Memory │ Postgres │ WAL)         │
                                │                                                    ├─ audit log (every attempt)
                                ├─ endpoint registry (per-endpoint secrets)          ├─ dead-letter store (+ replay)
                                └─ /metrics (Prometheus)                              └─ circuit breaker + rate limiter

operator ──▶ Next.js dashboard ──▶ API (deliveries / DLQ / endpoints)
```

The keystone is a single `Queue` interface
(`Enqueue` / `Lease` / `Ack` / `Nack` / `Close`). Three backends implement it and
all pass **one shared conformance suite**, so the risky from-scratch engine could
be built last without the rest of the system depending on its details.

## Delivery correctness

- **At-least-once, by design.** Exactly-once over HTTP to third parties is
  impossible (an endpoint can succeed then fail to respond), so Hookline promises
  at-least-once and forwards an idempotency key for consumer-side dedup — the same
  contract as Stripe/Svix/GitHub.
- **Fencing tokens.** Each lease carries a monotonic token. If a worker's lease
  lapses, the message is re-leased with a higher token; the original worker's
  `Ack`/`Nack` is then rejected as stale. This is what prevents an expired worker
  from double-acking or corrupting the retry schedule.
- **Record-before-remove.** The worker records a dead letter *before* removing the
  message from the queue, so a crash in between re-delivers rather than drops.

## The WAL queue (the headline)

`internal/queue/wal` is a durable queue built from scratch:

- Every state change (enqueue/lease/ack/nack) is appended as a **CRC-framed
  record** — `[len][crc32][payload]` — before the in-memory index is updated.
- On open, the log is **replayed** to rebuild exact state. A final frame left
  torn by an interrupted append fails its CRC and is **truncated** — crash
  recovery falls out of the framing.
- The log is **segmented**; **compaction** rewrites the live set as snapshot
  records into a higher-sequence segment and drops the obsolete ones, so the log
  doesn't grow without bound. Compaction is crash-safe: the snapshot has a higher
  sequence number than everything it replaces, so even a crash mid-cleanup
  recovers to the same state.

Because it sits behind the shared interface, the WAL passes the *identical*
conformance suite as the in-memory and Postgres backends — fencing, FIFO,
lease expiry, concurrent no-double-claim, and a model-based property test —
proving the hand-built engine matches the reference semantics exactly.

## Testing strategy

- **One conformance suite, three backends.** 16 cases (incl. a model-based
  randomized property test that drives random op sequences against an independent
  model, and a concurrent no-double-claim stress test) run unmodified against
  Memory, Postgres, and WAL.
- **Injected clocks** everywhere time matters, so lease expiry and backoff are
  tested deterministically with zero `sleep`s.
- **Crash tests** for the WAL: recovery, torn-tail truncation, and compaction,
  each reopening the queue and asserting state (including that fencing tokens
  survive a restart).
- **`go test -race`** on every CI run against a Postgres service container.
- **Playwright** e2e for the dashboard; the proxy is mocked with fixtures so UI
  behaviour (filtering, replay, registration) is tested deterministically.

## Observability & resilience

- **Prometheus** metrics for delivery outcomes/latency, HTTP requests/latency,
  and queue depth, exposed at `/metrics` with a provisioned **Grafana** dashboard.
  Instrumentation rides on existing hooks (the audit log, an HTTP middleware, a
  depth poller) so the hot-path packages stay Prometheus-free.
- **Per-endpoint circuit breakers** (closed/open/half-open) stop hammering a down
  consumer; **per-endpoint token-bucket rate limiting** respects agreed limits.
  Both are composable `Deliverer` middleware.

## Measured results

Single node, localhost sink, in-memory queue unless noted. Reproduce with
[`loadtest/`](../loadtest/README.md). These are measurements, not targets.

| Metric | Result |
| --- | --- |
| Ingestion throughput | ~40,000 events/sec accepted |
| Ingestion latency | p50 ~1 ms, p95 ~3 ms, p99 ~4 ms |
| End-to-end delivery | ~5,900 events/sec at concurrency 64 |
| Crash recovery (WAL) | 5,000 / 5,000 distinct events delivered across `kill -9` |
| Crash recovery (in-memory) | 68 / 5,000 — the rest lost with the process |

## Résumé bullets (measured only)

> Phrased for a new-grad SWE résumé. Every number is measured and reproducible
> with the load/chaos harness in `loadtest/`.

- Built **Hookline**, a self-hostable webhook delivery platform in **Go** on a
  **from-scratch write-ahead-log queue** (CRC-framed segmented log with crash
  recovery and compaction) — no Redis/Kafka — sustaining **~40,000 events/sec**
  ingestion at **p99 ~4 ms**.
- Guaranteed **at-least-once delivery** with lease-based claiming and **fencing
  tokens**; a chaos test showed the WAL backend delivering **5,000/5,000** events
  with **zero loss across a `kill -9`** mid-flight, vs. 68/5,000 for an in-memory
  queue.
- Designed a single `Queue` interface with **three interchangeable backends**
  (in-memory, PostgreSQL `FOR UPDATE SKIP LOCKED`, custom WAL) all passing **one
  shared conformance suite** — 16 cases including a model-based property test and
  a concurrent no-double-claim stress test, run under the **race detector** in CI.
- Implemented production operations: **HMAC-signed deliveries** with per-endpoint
  secrets, exponential backoff with jitter, **dead-letter queue + replay**,
  **per-endpoint circuit breakers and rate limiting**, **Prometheus/Grafana**
  observability, and a **Next.js + Playwright** operator dashboard.
