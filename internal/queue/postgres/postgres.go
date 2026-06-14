// Package postgres implements queue.Queue on PostgreSQL. It is the MVP
// backend: simple, transactional, and correct, so the delivery platform ships
// while the custom WAL engine is still being built behind the same interface.
//
// The claim path is the classic Postgres job-queue pattern:
//
//	SELECT ... FOR UPDATE SKIP LOCKED
//
// Concurrent workers each lock a disjoint set of ready rows — rows locked by
// another worker's in-flight Lease are skipped rather than waited on, so
// leasing never serializes workers behind each other.
//
// Time is injected (WithClock) rather than read from the database's now(), so
// the shared conformance suite can drive lease expiry and retry schedules
// deterministically against a real database.
package postgres

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/id"
	"github.com/sean-kim05/hookline/internal/queue"
)

// schema is applied by Migrate. Idempotent on purpose: the queue table is
// operational state, not domain data, so CREATE IF NOT EXISTS beats a
// migration framework at this stage. The delivery audit log (week 5) will get
// real versioned migrations.
const schema = `
CREATE TABLE IF NOT EXISTS queue_messages (
	id               TEXT PRIMARY KEY,
	event_id         TEXT NOT NULL,
	endpoint         TEXT NOT NULL,
	payload          BYTEA NOT NULL,
	content_type     TEXT NOT NULL DEFAULT '',
	idempotency_key  TEXT NOT NULL DEFAULT '',
	event_created_at TIMESTAMPTZ NOT NULL,
	enqueued_at      TIMESTAMPTZ NOT NULL,
	attempts         INT NOT NULL DEFAULT 0,
	ready_at         TIMESTAMPTZ NOT NULL,
	lease_token      BIGINT NOT NULL DEFAULT 0,
	lease_expiry     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS queue_messages_ready_idx
	ON queue_messages (ready_at, id);
`

// Queue is a PostgreSQL-backed queue.Queue.
type Queue struct {
	pool     *pgxpool.Pool
	now      func() time.Time
	ownsPool bool // true when Open created the pool, so Close should close it
}

// Compile-time check that Queue satisfies the queue.Queue interface.
var _ queue.Queue = (*Queue)(nil)

// Option configures a Queue.
type Option func(*Queue)

// WithClock overrides the queue's clock. Tests use this to drive lease expiry
// and retry scheduling deterministically.
func WithClock(now func() time.Time) Option {
	return func(q *Queue) { q.now = now }
}

// Open connects to the database at dsn and returns a Queue. Call Migrate
// before first use. Close releases the connection pool.
func Open(ctx context.Context, dsn string, opts ...Option) (*Queue, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	q := &Queue{pool: pool, now: time.Now, ownsPool: true}
	for _, opt := range opts {
		opt(q)
	}
	return q, nil
}

// NewQueue wraps an existing connection pool, letting the queue share one pool
// with the audit log, endpoint registry, and dead-letter store. The caller owns
// the pool; Close does not close it.
func NewQueue(pool *pgxpool.Pool, opts ...Option) *Queue {
	q := &Queue{pool: pool, now: time.Now, ownsPool: false}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Migrate creates the queue table if it does not exist.
func (q *Queue) Migrate(ctx context.Context) error {
	if _, err := q.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("postgres: migrate: %w", err)
	}
	return nil
}

// Enqueue accepts an event for delivery and returns its message ID.
func (q *Queue) Enqueue(ctx context.Context, ev event.Event) (string, error) {
	now := q.now()
	if ev.ID == "" {
		ev.ID = id.New()
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = now
	}

	mid := id.New()
	_, err := q.pool.Exec(ctx, `
		INSERT INTO queue_messages
			(id, event_id, endpoint, payload, content_type, idempotency_key,
			 event_created_at, enqueued_at, ready_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
		mid, ev.ID, ev.Endpoint, ev.Payload, ev.ContentType, ev.IdempotencyKey,
		ev.CreatedAt, now)
	if err != nil {
		return "", fmt.Errorf("postgres: enqueue: %w", err)
	}
	return mid, nil
}

// Lease atomically claims up to n ready messages. A message is ready when its
// ready_at has arrived and it is not under a live lease (lease_expiry NULL,
// cleared by Nack, or already lapsed). Claiming bumps the fencing token, which
// is what invalidates any prior lease on the message.
func (q *Queue) Lease(ctx context.Context, n int, leaseFor time.Duration) ([]queue.Lease, error) {
	now := q.now()

	// Match the interface contract used by MemoryQueue: a negative n means no
	// limit (LIMIT NULL in Postgres means LIMIT ALL).
	var limit any
	if n >= 0 {
		limit = n
	}

	rows, err := q.pool.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM queue_messages
			WHERE ready_at <= $1
			  AND (lease_expiry IS NULL OR lease_expiry <= $1)
			ORDER BY ready_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE queue_messages m
		SET lease_token  = m.lease_token + 1,
		    lease_expiry = $3,
		    attempts     = m.attempts + 1
		FROM picked
		WHERE m.id = picked.id
		RETURNING m.id, m.event_id, m.endpoint, m.payload, m.content_type,
		          m.idempotency_key, m.event_created_at, m.enqueued_at,
		          m.attempts, m.lease_token, m.ready_at`,
		now, limit, now.Add(leaseFor))
	if err != nil {
		return nil, fmt.Errorf("postgres: lease: %w", err)
	}
	defer rows.Close()

	type leased struct {
		l       queue.Lease
		readyAt time.Time
	}
	var got []leased
	for rows.Next() {
		var (
			m       queue.Message
			token   int64
			readyAt time.Time
		)
		if err := rows.Scan(&m.ID, &m.Event.ID, &m.Event.Endpoint, &m.Event.Payload,
			&m.Event.ContentType, &m.Event.IdempotencyKey, &m.Event.CreatedAt,
			&m.EnqueuedAt, &m.Attempts, &token, &readyAt); err != nil {
			return nil, fmt.Errorf("postgres: lease scan: %w", err)
		}
		got = append(got, leased{
			l:       queue.Lease{Message: m, Token: uint64(token), Expiry: now.Add(leaseFor)},
			readyAt: readyAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: lease rows: %w", err)
	}

	// RETURNING does not guarantee row order, so restore the claim order
	// (oldest ready_at first, ID as tiebreaker — same as MemoryQueue).
	sort.Slice(got, func(i, j int) bool {
		if got[i].readyAt.Equal(got[j].readyAt) {
			return got[i].l.Message.ID < got[j].l.Message.ID
		}
		return got[i].readyAt.Before(got[j].readyAt)
	})
	leases := make([]queue.Lease, len(got))
	for i, g := range got {
		leases[i] = g.l
	}
	return leases, nil
}

// Ack marks the message delivered and removes it, if the token is current.
//
// One statement answers both "did it delete" and "why not": the outer EXISTS
// reads the statement's snapshot, so when the delete matched nothing we can
// tell a stale token (row exists, token differs → ErrStaleLease) from an
// unknown or already-acked message (ErrNotFound).
func (q *Queue) Ack(ctx context.Context, mid string, token uint64) error {
	var deleted, existed bool
	err := q.pool.QueryRow(ctx, `
		WITH del AS (
			DELETE FROM queue_messages
			WHERE id = $1 AND lease_token = $2
			RETURNING id
		)
		SELECT EXISTS (SELECT 1 FROM del),
		       EXISTS (SELECT 1 FROM queue_messages WHERE id = $1)`,
		mid, int64(token)).Scan(&deleted, &existed)
	if err != nil {
		return fmt.Errorf("postgres: ack: %w", err)
	}
	return classify(deleted, existed)
}

// Nack reschedules the message after retryAfter, if the token is current.
// Clearing lease_expiry releases the lease; the token is left as-is and stays
// valid until the next Lease bumps it.
func (q *Queue) Nack(ctx context.Context, mid string, token uint64, retryAfter time.Duration) error {
	var updated, existed bool
	err := q.pool.QueryRow(ctx, `
		WITH upd AS (
			UPDATE queue_messages
			SET lease_expiry = NULL, ready_at = $3
			WHERE id = $1 AND lease_token = $2
			RETURNING id
		)
		SELECT EXISTS (SELECT 1 FROM upd),
		       EXISTS (SELECT 1 FROM queue_messages WHERE id = $1)`,
		mid, int64(token), q.now().Add(retryAfter)).Scan(&updated, &existed)
	if err != nil {
		return fmt.Errorf("postgres: nack: %w", err)
	}
	return classify(updated, existed)
}

func classify(applied, existed bool) error {
	switch {
	case applied:
		return nil
	case existed:
		return queue.ErrStaleLease
	default:
		return queue.ErrNotFound
	}
}

// Close releases the connection pool if this queue created it. When the pool
// was supplied via NewQueue it is owned by the caller and left open.
func (q *Queue) Close() error {
	if q.ownsPool {
		q.pool.Close()
	}
	return nil
}
