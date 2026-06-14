package delivery

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dlqSchema is applied by Migrate. The dead letter keeps the whole message —
// payload included — keyed by the queue message ID, so replay can re-enqueue
// the exact event that failed.
const dlqSchema = `
CREATE TABLE IF NOT EXISTS dead_letters (
	message_id      TEXT PRIMARY KEY,
	event_id        TEXT NOT NULL,
	endpoint        TEXT NOT NULL,
	payload         BYTEA NOT NULL,
	content_type    TEXT NOT NULL DEFAULT '',
	idempotency_key TEXT NOT NULL DEFAULT '',
	event_created_at TIMESTAMPTZ NOT NULL,
	attempts        INT NOT NULL,
	reason          TEXT NOT NULL,
	failed_at       TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS dead_letters_failed_idx ON dead_letters (failed_at DESC);
`

// PostgresDeadLetterSink is a durable DeadLetterStore.
type PostgresDeadLetterSink struct {
	pool *pgxpool.Pool
}

var _ DeadLetterStore = (*PostgresDeadLetterSink)(nil)

// NewPostgresDeadLetterSink returns a store over an existing pool (owned by the
// caller, typically shared with the queue).
func NewPostgresDeadLetterSink(pool *pgxpool.Pool) *PostgresDeadLetterSink {
	return &PostgresDeadLetterSink{pool: pool}
}

// Migrate creates the dead_letters table if it does not exist.
func (s *PostgresDeadLetterSink) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, dlqSchema); err != nil {
		return fmt.Errorf("delivery: dlq migrate: %w", err)
	}
	return nil
}

// DeadLetter records dl, upserting on message ID so a re-dead-lettered message
// (e.g. after a crash before the queue Ack) does not duplicate.
func (s *PostgresDeadLetterSink) DeadLetter(ctx context.Context, dl DeadLetter) error {
	m, ev := dl.Message, dl.Message.Event
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dead_letters
			(message_id, event_id, endpoint, payload, content_type, idempotency_key,
			 event_created_at, attempts, reason, failed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (message_id) DO UPDATE
			SET reason = EXCLUDED.reason, failed_at = EXCLUDED.failed_at,
			    attempts = EXCLUDED.attempts`,
		m.ID, ev.ID, ev.Endpoint, ev.Payload, ev.ContentType, ev.IdempotencyKey,
		ev.CreatedAt, m.Attempts, dl.Reason, dl.FailedAt)
	if err != nil {
		return fmt.Errorf("delivery: dlq record: %w", err)
	}
	return nil
}

// ListDeadLetters returns dead letters newest first, capped at limit.
func (s *PostgresDeadLetterSink) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	if limit <= 0 {
		limit = deadLetterDefaultLimit
	}
	rows, err := s.pool.Query(ctx, `
		SELECT message_id, event_id, endpoint, payload, content_type, idempotency_key,
		       event_created_at, attempts, reason, failed_at
		FROM dead_letters ORDER BY failed_at DESC, message_id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("delivery: dlq list: %w", err)
	}
	defer rows.Close()

	var out []DeadLetter
	for rows.Next() {
		dl, err := scanDeadLetter(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, dl)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("delivery: dlq list rows: %w", err)
	}
	return out, nil
}

// GetDeadLetter returns the dead letter for messageID.
func (s *PostgresDeadLetterSink) GetDeadLetter(ctx context.Context, messageID string) (DeadLetter, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT message_id, event_id, endpoint, payload, content_type, idempotency_key,
		       event_created_at, attempts, reason, failed_at
		FROM dead_letters WHERE message_id = $1`, messageID)
	dl, err := scanDeadLetter(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return DeadLetter{}, ErrDeadLetterNotFound
	}
	if err != nil {
		return DeadLetter{}, fmt.Errorf("delivery: dlq get: %w", err)
	}
	return dl, nil
}

// RemoveDeadLetter deletes the dead letter for messageID.
func (s *PostgresDeadLetterSink) RemoveDeadLetter(ctx context.Context, messageID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM dead_letters WHERE message_id = $1`, messageID)
	if err != nil {
		return fmt.Errorf("delivery: dlq remove: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrDeadLetterNotFound
	}
	return nil
}

// scanRow is the subset of pgx.Row/pgx.Rows that scanDeadLetter needs.
type scanRow interface {
	Scan(dest ...any) error
}

func scanDeadLetter(row scanRow) (DeadLetter, error) {
	var dl DeadLetter
	m := &dl.Message
	ev := &m.Event
	if err := row.Scan(&m.ID, &ev.ID, &ev.Endpoint, &ev.Payload, &ev.ContentType,
		&ev.IdempotencyKey, &ev.CreatedAt, &m.Attempts, &dl.Reason, &dl.FailedAt); err != nil {
		return DeadLetter{}, err
	}
	return dl, nil
}
