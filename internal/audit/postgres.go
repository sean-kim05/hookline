package audit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schema is applied by Migrate. The audit log is append-only operational
// history, so a single idempotent table plus a few read indexes is enough.
const schema = `
CREATE TABLE IF NOT EXISTS delivery_attempts (
	id           TEXT PRIMARY KEY,
	message_id   TEXT NOT NULL,
	event_id     TEXT NOT NULL,
	endpoint     TEXT NOT NULL,
	producer     TEXT NOT NULL DEFAULT '',
	attempt      INT  NOT NULL,
	outcome      TEXT NOT NULL,
	status_code  INT  NOT NULL DEFAULT 0,
	duration_ms  BIGINT NOT NULL DEFAULT 0,
	error        TEXT NOT NULL DEFAULT '',
	at           TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS delivery_attempts_at_idx       ON delivery_attempts (at DESC);
CREATE INDEX IF NOT EXISTS delivery_attempts_event_idx    ON delivery_attempts (event_id, at DESC);
CREATE INDEX IF NOT EXISTS delivery_attempts_outcome_idx  ON delivery_attempts (outcome, at DESC);
`

// PostgresLog is a durable, Postgres-backed Log.
type PostgresLog struct {
	pool *pgxpool.Pool
}

var _ Log = (*PostgresLog)(nil)

// NewPostgresLog returns a Log over an existing connection pool. The pool is
// owned by the caller (typically shared with the Postgres queue), so Close is
// intentionally not provided here.
func NewPostgresLog(pool *pgxpool.Pool) *PostgresLog {
	return &PostgresLog{pool: pool}
}

// Migrate creates the delivery_attempts table if it does not exist.
func (l *PostgresLog) Migrate(ctx context.Context) error {
	if _, err := l.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("audit: migrate: %w", err)
	}
	return nil
}

// Record inserts one attempt.
func (l *PostgresLog) Record(ctx context.Context, a Attempt) error {
	_, err := l.pool.Exec(ctx, `
		INSERT INTO delivery_attempts
			(id, message_id, event_id, endpoint, producer, attempt, outcome,
			 status_code, duration_ms, error, at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		a.ID, a.MessageID, a.EventID, a.Endpoint, a.Producer, a.Attempt,
		string(a.Outcome), a.StatusCode, a.Duration.Milliseconds(), a.Error, a.At)
	if err != nil {
		return fmt.Errorf("audit: record: %w", err)
	}
	return nil
}

// List returns matching attempts, newest first.
func (l *PostgresLog) List(ctx context.Context, f Filter) ([]Attempt, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = defaultLimit
	}

	// Build the WHERE clause from whichever filters are set. Positional args
	// keep it injection-safe.
	var conds []string
	var args []any
	add := func(cond string, val any) {
		args = append(args, val)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if f.EventID != "" {
		add("event_id = $%d", f.EventID)
	}
	if f.MessageID != "" {
		add("message_id = $%d", f.MessageID)
	}
	if f.Outcome != "" {
		add("outcome = $%d", string(f.Outcome))
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit)
	q := fmt.Sprintf(`
		SELECT id, message_id, event_id, endpoint, producer, attempt, outcome,
		       status_code, duration_ms, error, at
		FROM delivery_attempts
		%s
		ORDER BY at DESC, id DESC
		LIMIT $%d`, where, len(args))

	rows, err := l.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("audit: list: %w", err)
	}
	defer rows.Close()

	var out []Attempt
	for rows.Next() {
		var a Attempt
		var outcome string
		var durMS int64
		if err := rows.Scan(&a.ID, &a.MessageID, &a.EventID, &a.Endpoint, &a.Producer,
			&a.Attempt, &outcome, &a.StatusCode, &durMS, &a.Error, &a.At); err != nil {
			return nil, fmt.Errorf("audit: list scan: %w", err)
		}
		a.Outcome = Outcome(outcome)
		a.Duration = time.Duration(durMS) * time.Millisecond
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("audit: list rows: %w", err)
	}
	return out, nil
}
