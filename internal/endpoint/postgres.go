package endpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sean-kim05/hookline/internal/id"
)

// pgSchema is applied by Migrate. URL is unique so re-registering rotates the
// secret of the existing row rather than creating a duplicate.
const pgSchema = `
CREATE TABLE IF NOT EXISTS endpoints (
	id         TEXT PRIMARY KEY,
	url        TEXT NOT NULL UNIQUE,
	producer   TEXT NOT NULL DEFAULT '',
	secret     TEXT NOT NULL,
	disabled   BOOLEAN NOT NULL DEFAULT FALSE,
	created_at TIMESTAMPTZ NOT NULL
);
`

// PostgresRegistry is a durable, Postgres-backed Registry.
type PostgresRegistry struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

var _ Registry = (*PostgresRegistry)(nil)

// PostgresOption configures a PostgresRegistry.
type PostgresOption func(*PostgresRegistry)

// WithPostgresClock overrides the registry clock (used in tests).
func WithPostgresClock(now func() time.Time) PostgresOption {
	return func(r *PostgresRegistry) { r.now = now }
}

// NewPostgresRegistry returns a Registry over an existing pool (owned by the
// caller, typically shared with the queue).
func NewPostgresRegistry(pool *pgxpool.Pool, opts ...PostgresOption) *PostgresRegistry {
	r := &PostgresRegistry{pool: pool, now: time.Now}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Migrate creates the endpoints table if it does not exist.
func (r *PostgresRegistry) Migrate(ctx context.Context) error {
	if _, err := r.pool.Exec(ctx, pgSchema); err != nil {
		return fmt.Errorf("endpoint: migrate: %w", err)
	}
	return nil
}

// Register inserts e, or rotates the secret if its URL is already registered.
func (r *PostgresRegistry) Register(ctx context.Context, e Endpoint) (Endpoint, error) {
	url := normalizeURL(e.URL)
	if url == "" {
		return Endpoint{}, errors.New("endpoint: URL is required")
	}
	secret := e.Secret
	if secret == "" {
		secret = GenerateSecret()
	}
	ep := Endpoint{
		ID:        id.New(),
		URL:       url,
		Producer:  e.Producer,
		Secret:    secret,
		Disabled:  e.Disabled,
		CreatedAt: r.now(),
	}

	// ON CONFLICT rotates the secret in place. RETURNING gives us the row that
	// actually persisted (the existing ID/created_at on conflict).
	err := r.pool.QueryRow(ctx, `
		INSERT INTO endpoints (id, url, producer, secret, disabled, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (url) DO UPDATE
			SET secret   = EXCLUDED.secret,
			    disabled = EXCLUDED.disabled,
			    producer = CASE WHEN EXCLUDED.producer <> '' THEN EXCLUDED.producer ELSE endpoints.producer END
		RETURNING id, url, producer, secret, disabled, created_at`,
		ep.ID, ep.URL, ep.Producer, ep.Secret, ep.Disabled, ep.CreatedAt).
		Scan(&ep.ID, &ep.URL, &ep.Producer, &ep.Secret, &ep.Disabled, &ep.CreatedAt)
	if err != nil {
		return Endpoint{}, fmt.Errorf("endpoint: register: %w", err)
	}
	return ep, nil
}

// List returns endpoints for producer (all when empty), newest first.
func (r *PostgresRegistry) List(ctx context.Context, producer string) ([]Endpoint, error) {
	var rows pgx.Rows
	var err error
	if producer == "" {
		rows, err = r.pool.Query(ctx, `
			SELECT id, url, producer, secret, disabled, created_at
			FROM endpoints ORDER BY created_at DESC, id DESC`)
	} else {
		rows, err = r.pool.Query(ctx, `
			SELECT id, url, producer, secret, disabled, created_at
			FROM endpoints WHERE producer = $1 ORDER BY created_at DESC, id DESC`, producer)
	}
	if err != nil {
		return nil, fmt.Errorf("endpoint: list: %w", err)
	}
	defer rows.Close()

	var out []Endpoint
	for rows.Next() {
		var ep Endpoint
		if err := rows.Scan(&ep.ID, &ep.URL, &ep.Producer, &ep.Secret, &ep.Disabled, &ep.CreatedAt); err != nil {
			return nil, fmt.Errorf("endpoint: list scan: %w", err)
		}
		out = append(out, ep)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("endpoint: list rows: %w", err)
	}
	return out, nil
}

// Lookup returns the endpoint registered for url.
func (r *PostgresRegistry) Lookup(ctx context.Context, url string) (Endpoint, error) {
	var ep Endpoint
	err := r.pool.QueryRow(ctx, `
		SELECT id, url, producer, secret, disabled, created_at
		FROM endpoints WHERE url = $1`, normalizeURL(url)).
		Scan(&ep.ID, &ep.URL, &ep.Producer, &ep.Secret, &ep.Disabled, &ep.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Endpoint{}, ErrNotFound
	}
	if err != nil {
		return Endpoint{}, fmt.Errorf("endpoint: lookup: %w", err)
	}
	return ep, nil
}
