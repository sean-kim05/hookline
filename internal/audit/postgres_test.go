package audit_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sean-kim05/hookline/internal/audit"
)

// TestPostgresLog exercises the Postgres-backed audit log against a real
// database. It needs HOOKLINE_TEST_DATABASE_URL and skips otherwise, matching
// the queue conformance test, so plain `go test ./...` runs without a database.
func TestPostgresLog(t *testing.T) {
	dsn := os.Getenv("HOOKLINE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("HOOKLINE_TEST_DATABASE_URL not set; see README")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	log := audit.NewPostgresLog(pool)
	if err := log.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE delivery_attempts"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	base := time.Unix(1_700_000_000, 0).UTC()
	want := []audit.Attempt{
		{ID: "1", MessageID: "m1", EventID: "evt-A", Endpoint: "https://x.test", Producer: "acme", Attempt: 1, Outcome: audit.OutcomeRetrying, StatusCode: 500, Duration: 12 * time.Millisecond, Error: "boom", At: base},
		{ID: "2", MessageID: "m1", EventID: "evt-A", Endpoint: "https://x.test", Producer: "acme", Attempt: 2, Outcome: audit.OutcomeDelivered, StatusCode: 200, Duration: 8 * time.Millisecond, At: base.Add(time.Second)},
		{ID: "3", MessageID: "m2", EventID: "evt-B", Endpoint: "https://y.test", Attempt: 1, Outcome: audit.OutcomeDeadLettered, StatusCode: 0, Error: "timeout", At: base.Add(2 * time.Second)},
	}
	for _, a := range want {
		if err := log.Record(ctx, a); err != nil {
			t.Fatalf("record %s: %v", a.ID, err)
		}
	}

	all, err := log.List(ctx, audit.Filter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 || all[0].ID != "3" {
		t.Fatalf("want 3 rows newest-first (3,2,1), got %d with head %v", len(all), all)
	}
	// Round-trip fidelity on the richest row.
	if all[2].Duration != 12*time.Millisecond || all[2].StatusCode != 500 || all[2].Producer != "acme" {
		t.Fatalf("row 1 round-trip mismatch: %+v", all[2])
	}

	evtA, _ := log.List(ctx, audit.Filter{EventID: "evt-A"})
	if len(evtA) != 2 {
		t.Fatalf("event filter: want 2, got %d", len(evtA))
	}
	dlq, _ := log.List(ctx, audit.Filter{Outcome: audit.OutcomeDeadLettered})
	if len(dlq) != 1 || dlq[0].ID != "3" {
		t.Fatalf("dlq filter: want only 3, got %v", dlq)
	}
}
