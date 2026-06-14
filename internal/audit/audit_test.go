package audit_test

import (
	"context"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/audit"
)

func attempt(id, event string, outcome audit.Outcome, at time.Time) audit.Attempt {
	return audit.Attempt{
		ID:       id,
		EventID:  event,
		Endpoint: "https://example.test/hook",
		Attempt:  1,
		Outcome:  outcome,
		At:       at,
	}
}

func TestMemoryLogListNewestFirst(t *testing.T) {
	t.Parallel()
	log := audit.NewMemoryLog(0)
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 3; i++ {
		a := attempt(string(rune('a'+i)), "evt", audit.OutcomeRetrying, base.Add(time.Duration(i)*time.Second))
		if err := log.Record(ctx, a); err != nil {
			t.Fatalf("record: %v", err)
		}
	}

	got, err := log.List(ctx, audit.Filter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 attempts, got %d", len(got))
	}
	if got[0].ID != "c" || got[2].ID != "a" {
		t.Fatalf("want newest-first c,b,a; got %s,%s,%s", got[0].ID, got[1].ID, got[2].ID)
	}
}

func TestMemoryLogFilters(t *testing.T) {
	t.Parallel()
	log := audit.NewMemoryLog(0)
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()

	_ = log.Record(ctx, attempt("1", "evt-A", audit.OutcomeDelivered, base))
	_ = log.Record(ctx, attempt("2", "evt-B", audit.OutcomeDeadLettered, base.Add(time.Second)))
	_ = log.Record(ctx, attempt("3", "evt-A", audit.OutcomeRetrying, base.Add(2*time.Second)))

	byEvent, _ := log.List(ctx, audit.Filter{EventID: "evt-A"})
	if len(byEvent) != 2 {
		t.Fatalf("event filter: want 2, got %d", len(byEvent))
	}

	dlq, _ := log.List(ctx, audit.Filter{Outcome: audit.OutcomeDeadLettered})
	if len(dlq) != 1 || dlq[0].ID != "2" {
		t.Fatalf("outcome filter: want only attempt 2, got %+v", dlq)
	}
}

func TestMemoryLogCapacityEvictsOldest(t *testing.T) {
	t.Parallel()
	log := audit.NewMemoryLog(2)
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 5; i++ {
		_ = log.Record(ctx, attempt(string(rune('a'+i)), "evt", audit.OutcomeRetrying, base.Add(time.Duration(i)*time.Second)))
	}

	got, _ := log.List(ctx, audit.Filter{})
	if len(got) != 2 {
		t.Fatalf("want capacity 2, got %d", len(got))
	}
	if got[0].ID != "e" || got[1].ID != "d" {
		t.Fatalf("want most-recent e,d retained; got %s,%s", got[0].ID, got[1].ID)
	}
}

func TestMemoryLogLimit(t *testing.T) {
	t.Parallel()
	log := audit.NewMemoryLog(0)
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 10; i++ {
		_ = log.Record(ctx, attempt(string(rune('a'+i)), "evt", audit.OutcomeRetrying, base.Add(time.Duration(i)*time.Second)))
	}
	got, _ := log.List(ctx, audit.Filter{Limit: 3})
	if len(got) != 3 {
		t.Fatalf("want limit 3, got %d", len(got))
	}
}

func TestNopLog(t *testing.T) {
	t.Parallel()
	var l audit.Log = audit.NopLog{}
	if err := l.Record(context.Background(), audit.Attempt{}); err != nil {
		t.Fatalf("nop record: %v", err)
	}
	got, err := l.List(context.Background(), audit.Filter{})
	if err != nil || got != nil {
		t.Fatalf("nop list: got %v, %v", got, err)
	}
}
