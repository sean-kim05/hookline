package wal_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/queue"
	"github.com/sean-kim05/hookline/internal/queue/queuetest"
	"github.com/sean-kim05/hookline/internal/queue/wal"
)

// TestWALConformance runs the shared conformance suite against the WAL backend,
// proving the from-scratch durable queue satisfies the exact same contract
// (fencing, at-least-once, retry scheduling) as the in-memory and Postgres
// backends. fsync is disabled for test speed; durability is covered separately.
func TestWALConformance(t *testing.T) {
	queuetest.Run(t, func(t *testing.T, now func() time.Time) queue.Queue {
		q, err := wal.Open(t.TempDir(), wal.WithClock(now), wal.WithSync(false))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { q.Close() })
		return q
	})
}

// testClock is a manually-advanced clock for the recovery/compaction tests.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock { return &testClock{t: time.Unix(1_700_000_000, 0).UTC()} }
func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func ev(url string, payload string) event.Event {
	return event.Event{Endpoint: url, Payload: []byte(payload)}
}

func TestRecoveryRebuildsState(t *testing.T) {
	dir := t.TempDir()
	clk := newTestClock()
	ctx := context.Background()

	q, err := wal.Open(dir, wal.WithClock(clk.now))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := q.Enqueue(ctx, ev("https://a.test", `{"n":1}`)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Enqueue(ctx, ev("https://b.test", `{"n":2}`)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Lease one and leave it un-acked (in flight) across the restart.
	leases, err := q.Lease(ctx, 1, time.Minute)
	if err != nil || len(leases) != 1 {
		t.Fatalf("lease: %v len=%d", err, len(leases))
	}
	leasedID, leasedTok := leases[0].Message.ID, leases[0].Token
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: state must be exactly as it was.
	q2, err := wal.Open(dir, wal.WithClock(clk.now))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()

	if d, _ := q2.Depth(ctx); d != 2 {
		t.Fatalf("recovered depth = %d, want 2", d)
	}
	// The in-flight message is still leased (its lease has not expired), so a
	// fresh lease returns only the other message.
	ls, _ := q2.Lease(ctx, 10, time.Minute)
	if len(ls) != 1 {
		t.Fatalf("post-recovery lease returned %d, want 1 (other still leased)", len(ls))
	}
	if ls[0].Message.ID == leasedID {
		t.Fatal("recovery lost the in-flight lease; message was re-leased early")
	}
	// The fencing token survived the restart: the original token still acks.
	if err := q2.Ack(ctx, leasedID, leasedTok); err != nil {
		t.Fatalf("ack with pre-restart token failed: %v", err)
	}
	if d, _ := q2.Depth(ctx); d != 1 {
		t.Fatalf("after ack depth = %d, want 1", d)
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	clk := newTestClock()
	ctx := context.Background()

	q, _ := wal.Open(dir, wal.WithClock(clk.now))
	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(ctx, ev("https://a.test", `{"x":1}`)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	q.Close()

	// Simulate a crash mid-append: garbage appended to the active segment.
	seg := lastSegment(t, dir)
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	if _, err := f.Write([]byte("this is a torn partial frame, not valid")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	f.Close()

	// Recovery must discard the torn tail and keep the 3 good records.
	q2, err := wal.Open(dir, wal.WithClock(clk.now))
	if err != nil {
		t.Fatalf("reopen after torn write: %v", err)
	}
	if d, _ := q2.Depth(ctx); d != 3 {
		t.Fatalf("recovered depth = %d, want 3 (torn tail should be dropped)", d)
	}
	// Writes after truncation must be clean and themselves recoverable.
	if _, err := q2.Enqueue(ctx, ev("https://a.test", `{"x":2}`)); err != nil {
		t.Fatalf("enqueue after recovery: %v", err)
	}
	q2.Close()

	q3, _ := wal.Open(dir, wal.WithClock(clk.now))
	defer q3.Close()
	if d, _ := q3.Depth(ctx); d != 4 {
		t.Fatalf("final depth = %d, want 4", d)
	}
}

func TestCompactionCollapsesTombstones(t *testing.T) {
	dir := t.TempDir()
	clk := newTestClock()
	ctx := context.Background()

	// Small thresholds force segment rolls and an early compaction.
	q, err := wal.Open(dir,
		wal.WithClock(clk.now),
		wal.WithSync(false),
		wal.WithMaxSegmentBytes(2048),
		wal.WithCompactEvery(100),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Enqueue 120, lease them all, ack 110 — leaving 10 live among ~350
	// records, which is well past the compaction trigger.
	const total, keep = 120, 10
	leases, err := q.Lease(ctx, 0, time.Hour) // nothing yet
	if err != nil {
		t.Fatalf("warmup lease: %v", err)
	}
	for i := 0; i < total; i++ {
		if _, err := q.Enqueue(ctx, ev("https://a.test", `{"x":1}`)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	leases, err = q.Lease(ctx, total, time.Hour)
	if err != nil || len(leases) != total {
		t.Fatalf("lease all: %v len=%d", err, len(leases))
	}
	for i := 0; i < total-keep; i++ {
		if err := q.Ack(ctx, leases[i].Message.ID, leases[i].Token); err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
	}
	if d, _ := q.Depth(ctx); d != keep {
		t.Fatalf("depth = %d, want %d", d, keep)
	}

	// Compaction should have collapsed the many segments into roughly one.
	if got := countSegments(t, dir); got > 2 {
		t.Fatalf("after compaction there are %d segments, want <= 2", got)
	}
	q.Close()

	// Reopen from the compacted log: the 10 live messages survive, with their
	// leases (and fencing tokens) intact.
	q2, err := wal.Open(dir, wal.WithClock(clk.now), wal.WithSync(false))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	if d, _ := q2.Depth(ctx); d != keep {
		t.Fatalf("recovered depth = %d, want %d", d, keep)
	}
	// A surviving lease's token still acks after compaction + restart.
	survivor := leases[total-1]
	if err := q2.Ack(ctx, survivor.Message.ID, survivor.Token); err != nil {
		t.Fatalf("ack survivor after compaction: %v", err)
	}
	if d, _ := q2.Depth(ctx); d != keep-1 {
		t.Fatalf("after ack depth = %d, want %d", d, keep-1)
	}
}

func lastSegment(t *testing.T, dir string) string {
	t.Helper()
	matches, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(matches) == 0 {
		t.Fatal("no segments found")
	}
	// Glob returns sorted names; the zero-padded sequence makes lexical == seq.
	return matches[len(matches)-1]
}

func countSegments(t *testing.T, dir string) int {
	t.Helper()
	matches, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	return len(matches)
}
