package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/queue"
)

func msgTo(url string) queue.Message {
	return queue.Message{ID: "m", Event: event.Event{ID: "e", Endpoint: url}}
}

func failResult() Result    { return Result{Success: false, StatusCode: 500} }
func successResult() Result { return Result{Success: true, StatusCode: 200} }

func TestCircuitOpensAfterThreshold(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	stub := &stubDeliverer{fn: func(queue.Message) Result { return failResult() }}
	cb := NewCircuitBreaker(stub, 3, 30*time.Second).WithClock(clk.now)
	ctx := context.Background()
	m := msgTo("https://a.test/hook")

	for i := 0; i < 3; i++ {
		if res := cb.Deliver(ctx, m); res.Err != nil {
			t.Fatalf("attempt %d short-circuited early: %v", i, res.Err)
		}
	}
	// The breaker is now open: the next delivery fails fast without calling next.
	res := cb.Deliver(ctx, m)
	if !errors.Is(res.Err, ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", res.Err)
	}
	if stub.calls.Load() != 3 {
		t.Fatalf("breaker open but next was called %d times, want 3", stub.calls.Load())
	}
}

func TestCircuitHalfOpenRecovers(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	outcome := failResult()
	stub := &stubDeliverer{fn: func(queue.Message) Result { return outcome }}
	cb := NewCircuitBreaker(stub, 2, 30*time.Second).WithClock(clk.now)
	ctx := context.Background()
	m := msgTo("https://a.test/hook")

	cb.Deliver(ctx, m)
	cb.Deliver(ctx, m) // opens
	if res := cb.Deliver(ctx, m); !errors.Is(res.Err, ErrCircuitOpen) {
		t.Fatalf("expected open, got %v", res.Err)
	}

	// After cooldown, a single trial is allowed (half-open); make it succeed.
	clk.advance(30 * time.Second)
	outcome = successResult()
	if res := cb.Deliver(ctx, m); res.Err != nil || !res.Success {
		t.Fatalf("half-open trial should run and succeed, got %+v", res)
	}
	// Breaker is closed again: deliveries flow.
	if res := cb.Deliver(ctx, m); res.Err != nil {
		t.Fatalf("breaker should be closed after recovery, got %v", res.Err)
	}
}

func TestCircuitHalfOpenReopensOnFailure(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	stub := &stubDeliverer{fn: func(queue.Message) Result { return failResult() }}
	cb := NewCircuitBreaker(stub, 1, 10*time.Second).WithClock(clk.now)
	ctx := context.Background()
	m := msgTo("https://a.test/hook")

	cb.Deliver(ctx, m) // threshold 1 -> opens immediately
	if res := cb.Deliver(ctx, m); !errors.Is(res.Err, ErrCircuitOpen) {
		t.Fatalf("expected open, got %v", res.Err)
	}
	clk.advance(10 * time.Second)
	// Half-open trial fails -> reopen immediately.
	if res := cb.Deliver(ctx, m); res.Err != nil {
		t.Fatalf("half-open trial should be attempted, got %v", res.Err)
	}
	if res := cb.Deliver(ctx, m); !errors.Is(res.Err, ErrCircuitOpen) {
		t.Fatalf("failed trial should reopen the breaker, got %v", res.Err)
	}
}

func TestCircuitPerEndpointIsolation(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	stub := &stubDeliverer{fn: func(m queue.Message) Result {
		if m.Event.Endpoint == "https://bad.test/hook" {
			return failResult()
		}
		return successResult()
	}}
	cb := NewCircuitBreaker(stub, 2, time.Minute).WithClock(clk.now)
	ctx := context.Background()
	bad, good := msgTo("https://bad.test/hook"), msgTo("https://good.test/hook")

	cb.Deliver(ctx, bad)
	cb.Deliver(ctx, bad) // bad opens
	if res := cb.Deliver(ctx, bad); !errors.Is(res.Err, ErrCircuitOpen) {
		t.Fatalf("bad endpoint should be open, got %v", res.Err)
	}
	// The good endpoint is unaffected.
	if res := cb.Deliver(ctx, good); res.Err != nil || !res.Success {
		t.Fatalf("good endpoint should still deliver, got %+v", res)
	}
}

func TestCircuitSuccessResetsFailureCount(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	outcome := failResult()
	stub := &stubDeliverer{fn: func(queue.Message) Result { return outcome }}
	cb := NewCircuitBreaker(stub, 3, time.Minute).WithClock(clk.now)
	ctx := context.Background()
	m := msgTo("https://a.test/hook")

	outcome = failResult()
	cb.Deliver(ctx, m)
	cb.Deliver(ctx, m) // 2 consecutive failures (threshold 3, still closed)
	outcome = successResult()
	cb.Deliver(ctx, m) // success resets the count
	outcome = failResult()
	cb.Deliver(ctx, m)
	cb.Deliver(ctx, m) // 2 again -> still below threshold
	if res := cb.Deliver(ctx, m); errors.Is(res.Err, ErrCircuitOpen) {
		t.Fatal("breaker opened despite a success resetting the failure count")
	}
}
