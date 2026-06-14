package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/queue"
)

func TestRateLimiterAllowsBurstThenBlocks(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	stub := &stubDeliverer{fn: func(queue.Message) Result { return successResult() }}
	rl := NewRateLimiter(stub, 1, 3).WithClock(clk.now) // 1/s, burst 3
	ctx := context.Background()
	m := msgTo("https://a.test/hook")

	for i := 0; i < 3; i++ {
		if res := rl.Deliver(ctx, m); res.Err != nil {
			t.Fatalf("burst delivery %d was limited: %v", i, res.Err)
		}
	}
	res := rl.Deliver(ctx, m)
	if !errors.Is(res.Err, ErrRateLimited) {
		t.Fatalf("want ErrRateLimited after burst, got %v", res.Err)
	}
	if stub.calls.Load() != 3 {
		t.Fatalf("limited delivery still called next; calls=%d want 3", stub.calls.Load())
	}
}

func TestRateLimiterRefillsOverTime(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	stub := &stubDeliverer{fn: func(queue.Message) Result { return successResult() }}
	rl := NewRateLimiter(stub, 2, 2).WithClock(clk.now) // 2/s, burst 2
	ctx := context.Background()
	m := msgTo("https://a.test/hook")

	rl.Deliver(ctx, m)
	rl.Deliver(ctx, m) // burst exhausted
	if res := rl.Deliver(ctx, m); !errors.Is(res.Err, ErrRateLimited) {
		t.Fatalf("expected limited, got %v", res.Err)
	}
	// Half a second refills exactly one token at 2/s.
	clk.advance(500 * time.Millisecond)
	if res := rl.Deliver(ctx, m); res.Err != nil {
		t.Fatalf("token should have refilled, got %v", res.Err)
	}
	if res := rl.Deliver(ctx, m); !errors.Is(res.Err, ErrRateLimited) {
		t.Fatalf("only one token should have refilled, got %v", res.Err)
	}
}

func TestRateLimiterPerEndpoint(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	stub := &stubDeliverer{fn: func(queue.Message) Result { return successResult() }}
	rl := NewRateLimiter(stub, 1, 1).WithClock(clk.now)
	ctx := context.Background()

	a, b := msgTo("https://a.test/hook"), msgTo("https://b.test/hook")
	if res := rl.Deliver(ctx, a); res.Err != nil {
		t.Fatalf("a first: %v", res.Err)
	}
	if res := rl.Deliver(ctx, a); !errors.Is(res.Err, ErrRateLimited) {
		t.Fatalf("a second should be limited, got %v", res.Err)
	}
	// b has its own bucket and is unaffected by a's exhaustion.
	if res := rl.Deliver(ctx, b); res.Err != nil {
		t.Fatalf("b should have its own budget, got %v", res.Err)
	}
}

func TestRateLimiterDisabled(t *testing.T) {
	stub := &stubDeliverer{fn: func(queue.Message) Result { return successResult() }}
	rl := NewRateLimiter(stub, 0, 0) // disabled
	ctx := context.Background()
	m := msgTo("https://a.test/hook")
	for i := 0; i < 100; i++ {
		if res := rl.Deliver(ctx, m); res.Err != nil {
			t.Fatalf("disabled limiter blocked delivery %d: %v", i, res.Err)
		}
	}
}
