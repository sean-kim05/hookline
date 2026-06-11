package queuetest

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/queue"
)

// This file is the model-based property test: a deliberately simple,
// independent model of the Queue contract (a map and a few ifs — no locks, no
// SQL, no storage) is driven through long random op sequences in lockstep with
// the implementation under test. After every op the two must agree on which
// messages are leasable, attempt counts, and Ack/Nack outcomes. Random
// exploration finds interleavings (expire→nack→re-lease→stale-ack...) that
// hand-written cases miss; fixed seeds keep failures reproducible.

// modelMsg mirrors the per-message state the Queue contract implies.
type modelMsg struct {
	readyAt  time.Time
	token    uint64
	leased   bool
	leaseExp time.Time
	attempts int
}

// model is the reference: token must equal the message's current token or the
// call is stale; a message is ready when its readyAt has arrived and it is not
// under a live lease.
type model struct {
	msgs map[string]*modelMsg
}

func (m *model) enqueue(id string, now time.Time) {
	m.msgs[id] = &modelMsg{readyAt: now}
}

// leaseAll claims every ready message and returns id -> expected attempts.
func (m *model) leaseAll(now time.Time, leaseFor time.Duration) map[string]int {
	out := make(map[string]int)
	for id, msg := range m.msgs {
		if msg.leased && now.Before(msg.leaseExp) {
			continue
		}
		if msg.readyAt.After(now) {
			continue
		}
		msg.token++
		msg.leased = true
		msg.leaseExp = now.Add(leaseFor)
		msg.attempts++
		out[id] = msg.attempts
	}
	return out
}

func (m *model) ack(id string, token uint64) error {
	msg, ok := m.msgs[id]
	if !ok {
		return queue.ErrNotFound
	}
	if msg.token != token {
		return queue.ErrStaleLease
	}
	delete(m.msgs, id)
	return nil
}

func (m *model) nack(id string, token uint64, retryAfter time.Duration, now time.Time) error {
	msg, ok := m.msgs[id]
	if !ok {
		return queue.ErrNotFound
	}
	if msg.token != token {
		return queue.ErrStaleLease
	}
	msg.leased = false
	msg.readyAt = now.Add(retryAfter)
	return nil
}

// errClass collapses an error to the contract's three outcomes so impl and
// model can be compared without comparing exact error values.
func errClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, queue.ErrStaleLease):
		return "stale"
	case errors.Is(err, queue.ErrNotFound):
		return "notfound"
	default:
		return "unexpected:" + err.Error()
	}
}

func testModelBasedRandomOps(t *testing.T, q queue.Queue, clk *Clock) {
	for _, seed := range []int64{1, 2, 3} {
		runModelSeed(t, q, clk, seed)
	}
}

func runModelSeed(t *testing.T, q queue.Queue, clk *Clock, seed int64) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))
	m := &model{msgs: make(map[string]*modelMsg)}

	// implTok tracks the latest fencing token the impl issued per message, so
	// stale-token probes use impl-side numbering (token values are not part of
	// the contract — only monotonicity and current-vs-stale are).
	implTok := make(map[string]uint64)
	// gone remembers ids the model has acked, to probe ErrNotFound paths.
	var gone []string
	seq := 0

	// pick returns a random known message id, occasionally an already-acked one.
	pick := func() (id string, ok bool) {
		if len(gone) > 0 && rng.Intn(10) == 0 {
			return gone[rng.Intn(len(gone))], true
		}
		if len(m.msgs) == 0 {
			return "", false
		}
		ids := make([]string, 0, len(m.msgs))
		for id := range m.msgs {
			ids = append(ids, id)
		}
		sort.Strings(ids) // map order is random; sort for per-seed determinism
		return ids[rng.Intn(len(ids))], true
	}

	// tokens returns the impl and model tokens to use: usually current,
	// sometimes off-by-one to probe the stale path.
	tokens := func(id string) (impl, mod uint64) {
		impl = implTok[id]
		if msg, ok := m.msgs[id]; ok {
			mod = msg.token
		}
		if rng.Intn(4) == 0 {
			if impl > 0 {
				impl--
			}
			if mod > 0 {
				mod--
			}
		}
		return impl, mod
	}

	const ops = 400
	for i := 0; i < ops; i++ {
		switch op := rng.Intn(10); {
		case op < 3: // enqueue
			seq++
			id, err := q.Enqueue(ctx, testEvent(seq))
			if err != nil {
				t.Fatalf("seed %d op %d: Enqueue: %v", seed, i, err)
			}
			m.enqueue(id, clk.Now())

		case op < 6: // lease everything ready, compare sets and attempts
			leaseFor := time.Duration(1+rng.Intn(60_000)) * time.Millisecond
			leases, err := q.Lease(ctx, 1<<20, leaseFor)
			if err != nil {
				t.Fatalf("seed %d op %d: Lease: %v", seed, i, err)
			}
			want := m.leaseAll(clk.Now(), leaseFor)
			if len(leases) != len(want) {
				t.Fatalf("seed %d op %d: leased %d messages, model expects %d",
					seed, i, len(leases), len(want))
			}
			for _, l := range leases {
				wantAttempts, ok := want[l.Message.ID]
				if !ok {
					t.Fatalf("seed %d op %d: impl leased %q which model says is not ready",
						seed, i, l.Message.ID)
				}
				if l.Message.Attempts != wantAttempts {
					t.Fatalf("seed %d op %d: %q Attempts = %d, model expects %d",
						seed, i, l.Message.ID, l.Message.Attempts, wantAttempts)
				}
				if prev := implTok[l.Message.ID]; l.Token <= prev {
					t.Fatalf("seed %d op %d: %q token %d not greater than previous %d",
						seed, i, l.Message.ID, l.Token, prev)
				}
				implTok[l.Message.ID] = l.Token
			}

		case op < 8: // ack
			id, ok := pick()
			if !ok {
				continue
			}
			it, mt := tokens(id)
			got := errClass(q.Ack(ctx, id, it))
			want := errClass(m.ack(id, mt))
			if got != want {
				t.Fatalf("seed %d op %d: Ack(%q) = %s, model expects %s", seed, i, id, got, want)
			}
			if want == "ok" {
				gone = append(gone, id)
			}

		case op < 9: // nack
			id, ok := pick()
			if !ok {
				continue
			}
			retryAfter := time.Duration(rng.Intn(45_000)) * time.Millisecond
			it, mt := tokens(id)
			got := errClass(q.Nack(ctx, id, it, retryAfter))
			want := errClass(m.nack(id, mt, retryAfter, clk.Now()))
			if got != want {
				t.Fatalf("seed %d op %d: Nack(%q) = %s, model expects %s", seed, i, id, got, want)
			}

		default: // advance time past lease expiries and retry delays
			clk.Advance(time.Duration(1+rng.Intn(90_000)) * time.Millisecond)
		}
	}

	// Drain: far in the future everything pending must be leasable exactly as
	// the model predicts, and acking it all must empty both.
	clk.Advance(24 * time.Hour)
	leases, err := q.Lease(ctx, 1<<20, time.Minute)
	if err != nil {
		t.Fatalf("seed %d drain: Lease: %v", seed, err)
	}
	want := m.leaseAll(clk.Now(), time.Minute)
	if len(leases) != len(want) {
		t.Fatalf("seed %d drain: leased %d messages, model expects %d", seed, len(leases), len(want))
	}
	for _, l := range leases {
		if _, ok := want[l.Message.ID]; !ok {
			t.Fatalf("seed %d drain: impl leased unexpected %q", seed, l.Message.ID)
		}
		if err := q.Ack(ctx, l.Message.ID, l.Token); err != nil {
			t.Fatalf("seed %d drain: Ack(%q): %v", seed, l.Message.ID, err)
		}
		if err := m.ack(l.Message.ID, m.msgs[l.Message.ID].token); err != nil {
			t.Fatalf("seed %d drain: model ack(%q): %v", seed, l.Message.ID, err)
		}
	}
	if rest, _ := q.Lease(ctx, 1<<20, time.Minute); len(rest) != 0 {
		t.Fatalf("seed %d drain: %d messages remain after full drain", seed, len(rest))
	}
	if len(m.msgs) != 0 {
		// Should be impossible if the comparisons above held; guards the model.
		t.Fatalf("seed %d drain: model still holds %d messages", seed, len(m.msgs))
	}
}
