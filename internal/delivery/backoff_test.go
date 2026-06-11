package delivery

import (
	"testing"
	"time"
)

func TestBackoffFullJitterBounds(t *testing.T) {
	b := NewBackoff(time.Second, time.Minute)

	// rand=0 floors every delay at zero (a valid full-jitter sample).
	b.rand = func() float64 { return 0 }
	for attempt := 1; attempt <= 10; attempt++ {
		if d := b.Delay(attempt); d != 0 {
			t.Errorf("attempt %d with rand=0: delay = %v, want 0", attempt, d)
		}
	}

	// rand≈1 yields the pre-jitter ceiling: base*2^(attempt-1), capped at Max.
	b.rand = func() float64 { return 0.9999999 }
	want := []time.Duration{
		1 * time.Second, // attempt 1: base
		2 * time.Second, // attempt 2
		4 * time.Second, // attempt 3
		8 * time.Second, // attempt 4
		16 * time.Second,
		32 * time.Second,
		time.Minute, // attempt 7: 64s capped to Max=60s
		time.Minute, // stays capped
	}
	for i, w := range want {
		attempt := i + 1
		got := b.Delay(attempt)
		// Allow a hair under the ceiling because rand<1.
		if got > w || got < w-time.Millisecond {
			t.Errorf("attempt %d: ceiling delay = %v, want ~%v", attempt, got, w)
		}
	}
}

func TestBackoffNeverExceedsMax(t *testing.T) {
	b := NewBackoff(time.Second, 5*time.Second)
	b.rand = func() float64 { return 0.9999999 }
	for attempt := 1; attempt <= 100; attempt++ {
		if d := b.Delay(attempt); d > 5*time.Second {
			t.Fatalf("attempt %d: delay %v exceeds Max", attempt, d)
		}
	}
}

func TestBackoffClampsNonPositiveAttempt(t *testing.T) {
	b := NewBackoff(time.Second, time.Minute)
	b.rand = func() float64 { return 0.9999999 }
	// attempt 0 is treated as attempt 1 → base ceiling, not a panic or zero.
	if d := b.Delay(0); d < 999*time.Millisecond || d > time.Second {
		t.Errorf("Delay(0) = %v, want ~1s (clamped to attempt 1)", d)
	}
}
