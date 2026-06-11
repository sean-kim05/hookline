package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestSignerMatchesScheme(t *testing.T) {
	// Independently recompute the documented scheme — HMAC-SHA256 over
	// "<unix>.<body>", hex, v1-prefixed — and assert Sign produces exactly it.
	// This pins the wire format: a change here breaks every deployed consumer.
	s := NewSigner("whsec_test")
	ts := time.Unix(1_700_000_000, 0)
	body := []byte(`{"hello":"world"}`)

	mac := hmac.New(sha256.New, []byte("whsec_test"))
	mac.Write([]byte("1700000000.{\"hello\":\"world\"}"))
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))

	if got := s.Sign(ts, body); got != want {
		t.Errorf("Sign = %q, want %q", got, want)
	}
	if !s.Verify(ts, body, want) {
		t.Error("Verify rejected the scheme-correct signature")
	}
}

func TestSignerVerifyRejectsTamper(t *testing.T) {
	s := NewSigner("whsec_test")
	ts := time.Unix(1_700_000_000, 0)
	body := []byte(`{"amount":100}`)
	sig := s.Sign(ts, body)

	if s.Verify(ts, []byte(`{"amount":999}`), sig) {
		t.Error("Verify accepted a tampered body")
	}
	if s.Verify(ts.Add(time.Second), body, sig) {
		t.Error("Verify accepted a different timestamp (replay window defense)")
	}
	if NewSigner("different-secret").Verify(ts, body, sig) {
		t.Error("Verify accepted a signature made with a different secret")
	}
}

func TestSignerDeterministic(t *testing.T) {
	s := NewSigner("k")
	ts := time.Unix(42, 0)
	body := []byte("payload")
	if s.Sign(ts, body) != s.Sign(ts, body) {
		t.Error("Sign is not deterministic for the same inputs")
	}
}
