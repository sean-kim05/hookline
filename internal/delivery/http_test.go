package delivery

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/queue"
)

// eventForTest builds an event aimed at url. If opts["idempotency"] is set it
// becomes the event's idempotency key.
func eventForTest(url string, opts map[string]string) event.Event {
	return event.Event{
		ID:             "evt-1",
		Endpoint:       url,
		Payload:        []byte(`{"hello":"world"}`),
		IdempotencyKey: opts["idempotency"],
	}
}

func TestHTTPDelivererSuccessAndHeaders(t *testing.T) {
	var (
		gotBody    []byte
		gotHeaders http.Header
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	signer := NewSigner("whsec_test")
	d := NewHTTPDeliverer(srv.Client(), signer)

	msg := queue.Message{
		ID:       "msg-1",
		Attempts: 3,
		Event: eventForTest(srv.URL, map[string]string{
			"idempotency": "evt-42",
		}),
	}

	res := d.Deliver(context.Background(), msg)
	if !res.Success {
		t.Fatalf("Deliver: Success=false, status=%d, err=%v", res.StatusCode, res.Err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", res.StatusCode)
	}
	if string(gotBody) != string(msg.Event.Payload) {
		t.Errorf("body = %q, want %q", gotBody, msg.Event.Payload)
	}
	if gotHeaders.Get(HeaderID) != "msg-1" {
		t.Errorf("%s = %q, want msg-1", HeaderID, gotHeaders.Get(HeaderID))
	}
	if gotHeaders.Get(HeaderAttempt) != "3" {
		t.Errorf("%s = %q, want 3", HeaderAttempt, gotHeaders.Get(HeaderAttempt))
	}
	if gotHeaders.Get(HeaderIdempotencyKey) != "evt-42" {
		t.Errorf("%s = %q, want evt-42", HeaderIdempotencyKey, gotHeaders.Get(HeaderIdempotencyKey))
	}

	// The signature must verify against the timestamp the deliverer sent.
	tsHeader := gotHeaders.Get(HeaderTimestamp)
	unix, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		t.Fatalf("bad timestamp header %q: %v", tsHeader, err)
	}
	if !signer.Verify(time.Unix(unix, 0), gotBody, gotHeaders.Get(HeaderSignature)) {
		t.Error("signature header did not verify against the sent body and timestamp")
	}
}

func TestHTTPDelivererNon2xxIsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	d := NewHTTPDeliverer(srv.Client(), nil)
	res := d.Deliver(context.Background(), queue.Message{Event: eventForTest(srv.URL, nil)})
	if res.Success {
		t.Error("Success=true for a 500 response")
	}
	if res.StatusCode != http.StatusInternalServerError {
		t.Errorf("StatusCode = %d, want 500", res.StatusCode)
	}
}

func TestHTTPDelivererTransportErrorIsFailure(t *testing.T) {
	d := NewHTTPDeliverer(&http.Client{Timeout: time.Second}, nil)
	// Nothing is listening here, so Do returns a transport error.
	res := d.Deliver(context.Background(), queue.Message{
		Event: eventForTest("http://127.0.0.1:0/nope", nil),
	})
	if res.Success {
		t.Error("Success=true despite a transport error")
	}
	if res.Err == nil {
		t.Error("Err=nil despite a transport error")
	}
}
