// Command receiver is a tiny consumer endpoint for trying Hookline locally. It
// logs each delivery and verifies the HMAC signature using the same
// delivery.Signer that Hookline signs with — so it doubles as a worked example
// of consumer-side signature verification.
//
// Usage:
//
//	receiver -addr :9099 -secret whsec_dev
package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/sean-kim05/hookline/internal/delivery"
)

func main() {
	addr := flag.String("addr", ":9099", "listen address")
	secret := flag.String("secret", "whsec_dev", "shared HMAC signing secret")
	flag.Parse()

	signer := delivery.NewSigner(*secret)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		// Verify the signature against the timestamp Hookline sent. A real
		// consumer would also reject timestamps outside an acceptable window.
		valid := false
		if ts, err := strconv.ParseInt(r.Header.Get(delivery.HeaderTimestamp), 10, 64); err == nil {
			valid = signer.Verify(time.Unix(ts, 0), body, r.Header.Get(delivery.HeaderSignature))
		}

		log.Printf("delivery: event=%s attempt=%s signature_valid=%t body=%s",
			r.Header.Get(delivery.HeaderEventID),
			r.Header.Get(delivery.HeaderAttempt),
			valid,
			body,
		)

		if !valid {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("receiver listening on %s", *addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal(err)
	}
}
