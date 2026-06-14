// Command sink is a minimal, high-throughput delivery target for load and
// chaos testing. It records every delivery and the set of unique event IDs it
// has seen, so a test can measure delivery throughput and verify at-least-once
// (every submitted event arrives at least once, even across a crash).
//
//	sink -addr :9100
//	GET /stats -> {"deliveries":N,"unique":M}
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", ":9100", "listen address")
	failUntil := flag.Int64("fail-until", 0, "respond 500 for the first N deliveries (to force retries)")
	delay := flag.Duration("delay", 0, "artificial per-delivery processing delay (keeps work queued for chaos tests)")
	flag.Parse()

	var deliveries atomic.Int64
	var mu sync.Mutex
	unique := make(map[string]struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /hook", func(w http.ResponseWriter, r *http.Request) {
		if *delay > 0 {
			time.Sleep(*delay)
		}
		n := deliveries.Add(1)
		if id := r.Header.Get("X-Hookline-Event-Id"); id != "" {
			mu.Lock()
			unique[id] = struct{}{}
			mu.Unlock()
		}
		if n <= *failUntil {
			http.Error(w, "forced failure", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		u := len(unique)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"deliveries": deliveries.Load(),
			"unique":     int64(u),
		})
	})

	log.Printf("sink listening on %s (fail-until=%d)", *addr, *failUntil)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}
