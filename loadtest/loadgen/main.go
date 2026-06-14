// Command loadgen submits events to Hookline's ingestion API as fast as a fixed
// pool of workers can, then reports ingestion throughput and latency
// percentiles. It is the Go equivalent of the k6 script in loadtest/k6, usable
// without installing k6.
//
//	loadgen -url http://localhost:8080 -api-key dev-key \
//	        -target http://localhost:9100/hook -n 20000 -c 50
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	base := flag.String("url", "http://localhost:8080", "Hookline base URL")
	apiKey := flag.String("api-key", "dev-key", "producer API key")
	target := flag.String("target", "http://localhost:9100/hook", "consumer endpoint to deliver to")
	n := flag.Int("n", 20000, "number of events to submit")
	c := flag.Int("c", 50, "concurrent submitters")
	flag.Parse()

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *c * 2,
			MaxIdleConnsPerHost: *c * 2,
		},
	}
	body := []byte(fmt.Sprintf(`{"endpoint":%q,"payload":{"hello":"load"}}`, *target))

	var (
		ok, failed atomic.Int64
		latMu      sync.Mutex
		latencies  = make([]time.Duration, 0, *n)
		jobs       = make(chan int, *c*2)
		wg         sync.WaitGroup
	)

	worker := func() {
		defer wg.Done()
		for range jobs {
			start := time.Now()
			req, _ := http.NewRequest(http.MethodPost, *base+"/v1/events", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+*apiKey)
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			lat := time.Since(start)
			if err != nil {
				failed.Add(1)
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusAccepted {
				ok.Add(1)
			} else {
				failed.Add(1)
			}
			latMu.Lock()
			latencies = append(latencies, lat)
			latMu.Unlock()
		}
	}

	wg.Add(*c)
	for i := 0; i < *c; i++ {
		go worker()
	}

	start := time.Now()
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(p / 100 * float64(len(latencies)-1))
		return latencies[idx]
	}

	fmt.Printf("submitted:   %d (%d accepted, %d failed)\n", *n, ok.Load(), failed.Load())
	fmt.Printf("elapsed:     %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:  %.0f events/sec accepted\n", float64(ok.Load())/elapsed.Seconds())
	fmt.Printf("latency p50: %s\n", pct(50).Round(time.Microsecond))
	fmt.Printf("latency p95: %s\n", pct(95).Round(time.Microsecond))
	fmt.Printf("latency p99: %s\n", pct(99).Round(time.Microsecond))
	fmt.Printf("latency max: %s\n", pct(100).Round(time.Microsecond))

	if failed.Load() > 0 {
		os.Exit(1)
	}
}
