// Command loadtest drives a LessorMQ server with publishers and workers and reports
// throughput, latency per call, end-to-end latency and errors.
//
// Publishers publish at a fixed total rate (open loop: a slow server makes them fall
// behind, which is reported, rather than slowing the offered load). Workers claim with a
// wait, optionally spend some time on each message, and ack. Every message carries the
// time it was published, so the time from publish to ack is measured too.
//
//	go run . -url http://127.0.0.1:18080 -key lmq_... -rate 300 -workers 50 -duration 30m
//	go run . -url ... -key ... -burst 10000 -publishers 50 -workers 50
//
// Use a tenant of its own: the messages stay in the queue's completed set for its
// retention.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	url        string
	key        string
	queue      string
	rate       float64
	publishers int
	workers    int
	duration   time.Duration
	burst      int
	wait       int
	work       time.Duration
	bodyBytes  int
	report     time.Duration
	drain      time.Duration
	batch      int
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", os.Getenv("LESSORMQ_URL"), "server URL (LESSORMQ_URL)")
	flag.StringVar(&c.key, "key", os.Getenv("LESSORMQ_KEY"), "API key with publish and consume scopes (LESSORMQ_KEY)")
	flag.StringVar(&c.queue, "queue", fmt.Sprintf("loadtest-%d", time.Now().Unix()), "queue")
	flag.Float64Var(&c.rate, "rate", 100, "publishes per second, over all publishers (0: as fast as they can)")
	flag.IntVar(&c.publishers, "publishers", 20, "concurrent publishers")
	flag.IntVar(&c.workers, "workers", 20, "concurrent workers (claim, work, ack)")
	flag.DurationVar(&c.duration, "duration", time.Minute, "how long to publish")
	flag.IntVar(&c.burst, "burst", 0, "publish this many messages as fast as possible instead of at -rate")
	flag.IntVar(&c.wait, "wait", 2, "wait_seconds of each claim")
	flag.DurationVar(&c.work, "work", 0, "time a worker spends on each message")
	flag.IntVar(&c.bodyBytes, "body-bytes", 200, "approximate size of each body")
	flag.DurationVar(&c.report, "report", 10*time.Second, "how often to report")
	flag.IntVar(&c.batch, "batch", 0, "claim up to this many messages at once and ack them together (0: one at a time)")
	flag.DurationVar(&c.drain, "drain", 2*time.Minute, "how long workers may take to empty the queue after publishing stops")
	flag.Parse()

	if c.url == "" || c.key == "" {
		fmt.Fprintln(os.Stderr, "-url and -key (or LESSORMQ_URL and LESSORMQ_KEY) are required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	os.Exit(run(ctx, c))
}

// Op names a call whose latency is recorded.
type Op string

const (
	opPublish Op = "publish"
	opClaim   Op = "claim"
	opAck     Op = "ack"
	opE2E     Op = "publish→ack"
)

var ops = []Op{opPublish, opClaim, opAck, opE2E}

type stats struct {
	mu        sync.Mutex
	latencies map[Op][]time.Duration
	errors    map[string]int

	published atomic.Int64
	acked     atomic.Int64
	empty     atomic.Int64
	behind    atomic.Int64
}

func newStats() *stats {
	return &stats{latencies: map[Op][]time.Duration{}, errors: map[string]int{}}
}

func (s *stats) observe(op Op, d time.Duration) {
	s.mu.Lock()
	s.latencies[op] = append(s.latencies[op], d)
	s.mu.Unlock()
}

func (s *stats) fail(op Op, what string) {
	s.mu.Lock()
	s.errors[string(op)+" "+what]++
	s.mu.Unlock()
}

// take returns the latencies and errors so far and starts a new interval.
func (s *stats) take() (map[Op][]time.Duration, map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, e := s.latencies, s.errors
	s.latencies, s.errors = map[Op][]time.Duration{}, map[string]int{}

	return l, e
}

type client struct {
	http *http.Client
	url  string
	key  string
}

func (c *client) post(ctx context.Context, path string, body any) (int, []byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)

	return resp.StatusCode, data, err
}

func run(ctx context.Context, c config) int {
	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        c.publishers + c.workers + 10,
		MaxIdleConnsPerHost: c.publishers + c.workers + 10,
		IdleConnTimeout:     90 * time.Second,
	}
	cl := &client{
		http: &http.Client{Transport: transport, Timeout: time.Duration(c.wait+30) * time.Second},
		url:  strings.TrimRight(c.url, "/"),
		key:  c.key,
	}
	st := newStats()
	padding := strings.Repeat("x", max(0, c.bodyBytes-60))

	fmt.Printf("queue %s: ", c.queue)
	if c.burst > 0 {
		fmt.Printf("burst of %d with %d publishers", c.burst, c.publishers)
	} else {
		fmt.Printf("%.0f publishes/s for %s with %d publishers", c.rate, c.duration, c.publishers)
	}
	fmt.Printf(", %d workers (wait %ds, work %s), bodies of about %d bytes\n\n", c.workers, c.wait, c.work, c.bodyBytes)

	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()

	var workers sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		workers.Add(1)
		go func(n int) {
			defer workers.Done()
			work(workCtx, cl, c, st, fmt.Sprintf("loadtest-%d", n))
		}(i)
	}

	start := time.Now()
	reporter := startReporter(ctx, c, st, start)

	publishCtx, stopPublish := context.WithTimeout(ctx, c.duration)
	if c.burst > 0 {
		stopPublish()
		publishCtx, stopPublish = context.WithCancel(ctx)
	}
	publish(publishCtx, cl, c, st, padding)
	stopPublish()
	published := time.Since(start)

	// Let the workers empty the queue.
	drainStart := time.Now()
	for st.acked.Load() < st.published.Load() && time.Since(drainStart) < c.drain && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
	}
	drained := time.Since(drainStart)
	stopWork()
	workers.Wait()
	reporter.stop()

	fmt.Printf("\n== Summary over %s (publishing %s, emptying the queue after that %s)\n", time.Since(start).Round(time.Millisecond), published.Round(time.Millisecond), drained.Round(time.Millisecond))
	fmt.Printf("published %d (%.1f/s), acked %d, left in the queue %d, publishes behind schedule %d, empty claims %d\n",
		st.published.Load(), float64(st.published.Load())/published.Seconds(), st.acked.Load(),
		st.published.Load()-st.acked.Load(), st.behind.Load(), st.empty.Load())
	reporter.printTotals()

	if reporter.totalErrors() > 0 || st.acked.Load() < st.published.Load() {
		return 1
	}

	return 0
}

// publish sends messages at c.rate over c.publishers goroutines, or a burst of c.burst
// as fast as they can.
func publish(ctx context.Context, cl *client, c config, st *stats, padding string) {
	jobs := make(chan struct{}, c.publishers)
	var wg sync.WaitGroup

	for i := 0; i < c.publishers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				body := map[string]any{"published_at": time.Now().UnixNano(), "pad": padding}
				t := time.Now()
				status, data, err := cl.post(ctx, "/v1/queues/"+c.queue+"/messages", map[string]any{"body": body})
				d := time.Since(t)

				switch {
				case err != nil:
					if ctx.Err() == nil {
						st.fail(opPublish, "network: "+short(err))
					}
				case status != http.StatusCreated:
					st.fail(opPublish, fmt.Sprintf("%d %s", status, code(data)))
				default:
					st.observe(opPublish, d)
					st.published.Add(1)
				}
			}
		}()
	}

	switch {
	case c.burst > 0:
		for i := 0; i < c.burst && ctx.Err() == nil; i++ {
			jobs <- struct{}{}
		}
	case c.rate <= 0:
		for ctx.Err() == nil {
			jobs <- struct{}{}
		}
	default:
		interval := time.Duration(float64(time.Second) / c.rate)
		next := time.Now()

		for ctx.Err() == nil {
			now := time.Now()
			if now.Before(next) {
				time.Sleep(next.Sub(now))
			}

			select {
			case jobs <- struct{}{}:
			default:
				// Every publisher is busy: the server is slower than the offered rate.
				st.behind.Add(1)
				select {
				case jobs <- struct{}{}:
				case <-ctx.Done():
				}
			}

			next = next.Add(interval)
			if lag := time.Since(next); lag > time.Second {
				// Do not catch up in a rush after a stall; carry on at the rate.
				next = time.Now()
			}
		}
	}

	close(jobs)
	wg.Wait()
}

type lease struct {
	Receipt string `json:"receipt"`
	Message struct {
		ID   string `json:"id"`
		Body struct {
			PublishedAt int64 `json:"published_at"`
		} `json:"body"`
	} `json:"message"`
}

// workBatch claims up to c.batch messages at once, waits c.work, and acks them together.
func workBatch(ctx context.Context, cl *client, c config, st *stats, consumer string) {
	for ctx.Err() == nil {
		t := time.Now()
		status, data, err := cl.post(ctx, "/v1/queues/"+c.queue+"/claims",
			map[string]any{"wait_seconds": c.wait, "consumer": consumer, "max_messages": c.batch})
		d := time.Since(t)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				st.fail(opClaim, "network: "+short(err))
				time.Sleep(100 * time.Millisecond)
			}
			continue
		case status == http.StatusNoContent:
			st.empty.Add(1)
			continue
		case status != http.StatusOK:
			st.fail(opClaim, fmt.Sprintf("%d %s", status, code(data)))
			time.Sleep(100 * time.Millisecond)
			continue
		}
		st.observe(opClaim, d)
		var batch struct {
			Items []lease `json:"items"`
		}
		if err := json.Unmarshal(data, &batch); err != nil || len(batch.Items) == 0 {
			st.fail(opClaim, "unreadable batch")
			continue
		}
		if c.work > 0 {
			time.Sleep(c.work)
		}
		leases := make([]map[string]string, len(batch.Items))
		for i, l := range batch.Items {
			leases[i] = map[string]string{"message": l.Message.ID, "receipt": l.Receipt}
		}
		t = time.Now()
		status, data, err = cl.post(context.Background(), "/v1/acks", map[string]any{"leases": leases})
		d = time.Since(t)
		switch {
		case err != nil:
			st.fail(opAck, "network: "+short(err))
		case status != http.StatusOK:
			st.fail(opAck, fmt.Sprintf("%d %s", status, code(data)))
		default:
			var res struct {
				Items []struct {
					Outcome string `json:"outcome"`
				} `json:"items"`
			}
			json.Unmarshal(data, &res)
			st.observe(opAck, d)
			for i, r := range res.Items {
				if r.Outcome != "acked" {
					st.fail(opAck, "batch: "+r.Outcome)
					continue
				}
				st.observe(opE2E, time.Since(time.Unix(0, batch.Items[i].Message.Body.PublishedAt)))
				st.acked.Add(1)
			}
		}
	}
}

// work claims, waits c.work, and acks, until ctx ends.
func work(ctx context.Context, cl *client, c config, st *stats, consumer string) {
	if c.batch > 0 {
		workBatch(ctx, cl, c, st, consumer)
		return
	}
	for ctx.Err() == nil {
		t := time.Now()
		status, data, err := cl.post(ctx, "/v1/queues/"+c.queue+"/claims", map[string]any{"wait_seconds": c.wait, "consumer": consumer})
		d := time.Since(t)

		switch {
		case err != nil:
			if ctx.Err() == nil {
				st.fail(opClaim, "network: "+short(err))
				time.Sleep(100 * time.Millisecond)
			}
			continue
		case status == http.StatusNoContent:
			st.empty.Add(1)
			continue
		case status != http.StatusOK:
			st.fail(opClaim, fmt.Sprintf("%d %s", status, code(data)))
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Only claims that got a message: an empty one is as long as its wait.
		st.observe(opClaim, d)

		var l lease
		if err := json.Unmarshal(data, &l); err != nil {
			st.fail(opClaim, "unreadable lease")
			continue
		}

		if c.work > 0 {
			time.Sleep(c.work)
		}

		t = time.Now()
		// The ack is not cancelled with ctx: a message taken is finished.
		status, data, err = cl.post(context.Background(), "/v1/messages/"+l.Message.ID+"/ack", map[string]any{"receipt": l.Receipt})
		d = time.Since(t)

		switch {
		case err != nil:
			st.fail(opAck, "network: "+short(err))
		case status != http.StatusNoContent:
			st.fail(opAck, fmt.Sprintf("%d %s", status, code(data)))
		default:
			st.observe(opAck, d)
			st.observe(opE2E, time.Since(time.Unix(0, l.Message.Body.PublishedAt)))
			st.acked.Add(1)
		}
	}
}

type reporter struct {
	done      chan struct{}
	finished  sync.WaitGroup
	mu        sync.Mutex
	all       map[Op][]time.Duration
	allErrors map[string]int
	st        *stats
}

func startReporter(ctx context.Context, c config, st *stats, start time.Time) *reporter {
	r := &reporter{done: make(chan struct{}), all: map[Op][]time.Duration{}, allErrors: map[string]int{}, st: st}
	r.finished.Add(1)

	go func() {
		defer r.finished.Done()
		ticker := time.NewTicker(c.report)
		defer ticker.Stop()
		lastPublished, lastAcked, last := int64(0), int64(0), time.Now()

		for {
			select {
			case <-ticker.C:
			case <-r.done:
				r.collect()
				return
			case <-ctx.Done():
				r.collect()
				return
			}

			latencies, errors := r.collect()
			now := time.Now()
			seconds := now.Sub(last).Seconds()
			published, acked := st.published.Load(), st.acked.Load()

			fmt.Printf("[%6s] published %6.1f/s  acked %6.1f/s  in queue %6d", now.Sub(start).Round(time.Second),
				float64(published-lastPublished)/seconds, float64(acked-lastAcked)/seconds, published-acked)
			for _, op := range ops {
				fmt.Printf("  %s p50 %s p99 %s", op, ms(percentile(latencies[op], 50)), ms(percentile(latencies[op], 99)))
			}
			fmt.Println()
			for what, n := range errors {
				fmt.Printf("          error: %s ×%d\n", what, n)
			}

			lastPublished, lastAcked, last = published, acked, now
		}
	}()

	return r
}

func (r *reporter) collect() (map[Op][]time.Duration, map[string]int) {
	latencies, errors := r.st.take()

	r.mu.Lock()
	defer r.mu.Unlock()
	for op, l := range latencies {
		r.all[op] = append(r.all[op], l...)
	}
	for what, n := range errors {
		r.allErrors[what] += n
	}

	return latencies, errors
}

func (r *reporter) stop() {
	close(r.done)
	r.finished.Wait()
}

func (r *reporter) totalErrors() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.allErrors {
		n += e
	}

	return n
}

func (r *reporter) printTotals() {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf("%-12s %8s %9s %9s %9s %9s %9s\n", "", "calls", "p50", "p95", "p99", "p99.9", "max")
	for _, op := range ops {
		l := r.all[op]
		fmt.Printf("%-12s %8d %9s %9s %9s %9s %9s\n", op, len(l), ms(percentile(l, 50)), ms(percentile(l, 95)),
			ms(percentile(l, 99)), ms(percentile(l, 99.9)), ms(percentile(l, 100)))
	}

	if len(r.allErrors) == 0 {
		fmt.Println("no errors")
		return
	}

	keys := make([]string, 0, len(r.allErrors))
	for what := range r.allErrors {
		keys = append(keys, what)
	}
	sort.Strings(keys)
	for _, what := range keys {
		fmt.Printf("error: %s ×%d\n", what, r.allErrors[what])
	}
}

func percentile(l []time.Duration, p float64) time.Duration {
	if len(l) == 0 {
		return 0
	}

	sorted := append([]time.Duration(nil), l...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	i := int(float64(len(sorted)-1) * p / 100)

	return sorted[i]
}

func ms(d time.Duration) string {
	if d == 0 {
		return "-"
	}

	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func code(body []byte) string {
	var problem struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(body, &problem)

	return problem.Code
}

func short(err error) string {
	s := err.Error()
	if i := strings.LastIndex(s, ": "); i >= 0 {
		return s[i+2:]
	}

	return s
}
