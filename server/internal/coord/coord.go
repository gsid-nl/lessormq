// Package coord holds the state LessorMQ keeps beside the database: caches, rate limits,
// the wake-ups of waiting claims, the workers seen per queue. None of it needs to last:
// the database is the truth, and all of it is rebuilt from there or simply starts over.
//
// One node keeps it in memory. Several nodes share it through Redis (REDIS_URL): then a
// publish on one node wakes a claim waiting on another, a rate limit holds for the
// tenant as a whole, and the list of workers spans every node.
package coord

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Coord is that state.
type Coord interface {
	// Shared tells whether the state is shared between nodes (Redis).
	Shared() bool
	Ping(ctx context.Context) error

	// Cache: a value for a while. Get reports false when there is none.
	Get(ctx context.Context, key string) (string, bool)
	Set(ctx context.Context, key, value string, ttl time.Duration)
	Del(ctx context.Context, keys ...string)
	DelPrefix(ctx context.Context, prefix string)

	// Local is a cache that every node keeps itself, dropped on every node by Forget and
	// ForgetPrefix: read on every claim, so it costs no round trip.
	Local() *Local
	Forget(ctx context.Context, key string)
	ForgetPrefix(ctx context.Context, prefix string)

	// Gate is true for the first caller in each interval: at most once per interval.
	Gate(ctx context.Context, key string, interval time.Duration) bool
	// TakeToken takes a token from the bucket key: rate tokens a second, as many at most.
	// When there is none it says how many seconds to wait.
	TakeToken(ctx context.Context, key string, rate int64) (bool, float64, error)
	// IncrIfSet counts a cached number on, only while one is cached.
	IncrIfSet(ctx context.Context, key string)

	// Notify wakes one claim waiting on key, or keeps a token for the next one.
	Notify(ctx context.Context, key string)
	// Wait waits at most d for a token of key: true when woken.
	Wait(ctx context.Context, key string, d time.Duration) bool

	// Hashes and sorted sets for the workers of a queue.
	HSet(ctx context.Context, key, field, value string, ttl time.Duration)
	HGetAll(ctx context.Context, keys ...string) []map[string]string
	ZAdd(ctx context.Context, key, member string, score float64, ttl time.Duration)
	ZRem(ctx context.Context, key, member string)
	// ZAbove drops the members scored up to min and returns the others.
	ZAbove(ctx context.Context, key string, min float64) map[string]float64

	Close() error
}

// Local is a cache with a time to live, kept in this process.
type Local struct {
	mu      sync.Mutex
	entries map[string]localEntry
}

type localEntry struct {
	value   any
	expires time.Time
}

func NewLocal() *Local { return &Local{entries: map[string]localEntry{}} }

func (l *Local) Get(key string) (any, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[key]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.value, true
}

func (l *Local) Set(key string, value any, ttl time.Duration) {
	l.mu.Lock()
	l.entries[key] = localEntry{value: value, expires: time.Now().Add(ttl)}
	l.mu.Unlock()
}

func (l *Local) Del(key string) {
	l.mu.Lock()
	delete(l.entries, key)
	l.mu.Unlock()
}

func (l *Local) DelPrefix(prefix string) {
	l.mu.Lock()
	for k := range l.entries {
		if strings.HasPrefix(k, prefix) {
			delete(l.entries, k)
		}
	}
	l.mu.Unlock()
}

func (l *Local) prune() {
	now := time.Now()
	l.mu.Lock()
	for k, e := range l.entries {
		if now.After(e.expires) {
			delete(l.entries, k)
		}
	}
	l.mu.Unlock()
}

// wakeups are the claims waiting per key on this node, and the tokens of events that came
// while none waited: one event wakes one claim, and a token stays until taken (or a
// minute), so an event between a claim finding nothing and starting to wait still wakes it.
type wakeups struct {
	mu   sync.Mutex
	keys map[string]*wakeKey
}

type wakeKey struct {
	waiters []chan struct{}
	tokens  []time.Time
}

const (
	tokensKept = 50
	tokenTTL   = time.Minute
)

func newWakeups() *wakeups { return &wakeups{keys: map[string]*wakeKey{}} }

func (w *wakeups) notify(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	k := w.keys[key]
	if k == nil {
		k = &wakeKey{}
		w.keys[key] = k
	}
	if len(k.waiters) > 0 {
		ch := k.waiters[0]
		k.waiters = k.waiters[1:]
		close(ch)
		return
	}
	k.tokens = append(k.tokens, time.Now().Add(tokenTTL))
	if len(k.tokens) > tokensKept {
		k.tokens = k.tokens[len(k.tokens)-tokensKept:]
	}
}

func (w *wakeups) wait(ctx context.Context, key string, d time.Duration) bool {
	w.mu.Lock()
	k := w.keys[key]
	if k == nil {
		k = &wakeKey{}
		w.keys[key] = k
	}
	now := time.Now()
	for len(k.tokens) > 0 {
		t := k.tokens[0]
		k.tokens = k.tokens[1:]
		if now.Before(t) {
			w.mu.Unlock()
			return true
		}
	}
	ch := make(chan struct{})
	k.waiters = append(k.waiters, ch)
	w.mu.Unlock()

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
	case <-ctx.Done():
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, c := range k.waiters {
		if c == ch {
			k.waiters = append(k.waiters[:i], k.waiters[i+1:]...)
			return false
		}
	}
	// Woken just as the time ran out: the token is this claim's.
	return true
}

func (w *wakeups) prune() {
	now := time.Now()
	w.mu.Lock()
	for key, k := range w.keys {
		live := k.tokens[:0]
		for _, t := range k.tokens {
			if now.Before(t) {
				live = append(live, t)
			}
		}
		k.tokens = live
		if len(k.tokens) == 0 && len(k.waiters) == 0 {
			delete(w.keys, key)
		}
	}
	w.mu.Unlock()
}
