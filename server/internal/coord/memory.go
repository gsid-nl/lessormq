package coord

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"
)

// Memory keeps everything in this process: for an installation of one node.
type Memory struct {
	local   *Local
	wake    *wakeups
	mu      sync.Mutex
	kv      map[string]kvEntry
	buckets map[string]*bucket
	hashes  map[string]*hash
	zsets   map[string]*zset
	stop    chan struct{}
}

type kvEntry struct {
	value   string
	expires time.Time
}

type bucket struct {
	tokens float64
	at     time.Time
}

type hash struct {
	fields  map[string]string
	expires time.Time
}

type zset struct {
	members map[string]float64
	expires time.Time
}

func NewMemory() *Memory {
	m := &Memory{
		local:   NewLocal(),
		wake:    newWakeups(),
		kv:      map[string]kvEntry{},
		buckets: map[string]*bucket{},
		hashes:  map[string]*hash{},
		zsets:   map[string]*zset{},
		stop:    make(chan struct{}),
	}
	go m.janitor()
	return m
}

func (m *Memory) janitor() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			m.prune()
		}
	}
}

func (m *Memory) prune() {
	now := time.Now()
	m.local.prune()
	m.wake.prune()
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, e := range m.kv {
		if now.After(e.expires) {
			delete(m.kv, k)
		}
	}
	for k, b := range m.buckets {
		if now.Sub(b.at) > time.Minute {
			delete(m.buckets, k)
		}
	}
	for k, h := range m.hashes {
		if now.After(h.expires) {
			delete(m.hashes, k)
		}
	}
	for k, z := range m.zsets {
		if now.After(z.expires) {
			delete(m.zsets, k)
		}
	}
}

func (m *Memory) Shared() bool                       { return false }
func (m *Memory) Ping(context.Context) error         { return nil }
func (m *Memory) Local() *Local                      { return m.local }
func (m *Memory) Close() error                       { close(m.stop); return nil }
func (m *Memory) Forget(_ context.Context, k string) { m.local.Del(k) }
func (m *Memory) ForgetPrefix(_ context.Context, p string) {
	m.local.DelPrefix(p)
}

func (m *Memory) Get(_ context.Context, key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.kv[key]
	if !ok || time.Now().After(e.expires) {
		return "", false
	}
	return e.value, true
}

func (m *Memory) Set(_ context.Context, key, value string, ttl time.Duration) {
	m.mu.Lock()
	m.kv[key] = kvEntry{value: value, expires: time.Now().Add(ttl)}
	m.mu.Unlock()
}

func (m *Memory) Del(_ context.Context, keys ...string) {
	m.mu.Lock()
	for _, k := range keys {
		delete(m.kv, k)
	}
	m.mu.Unlock()
}

func (m *Memory) DelPrefix(_ context.Context, prefix string) {
	m.mu.Lock()
	for k := range m.kv {
		if strings.HasPrefix(k, prefix) {
			delete(m.kv, k)
		}
	}
	m.mu.Unlock()
}

func (m *Memory) Gate(_ context.Context, key string, interval time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := "gate:" + key
	if e, ok := m.kv[k]; ok && time.Now().Before(e.expires) {
		return false
	}
	m.kv[k] = kvEntry{value: "1", expires: time.Now().Add(interval)}
	return true
}

func (m *Memory) TakeToken(_ context.Context, key string, rate int64) (bool, float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	r := float64(rate)
	b := m.buckets[key]
	if b == nil {
		b = &bucket{tokens: r, at: now}
		m.buckets[key] = b
	}
	b.tokens = math.Min(r, b.tokens+math.Max(0, now.Sub(b.at).Seconds())*r)
	b.at = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0, nil
	}
	return false, (1 - b.tokens) / r, nil
}

func (m *Memory) IncrIfSet(_ context.Context, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.kv[key]
	if !ok || time.Now().After(e.expires) {
		return
	}
	n := 0
	for _, c := range e.value {
		if c < '0' || c > '9' {
			return
		}
		n = n*10 + int(c-'0')
	}
	e.value = itoa(n + 1)
	m.kv[key] = e
}

func (m *Memory) Notify(_ context.Context, key string) { m.wake.notify(key) }

func (m *Memory) Wait(ctx context.Context, key string, d time.Duration) bool {
	return m.wake.wait(ctx, key, d)
}

func (m *Memory) HSet(_ context.Context, key, field, value string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.hashes[key]
	if h == nil {
		h = &hash{fields: map[string]string{}}
		m.hashes[key] = h
	}
	h.fields[field] = value
	h.expires = time.Now().Add(ttl)
}

func (m *Memory) HGetAll(_ context.Context, keys ...string) []map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]map[string]string, len(keys))
	now := time.Now()
	for i, k := range keys {
		out[i] = map[string]string{}
		if h := m.hashes[k]; h != nil && now.Before(h.expires) {
			for f, v := range h.fields {
				out[i][f] = v
			}
		}
	}
	return out
}

func (m *Memory) ZAdd(_ context.Context, key, member string, score float64, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	z := m.zsets[key]
	if z == nil {
		z = &zset{members: map[string]float64{}}
		m.zsets[key] = z
	}
	z.members[member] = score
	z.expires = time.Now().Add(ttl)
}

func (m *Memory) ZRem(_ context.Context, key, member string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if z := m.zsets[key]; z != nil {
		delete(z.members, member)
	}
}

func (m *Memory) ZAbove(_ context.Context, key string, min float64) map[string]float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]float64{}
	z := m.zsets[key]
	if z == nil {
		return out
	}
	for member, score := range z.members {
		if score <= min {
			delete(z.members, member)
			continue
		}
		out[member] = score
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
