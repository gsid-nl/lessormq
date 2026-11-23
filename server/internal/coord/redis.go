package coord

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis shares the state between the nodes of an installation. Waiting claims and the
// settings cache stay in each node's memory; a wake-up or a dropped setting reaches the
// other nodes over a pub/sub channel, so a claim costs no Redis round trip.
//
// When Redis cannot be reached, nothing that matters is lost: caches miss, rate limits
// are not applied, the lease sweep gate falls back to this node's own, and wake-ups
// stay on the node they happened on.
type Redis struct {
	client  *redis.Client
	node    string
	mem     *Memory
	log     *slog.Logger
	pubsub  *redis.PubSub
	cancel  context.CancelFunc
	channel string
}

const eventsChannel = "lessormq:events"

// NewRedis connects to REDIS_URL (redis://host:6379/0).
func NewRedis(ctx context.Context, rawURL string, log *slog.Logger) (*Redis, error) {
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = 2 * time.Second
	opts.WriteTimeout = 2 * time.Second
	client := redis.NewClient(opts)
	r := &Redis{
		client:  client,
		node:    randomNode(),
		mem:     NewMemory(),
		log:     log,
		channel: eventsChannel,
	}
	subCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.pubsub = client.Subscribe(subCtx, r.channel)
	go r.listen(subCtx)
	return r, nil
}

func randomNode() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func (r *Redis) listen(ctx context.Context) {
	ch := r.pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			node, rest, _ := strings.Cut(msg.Payload, " ")
			if node == r.node {
				continue
			}
			kind, key, _ := strings.Cut(rest, " ")
			switch kind {
			case "w":
				r.mem.wake.notify(key)
			case "f":
				r.mem.local.Del(key)
			case "p":
				r.mem.local.DelPrefix(key)
			}
		}
	}
}

func (r *Redis) publish(ctx context.Context, kind, key string) {
	if err := r.client.Publish(ctx, r.channel, r.node+" "+kind+" "+key).Err(); err != nil {
		r.warn("Could not reach the other nodes", err)
	}
}

func (r *Redis) warn(msg string, err error) {
	r.log.Warn(msg, "error", err.Error())
}

func (r *Redis) Shared() bool                   { return true }
func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }
func (r *Redis) Local() *Local                  { return r.mem.local }

func (r *Redis) Close() error {
	r.cancel()
	r.pubsub.Close()
	r.mem.Close()
	return r.client.Close()
}

func (r *Redis) Forget(ctx context.Context, key string) {
	r.mem.local.Del(key)
	r.publish(ctx, "f", key)
}

func (r *Redis) ForgetPrefix(ctx context.Context, prefix string) {
	r.mem.local.DelPrefix(prefix)
	r.publish(ctx, "p", prefix)
}

func (r *Redis) Get(ctx context.Context, key string) (string, bool) {
	v, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			r.warn("Cache unavailable", err)
		}
		return "", false
	}
	return v, true
}

func (r *Redis) Set(ctx context.Context, key, value string, ttl time.Duration) {
	if err := r.client.Set(ctx, key, value, ttl).Err(); err != nil {
		r.warn("Could not cache", err)
	}
}

func (r *Redis) Del(ctx context.Context, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		r.warn("Could not drop from the cache", err)
	}
}

func (r *Redis) DelPrefix(ctx context.Context, prefix string) {
	iter := r.client.Scan(ctx, 0, prefix+"*", 500).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		r.warn("Could not drop from the cache", err)
		return
	}
	r.Del(ctx, keys...)
}

func (r *Redis) Gate(ctx context.Context, key string, interval time.Duration) bool {
	ok, err := r.client.SetNX(ctx, "queue_throttle:"+key, "1", interval).Result()
	if err != nil {
		r.log.Debug("Throttle gate unavailable, using this node's", "key", key, "error", err.Error())
		return r.mem.Gate(ctx, key, interval)
	}
	return ok
}

// takeToken takes a token from the bucket in KEYS[1]: ARGV[1] tokens a second, as many at
// most, ARGV[2] the time now. Returns {1, 0} when there was one, {0, seconds to wait}.
var takeToken = redis.NewScript(`
local rate = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'at')
local tokens = tonumber(bucket[1]) or rate
local at = tonumber(bucket[2]) or now
tokens = math.min(rate, tokens + math.max(0, now - at) * rate)
local taken = 0
local wait = 0
if tokens >= 1 then
    tokens = tokens - 1
    taken = 1
else
    wait = (1 - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'at', tostring(now))
redis.call('EXPIRE', KEYS[1], 60)
return {taken, tostring(wait)}
`)

func (r *Redis) TakeToken(ctx context.Context, key string, rate int64) (bool, float64, error) {
	now := strconv.FormatFloat(float64(time.Now().UnixMicro())/1e6, 'f', 6, 64)
	res, err := takeToken.Run(ctx, r.client, []string{key}, rate, now).Slice()
	if err != nil {
		return true, 0, err
	}
	taken, _ := res[0].(int64)
	wait, _ := strconv.ParseFloat(res[1].(string), 64)
	return taken == 1, wait, nil
}

var incrIfSet = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
    return redis.call('INCR', KEYS[1])
end
return 0
`)

func (r *Redis) IncrIfSet(ctx context.Context, key string) {
	if err := incrIfSet.Run(ctx, r.client, []string{key}).Err(); err != nil && err != redis.Nil {
		r.warn("Could not count on", err)
	}
}

func (r *Redis) Notify(ctx context.Context, key string) {
	r.mem.wake.notify(key)
	r.publish(ctx, "w", key)
}

func (r *Redis) Wait(ctx context.Context, key string, d time.Duration) bool {
	return r.mem.wake.wait(ctx, key, d)
}

func (r *Redis) HSet(ctx context.Context, key, field, value string, ttl time.Duration) {
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, field, value)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		r.warn("Could not record a worker", err)
	}
}

func (r *Redis) HGetAll(ctx context.Context, keys ...string) []map[string]string {
	out := make([]map[string]string, len(keys))
	pipe := r.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.HGetAll(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		r.warn("Could not read the workers", err)
	}
	for i, c := range cmds {
		out[i] = c.Val()
		if out[i] == nil {
			out[i] = map[string]string{}
		}
	}
	return out
}

func (r *Redis) ZAdd(ctx context.Context, key, member string, score float64, ttl time.Duration) {
	pipe := r.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: member})
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		r.warn("Could not record a waiting worker", err)
	}
}

func (r *Redis) ZRem(ctx context.Context, key, member string) {
	if err := r.client.ZRem(ctx, key, member).Err(); err != nil {
		r.warn("Could not record a waiting worker", err)
	}
}

func (r *Redis) ZAbove(ctx context.Context, key string, min float64) map[string]float64 {
	out := map[string]float64{}
	pipe := r.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatFloat(min, 'f', 6, 64))
	rng := pipe.ZRangeWithScores(ctx, key, 0, -1)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		r.warn("Could not read the workers", err)
		return out
	}
	for _, z := range rng.Val() {
		if m, ok := z.Member.(string); ok {
			out[m] = z.Score
		}
	}
	return out
}
