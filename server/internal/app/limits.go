package app

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/db"
)

// The tenant limits that publishes and claims run into: the rates, and the number of open
// messages. A tenant over one gets 429 with Retry-After; one without quotas costs nothing.
// Rates are token buckets (shared through Redis when there is one): a tenant may do its
// limit per second, in bursts of up to a second's worth. The open messages are counted in
// the database at most every few seconds and counted on between those counts. When the
// state cannot be reached no limit is applied: a quota protects the installation, it is
// not worth refusing work over.

const (
	openTTL        = 5 * time.Second
	openRetryAfter = 10
)

func (a *App) beforePublish(ctx context.Context, tenant string, c *QueueConfig) error {
	if err := a.takeToken(ctx, tenant, "publish", c.Quota("publish_per_second")); err != nil {
		return err
	}
	limit := c.Quota("max_open_messages")
	if limit == nil {
		return nil
	}
	open, err := a.openMessages(ctx, tenant)
	if err != nil {
		return err
	}
	if open >= *limit {
		return RateLimited(openRetryAfter, fmt.Sprintf(
			"This tenant has its maximum of %d open messages; publish again once some are done.", *limit))
	}
	return nil
}

// afterPublish counts a new message on, so publishes between two counts in the database
// cannot run far past the limit.
func (a *App) afterPublish(ctx context.Context, tenant string, c *QueueConfig) {
	if c.Quota("max_open_messages") != nil {
		a.Coord.IncrIfSet(ctx, "quota_open:"+tenant)
	}
}

func (a *App) beforeClaim(ctx context.Context, tenant string, c *QueueConfig) error {
	return a.takeToken(ctx, tenant, "claim", c.Quota("claim_per_second"))
}

func (a *App) takeToken(ctx context.Context, tenant, action string, perSecond *int64) error {
	if perSecond == nil {
		return nil
	}
	taken, wait, err := a.Coord.TakeToken(ctx, "quota_rate:"+action+":"+tenant, *perSecond)
	if err != nil {
		a.Log.Warn("Rate limit unavailable", "error", err.Error())
		return nil
	}
	if !taken {
		return RateLimited(int(math.Ceil(wait)), fmt.Sprintf("This tenant may %s %d times a second.", action, *perSecond))
	}
	return nil
}

func (a *App) openMessages(ctx context.Context, tenant string) (int64, error) {
	key := "quota_open:" + tenant
	if v, ok := a.Coord.Get(ctx, key); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n, nil
		}
	}
	queues, err := a.QueueNames(ctx, tenant)
	if err != nil {
		return 0, err
	}
	usage, err := a.tenantUsage(ctx, tenant, queues)
	if err != nil {
		return 0, err
	}
	open := usage["open_messages"]
	a.Coord.Set(ctx, key, strconv.FormatInt(open, 10), openTTL)
	return open, nil
}

// tenantUsage is a tenant's open and dead-letter messages over its queues. Only those rows
// are read, through ranges of (tenant_id, queue, status).
func (a *App) tenantUsage(ctx context.Context, tenant string, queues []string) (map[string]int64, error) {
	usage := map[string]int64{"queues": int64(len(queues)), "open_messages": 0, "failed_messages": 0}
	if len(queues) == 0 {
		return usage, nil
	}
	rows, err := a.DB.Query(ctx, a.DB, `SELECT status, COUNT(*) AS n FROM messages
		WHERE tenant_id = :tenant AND queue IN (:queues) AND status IN ('pending', 'processing', 'failed')
		GROUP BY status`, db.Args{"tenant": tenant, "queues": queues})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		if status == Failed {
			usage["failed_messages"] += n
		} else {
			usage["open_messages"] += n
		}
	}
	return usage, rows.Err()
}

// waitingClaims counts the claims waiting on this node. A waiting claim costs a goroutine
// and nothing in the database, so the default is no limit; MAX_WAITING_CLAIMS sets one.
type waitingClaims struct {
	mu sync.Mutex
	n  int
}

func (a *App) enterWaiting() bool {
	if a.Cfg.MaxWaitingClaims <= 0 {
		return true
	}
	a.waiting.mu.Lock()
	defer a.waiting.mu.Unlock()
	if a.waiting.n >= a.Cfg.MaxWaitingClaims {
		return false
	}
	a.waiting.n++
	return true
}

func (a *App) leaveWaiting() {
	if a.Cfg.MaxWaitingClaims <= 0 {
		return
	}
	a.waiting.mu.Lock()
	a.waiting.n--
	a.waiting.mu.Unlock()
}

func wakeKey(tenant, queue string) string { return "claim_wake:" + tenant + ":" + queue }

// notify wakes a claim waiting on the queue: a publish, retry, release, replay or resume.
func (a *App) notify(ctx context.Context, tenant, queue string) {
	a.Coord.Notify(ctx, wakeKey(tenant, queue))
}

// sweepIfDue ends a queue's expired leases, at most once per LEASE_SWEEP_SECONDS over all
// nodes: the first claim in each interval does it.
func (a *App) sweepIfDue(ctx context.Context, tenant, queue string, c *QueueConfig) {
	interval := time.Duration(max(1, a.Cfg.LeaseSweepSeconds)) * time.Second
	if !a.Coord.Gate(ctx, "recovery:"+tenant+":"+queue, interval) {
		return
	}
	result, err := a.expireLeases(ctx, tenant, queue, c, now())
	if err != nil {
		a.Log.Warn("Lease sweep failed", "tenant", tenant, "queue", queue, "error", err.Error())
		return
	}
	if result["retried"]+result["dead_lettered"]+result["discarded"] > 0 {
		a.Log.Info("Expired leases ended", "tenant", tenant, "queue", queue, "retried", result["retried"],
			"dead_lettered", result["dead_lettered"], "discarded", result["discarded"])
	}
	// Retried after their backoff: waiting claims learn when.
	if result["retried"] > 0 {
		a.notify(ctx, tenant, queue)
	}
}

// Workers per queue: who claims from it (by the consumer name of the claim; the client
// libraries send host:pid by default), with the client they run, their address, their
// last claim and whether they wait now. Which messages a worker holds comes from the
// database (consumed_by). A worker counts as at work while it claimed within the last
// minute or holds a message, and is listed for ten minutes after its last claim.
const (
	workerRecent = 60 * time.Second
	workerListed = 600 * time.Second
	workerTTL    = time.Hour
)

// WorkerName is the name a claim without a consumer counts under.
func WorkerName(consumer string) string {
	if consumer == "" {
		return "(no name)"
	}
	return consumer
}

func seenKey(tenant, queue string) string    { return "workers:" + tenant + ":" + queue }
func waitingKey(tenant, queue string) string { return "workers_waiting:" + tenant + ":" + queue }

type workerEntry struct {
	Seen    float64 `json:"seen"`
	Client  *string `json:"client"`
	Address *string `json:"address"`
}

func (a *App) workerClaimed(ctx context.Context, tenant, queue, consumer, client, address string) {
	e := workerEntry{Seen: float64(time.Now().UnixMicro()) / 1e6}
	if client != "" {
		runes := []rune(client)
		if len(runes) > 200 {
			runes = runes[:200]
		}
		s := string(runes)
		e.Client = &s
	}
	if address != "" {
		e.Address = &address
	}
	b, _ := json.Marshal(e)
	a.Coord.HSet(ctx, seenKey(tenant, queue), WorkerName(consumer), string(b), workerTTL)
}

func (a *App) workerWaiting(ctx context.Context, tenant, queue, consumer string, until time.Time) {
	a.Coord.ZAdd(ctx, waitingKey(tenant, queue), WorkerName(consumer), float64(until.UnixMicro())/1e6, workerTTL)
}

func (a *App) workerDoneWaiting(ctx context.Context, tenant, queue, consumer string) {
	a.Coord.ZRem(ctx, waitingKey(tenant, queue), WorkerName(consumer))
}
