package app

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/db"
)

// Periods of a history: its length and the size of its buckets, in seconds.
var Periods = map[string][2]int64{
	"1h":  {3600, 60},
	"6h":  {21600, 300},
	"24h": {86400, 900},
	"7d":  {604800, 7200},
}

var periodOrder = []string{"1h", "6h", "24h", "7d"}

const (
	DefaultPeriod = "24h"
	// longestRun: a completed message is looked for among those that started at most this
	// long before the period, so the scan stays within the period's index range.
	longestRun = 3600
)

// CheckPeriod validates ?period=.
func CheckPeriod(period string) error {
	if _, ok := Periods[period]; !ok {
		return ValidationField("period", "One of 1h, 6h, 24h, 7d.")
	}
	return nil
}

// History is what happened to queues over a period, bucket by bucket: published,
// completed and dead-lettered, with how long messages waited and ran. queues is per
// tenant; several tenants are added up, durations class by class, so a percentile is
// that of all the messages.
func (a *App) History(ctx context.Context, queues map[string][]string, period string, t time.Time) (map[string]any, error) {
	p := Periods[period]
	length, size := p[0], p[1]
	buckets := length / size
	from := (t.Unix()/size + 1 - buckets) * size

	tenants := make([]string, 0, len(queues))
	for tenant, names := range queues {
		sort.Strings(names)
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	keyParts, _ := json.Marshal(struct {
		T []string
		Q map[string][]string
	}{tenants, queues})
	sum := sha1.Sum(keyParts)
	key := fmt.Sprintf("queue_history:%s:%d:%s", period, from, hex.EncodeToString(sum[:]))
	if v, ok := a.Coord.Get(ctx, key); ok {
		var cached map[string]any
		if json.Unmarshal([]byte(v), &cached) == nil {
			return cached, nil
		}
	}

	published := make([]int64, buckets)
	completed := make([]int64, buckets)
	deadLettered := make([]int64, buckets)
	waits := make([]*Durations, buckets)
	runs := make([]*Durations, buckets)
	for i := range waits {
		waits[i], runs[i] = &Durations{}, &Durations{}
	}
	for _, tenant := range tenants {
		names := queues[tenant]
		if len(names) == 0 {
			continue
		}
		if err := a.historyPublished(ctx, tenant, names, from, size, published); err != nil {
			return nil, err
		}
		if err := a.historyDeadLettered(ctx, tenant, names, from, size, deadLettered); err != nil {
			return nil, err
		}
		if err := a.historyCompleted(ctx, tenant, names, from, size, completed, waits, runs); err != nil {
			return nil, err
		}
	}
	allWaits, allRuns := &Durations{}, &Durations{}
	series := make([]any, buckets)
	var totalP, totalC, totalD int64
	for i := int64(0); i < buckets; i++ {
		allWaits.Merge(waits[i])
		allRuns.Merge(runs[i])
		totalP += published[i]
		totalC += completed[i]
		totalD += deadLettered[i]
		series[i] = map[string]any{
			"at":            formatTimeV(time.Unix(from+i*size, 0)),
			"published":     published[i],
			"completed":     completed[i],
			"dead_lettered": deadLettered[i],
			"wait_seconds":  nilIfEmpty(waits[i].View()),
			"run_seconds":   nilIfEmpty(runs[i].View()),
		}
	}
	result := map[string]any{
		"period":         period,
		"bucket_seconds": size,
		"from":           formatTimeV(time.Unix(from, 0)),
		"to":             formatTimeV(t),
		"totals": map[string]any{
			"published":     totalP,
			"completed":     totalC,
			"dead_lettered": totalD,
			"wait_seconds":  nilIfEmpty(allWaits.View()),
			"run_seconds":   nilIfEmpty(allRuns.View()),
		},
		"buckets": series,
	}
	if b, err := json.Marshal(result); err == nil {
		ttl := time.Duration(max(10, min(300, size/10))) * time.Second
		a.Coord.Set(ctx, key, string(b), ttl)
	}
	return result, nil
}

func nilIfEmpty(m map[string]any) any {
	if m == nil {
		return nil
	}
	return m
}

func historyArgs(tenant string, queues []string, from, size int64) db.Args {
	return db.Args{
		"tenant": tenant, "queues": queues, "from": from, "size": size,
		"from_time": db.T(time.Unix(from, 0)),
	}
}

func (a *App) perBucket(ctx context.Context, inner string, args db.Args, into []int64) error {
	rows, err := a.DB.Query(ctx, a.DB, "SELECT bucket, COUNT(*) AS n FROM ("+inner+") timed GROUP BY bucket", args)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var bucket, n numeric
		if err := rows.Scan(&bucket, &n); err != nil {
			return err
		}
		if bucket.n >= 0 && bucket.n < int64(len(into)) {
			into[bucket.n] += n.n
		}
	}
	return rows.Err()
}

// historyPublished: the id range keeps the read to the period, as ids are ordered by the
// time they were made and idx_tenant_claim holds them per (tenant_id, queue, status).
func (a *App) historyPublished(ctx context.Context, tenant string, queues []string, from, size int64, into []int64) error {
	epoch := a.DB.D.EpochSeconds
	args := historyArgs(tenant, queues, from, size)
	args["statuses"] = []string{Pending, Processing, Completed, Failed}
	args["first_id"] = MessageIDAt(time.Unix(from, 0)).Bytes()
	return a.perBucket(ctx, `SELECT FLOOR((`+epoch("created_at")+` - :from) / :size) AS bucket
		FROM messages
		WHERE tenant_id = :tenant AND queue IN (:queues) AND status IN (:statuses)
		  AND id >= :first_id AND created_at >= :from_time`, args, into)
}

func (a *App) historyDeadLettered(ctx context.Context, tenant string, queues []string, from, size int64, into []int64) error {
	epoch := a.DB.D.EpochSeconds
	return a.perBucket(ctx, `SELECT FLOOR((`+epoch("failed_at")+` - :from) / :size) AS bucket
		FROM messages
		WHERE tenant_id = :tenant AND queue IN (:queues) AND status = 'failed'
		  AND failed_at >= :from_time`, historyArgs(tenant, queues, from, size), into)
}

func (a *App) historyCompleted(ctx context.Context, tenant string, queues []string, from, size int64,
	into []int64, waits, runs []*Durations) error {
	epoch, between := a.DB.D.EpochSeconds, a.DB.D.SecondsBetween
	args := historyArgs(tenant, queues, from, size)
	args["scan_from"] = db.T(time.Unix(from-longestRun, 0))
	rows, err := a.DB.Query(ctx, a.DB, `SELECT bucket, wait_class, run_class, COUNT(*) AS n,
			SUM(wait) AS wait_sum, MAX(wait) AS wait_max, SUM(run) AS run_sum, MAX(run) AS run_max
		FROM (
			SELECT bucket, wait, run, `+DurationClassSQL("wait")+` AS wait_class, `+DurationClassSQL("run")+` AS run_class
			FROM (
				SELECT FLOOR((`+epoch("processed_at")+` - :from) / :size) AS bucket,
					`+between("COALESCE(processing_after, created_at)", "processing_started_at")+` AS wait,
					`+between("processing_started_at", "processed_at")+` AS run
				FROM messages
				WHERE tenant_id = :tenant AND queue IN (:queues) AND status = 'completed'
				  AND processing_started_at >= :scan_from AND processed_at >= :from_time
			) finished
		) classed
		GROUP BY bucket, wait_class, run_class`, args)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var bucket, waitClass, runClass, n, waitSum, waitMax, runSum, runMax numericF
		if err := rows.Scan(&bucket, &waitClass, &runClass, &n, &waitSum, &waitMax, &runSum, &runMax); err != nil {
			return err
		}
		b := int64(bucket.f)
		if b < 0 || b >= int64(len(into)) {
			continue
		}
		into[b] += int64(n.f)
		waits[b].Add(int(waitClass.f), int64(n.f), waitSum.f, waitMax.f)
		runs[b].Add(int(runClass.f), int64(n.f), runSum.f, runMax.f)
	}
	return rows.Err()
}

// numericF scans any number as a float64.
type numericF struct{ f float64 }

func (x *numericF) Scan(v any) error {
	var n numeric
	switch y := v.(type) {
	case float64:
		x.f = y
		return nil
	case []byte, string:
		var s string
		if b, ok := y.([]byte); ok {
			s = string(b)
		} else {
			s = y.(string)
		}
		_, err := fmt.Sscanf(s, "%g", &x.f)
		return err
	}
	if err := n.Scan(v); err != nil {
		return err
	}
	x.f = float64(n.n)
	return nil
}
