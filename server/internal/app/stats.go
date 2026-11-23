package app

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/db"
)

// QueueStats are a queue's figures (QueueStats in the spec), with max_retry for the
// metrics and workers for the figures.
type QueueStats struct {
	Ready, Delayed, Processing, Completed, Failed int64
	OldestReadySeconds, OldestProcessingSeconds   int64
	MaxRetry                                      int64
	Workers                                       int64
}

func (s *QueueStats) View() map[string]any {
	return map[string]any{
		"ready": s.Ready, "delayed": s.Delayed, "processing": s.Processing, "completed": s.Completed,
		"failed": s.Failed, "oldest_ready_seconds": s.OldestReadySeconds,
		"oldest_processing_seconds": s.OldestProcessingSeconds, "workers": s.Workers,
	}
}

const completedTTL = 30 * time.Second

func completedKey(tenant, queue string) string { return "queue_completed:" + tenant + ":" + queue }

func (a *App) forgetCompleted(ctx context.Context, tenant, queue string) {
	a.Coord.Del(ctx, completedKey(tenant, queue))
}

// Stats are the figures of a tenant's queues. Only open and dead-letter rows are read,
// through ranges of (tenant_id, queue, status); the completed ones, most of them, are
// counted per queue at most every 30 seconds.
func (a *App) Stats(ctx context.Context, tenant string, queues []string, t time.Time) (map[string]*QueueStats, error) {
	stats := make(map[string]*QueueStats, len(queues))
	if len(queues) == 0 {
		return stats, nil
	}
	for _, q := range queues {
		stats[q] = &QueueStats{}
	}
	args := db.Args{"tenant": tenant, "queues": queues}
	rows, err := a.DB.Query(ctx, a.DB, `SELECT queue, COUNT(*) AS n FROM messages
		WHERE tenant_id = :tenant AND queue IN (:queues) AND status = 'failed' GROUP BY queue`, args)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var q string
		var n int64
		if err := rows.Scan(&q, &n); err != nil {
			rows.Close()
			return nil, err
		}
		if s := stats[q]; s != nil {
			s.Failed = n
		}
	}
	rows.Close()

	args["now"] = db.T(t)
	args["now2"] = db.T(t)
	args["now3"] = db.T(t)
	args["now4"] = db.T(t)
	rows, err = a.DB.Query(ctx, a.DB, `SELECT queue,
			SUM(CASE WHEN status = 'pending' AND (processing_after IS NULL OR processing_after <= :now)
				THEN 1 ELSE 0 END) AS ready,
			SUM(CASE WHEN status = 'pending' AND processing_after > :now2 THEN 1 ELSE 0 END) AS delayed_count,
			SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing,
			MIN(CASE WHEN status = 'pending' AND (processing_after IS NULL OR processing_after <= :now3)
				THEN COALESCE(processing_after, created_at) END) AS oldest_ready,
			MIN(CASE WHEN status = 'processing' THEN processing_started_at END) AS oldest_processing,
			MAX(retry_count) AS max_retry
		FROM messages
		WHERE tenant_id = :tenant AND queue IN (:queues) AND status IN ('pending', 'processing')
		GROUP BY queue`, args)
	if err != nil {
		return nil, err
	}
	age := func(n db.NullTime) int64 {
		if !n.Valid {
			return 0
		}
		return max(0, t.Unix()-n.Time.Unix())
	}
	for rows.Next() {
		var (
			q                          string
			ready, delayed, processing numeric
			oldestReady, oldestProc    db.NullTime
			maxRetry                   numeric
		)
		if err := rows.Scan(&q, &ready, &delayed, &processing, &oldestReady, &oldestProc, &maxRetry); err != nil {
			rows.Close()
			return nil, err
		}
		s := stats[q]
		if s == nil {
			continue
		}
		s.Ready, s.Delayed, s.Processing = ready.n, delayed.n, processing.n
		s.OldestReadySeconds = age(oldestReady)
		s.OldestProcessingSeconds = age(oldestProc)
		s.MaxRetry = maxRetry.n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, q := range queues {
		key := completedKey(tenant, q)
		if v, ok := a.Coord.Get(ctx, key); ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				stats[q].Completed = n
				continue
			}
		}
		var n int64
		if err := a.DB.Row(ctx, a.DB, `SELECT COUNT(*) FROM messages
			WHERE tenant_id = :tenant AND queue = :queue AND status = 'completed'`,
			db.Args{"tenant": tenant, "queue": q}).Scan(&n); err != nil {
			return nil, err
		}
		stats[q].Completed = n
		a.Coord.Set(ctx, key, strconv.FormatInt(n, 10), completedTTL)
	}

	holding, err := a.processingByConsumer(ctx, tenant, queues)
	if err != nil {
		return nil, err
	}
	for q, n := range a.workerCounts(ctx, tenant, queues, holding) {
		stats[q].Workers = n
	}
	return stats, nil
}

// numeric scans a SUM or MAX, which drivers hand over as a number, bytes or a string.
type numeric struct{ n int64 }

func (x *numeric) Scan(v any) error {
	switch y := v.(type) {
	case nil:
		x.n = 0
	case int64:
		x.n = y
	case float64:
		x.n = int64(y)
	case []byte:
		f, err := strconv.ParseFloat(string(y), 64)
		if err != nil {
			return err
		}
		x.n = int64(f)
	case string:
		f, err := strconv.ParseFloat(y, 64)
		if err != nil {
			return err
		}
		x.n = int64(f)
	default:
		return fmt.Errorf("cannot scan %T as a number", v)
	}
	return nil
}

// processingByConsumer is which consumers hold messages of these queues now, and how many.
func (a *App) processingByConsumer(ctx context.Context, tenant string, queues []string) (map[string]map[string]int64, error) {
	holding := make(map[string]map[string]int64, len(queues))
	for _, q := range queues {
		holding[q] = map[string]int64{}
	}
	if len(queues) == 0 {
		return holding, nil
	}
	rows, err := a.DB.Query(ctx, a.DB, `SELECT queue, consumed_by, COUNT(*) AS n FROM messages
		WHERE tenant_id = :tenant AND queue IN (:queues) AND status = 'processing'
		GROUP BY queue, consumed_by`, db.Args{"tenant": tenant, "queues": queues})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var q string
		var consumer *string
		var n int64
		if err := rows.Scan(&q, &consumer, &n); err != nil {
			return nil, err
		}
		name := WorkerName("")
		if consumer != nil {
			name = WorkerName(*consumer)
		}
		if holding[q] != nil {
			holding[q][name] += n
		}
	}
	return holding, rows.Err()
}

func readWorkerEntry(s string) (workerEntry, bool) {
	var e workerEntry
	if json.Unmarshal([]byte(s), &e) != nil || e.Seen == 0 {
		return e, false
	}
	return e, true
}

// workerCounts is how many workers are at work on each queue: claimed within the last
// minute, or hold a message.
func (a *App) workerCounts(ctx context.Context, tenant string, queues []string, holding map[string]map[string]int64) map[string]int64 {
	counts := make(map[string]int64, len(queues))
	keys := make([]string, len(queues))
	for i, q := range queues {
		keys[i] = seenKey(tenant, q)
	}
	hashes := a.Coord.HGetAll(ctx, keys...)
	recent := float64(time.Now().Add(-workerRecent).UnixMicro()) / 1e6
	for i, q := range queues {
		names := map[string]bool{}
		for name := range holding[q] {
			names[name] = true
		}
		for name, raw := range hashes[i] {
			if e, ok := readWorkerEntry(raw); ok && e.Seen >= recent {
				names[name] = true
			}
		}
		counts[q] = int64(len(names))
	}
	return counts
}

// Workers lists the workers of a queue: those that claimed in the last ten minutes, and
// those that hold a message, newest claim first.
func (a *App) Workers(ctx context.Context, p *Principal, queue string) (map[string]any, error) {
	if err := p.RequireScopes(ScopeConsume, ScopeMetrics, ScopeAdmin); err != nil {
		return nil, err
	}
	tenant, err := p.Authorise(queue)
	if err != nil {
		return nil, err
	}
	all, err := a.processingByConsumer(ctx, tenant, []string{queue})
	if err != nil {
		return nil, err
	}
	holding := all[queue]
	seen := a.Coord.HGetAll(ctx, seenKey(tenant, queue))[0]
	nowF := float64(time.Now().UnixMicro()) / 1e6
	waiting := a.Coord.ZAbove(ctx, waitingKey(tenant, queue), nowF)

	type worker struct {
		view map[string]any
		at   string
	}
	workers := map[string]worker{}
	view := func(name string, e *workerEntry) worker {
		var at any
		atKey := ""
		var client, address any
		active := holding[name] > 0
		if e != nil {
			sec := int64(e.Seen)
			t := time.Unix(sec, int64((e.Seen-float64(sec))*1e9)).UTC()
			at = formatTimeV(t)
			atKey = formatTimeV(t)
			if e.Client != nil && *e.Client != "" {
				client = *e.Client
			}
			if e.Address != nil {
				address = *e.Address
			}
			active = active || e.Seen >= nowF-workerRecent.Seconds()
		}
		_, isWaiting := waiting[name]
		return worker{at: atKey, view: map[string]any{
			"consumer":      name,
			"last_claim_at": at,
			"waiting":       isWaiting,
			"processing":    holding[name],
			"client":        client,
			"address":       address,
			"active":        active,
		}}
	}
	for name, raw := range seen {
		e, ok := readWorkerEntry(raw)
		if !ok {
			continue
		}
		if _, holds := holding[name]; e.Seen < nowF-workerListed.Seconds() && !holds {
			continue
		}
		workers[name] = view(name, &e)
	}
	for name := range holding {
		if _, ok := workers[name]; !ok {
			workers[name] = view(name, nil)
		}
	}
	list := make([]worker, 0, len(workers))
	for _, w := range workers {
		list = append(list, w)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].at != list[j].at {
			return list[i].at > list[j].at
		}
		return list[i].view["consumer"].(string) < list[j].view["consumer"].(string)
	})
	items := make([]any, len(list))
	for i, w := range list {
		items[i] = w.view
	}
	return map[string]any{"items": items}, nil
}

// QueueView is a queue as the API shows it.
func QueueView(name string, c *QueueConfig, s *QueueStats) map[string]any {
	return map[string]any{
		"name":      name,
		"settings":  c.View(),
		"stats":     s.View(),
		"paused_at": formatTime(c.PausedAt),
	}
}

// ListQueues lists the tenant's queues the key may use, with settings and figures.
func (a *App) ListQueues(ctx context.Context, p *Principal) (map[string]any, error) {
	if err := p.RequireScopes(ScopeAdmin); err != nil {
		return nil, err
	}
	tenant, _ := p.Tenant()
	all, err := a.QueueNames(ctx, tenant)
	if err != nil {
		return nil, err
	}
	names := []string{}
	for _, n := range all {
		if p.MayUseQueue(n) {
			names = append(names, n)
		}
	}
	configs, err := a.ForQueues(ctx, tenant, names)
	if err != nil {
		return nil, err
	}
	stats, err := a.Stats(ctx, tenant, names, now())
	if err != nil {
		return nil, err
	}
	items := make([]any, len(names))
	for i, n := range names {
		items[i] = QueueView(n, configs[n], stats[n])
	}
	return map[string]any{"items": items}, nil
}

// GetQueue is one queue with its settings and figures.
func (a *App) GetQueue(ctx context.Context, p *Principal, queue string) (map[string]any, error) {
	if err := p.RequireScopes(ScopeConsume, ScopeMetrics, ScopeAdmin); err != nil {
		return nil, err
	}
	tenant, err := p.Authorise(queue)
	if err != nil {
		return nil, err
	}
	c, err := a.ForQueue(ctx, tenant, queue)
	if err != nil {
		return nil, err
	}
	if !c.Exists {
		return nil, NotFound(fmt.Sprintf("No queue %q.", queue))
	}
	stats, err := a.Stats(ctx, tenant, []string{queue}, now())
	if err != nil {
		return nil, err
	}
	return QueueView(queue, c, stats[queue]), nil
}

// PatchQueue changes a queue's settings; a resumed queue, or one whose settings changed,
// wakes the claims waiting on it.
func (a *App) PatchQueue(ctx context.Context, p *Principal, queue string, in *Input) (map[string]any, error) {
	if err := p.RequireScopes(ScopeAdmin); err != nil {
		return nil, err
	}
	tenant, err := p.Authorise(queue)
	if err != nil {
		return nil, err
	}
	c, err := a.UpdateQueue(ctx, tenant, queue, in.All())
	if err != nil {
		return nil, err
	}
	if !c.Paused() {
		a.notify(ctx, tenant, queue)
	}
	stats, err := a.Stats(ctx, tenant, []string{queue}, now())
	if err != nil {
		return nil, err
	}
	return QueueView(queue, c, stats[queue]), nil
}

// Metrics are the Prometheus metrics of every queue of these tenants.
func (a *App) Metrics(ctx context.Context, tenants []string, mayUse func(string) bool) (string, error) {
	type metric struct{ name, help string }
	metrics := []metric{
		{"lessormq_messages", "Open messages per queue: ready for a worker, delayed until later, or processing."},
		{"lessormq_dead_letter_messages", "Messages that ran out of attempts or failed permanently, kept to replay."},
		{"lessormq_oldest_ready_seconds", "How long the oldest message a worker could take has been waiting."},
		{"lessormq_oldest_processing_seconds", "How long the longest-held claimed message has been processing."},
		{"lessormq_max_retry_count", "Highest retry count among open messages."},
		{"lessormq_paused", "Whether the queue is paused (1) or not (0)."},
		{"lessormq_workers", "Workers that claimed from the queue in the last minute or hold one of its messages."},
	}
	series := map[string][]string{}
	t := now()
	for _, tenant := range tenants {
		all, err := a.QueueNames(ctx, tenant)
		if err != nil {
			return "", err
		}
		var names []string
		for _, n := range all {
			if mayUse == nil || mayUse(n) {
				names = append(names, n)
			}
		}
		if len(names) == 0 {
			continue
		}
		configs, err := a.ForQueues(ctx, tenant, names)
		if err != nil {
			return "", err
		}
		stats, err := a.Stats(ctx, tenant, names, t)
		if err != nil {
			return "", err
		}
		for _, q := range names {
			s := stats[q]
			label := fmt.Sprintf(`tenant="%s",queue="%s"`, promEscape(tenant), promEscape(q))
			series["lessormq_messages"] = append(series["lessormq_messages"],
				fmt.Sprintf(`{%s,state="ready"} %d`, label, s.Ready),
				fmt.Sprintf(`{%s,state="delayed"} %d`, label, s.Delayed),
				fmt.Sprintf(`{%s,state="processing"} %d`, label, s.Processing))
			add := func(m string, v int64) { series[m] = append(series[m], fmt.Sprintf("{%s} %d", label, v)) }
			add("lessormq_dead_letter_messages", s.Failed)
			add("lessormq_oldest_ready_seconds", s.OldestReadySeconds)
			add("lessormq_oldest_processing_seconds", s.OldestProcessingSeconds)
			add("lessormq_max_retry_count", s.MaxRetry)
			paused := int64(0)
			if configs[q].Paused() {
				paused = 1
			}
			add("lessormq_paused", paused)
			add("lessormq_workers", s.Workers)
		}
	}
	var b strings.Builder
	for _, m := range metrics {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", m.name, m.help, m.name)
		for _, sample := range series[m.name] {
			b.WriteString(m.name + sample + "\n")
		}
	}
	return b.String(), nil
}

func promEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

// TenantIDs are the tenants that are not being deleted.
func (a *App) TenantIDs(ctx context.Context) ([]string, error) {
	rows, err := a.DB.Query(ctx, a.DB, "SELECT id FROM tenants WHERE status <> 'deleting' ORDER BY id", db.Args{})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
