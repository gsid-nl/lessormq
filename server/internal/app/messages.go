package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/db"
)

// The v1 message protocol (docs/protocol.md): publish, claim, extend, ack, fail, release.
// Everything a client may not decide is decided here, from the queue's settings.

const (
	receiptPattern = `[0-9a-f]{32}`
	maxWait        = 20
	insertAttempts = 3
)

// ClientInfo is what a claim records of the worker's request.
type ClientInfo struct {
	UserAgent string
	Address   string
}

// Publish puts a message in a queue. duplicate is true when an open message with the same
// dedup_key was there already, and is returned instead.
func (a *App) Publish(ctx context.Context, p *Principal, queue string, in *Input) (map[string]any, bool, error) {
	tenant, err := p.Authorise(queue)
	if err != nil {
		return nil, false, err
	}
	in.AllowOnly("body", "headers", "dedup_key", "delay_seconds", "available_at", "max_attempts")
	if !in.Has("body") {
		in.AddError("body", "is required")
	} else if in.IsNull("body") {
		in.AddError("body", "must not be null")
	}
	headers := readHeaders(in)
	var delay *int64
	if in.Has("delay_seconds") {
		if n, ok := in.Int("delay_seconds", 0, nil); ok {
			delay = &n
		}
	}
	availableAt := readTime(in, "available_at", true)
	var maxAttempts *int64
	if in.Has("max_attempts") {
		if n, ok := in.Int("max_attempts", 1, nil); ok {
			maxAttempts = &n
		}
	}
	if delay != nil && availableAt != nil {
		in.AddError("available_at", "not together with delay_seconds")
	}
	dedupKey, hasDedup := in.String("dedup_key", false, 255, "")
	if err := in.Validate(); err != nil {
		return nil, false, err
	}

	c, err := a.ForQueue(ctx, tenant, queue)
	if err != nil {
		return nil, false, err
	}
	var body bytes.Buffer
	if err := json.Compact(&body, in.Raw("body")); err != nil {
		return nil, false, ValidationField("body", "must be valid JSON")
	}
	headerJSON, _ := json.Marshal(headersObject(headers))
	if limit := c.Settings.Int("max_message_bytes"); int64(body.Len()+len(headerJSON)) > limit {
		return nil, false, PayloadTooLarge(limit)
	}
	// A queue the tenant has no room for is refused first: waiting would not help.
	if err := a.ensureQueue(ctx, tenant, queue, c); err != nil {
		return nil, false, err
	}
	if err := a.beforePublish(ctx, tenant, c); err != nil {
		return nil, false, err
	}
	if delay != nil && *delay > 0 {
		t := now().Add(time.Duration(*delay) * time.Second)
		availableAt = &t
	}
	if maxAttempts != nil {
		n := min(*maxAttempts, c.Settings.Int("max_attempts_limit"))
		maxAttempts = &n
	}

	m := &Message{
		TenantID:        tenant,
		Queue:           queue,
		Body:            body.Bytes(),
		Headers:         headers,
		Status:          Pending,
		ProcessingAfter: truncate(availableAt),
		MaxAttempts:     maxAttempts,
	}
	if hasDedup {
		m.DedupKey = &dedupKey
	}
	m, duplicate, err := a.createMessage(ctx, m)
	if err != nil {
		return nil, false, err
	}
	if !duplicate {
		a.afterPublish(ctx, tenant, c)
		a.notify(ctx, tenant, queue)
	}
	available := m.CreatedAt
	if m.ProcessingAfter != nil {
		available = *m.ProcessingAfter
	}
	return map[string]any{
		"id":           m.ID.String(),
		"queue":        queue,
		"status":       m.Status,
		"created_at":   formatTimeV(m.CreatedAt),
		"available_at": formatTimeV(available),
		"duplicate":    duplicate,
	}, duplicate, nil
}

// createMessage inserts a new message, or returns the open one with the same dedup_key.
// The key is enforced by a unique index, not by checking first: two publishes with the
// same key at once both try to insert, one loses on the index and returns the winner's.
// Two ids drawn in the same instant can collide too; that insert wrote nothing, so a
// retry with a new id is the same publish.
func (a *App) createMessage(ctx context.Context, m *Message) (*Message, bool, error) {
	if m.DedupKey != nil {
		existing, err := a.findOpenByDedupKey(ctx, m.TenantID, m.Queue, *m.DedupKey)
		if err != nil || existing != nil {
			return existing, existing != nil, err
		}
	}
	for attempt := 1; ; attempt++ {
		m.ID = NewMessageID()
		m.CreatedAt = now().Truncate(time.Millisecond)
		m.UpdatedAt = m.CreatedAt
		err := a.insertMessage(ctx, m)
		if err == nil {
			return m, false, nil
		}
		if !db.IsUniqueViolation(err) {
			return nil, false, err
		}
		if m.DedupKey != nil {
			existing, ferr := a.findOpenByDedupKey(ctx, m.TenantID, m.Queue, *m.DedupKey)
			if ferr != nil {
				return nil, false, ferr
			}
			if existing != nil {
				return existing, true, nil
			}
		}
		if attempt >= insertAttempts {
			return nil, false, err
		}
		a.Log.Warn("Message id collided, retrying with a fresh id", "queue", m.Queue, "attempt", attempt)
	}
}

// maxBatch is the most messages one claim or one ack takes.
const maxBatch = 100

// Claim leases the next message of a queue to the caller, or returns nil when nothing
// became ready within wait_seconds. With max_messages it leases as many as are ready, up
// to that number, and answers them as a list: it waits only while there is none.
func (a *App) Claim(ctx context.Context, p *Principal, queue string, in *Input, client ClientInfo) (map[string]any, error) {
	tenant, err := p.Authorise(queue)
	if err != nil {
		return nil, err
	}
	in.AllowOnly("consumer", "route", "wait_seconds", "lease_seconds", "max_messages")
	consumer, _ := in.String("consumer", false, 255, "")
	batch := in.Has("max_messages")
	count := int64(1)
	if batch {
		limit := int64(maxBatch)
		count, _ = in.Int("max_messages", 1, &limit)
	}
	route, _ := in.String("route", false, 255, RoutePattern)
	var wait int64
	if in.Has("wait_seconds") {
		limit := int64(maxWait)
		wait, _ = in.Int("wait_seconds", 0, &limit)
	}
	var leaseAsked *int64
	if in.Has("lease_seconds") {
		if n, ok := in.Int("lease_seconds", 1, nil); ok {
			leaseAsked = &n
		}
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}

	c, err := a.ForQueue(ctx, tenant, queue)
	if err != nil {
		return nil, err
	}
	if err := a.beforeClaim(ctx, tenant, c); err != nil {
		return nil, err
	}
	a.workerClaimed(ctx, tenant, queue, consumer, client.UserAgent, client.Address)
	lease := c.Settings.Int("lease_seconds")
	if leaseAsked != nil {
		lease = *leaseAsked
	}
	lease = min(lease, c.Settings.Int("max_lease_seconds"))
	deadline := time.Now().Add(time.Duration(wait) * time.Second)
	swept := false
	waiting := false
	capped := false
	defer func() {
		if waiting {
			a.leaveWaiting()
			a.workerDoneWaiting(context.WithoutCancel(ctx), tenant, queue, consumer)
		}
	}()

	for {
		if !c.Paused() {
			if !swept {
				a.sweepIfDue(ctx, tenant, queue, c)
				swept = true
			}
			// A worker that went away while it waited gets no message: it could not take it.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			claimed, err := a.claimNext(ctx, tenant, queue, consumer, route, c.Avoidance(route), lease, int(count))
			if err != nil {
				return nil, err
			}
			if len(claimed) > 0 && !batch {
				return leaseView(claimed[0], c), nil
			}
			if len(claimed) > 0 {
				items := make([]any, len(claimed))
				for i, m := range claimed {
					items[i] = leaseView(m, c)
				}
				return map[string]any{"items": items}, nil
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		if !waiting && !capped {
			if a.enterWaiting() {
				waiting = true
				a.workerWaiting(ctx, tenant, queue, consumer, deadline)
			} else {
				// Past the node's limit a claim waits at most a second, so its client neither
				// hot-loops nor holds on.
				capped = true
				if d := time.Now().Add(time.Second); d.Before(deadline) {
					deadline = d
				}
				remaining = time.Until(deadline)
			}
		}
		until, err := a.untilDue(ctx, tenant, queue, c, remaining)
		if err != nil {
			return nil, err
		}
		waitCtx, cancel := a.waitContext(ctx)
		woken := a.Coord.Wait(waitCtx, wakeKey(tenant, queue), until)
		cancel()
		if a.stopped() {
			return nil, nil
		}
		if woken {
			// Woken: perhaps by a resume, which these settings do not know of.
			if c.Paused() {
				if c, err = a.reloadSettings(ctx, tenant, queue); err != nil {
					return nil, err
				}
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

// untilDue is how long a claim that found nothing may wait: until its time is up, or until
// the queue's next delayed message is due. A paused queue has only the time to wait for.
func (a *App) untilDue(ctx context.Context, tenant, queue string, c *QueueConfig, remaining time.Duration) (time.Duration, error) {
	if c.Paused() {
		return remaining, nil
	}
	next, err := a.nextAvailableAt(ctx, tenant, queue, now())
	if err != nil || next == nil {
		return remaining, err
	}
	// Stored to the second: a little after, so the message is due when the claim runs.
	due := time.Until(*next) + 50*time.Millisecond
	return max(50*time.Millisecond, min(remaining, due)), nil
}

func leaseView(m *Message, c *QueueConfig) map[string]any {
	return map[string]any{
		"receipt":     *m.Receipt,
		"lease_until": formatTime(m.LeaseUntil),
		"message": map[string]any{
			"id":           m.ID.String(),
			"queue":        m.Queue,
			"body":         m.Body,
			"headers":      headersObject(m.Headers),
			"attempt":      m.RetryCount + 1,
			"max_attempts": c.MaxAttemptsFor(m.MaxAttempts),
			"created_at":   formatTimeV(m.CreatedAt),
		},
	}
}

// state is the message's lease state, after checking the key may use its queue.
func (a *App) state(ctx context.Context, p *Principal, rawID string) (string, MessageID, *leaseState, error) {
	tenant, err := p.Tenant()
	if err != nil {
		return "", MessageID{}, nil, err
	}
	id, ok := ParseMessageID(rawID)
	var s *leaseState
	if ok {
		if s, err = a.leaseState(ctx, tenant, id); err != nil {
			return "", id, nil, err
		}
	}
	if s == nil {
		return "", id, nil, NotFound("No message \"" + rawID + "\".")
	}
	if _, err := p.Authorise(s.Queue); err != nil {
		return "", id, nil, err
	}
	return tenant, id, s, nil
}

func readReceipt(in *Input) string {
	r, _ := in.String("receipt", true, 64, receiptPattern)
	return r
}

// Extend moves the end of the caller's lease.
func (a *App) Extend(ctx context.Context, p *Principal, rawID string, in *Input) (map[string]any, error) {
	in.AllowOnly("receipt", "lease_seconds")
	receipt := readReceipt(in)
	var asked *int64
	if in.Has("lease_seconds") {
		if n, ok := in.Int("lease_seconds", 1, nil); ok {
			asked = &n
		}
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}
	tenant, id, s, err := a.state(ctx, p, rawID)
	if err != nil {
		return nil, err
	}
	c, err := a.ForQueue(ctx, tenant, s.Queue)
	if err != nil {
		return nil, err
	}
	lease := c.Settings.Int("lease_seconds")
	if asked != nil {
		lease = *asked
	}
	lease = min(lease, c.Settings.Int("max_lease_seconds"))
	until := now().Truncate(time.Second).Add(time.Duration(lease) * time.Second)
	ok, err := a.extendLease(ctx, tenant, id, receipt, until)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, LeaseLost()
	}
	return map[string]any{"lease_until": formatTimeV(until)}, nil
}

// Ack completes the caller's lease. A repeated ack of the same lease is answered as the
// first one was.
func (a *App) Ack(ctx context.Context, p *Principal, rawID string, in *Input) error {
	in.AllowOnly("receipt")
	receipt := readReceipt(in)
	if err := in.Validate(); err != nil {
		return err
	}
	tenant, err := p.Tenant()
	if err != nil {
		return err
	}
	id, ok := ParseMessageID(rawID)
	if !ok {
		return NotFound("No message \"" + rawID + "\".")
	}
	// A key for every queue needs no look first: the guarded update is the whole ack.
	if len(p.QueuePatterns) == 0 {
		done, err := a.updateGuarded(ctx, id, tenant, map[string]any{
			"status": Completed, "processed_at": db.T(now()), "lease_until": nil,
		}, Processing, receipt)
		if err != nil || done {
			return err
		}
	}
	_, _, s, err := a.state(ctx, p, rawID)
	if err != nil {
		return err
	}
	if len(p.QueuePatterns) > 0 {
		done, err := a.updateGuarded(ctx, id, tenant, map[string]any{
			"status": Completed, "processed_at": db.T(now()), "lease_until": nil,
		}, Processing, receipt)
		if err != nil || done {
			return err
		}
		if s, err = a.leaseState(ctx, tenant, id); err != nil || s == nil {
			if err == nil {
				err = LeaseLost()
			}
			return err
		}
	}
	if s.Status != Completed || s.Receipt == nil || *s.Receipt != receipt {
		return LeaseLost()
	}
	return nil
}

// Fail reports the caller's attempt as failed; the server decides what follows.
func (a *App) Fail(ctx context.Context, p *Principal, rawID string, in *Input) (map[string]any, error) {
	in.AllowOnly("receipt", "error", "permanent")
	receipt := readReceipt(in)
	report := readErrorReport(in)
	permanent, _ := in.Bool("permanent")
	if err := in.Validate(); err != nil {
		return nil, err
	}
	tenant, err := p.Tenant()
	if err != nil {
		return nil, err
	}
	id, ok := ParseMessageID(rawID)
	var m *Message
	if ok {
		if m, err = a.loadMessage(ctx, a.DB, tenant, id); err != nil {
			return nil, err
		}
	}
	if m == nil {
		return nil, NotFound("No message \"" + rawID + "\".")
	}
	if _, err := p.Authorise(m.Queue); err != nil {
		return nil, err
	}
	c, err := a.ForQueue(ctx, tenant, m.Queue)
	if err != nil {
		return nil, err
	}
	t := now()
	if !holdsLease(m, receipt, t) {
		return a.repeatedFail(ctx, tenant, id, receipt, c, m)
	}

	retries := m.RetryCount
	retryLimit := c.RetryLimit(m.MaxAttempts)
	outcome, delay := c.Decide(retries, retryLimit, permanent)
	details, _ := json.Marshal(map[string]any{
		"type":      report.Type,
		"message":   report.Message,
		"details":   report.Details,
		"at":        formatTimeV(t),
		"attempt":   retries + 1,
		"permanent": permanent,
		"expired":   false,
		"consumer":  m.ConsumedBy,
		"route":     m.Route,
	})
	values := map[string]any{"error_message": report.Message, "error_details": string(details), "lease_until": nil}
	if m.Route != nil {
		routes := map[string]string{}
		for k, v := range m.FailedRoutes {
			routes[k] = v
		}
		routes[*m.Route] = db.T(t)
		b, _ := json.Marshal(routes)
		values["failed_routes"] = string(b)
	}
	var availableAt *time.Time
	var done bool
	switch outcome {
	case Discarded:
		done, err = a.deleteGuarded(ctx, id, tenant, Processing, receipt)
	case DeadLettered:
		values["status"] = Failed
		values["failed_at"] = db.T(t)
		done, err = a.updateGuarded(ctx, id, tenant, values, Processing, receipt)
	default:
		at := t.Add(time.Duration(delay) * time.Second)
		availableAt = &at
		values["status"] = Pending
		values["retry_count"] = retries + 1
		values["processing_after"] = db.T(at)
		values["consumed_by"] = nil
		values["processing_started_at"] = nil
		done, err = a.updateGuarded(ctx, id, tenant, values, Processing, receipt)
	}
	if err != nil {
		return nil, err
	}
	if !done {
		return nil, LeaseLost()
	}
	if outcome == Retrying {
		a.notify(ctx, tenant, m.Queue)
	}
	a.Log.Info("Message failed", "message_id", rawID, "outcome", outcome, "attempt", retries+1,
		"permanent", permanent, "error_type", report.Type)
	return map[string]any{
		"outcome":      outcome,
		"available_at": formatTime(availableAt),
		"attempt":      retries + 1,
		"max_attempts": retryLimit + 1,
	}, nil
}

// repeatedFail answers a fail whose receipt no longer holds the lease: if that receipt's
// own fail already went through (and nobody claimed the message since), as then.
func (a *App) repeatedFail(ctx context.Context, tenant string, id MessageID, receipt string, c *QueueConfig,
	m *Message) (map[string]any, error) {
	s, err := a.leaseState(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	if s == nil {
		s = &leaseState{Queue: m.Queue, Status: m.Status, Receipt: m.Receipt, RetryCount: m.RetryCount,
			MaxAttempts: m.MaxAttempts, ProcessingAfter: m.ProcessingAfter}
	}
	retryLimit := c.RetryLimit(s.MaxAttempts)
	// The lease ran out before this fail: a late report, not a repeated one.
	if s.Expired {
		return nil, LeaseLost()
	}
	same := s.Receipt != nil && *s.Receipt == receipt
	if same && s.Status == Pending {
		return map[string]any{
			"outcome":      Retrying,
			"available_at": formatTime(s.ProcessingAfter),
			"attempt":      s.RetryCount,
			"max_attempts": retryLimit + 1,
		}, nil
	}
	if same && s.Status == Failed {
		return map[string]any{
			"outcome":      DeadLettered,
			"available_at": nil,
			"attempt":      s.RetryCount + 1,
			"max_attempts": retryLimit + 1,
		}, nil
	}
	return nil, LeaseLost()
}

func holdsLease(m *Message, receipt string, t time.Time) bool {
	return m.Status == Processing && m.Receipt != nil && *m.Receipt == receipt &&
		m.LeaseUntil != nil && m.LeaseUntil.After(t)
}

// Release hands the message back without counting the attempt.
func (a *App) Release(ctx context.Context, p *Principal, rawID string, in *Input) error {
	in.AllowOnly("receipt", "delay_seconds")
	receipt := readReceipt(in)
	var delay int64
	if in.Has("delay_seconds") {
		delay, _ = in.Int("delay_seconds", 0, nil)
	}
	if err := in.Validate(); err != nil {
		return err
	}
	tenant, id, s, err := a.state(ctx, p, rawID)
	if err != nil {
		return err
	}
	// Without a delay the message is ready now; the wait counts from here, not from
	// when it was published (time spent processing is no waiting).
	after := db.T(now())
	if delay > 0 {
		after = db.T(now().Add(time.Duration(delay) * time.Second))
	}
	// The attempt is not counted: retry_count stays as it is.
	done, err := a.updateGuarded(ctx, id, tenant, map[string]any{
		"status": Pending, "processing_after": after, "consumed_by": nil, "processing_started_at": nil, "lease_until": nil,
	}, Processing, receipt)
	if err != nil {
		return err
	}
	if !done {
		return LeaseLost()
	}
	a.notify(ctx, tenant, s.Queue)
	return nil
}

type errorReport struct {
	Type    *string
	Message string
	Details any
}

func readErrorReport(in *Input) errorReport {
	if !in.Has("error") {
		in.AddError("error", "is required")
		return errorReport{}
	}
	obj, ok := AsObject(in.Value("error"))
	if !ok {
		in.AddError("error", "must be an object")
		return errorReport{}
	}
	raw, _ := json.Marshal(obj)
	report, err := InputFromRaw(raw)
	if err != nil {
		in.AddError("error", "must be an object")
		return errorReport{}
	}
	report.AllowOnly("type", "message", "details")
	var r errorReport
	if t, ok := report.String("type", false, 255, ""); ok {
		r.Type = &t
	}
	r.Message, _ = report.String("message", true, 65535, "")
	for _, e := range report.Errors() {
		in.AddError("error."+e.Field, e.Message)
	}
	r.Details = obj["details"]
	return r
}

func readHeaders(in *Input) map[string]string {
	if !in.Has("headers") {
		return nil
	}
	obj, ok := AsObject(in.Value("headers"))
	if !ok {
		in.AddError("headers", "must be an object of strings")
		return nil
	}
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		s, ok := v.(string)
		if !ok {
			in.AddError("headers."+k, "must be a string")
			continue
		}
		out[k] = s
	}
	return out
}

func headersObject(h map[string]string) map[string]string {
	if h == nil {
		return map[string]string{}
	}
	return h
}

// readTime reads an RFC 3339 date-time.
func readTime(in *Input, field string, allowFraction bool) *time.Time {
	if !in.Has(field) {
		return nil
	}
	s, _ := in.Value(field).(string)
	t, err := time.Parse(time.RFC3339, s)
	if err != nil || (!allowFraction && bytes.ContainsRune([]byte(s), '.')) {
		in.AddError(field, "must be an RFC 3339 date-time")
		return nil
	}
	t = t.UTC()
	return &t
}

func truncate(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := t.Truncate(time.Second)
	return &v
}

// AckMany completes several leases in one request: one statement for all of them when
// every one is current. Each lease gets its own outcome, in the order of the request:
// acked (a repeated ack included), lease_lost, not_found or forbidden.
func (a *App) AckMany(ctx context.Context, p *Principal, in *Input) (map[string]any, error) {
	in.AllowOnly("leases")
	type item struct {
		message, receipt, outcome string
		id                        MessageID
		valid                     bool
	}
	var items []*item
	list, ok := in.Value("leases").([]any)
	switch {
	case !in.Has("leases"):
		in.AddError("leases", "is required")
	case !ok || len(list) == 0 || len(list) > maxBatch:
		in.AddError("leases", fmt.Sprintf("must be an array of 1 to %d leases", maxBatch))
	default:
		for i, raw := range list {
			obj, ok := raw.(map[string]any)
			message, _ := obj["message"].(string)
			receipt, _ := obj["receipt"].(string)
			if !ok || message == "" || !Matches(receiptPattern, receipt) || len(obj) != 2 {
				in.AddError(fmt.Sprintf("leases[%d]", i), "must be {message, receipt} of a claim")
				continue
			}
			it := &item{message: message, receipt: receipt, outcome: "not_found"}
			it.id, it.valid = ParseMessageID(message)
			items = append(items, it)
		}
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}
	tenant, err := p.Tenant()
	if err != nil {
		return nil, err
	}

	var ids [][]byte
	var receipts []string
	seen := map[MessageID]bool{}
	for _, it := range items {
		if it.valid && !seen[it.id] {
			seen[it.id] = true
			ids = append(ids, it.id.Bytes())
			receipts = append(receipts, it.receipt)
		}
	}
	// A key limited to some queues may only ack in those: look first which queue each is in.
	if len(p.QueuePatterns) > 0 && len(ids) > 0 {
		queues, err := a.queuesOf(ctx, tenant, ids)
		if err != nil {
			return nil, err
		}
		ids, receipts = nil, nil
		for _, it := range items {
			q, found := queues[it.id]
			switch {
			case !it.valid || !found:
				it.valid = false
			case !p.MayUseQueue(q):
				it.valid = false
				it.outcome = "forbidden"
			default:
				ids = append(ids, it.id.Bytes())
				receipts = append(receipts, it.receipt)
			}
		}
	}
	if len(ids) > 0 {
		t := now()
		// A message is acked only when its own receipt is among those sent: receipts are
		// random, so one lease's receipt never completes another message.
		n, err := db.RetryOnDeadlock(ctx, func() (int64, error) {
			return a.DB.Exec(ctx, a.DB, `UPDATE messages SET status = 'completed', processed_at = :now, lease_until = NULL,
				dedup_open = NULL, updated_at = :now
				WHERE tenant_id = :tenant AND id IN (:ids) AND receipt IN (:receipts)
				AND status = 'processing' AND lease_until > :now`,
				db.Args{"now": db.T(t), "tenant": tenant, "ids": ids, "receipts": receipts})
		})
		if err != nil {
			return nil, err
		}
		if int(n) == len(ids) {
			for _, it := range items {
				if it.valid {
					it.outcome = "acked"
				}
			}
		} else {
			// Not all of them: what each one is now tells acked (perhaps by an earlier ack of
			// the same lease) from lost.
			states, err := a.receiptsOf(ctx, tenant, ids)
			if err != nil {
				return nil, err
			}
			for _, it := range items {
				if !it.valid {
					continue
				}
				st, found := states[it.id]
				switch {
				case !found:
					it.outcome = "not_found"
				case st[0] == Completed && st[1] == it.receipt:
					it.outcome = "acked"
				default:
					it.outcome = "lease_lost"
				}
			}
		}
	}
	out := make([]any, len(items))
	for i, it := range items {
		out[i] = map[string]any{"message": it.message, "outcome": it.outcome}
	}
	return map[string]any{"items": out}, nil
}

func (a *App) queuesOf(ctx context.Context, tenant string, ids [][]byte) (map[MessageID]string, error) {
	rows, err := a.DB.Query(ctx, a.DB, "SELECT id, queue FROM messages WHERE tenant_id = :tenant AND id IN (:ids)",
		db.Args{"tenant": tenant, "ids": ids})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[MessageID]string{}
	for rows.Next() {
		var raw []byte
		var q string
		if err := rows.Scan(&raw, &q); err != nil {
			return nil, err
		}
		if id, err := MessageIDFromBytes(raw); err == nil {
			out[id] = q
		}
	}
	return out, rows.Err()
}

func (a *App) receiptsOf(ctx context.Context, tenant string, ids [][]byte) (map[MessageID][2]string, error) {
	rows, err := a.DB.Query(ctx, a.DB, "SELECT id, status, receipt FROM messages WHERE tenant_id = :tenant AND id IN (:ids)",
		db.Args{"tenant": tenant, "ids": ids})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[MessageID][2]string{}
	for rows.Next() {
		var raw []byte
		var status string
		var receipt *string
		if err := rows.Scan(&raw, &status, &receipt); err != nil {
			return nil, err
		}
		r := ""
		if receipt != nil {
			r = *receipt
		}
		if id, err := MessageIDFromBytes(raw); err == nil {
			out[id] = [2]string{status, r}
		}
	}
	return out, rows.Err()
}
