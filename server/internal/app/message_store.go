package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/db"
)

// Message statuses.
const (
	Pending    = "pending"
	Processing = "processing"
	Completed  = "completed"
	Failed     = "failed"
)

// Message is a row of messages.
type Message struct {
	ID                  MessageID
	TenantID            string
	Queue               string
	Body                json.RawMessage
	Headers             map[string]string
	Status              string
	ProcessingStartedAt *time.Time
	ProcessingAfter     *time.Time
	ProcessedAt         *time.Time
	FailedAt            *time.Time
	RetryCount          int64
	MaxAttempts         *int64
	ConsumedBy          *string
	Receipt             *string
	LeaseUntil          *time.Time
	Route               *string
	FailedRoutes        map[string]string
	DedupKey            *string
	ErrorMessage        *string
	ErrorDetails        *string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Attempts are the attempts started so far: retry_count counts the failed attempts that
// led to another one (an expired lease included, a release not); the attempt that is
// running, that succeeded or that was the last to fail comes on top.
func (m *Message) Attempts() int64 {
	if m.Status == Pending {
		return m.RetryCount
	}
	return m.RetryCount + 1
}

const messageColumns = `id, tenant_id, queue, body, headers, status, processing_started_at, processing_after,
	processed_at, failed_at, retry_count, max_attempts, consumed_by, receipt, lease_until, route, failed_routes,
	dedup_key, error_message, error_details, created_at, updated_at`

type scanner interface{ Scan(dest ...any) error }

func scanMessage(s scanner) (*Message, error) {
	var (
		m                                                            Message
		id                                                           []byte
		body                                                         string
		headers, consumed, receipt, route, failedRoutes              sql.NullString
		dedup, errMsg, errDetails                                    sql.NullString
		started, after, processed, failedAt, lease, created, updated db.NullTime
		maxAttempts                                                  sql.NullInt64
	)
	if err := s.Scan(&id, &m.TenantID, &m.Queue, &body, &headers, &m.Status, &started, &after, &processed,
		&failedAt, &m.RetryCount, &maxAttempts, &consumed, &receipt, &lease, &route, &failedRoutes, &dedup,
		&errMsg, &errDetails, &created, &updated); err != nil {
		return nil, err
	}
	mid, err := MessageIDFromBytes(id)
	if err != nil {
		return nil, err
	}
	m.ID = mid
	m.Body = json.RawMessage(body)
	if headers.Valid && headers.String != "" && headers.String != "null" {
		var h map[string]string
		if json.Unmarshal([]byte(headers.String), &h) == nil {
			m.Headers = h
		}
	}
	m.ProcessingStartedAt = started.Ptr()
	m.ProcessingAfter = after.Ptr()
	m.ProcessedAt = processed.Ptr()
	m.FailedAt = failedAt.Ptr()
	m.LeaseUntil = lease.Ptr()
	m.CreatedAt = created.Time
	m.UpdatedAt = updated.Time
	if maxAttempts.Valid {
		v := maxAttempts.Int64
		m.MaxAttempts = &v
	}
	m.ConsumedBy = nullString(consumed)
	m.Receipt = nullString(receipt)
	m.Route = nullString(route)
	m.DedupKey = nullString(dedup)
	m.ErrorMessage = nullString(errMsg)
	m.ErrorDetails = nullString(errDetails)
	if failedRoutes.Valid && failedRoutes.String != "" && failedRoutes.String != "null" {
		var fr map[string]string
		if json.Unmarshal([]byte(failedRoutes.String), &fr) == nil {
			m.FailedRoutes = fr
		}
	}
	return &m, nil
}

func nullString(s sql.NullString) *string {
	if !s.Valid {
		return nil
	}
	v := s.String
	return &v
}

func strPtr(s string) *string { return &s }

// loadMessage is a message of the tenant by id, or nil.
func (a *App) loadMessage(ctx context.Context, q db.Querier, tenant string, id MessageID) (*Message, error) {
	row := a.DB.Row(ctx, q, "SELECT "+messageColumns+" FROM messages WHERE id = :id AND tenant_id = :tenant",
		db.Args{"id": id.Bytes(), "tenant": tenant})
	m, err := scanMessage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return m, err
}

// insertMessage puts a new message in the table.
func (a *App) insertMessage(ctx context.Context, m *Message) error {
	headers := any(nil)
	if len(m.Headers) > 0 {
		b, _ := json.Marshal(m.Headers)
		headers = string(b)
	}
	dedup := any(nil)
	if m.DedupKey != nil {
		dedup = *m.DedupKey
	}
	maxAttempts := any(nil)
	if m.MaxAttempts != nil {
		maxAttempts = *m.MaxAttempts
	}
	_, err := a.DB.Exec(ctx, a.DB, `INSERT INTO messages (id, tenant_id, queue, body, headers, status, processing_after,
			retry_count, max_attempts, dedup_key, dedup_open, created_at, updated_at)
		VALUES (:id, :tenant, :queue, :body, :headers, 'pending', :after, 0, :max_attempts, :dedup, :dedup, :now, :now)`,
		db.Args{
			"id": m.ID.Bytes(), "tenant": m.TenantID, "queue": m.Queue, "body": string(m.Body), "headers": headers,
			"after": db.TPtr(m.ProcessingAfter), "max_attempts": maxAttempts, "dedup": dedup, "now": db.T(m.CreatedAt),
		})
	return err
}

// findOpenByDedupKey is the open message of a queue with this dedup_key, if there is one;
// served by uniq_open_dedup (tenant_id, queue, dedup_open).
func (a *App) findOpenByDedupKey(ctx context.Context, tenant, queue, key string) (*Message, error) {
	row := a.DB.Row(ctx, a.DB, "SELECT "+messageColumns+
		" FROM messages WHERE tenant_id = :tenant AND queue = :queue AND dedup_open = :key",
		db.Args{"tenant": tenant, "queue": queue, "key": key})
	m, err := scanMessage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return m, err
}

// claimNext atomically claims up to n pending messages of a queue, oldest first.
//
//  1. Look for a claimable id with a plain read, no transaction; stop if there is none:
//     nearly every poll finds the queue empty, and this answers that in one statement.
//  2. In one READ COMMITTED transaction, SELECT the first claimable ids FOR UPDATE SKIP
//     LOCKED and UPDATE those rows by primary key, each with a receipt of its own.
//  3. Read the claimed rows by primary key.
//
// READ COMMITTED drops the gap locks, so a claim only ever holds the rows it is taking,
// and SKIP LOCKED makes a worker walk past rows another worker already holds. SQLite has
// no row locks and one writer at a time, so there step 2 is one UPDATE ... RETURNING.
func (a *App) claimNext(ctx context.Context, tenant, queue, consumer, route string, avoidSeconds *int64,
	leaseSeconds int64, n int) ([]*Message, error) {
	t := now()
	args := db.Args{"tenant": tenant, "queue": queue, "now": db.T(t)}
	routeCondition := ""
	// Skip a message that failed on this route within the cooldown.
	if route != "" && avoidSeconds != nil {
		routeCondition = "AND (failed_routes IS NULL OR COALESCE(" +
			a.DB.D.JSONMember("failed_routes", ":route_path") + ", '0') < :avoid_before)"
		args["route_path"] = a.DB.D.JSONMemberArgument(route)
		args["avoid_before"] = db.T(t.Add(-time.Duration(*avoidSeconds) * time.Second))
	}
	// Served by idx_tenant_claim (tenant_id, queue, status, id, processing_after): the scan
	// stops at the first claimable rows instead of sorting the backlog.
	where := `tenant_id = :tenant AND queue = :queue AND status = 'pending'
		AND (processing_after IS NULL OR processing_after <= :now) ` + routeCondition

	var probe []byte
	err := a.DB.Row(ctx, a.DB, "SELECT id FROM messages WHERE "+where+" ORDER BY id ASC LIMIT 1", args).Scan(&probe)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	routeValue := any(nil)
	if route != "" {
		routeValue = route
	}
	set := `status = 'processing', consumed_by = :consumer, lease_until = :lease_until,
		route = :route, processing_started_at = :now, updated_at = :now`
	args["consumer"] = consumer
	// lease_until has whole seconds, which MySQL and PostgreSQL would round up to: a lease
	// of 2 s could last 2.5. Cut to the second, as extend does.
	args["lease_until"] = db.T(t.Truncate(time.Second).Add(time.Duration(leaseSeconds) * time.Second))
	args["route"] = routeValue
	args["n"] = n

	return db.RetryOnDeadlock(ctx, func() ([]*Message, error) {
		if a.DB.D.ClaimsInOneStatement() {
			rows, err := a.DB.Query(ctx, a.DB, "UPDATE messages SET "+set+", receipt = lower(hex(randomblob(16)))"+
				" WHERE id IN (SELECT id FROM messages WHERE "+where+" ORDER BY id ASC LIMIT :n) RETURNING "+
				messageColumns, args)
			if err != nil {
				return nil, err
			}
			return scanMessages(rows)
		}
		tx, err := a.DB.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		// Only the id: idx_tenant_claim covers it. Asked for the whole row, MariaDB walks the
		// primary key in id order over every message of the table instead.
		rows, err := a.DB.Query(ctx, tx, "SELECT id FROM messages WHERE "+where+
			" ORDER BY id ASC LIMIT :n"+a.DB.D.SkipLocked(), args)
		if err != nil {
			return nil, err
		}
		var ids [][]byte
		for rows.Next() {
			var id []byte
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			return nil, tx.Commit()
		}
		receipts := "CASE id"
		for i, id := range ids {
			receipts += fmt.Sprintf(" WHEN :id_%d THEN :receipt_%d", i, i)
			args[fmt.Sprintf("id_%d", i)] = id
			args[fmt.Sprintf("receipt_%d", i)] = RandomHex(16)
		}
		args["ids"] = ids
		if _, err := a.DB.Exec(ctx, tx, "UPDATE messages SET "+set+", receipt = "+receipts+" END"+
			" WHERE id IN (:ids) AND tenant_id = :tenant AND status = 'pending'", args); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		claimed, err := a.DB.Query(ctx, a.DB, "SELECT "+messageColumns+
			" FROM messages WHERE id IN (:ids) AND tenant_id = :tenant AND status = 'processing' ORDER BY id",
			db.Args{"ids": ids, "tenant": tenant})
		if err != nil {
			return nil, err
		}
		return scanMessages(claimed)
	})
}

func scanMessages(rows *sql.Rows) ([]*Message, error) {
	defer rows.Close()
	var out []*Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// updateGuarded writes one message's state change straight to its row, by primary key,
// only if the row is still in the status the caller read, and with a receipt only while
// that receipt holds the current, unexpired lease. A finished message frees its dedup_key.
func (a *App) updateGuarded(ctx context.Context, id MessageID, tenant string, values map[string]any,
	expected string, receipt string) (bool, error) {
	t := now()
	values["updated_at"] = db.T(t)
	if s, _ := values["status"].(string); s == Completed || s == Failed {
		values["dedup_open"] = nil
	}
	args := db.Args{}
	assignments := make([]string, 0, len(values))
	for col, v := range values {
		assignments = append(assignments, col+" = :v_"+col)
		args["v_"+col] = v
	}
	query := "UPDATE messages SET " + strings.Join(assignments, ", ") +
		" WHERE id = :id AND tenant_id = :tenant AND status = :expected"
	args["id"] = id.Bytes()
	args["tenant"] = tenant
	args["expected"] = expected
	if receipt != "" {
		query += " AND receipt = :guard_receipt AND lease_until > :guard_now"
		args["guard_receipt"] = receipt
		args["guard_now"] = db.T(t)
	}
	n, err := db.RetryOnDeadlock(ctx, func() (int64, error) { return a.DB.Exec(ctx, a.DB, query, args) })
	return n > 0, err
}

// deleteGuarded deletes a message only if it is still in the status the caller read.
func (a *App) deleteGuarded(ctx context.Context, id MessageID, tenant, expected, receipt string) (bool, error) {
	query := "DELETE FROM messages WHERE id = :id AND tenant_id = :tenant AND status = :status"
	args := db.Args{"id": id.Bytes(), "tenant": tenant, "status": expected}
	if receipt != "" {
		query += " AND receipt = :receipt AND lease_until > :now"
		args["receipt"] = receipt
		args["now"] = db.T(now())
	}
	n, err := db.RetryOnDeadlock(ctx, func() (int64, error) { return a.DB.Exec(ctx, a.DB, query, args) })
	return n > 0, err
}

// leaseState is the lease state of one message: what extend, ack, fail and release check
// the receipt against, and what makes a repeated ack or fail answer as the first did.
type leaseState struct {
	Queue           string
	Status          string
	Receipt         *string
	LeaseUntil      *time.Time
	ProcessingAfter *time.Time
	RetryCount      int64
	MaxAttempts     *int64
	// Expired tells whether the last attempt ended by its lease running out.
	Expired bool
}

func (a *App) leaseState(ctx context.Context, tenant string, id MessageID) (*leaseState, error) {
	var (
		s           leaseState
		receipt     sql.NullString
		lease       db.NullTime
		after       db.NullTime
		maxAttempts sql.NullInt64
		expired     sql.NullString
	)
	err := a.DB.Row(ctx, a.DB, `SELECT queue, status, receipt, lease_until, processing_after, retry_count, max_attempts, `+
		a.DB.D.JSONValue("error_details", "expired")+` AS expired
		FROM messages WHERE id = :id AND tenant_id = :tenant`,
		db.Args{"id": id.Bytes(), "tenant": tenant}).
		Scan(&s.Queue, &s.Status, &receipt, &lease, &after, &s.RetryCount, &maxAttempts, &expired)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.Receipt = nullString(receipt)
	s.LeaseUntil = lease.Ptr()
	s.ProcessingAfter = after.Ptr()
	if maxAttempts.Valid {
		v := maxAttempts.Int64
		s.MaxAttempts = &v
	}
	// A JSON true reads as 1 on MariaDB and SQLite, as true on PostgreSQL and MySQL.
	s.Expired = expired.Valid && (expired.String == "1" || expired.String == "true")
	return &s, nil
}

// extendLease moves a current lease's end; false when the receipt is not the current lease.
func (a *App) extendLease(ctx context.Context, tenant string, id MessageID, receipt string, until time.Time) (bool, error) {
	n, err := db.RetryOnDeadlock(ctx, func() (int64, error) {
		return a.DB.Exec(ctx, a.DB, `UPDATE messages SET lease_until = :until, updated_at = :now
			WHERE id = :id AND tenant_id = :tenant AND status = 'processing'
			AND receipt = :receipt AND lease_until > :now`,
			db.Args{"until": db.T(until), "now": db.T(now()), "id": id.Bytes(), "tenant": tenant, "receipt": receipt})
	})
	return n > 0, err
}

// nextAvailableAt is when the next pending message of a queue that is not ready yet will
// be, or nil: what a waiting claim wakes up for when nothing announces it.
func (a *App) nextAvailableAt(ctx context.Context, tenant, queue string, t time.Time) (*time.Time, error) {
	var next db.NullTime
	err := a.DB.Row(ctx, a.DB, `SELECT MIN(processing_after) FROM messages
		WHERE tenant_id = :tenant AND queue = :queue AND status = 'pending' AND processing_after > :now`,
		db.Args{"tenant": tenant, "queue": queue, "now": db.T(t)}).Scan(&next)
	if err != nil {
		return nil, err
	}
	return next.Ptr(), nil
}

// expireLeases ends every expired lease of a queue. An expired lease is a failed attempt:
// the message goes back to pending after the queue's backoff, or, when that was its last
// attempt, to the dead-letter set or away (on_exhausted).
func (a *App) expireLeases(ctx context.Context, tenant, queue string, c *QueueConfig, t time.Time) (map[string]int64, error) {
	d := a.DB.D
	expired := `tenant_id = :tenant AND queue = :queue AND status = 'processing' AND lease_until < :now`
	exhausted := "retry_count + 1 >= " + d.Least("COALESCE(max_attempts, :max_attempts)", ":limit")
	errorDetails := d.JSONObject([][2]string{
		{"message", "'Lease expired'"},
		{"expired", d.JSONBool(true)},
		{"at", d.JSONString(":at")},
		{"attempt", "retry_count + 1"},
		{"consumer", "consumed_by"},
		{"route", "route"},
		{"permanent", d.JSONBool(false)},
	})
	// The attempt's route failed now; a route is a plain JSON key (RoutePattern).
	failedRoutes := "CASE WHEN route IS NULL THEN failed_routes ELSE " +
		d.JSONWith("failed_routes", "route", d.JSONString(":now")) + " END"
	delay := d.Least(":initial * "+d.Power(":multiplier", "retry_count"), ":max_delay")
	backoff := d.PlusSeconds(":now", "FLOOR("+delay+")")
	args := db.Args{
		"tenant": tenant, "queue": queue, "now": db.T(t),
		"max_attempts": c.Settings.Int("max_attempts"), "limit": c.Settings.Int("max_attempts_limit"),
		"at": t.UTC().Format("2006-01-02T15:04:05Z"),
	}
	return db.RetryOnDeadlock(ctx, func() (map[string]int64, error) {
		result := map[string]int64{"retried": 0, "dead_lettered": 0, "discarded": 0}
		tx, err := a.DB.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		if c.Settings.Str("on_exhausted") == "discard" {
			n, err := a.DB.Exec(ctx, tx, "DELETE FROM messages WHERE "+expired+" AND "+exhausted, args)
			if err != nil {
				return nil, err
			}
			result["discarded"] = n
		} else {
			// error_details first: MariaDB and MySQL assign left to right, so it still sees
			// the retry_count and consumed_by of the attempt that expired.
			n, err := a.DB.Exec(ctx, tx, `UPDATE messages SET error_message = 'Lease expired', error_details = `+
				errorDetails+`, failed_routes = `+failedRoutes+`, dedup_open = NULL, status = 'failed',
				failed_at = :now, lease_until = NULL, updated_at = :now
				WHERE `+expired+` AND `+exhausted, args)
			if err != nil {
				return nil, err
			}
			result["dead_lettered"] = n
		}
		retryArgs := db.Args{
			"initial":    c.Settings.Int("backoff.initial_seconds"),
			"multiplier": c.Settings.Float("backoff.multiplier"),
			"max_delay":  c.Settings.Int("backoff.max_seconds"),
		}
		for k, v := range args {
			retryArgs[k] = v
		}
		n, err := a.DB.Exec(ctx, tx, `UPDATE messages SET error_message = 'Lease expired', error_details = `+
			errorDetails+`, failed_routes = `+failedRoutes+`, processing_after = `+backoff+`,
			retry_count = retry_count + 1, status = 'pending', consumed_by = NULL, processing_started_at = NULL,
			lease_until = NULL, updated_at = :now
			WHERE `+expired, retryArgs)
		if err != nil {
			return nil, err
		}
		result["retried"] = n
		return result, tx.Commit()
	})
}

func formatTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func formatTimeV(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05Z") }

var errNoTenant = fmt.Errorf("no tenant")
