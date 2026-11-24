package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gsid-nl/lessormq/server/internal/db"
)

const (
	searchMax    = 200
	defaultLimit = 20
	maxLimit     = 100
)

// replayAssignments put a finished message back in line: attempts to zero, no error, no
// lease, available now. processing_after = now, not NULL: the wait (oldest_ready_seconds,
// history) counts from the replay, not from when the message was first published.
const replayAssignments = `status = 'pending', retry_count = 0, processing_after = :now, processing_started_at = NULL,
	processed_at = NULL, failed_at = NULL, consumed_by = NULL, receipt = NULL, lease_until = NULL,
	route = NULL, failed_routes = NULL, error_message = NULL, error_details = NULL, updated_at = :now,
	dedup_open = dedup_key`

// PageLimit reads ?limit=, 1 to 100, 20 when absent.
func PageLimit(raw *string) (int, error) {
	if raw == nil {
		return defaultLimit, nil
	}
	var n int
	if _, err := fmt.Sscanf(*raw, "%d", &n); err != nil || fmt.Sprint(n) != strings.TrimSpace(*raw) || n < 1 || n > maxLimit {
		return 0, ValidationField("limit", fmt.Sprintf("must be an integer from 1 to %d", maxLimit))
	}
	return n, nil
}

// PageAfter reads ?cursor=: the sort key of the last item, encoded so clients treat it as
// opaque.
func PageAfter(raw string, keyPattern string) (string, error) {
	if raw == "" {
		return "", nil
	}
	b, err := base64.StdEncoding.DecodeString(padBase64(strings.NewReplacer("-", "+", "_", "/").Replace(raw)))
	if err != nil || !Matches(keyPattern, string(b)) {
		return "", ValidationField("cursor", "is not a cursor from this API")
	}
	return string(b), nil
}

func padBase64(s string) string {
	if m := len(s) % 4; m != 0 {
		s += strings.Repeat("=", 4-m)
	}
	return s
}

// Cursor encodes a sort key.
func Cursor(key string) string {
	return strings.TrimRight(strings.NewReplacer("+", "-", "/", "_").Replace(base64.StdEncoding.EncodeToString([]byte(key))), "=")
}

// ListQuery are the filters of a queue's message list.
type ListQuery struct {
	Status, ErrorType, Search, Cursor string
	Limit                             *string
}

// ListMessages is one page of a queue's messages, newest first.
func (a *App) ListMessages(ctx context.Context, p *Principal, queue string, q ListQuery) (map[string]any, error) {
	tenant, err := p.Authorise(queue)
	if err != nil {
		return nil, err
	}
	if q.Status != "" && q.Status != Pending && q.Status != Processing && q.Status != Completed && q.Status != Failed {
		return nil, ValidationField("status", "unknown status")
	}
	search := strings.TrimSpace(q.Search)
	if utf8.RuneCountInString(search) > searchMax {
		return nil, ValidationField("search", fmt.Sprintf("at most %d characters", searchMax))
	}
	limit, err := PageLimit(q.Limit)
	if err != nil {
		return nil, err
	}
	after, err := PageAfter(q.Cursor, UUIDv1Pattern)
	if err != nil {
		return nil, err
	}
	page, err := a.pageMessages(ctx, tenant, queue, q.Status, q.ErrorType, limit+1, after, search)
	if err != nil {
		return nil, err
	}
	var next any
	if len(page) > limit {
		page = page[:limit]
		next = Cursor(page[len(page)-1].ID.String())
	}
	c, err := a.ForQueue(ctx, tenant, queue)
	if err != nil {
		return nil, err
	}
	items := make([]any, len(page))
	for i, m := range page {
		items[i] = messageView(m, c)
	}
	return map[string]any{"items": items, "next_cursor": next}, nil
}

// pageMessages reads a page of a queue's messages, newest first, after the message after.
// search is a message id or text in the body: an id finds that message and the messages
// whose body names it; text is matched without regard to case, as stored and as JSON
// escapes it. Either way it reads the bodies of the queue's messages until the page is
// full: a search that finds little reads the whole queue, which an admin search may do.
func (a *App) pageMessages(ctx context.Context, tenant, queue, status, errorType string, limit int, after, search string) ([]*Message, error) {
	where := "tenant_id = :tenant AND queue = :queue"
	args := db.Args{"tenant": tenant, "queue": queue}
	if status != "" {
		where += " AND status = :status"
		args["status"] = status
	}
	if errorType != "" {
		where += " AND " + a.DB.D.JSONValue("error_details", "type") + " = :error_type"
		args["error_type"] = errorType
	}
	if after != "" {
		id, _ := ParseMessageID(after)
		where += " AND id < :after"
		args["after"] = id.Bytes()
	}
	if search != "" {
		var matches []string
		for i, needle := range searchNeedles(search) {
			name := fmt.Sprintf("search_%d", i)
			matches = append(matches, "LOWER("+a.DB.D.JSONText("body")+") LIKE :"+name+" ESCAPE '!'")
			args[name] = "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(needle) + "%"
		}
		if id, ok := ParseMessageID(search); ok {
			matches = append(matches, "id = :search_id")
			args["search_id"] = id.Bytes()
		}
		where += " AND (" + strings.Join(matches, " OR ") + ")"
	}
	rows, err := a.DB.Query(ctx, a.DB, fmt.Sprintf("SELECT %s FROM messages WHERE %s ORDER BY id DESC LIMIT %d",
		messageColumns, where, limit), args)
	if err != nil {
		return nil, err
	}
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

// searchNeedles are the text as typed, and as it reads inside a JSON string: escaped as
// the PHP server stored bodies (slashes and every non-ASCII character escaped) and as
// this one does (as sent).
func searchNeedles(search string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(s string) {
		s = strings.ToLower(s)
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	add(search)
	if b, err := json.Marshal(search); err == nil {
		add(string(b[1 : len(b)-1]))
	}
	add(phpJSONString(search))
	return out
}

// phpJSONString is a string as PHP's json_encode writes it, without the quotes.
func phpJSONString(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '/':
			b.WriteString(`\/`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20:
			fmt.Fprintf(&b, `\u%04x`, r)
		case r < 0x80:
			b.WriteRune(r)
		case r > 0xFFFF:
			r -= 0x10000
			fmt.Fprintf(&b, `\u%04x\u%04x`, 0xD800+(r>>10), 0xDC00+(r&0x3FF))
		default:
			fmt.Fprintf(&b, `\u%04x`, r)
		}
	}
	return b.String()
}

func messageView(m *Message, c *QueueConfig) map[string]any {
	available := m.CreatedAt
	if m.ProcessingAfter != nil {
		available = *m.ProcessingAfter
	}
	routes := map[string]any{}
	for route, at := range m.FailedRoutes {
		if t, err := db.ParseTime(at); err == nil {
			routes[route] = formatTimeV(t)
		}
	}
	v := map[string]any{
		"id":            m.ID.String(),
		"queue":         m.Queue,
		"status":        m.Status,
		"body":          m.Body,
		"headers":       headersObject(m.Headers),
		"dedup_key":     m.DedupKey,
		"attempts":      m.Attempts(),
		"max_attempts":  c.MaxAttemptsFor(m.MaxAttempts),
		"available_at":  formatTimeV(available),
		"lease_until":   nil,
		"consumer":      m.ConsumedBy,
		"last_error":    lastError(m),
		"failed_routes": routes,
		"created_at":    formatTimeV(m.CreatedAt),
		"updated_at":    formatTimeV(m.UpdatedAt),
		"completed_at":  nil,
		"failed_at":     nil,
	}
	switch m.Status {
	case Processing:
		v["lease_until"] = formatTime(m.LeaseUntil)
	case Completed:
		v["completed_at"] = formatTime(m.ProcessedAt)
	case Failed:
		v["failed_at"] = formatTime(m.FailedAt)
	}
	return v
}

func lastError(m *Message) any {
	var details map[string]any
	if m.ErrorDetails != nil {
		details, _ = AsObject(DecodeAny([]byte(*m.ErrorDetails)))
	}
	if details == nil && m.ErrorMessage == nil {
		return nil
	}
	if details == nil {
		details = map[string]any{}
	}
	// The failed attempt: the one before the current, unless it was the last one.
	attempt := m.RetryCount
	if m.Status == Failed {
		attempt++
	}
	message := ""
	if s, ok := details["message"].(string); ok {
		message = s
	} else if m.ErrorMessage != nil {
		message = *m.ErrorMessage
	}
	at := formatTimeV(m.UpdatedAt)
	if s, ok := details["at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			at = formatTimeV(t)
		} else if t, err := db.ParseTime(s); err == nil {
			at = formatTimeV(t)
		}
	}
	if n, ok := AsInt(details["attempt"]); ok {
		attempt = n
	}
	return map[string]any{
		"type":      details["type"],
		"message":   message,
		"details":   details["details"],
		"at":        at,
		"attempt":   attempt,
		"permanent": truthy(details["permanent"]),
		"expired":   truthy(details["expired"]),
		"route":     details["route"],
		"consumer":  details["consumer"],
	}
}

func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case json.Number:
		return x != "0"
	case string:
		return x != "" && x != "0"
	}
	return false
}

// findAdmin is a message of the caller's tenant by id, after checking its queue.
func (a *App) findAdmin(ctx context.Context, p *Principal, rawID string) (*Message, error) {
	tenant, err := p.Tenant()
	if err != nil {
		return nil, err
	}
	var m *Message
	if Matches(UUIDv1Pattern, rawID) {
		id, _ := ParseMessageID(rawID)
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
	return m, nil
}

// GetMessage is one message as an admin sees it.
func (a *App) GetMessage(ctx context.Context, p *Principal, rawID string) (map[string]any, error) {
	m, err := a.findAdmin(ctx, p, rawID)
	if err != nil {
		return nil, err
	}
	c, err := a.ForQueue(ctx, m.TenantID, m.Queue)
	if err != nil {
		return nil, err
	}
	return messageView(m, c), nil
}

// DeleteMessage deletes a message unless it is in flight.
func (a *App) DeleteMessage(ctx context.Context, p *Principal, rawID string) error {
	m, err := a.findAdmin(ctx, p, rawID)
	if err != nil {
		return err
	}
	n, err := db.RetryOnDeadlock(ctx, func() (int64, error) {
		return a.DB.Exec(ctx, a.DB, "DELETE FROM messages WHERE id = :id AND tenant_id = :tenant AND status <> 'processing'",
			db.Args{"id": m.ID.Bytes(), "tenant": m.TenantID})
	})
	if err != nil {
		return err
	}
	if n == 0 {
		return InvalidState("A message that is processing cannot be deleted; its lease will end.")
	}
	a.forgetCompleted(ctx, m.TenantID, m.Queue)
	return nil
}

// ReplayMessage puts a failed or completed message back in line.
func (a *App) ReplayMessage(ctx context.Context, p *Principal, rawID string) (map[string]any, error) {
	m, err := a.findAdmin(ctx, p, rawID)
	if err != nil {
		return nil, err
	}
	n, err := db.RetryOnDeadlock(ctx, func() (int64, error) {
		return a.DB.Exec(ctx, a.DB, "UPDATE messages SET "+replayAssignments+
			" WHERE id = :id AND tenant_id = :tenant AND status IN ('failed', 'completed')",
			db.Args{"id": m.ID.Bytes(), "tenant": m.TenantID, "now": db.T(now())})
	})
	if db.IsUniqueViolation(err) {
		dedup := ""
		if m.DedupKey != nil {
			dedup = *m.DedupKey
		}
		return nil, InvalidState(fmt.Sprintf("Another open message in queue %q has dedup_key %q.", m.Queue, dedup))
	}
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, InvalidState("Only a failed or completed message can be replayed.")
	}
	a.forgetCompleted(ctx, m.TenantID, m.Queue)
	a.notify(ctx, m.TenantID, m.Queue)
	a.Log.Info("Message replayed", "message_id", rawID)
	return a.GetMessage(ctx, p, rawID)
}

// Bulk replays or deletes the finished messages of a queue that match a filter, oldest
// first, at most limit.
func (a *App) Bulk(ctx context.Context, p *Principal, queue, action string, in *Input) (map[string]any, error) {
	tenant, err := p.Authorise(queue)
	if err != nil {
		return nil, err
	}
	in.AllowOnly("status", "error_type", "before", "limit")
	status, _ := in.Choice("status", []string{Completed, Failed}, true)
	errorType, _ := in.String("error_type", false, 255, "")
	before := readTime(in, "before", false)
	limit := int64(1000)
	if in.Has("limit") {
		n, ok := AsInt(in.Value("limit"))
		if !ok || n < 1 || n > 10000 {
			in.AddError("limit", "must be an integer from 1 to 10000")
		}
		limit = n
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}
	where := "tenant_id = :tenant AND queue = :queue AND status = :status"
	args := db.Args{"tenant": tenant, "queue": queue, "status": status, "limit": limit, "now": db.T(now())}
	if errorType != "" {
		where += " AND " + a.DB.D.JSONValue("error_details", "type") + " = :error_type"
		args["error_type"] = errorType
	}
	if before != nil {
		where += " AND updated_at < :before"
		args["before"] = db.T(*before)
	}
	var count int64
	switch {
	case action == "replay" && !a.DB.D.ReplaysInOneStatement():
		count, err = a.replayEach(ctx, where, args)
	case action == "replay":
		// IGNORE: a row whose dedup_key an open message already holds stays where it is.
		count, err = db.RetryOnDeadlock(ctx, func() (int64, error) {
			return a.DB.Exec(ctx, a.DB, "UPDATE IGNORE messages SET "+replayAssignments+" WHERE "+where+
				" ORDER BY id LIMIT :limit", args)
		})
	default:
		count, err = db.RetryOnDeadlock(ctx, func() (int64, error) {
			return a.DB.Exec(ctx, a.DB, a.DB.D.DeleteMessages(where, "id"), args)
		})
	}
	if err != nil {
		return nil, err
	}
	a.forgetCompleted(ctx, tenant, queue)
	if action == "replay" && count > 0 {
		a.notify(ctx, tenant, queue)
	}
	a.Log.Info("Bulk "+action, "tenant", tenant, "queue", queue, "status", status, "error_type", errorType, "count", count)
	return map[string]any{"count": count}, nil
}

// replayEach replays one message at a time, oldest first, where no UPDATE can skip the
// rows that would break uniq_open_dedup: such a message stays where it is.
func (a *App) replayEach(ctx context.Context, where string, args db.Args) (int64, error) {
	rows, err := a.DB.Query(ctx, a.DB, "SELECT id FROM messages WHERE "+where+" ORDER BY id LIMIT :limit", args)
	if err != nil {
		return 0, err
	}
	var ids [][]byte
	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	var replayed int64
	for _, id := range ids {
		n, err := db.RetryOnDeadlock(ctx, func() (int64, error) {
			return a.DB.Exec(ctx, a.DB, "UPDATE messages SET "+replayAssignments+
				" WHERE id = :id AND tenant_id = :tenant AND status = :status",
				db.Args{"id": id, "tenant": args["tenant"], "status": args["status"], "now": args["now"]})
		})
		if err != nil {
			if db.IsUniqueViolation(err) {
				continue
			}
			return replayed, err
		}
		replayed += n
	}
	return replayed, nil
}
