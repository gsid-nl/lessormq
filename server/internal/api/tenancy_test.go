package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/app"
	"github.com/gsid-nl/lessormq/server/internal/coord"
	"github.com/gsid-nl/lessormq/server/internal/db"
)

// Every query on messages must filter on tenant_id: message ids are not secret, so a
// forgotten WHERE would quietly show or change another tenant's messages. This walks
// every endpoint on SQLite and checks each statement the server sent.

var messagesTable = regexp.MustCompile(`(?i)\b(from|update|into|update ignore)\s+messages\b`)

type recorder struct {
	mu      sync.Mutex
	queries []string
}

func (r *recorder) observe(q string) {
	r.mu.Lock()
	r.queries = append(r.queries, q)
	r.mu.Unlock()
}

type caller struct {
	t   *testing.T
	url string
}

func (c caller) call(key, method, path string, body any, tenant string) (int, map[string]any) {
	c.t.Helper()
	var buf io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, c.url+path, buf)
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	if tenant != "" {
		req.Header.Set(app.TenantHeader, tenant)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var out map[string]any
	json.Unmarshal(raw, &out)
	return res.StatusCode, out
}

func TestEveryMessagesQueryFiltersOnTenant(t *testing.T) {
	ctx := context.Background()
	database, err := db.Open(ctx, "sqlite:///"+t.TempDir()+"/lmq.db", db.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := app.New(database, coord.NewMemory(), log, app.Config{LeaseSweepSeconds: 1})
	srv := httptest.NewServer((&Server{App: a, Log: log}).Handler())
	defer srv.Close()
	_, operator, err := a.CreateOperatorKey(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	c := caller{t: t, url: srv.URL}
	keys := map[string]string{}
	for _, id := range []string{"ta", "tb"} {
		status, body := c.call(operator, "POST", "/v1/tenants", map[string]any{"id": id, "name": id}, "")
		if status != 201 {
			t.Fatalf("tenant %s: %d %v", id, status, body)
		}
		keys[id] = body["admin_key"].(map[string]any)["secret"].(string)
	}

	rec := &recorder{}
	database.Observe = rec.observe
	key := keys["ta"]
	q := "/v1/queues/main"
	expect := func(want int, method, path string, body any) map[string]any {
		t.Helper()
		status, out := c.call(key, method, path, body, "")
		if status != want {
			t.Fatalf("%s %s: %d, want %d: %v", method, path, status, want, out)
		}
		return out
	}
	expect(200, "PATCH", q, map[string]any{"max_attempts": 2, "lease_seconds": 1, "backoff": map[string]any{"initial_seconds": 0, "jitter": 0}})
	m1 := expect(201, "POST", q+"/messages", map[string]any{"body": map[string]any{"n": 1}, "dedup_key": "k"})
	expect(200, "POST", q+"/messages", map[string]any{"body": map[string]any{"n": 1}, "dedup_key": "k"})
	expect(201, "POST", q+"/messages", map[string]any{"body": map[string]any{"n": 2}, "delay_seconds": 60})
	for i := 3; i <= 6; i++ {
		expect(201, "POST", q+"/messages", map[string]any{"body": map[string]any{"n": i}})
	}

	l := expect(200, "POST", q+"/claims", map[string]any{"route": "r1"})
	id := l["message"].(map[string]any)["id"].(string)
	expect(200, "POST", "/v1/messages/"+id+"/extend", map[string]any{"receipt": l["receipt"]})
	expect(204, "POST", "/v1/messages/"+id+"/ack", map[string]any{"receipt": l["receipt"]})
	expect(204, "POST", "/v1/messages/"+id+"/ack", map[string]any{"receipt": l["receipt"]})

	l = expect(200, "POST", q+"/claims", nil)
	id = l["message"].(map[string]any)["id"].(string)
	expect(200, "POST", "/v1/messages/"+id+"/fail", map[string]any{"receipt": l["receipt"], "error": map[string]any{"type": "E", "message": "no"}, "permanent": true})
	expect(200, "POST", "/v1/messages/"+id+"/fail", map[string]any{"receipt": l["receipt"], "error": map[string]any{"message": "again"}})
	dead := id

	l = expect(200, "POST", q+"/claims", nil)
	id = l["message"].(map[string]any)["id"].(string)
	expect(204, "POST", "/v1/messages/"+id+"/release", map[string]any{"receipt": l["receipt"]})

	batch := expect(200, "POST", q+"/claims", map[string]any{"max_messages": 2})
	var leases []any
	for _, it := range batch["items"].([]any) {
		lease := it.(map[string]any)
		leases = append(leases, map[string]any{"message": lease["message"].(map[string]any)["id"], "receipt": lease["receipt"]})
	}
	leases = append(leases, map[string]any{"message": m1["id"], "receipt": "0123456789abcdef0123456789abcdef"})
	expect(200, "POST", "/v1/acks", map[string]any{"leases": leases})

	// An expired lease, ended by the sweep of the next claim.
	expect(200, "POST", q+"/claims", nil)
	time.Sleep(2100 * time.Millisecond)
	expect(200, "POST", q+"/claims", nil)

	expect(200, "GET", q+"/messages?status=failed&error_type=E", nil)
	expect(200, "GET", q+"/messages?search=n&limit=1", nil)
	expect(200, "GET", q+"/messages?search="+dead, nil)
	expect(200, "GET", "/v1/messages/"+dead, nil)
	expect(200, "POST", "/v1/messages/"+dead+"/replay", nil)
	l = expect(200, "POST", q+"/claims", nil)
	expect(204, "POST", "/v1/messages/"+l["message"].(map[string]any)["id"].(string)+"/ack", map[string]any{"receipt": l["receipt"]})
	expect(204, "DELETE", "/v1/messages/"+dead, nil)
	expect(200, "POST", q+"/replay", map[string]any{"status": "completed", "limit": 1})
	expect(200, "POST", q+"/purge", map[string]any{"status": "failed"})
	expect(200, "GET", q, nil)
	expect(200, "GET", "/v1/queues", nil)
	expect(200, "GET", q+"/workers", nil)
	expect(200, "GET", q+"/history?period=1h", nil)
	expect(200, "GET", "/v1/history", nil)
	expect(200, "GET", "/v1/tenant", nil)
	if status, _ := c.call(key, "GET", "/v1/metrics", nil, ""); status != 200 {
		t.Fatalf("metrics: %d", status)
	}
	if status, _ := c.call(operator, "GET", "/v1/history", nil, ""); status != 200 {
		t.Fatalf("operator history: %d", status)
	}
	if status, _ := c.call(operator, "GET", "/v1/history?tenants=ta,tb", nil, ""); status != 200 {
		t.Fatalf("operator history of some tenants: %d", status)
	}
	if status, _ := c.call(operator, "GET", "/v1/tenants/ta", nil, ""); status != 200 {
		t.Fatalf("operator tenant: %d", status)
	}
	if status, _ := c.call(keys["tb"], "GET", "/v1/messages/"+m1["id"].(string), nil, ""); status != 404 {
		t.Fatalf("another tenant's message: %d", status)
	}
	resp, _ := http.Get(srv.URL + "/metrics")
	resp.Body.Close()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	checked := 0
	for _, query := range rec.queries {
		if !messagesTable.MatchString(query) {
			continue
		}
		checked++
		if !strings.Contains(query, "tenant_id") {
			t.Errorf("a query on messages without tenant_id:\n%s", query)
		}
	}
	if checked < 30 {
		t.Fatalf("only %d queries on messages seen: the walk did not get far", checked)
	}
}
