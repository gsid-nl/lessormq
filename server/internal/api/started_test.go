package api

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/app"
	"github.com/gsid-nl/lessormq/server/internal/coord"
	"github.com/gsid-nl/lessormq/server/internal/db"
)

// A message says when its current or last attempt was claimed, so an administrator sees
// how long it waited and ran: kept after an ack or a dead letter, gone while it waits again.
func TestAMessageSaysWhenItWasClaimed(t *testing.T) {
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
	a := app.New(database, coord.NewMemory(), log, app.Config{LeaseSweepSeconds: 60})
	srv := httptest.NewServer((&Server{App: a, Log: log}).Handler())
	defer srv.Close()
	_, operator, err := a.CreateOperatorKey(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	c := caller{t: t, url: srv.URL}
	status, body := c.call(operator, "POST", "/v1/tenants", map[string]any{"id": "ta", "name": "ta"}, "")
	if status != 201 {
		t.Fatalf("tenant: %d %v", status, body)
	}
	key := body["admin_key"].(map[string]any)["secret"].(string)
	expect := func(want int, method, path string, body any) map[string]any {
		t.Helper()
		status, out := c.call(key, method, path, body, "")
		if status != want {
			t.Fatalf("%s %s: %d, want %d: %v", method, path, status, want, out)
		}
		return out
	}
	q := "/v1/queues/main"
	started := func(id string) any {
		t.Helper()
		return expect(200, "GET", "/v1/messages/"+id, nil)["started_at"]
	}
	claimedAround := func(id string, from time.Time) {
		t.Helper()
		v, _ := started(id).(string)
		at, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t.Fatalf("started_at %q: %v", v, err)
		}
		if at.Before(from.Add(-time.Second)) || at.After(time.Now().Add(time.Second)) {
			t.Errorf("started_at %s, claimed after %s", at, from)
		}
	}

	m := expect(201, "POST", q+"/messages", map[string]any{"body": map[string]any{}})
	id := m["id"].(string)
	if v := started(id); v != nil {
		t.Errorf("pending: started_at %v, want null", v)
	}
	from := time.Now()
	l := expect(200, "POST", q+"/claims", nil)
	claimedAround(id, from)
	expect(204, "POST", "/v1/messages/"+id+"/release", map[string]any{"receipt": l["receipt"]})
	if v := started(id); v != nil {
		t.Errorf("released: started_at %v, want null", v)
	}
	from = time.Now()
	l = expect(200, "POST", q+"/claims", nil)
	expect(204, "POST", "/v1/messages/"+id+"/ack", map[string]any{"receipt": l["receipt"]})
	claimedAround(id, from)

	m = expect(201, "POST", q+"/messages", map[string]any{"body": map[string]any{"n": 2}})
	id = m["id"].(string)
	from = time.Now()
	l = expect(200, "POST", q+"/claims", nil)
	expect(200, "POST", "/v1/messages/"+id+"/fail", map[string]any{"receipt": l["receipt"], "error": map[string]any{"message": "no"}, "permanent": true})
	claimedAround(id, from)
	expect(200, "POST", "/v1/messages/"+id+"/replay", nil)
	if v := started(id); v != nil {
		t.Errorf("replayed: started_at %v, want null", v)
	}
}
