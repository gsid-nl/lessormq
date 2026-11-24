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

// A message put back in line by a replay or a release waits from that moment: an old
// message replayed from failed showed as waiting since it was first published, which
// raised a "queue stuck" alert while the queue drained in minutes.
func TestAMessagePutBackWaitsFromThen(t *testing.T) {
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
	// Published 12 hours ago.
	published := func() {
		t.Helper()
		if _, err := database.ExecContext(ctx, "UPDATE messages SET created_at = ? WHERE tenant_id = 'ta'",
			db.T(time.Now().UTC().Add(-12*time.Hour))); err != nil {
			t.Fatal(err)
		}
	}
	oldestReady := func() float64 {
		t.Helper()
		out := expect(200, "GET", q, nil)
		stats, _ := out["stats"].(map[string]any)
		v, ok := stats["oldest_ready_seconds"].(float64)
		if !ok || stats["ready"] != float64(1) {
			t.Fatalf("queue stats: %v", out)
		}
		return v
	}

	m := expect(201, "POST", q+"/messages", map[string]any{"body": map[string]any{}})
	id := m["id"].(string)
	l := expect(200, "POST", q+"/claims", nil)
	expect(200, "POST", "/v1/messages/"+id+"/fail", map[string]any{"receipt": l["receipt"], "error": map[string]any{"message": "no"}, "permanent": true})
	published()
	expect(200, "POST", "/v1/messages/"+id+"/replay", nil)
	if v := oldestReady(); v > 60 {
		t.Errorf("after a replay the message has waited %.0f s", v)
	}

	l = expect(200, "POST", q+"/claims", nil)
	published()
	expect(204, "POST", "/v1/messages/"+id+"/release", map[string]any{"receipt": l["receipt"]})
	if v := oldestReady(); v > 60 {
		t.Errorf("after a release the message has waited %.0f s", v)
	}
}
