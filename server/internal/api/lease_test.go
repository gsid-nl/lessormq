package api

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gsid-nl/lessormq/server/internal/app"
	"github.com/gsid-nl/lessormq/server/internal/coord"
	"github.com/gsid-nl/lessormq/server/internal/db"
)

// lease_until is a column of whole seconds: a lease end with milliseconds would be rounded
// up by MySQL and PostgreSQL, so a lease could last half a second longer than its length.
// SQLite keeps the text it is given, which shows what was written.
func TestALeaseEndsOnAWholeSecond(t *testing.T) {
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
	if status, out := c.call(key, "POST", "/v1/queues/main/messages", map[string]any{"body": map[string]any{}}, ""); status != 201 {
		t.Fatalf("publish: %d %v", status, out)
	}
	if status, out := c.call(key, "POST", "/v1/queues/main/claims", map[string]any{}, ""); status != 200 {
		t.Fatalf("claim: %d %v", status, out)
	}

	var stored string
	if err := database.QueryRowContext(ctx, "SELECT lease_until FROM messages WHERE tenant_id = 'ta'").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if i := strings.IndexByte(stored, '.'); i >= 0 && strings.Trim(stored[i+1:], "0") != "" {
		t.Fatalf("lease_until has a fraction of a second: %s", stored)
	}
}
