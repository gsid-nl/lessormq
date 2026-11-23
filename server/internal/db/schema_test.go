package db

import (
	"context"
	"testing"
)

func TestMigrateRunsEachMigrationOnce(t *testing.T) {
	ctx := context.Background()
	d, err := Open(ctx, "sqlite:///"+t.TempDir()+"/lmq.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	versions := func() map[string]bool {
		t.Helper()
		rows, err := d.QueryContext(ctx, "SELECT version FROM "+migrationsTable)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]bool{}
		for rows.Next() {
			var v string
			rows.Scan(&v)
			got[v] = true
		}
		return got
	}
	want := len(series["sqlite"]) + len(migrations)

	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := versions(); len(got) != want || !got[migrations[0].version] {
		t.Fatalf("a new database: %v", got)
	}

	// A database of LessorMQ 0.3 has its series and none of the migrations since.
	for _, m := range migrations {
		if _, err := d.ExecContext(ctx, "DELETE FROM "+migrationsTable+" WHERE version = ?", m.version); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := versions(); len(got) != want {
		t.Fatalf("a database of 0.3: %v", got)
	}
}
