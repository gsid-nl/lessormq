package db

import (
	"reflect"
	"testing"
	"time"
)

func TestBindNamedParameters(t *testing.T) {
	d := &DB{D: postgreSQL{}}
	q, args := d.Bind(`SELECT ':skip', x::jsonb FROM t WHERE a = :a AND b IN (:list) AND c = :a`,
		Args{"a": 1, "list": []string{"x", "y"}})
	want := `SELECT ':skip', x::jsonb FROM t WHERE a = $1 AND b IN ($2, $3) AND c = $4`
	if q != want {
		t.Fatalf("query\n got %s\nwant %s", q, want)
	}
	if !reflect.DeepEqual(args, []any{1, "x", "y", 1}) {
		t.Fatalf("args %v", args)
	}
	m := &DB{D: mariaDB{}}
	q, _ = m.Bind("SELECT 1 WHERE a IN (:none) AND b = 'it''s :not'", Args{"none": []string{}})
	if q != "SELECT 1 WHERE a IN (NULL) AND b = 'it''s :not'" {
		t.Fatalf("query %s", q)
	}
}

func TestNullTime(t *testing.T) {
	var n NullTime
	if err := n.Scan([]byte("2026-11-23 10:00:05")); err != nil || !n.Valid || T(n.Time) != "2026-11-23 10:00:05.000" {
		t.Fatalf("scan: %v %v", err, n)
	}
	if err := n.Scan([]byte("2026-11-23 10:00:05.042")); err != nil || T(n.Time) != "2026-11-23 10:00:05.042" {
		t.Fatalf("milliseconds: %v %v", err, n)
	}
	if err := n.Scan(time.Date(2026, 11, 23, 10, 0, 5, 42e6, time.FixedZone("x", 3600))); err != nil ||
		T(n.Time) != "2026-11-23 10:00:05.042" {
		t.Fatalf("time: %v %v", err, n)
	}
	if err := n.Scan(nil); err != nil || n.Valid {
		t.Fatalf("null: %v", n)
	}
}
