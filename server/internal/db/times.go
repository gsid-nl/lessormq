package db

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// SQLTime is how every time is stored and compared: UTC, to the second, without a zone.
const SQLTime = "2006-01-02 15:04:05"

// T formats a time as a stored DATETIME value.
func T(t time.Time) string { return t.UTC().Format(SQLTime) }

// TPtr is T for a time that may be absent (NULL).
func TPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return T(*t)
}

// ParseTime reads a stored DATETIME value as the UTC time it is.
func ParseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{SQLTime, "2006-01-02 15:04:05.999999", time.RFC3339Nano, "2006-01-02T15:04:05"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("db: not a time: %q", s)
}

// NullTime scans a DATETIME column, whichever way the driver hands it over: a time.Time
// (PostgreSQL, SQLite), or bytes or a string (MySQL and MariaDB without parseTime).
type NullTime struct {
	Time  time.Time
	Valid bool
}

func (n *NullTime) Scan(v any) error {
	n.Valid = false
	switch x := v.(type) {
	case nil:
		return nil
	case time.Time:
		// A timestamp without a zone is read as UTC, whatever zone the driver gave it.
		n.Time = time.Date(x.Year(), x.Month(), x.Day(), x.Hour(), x.Minute(), x.Second(), 0, time.UTC)
	case []byte:
		t, err := ParseTime(string(x))
		if err != nil {
			return err
		}
		n.Time = t
	case string:
		t, err := ParseTime(x)
		if err != nil {
			return err
		}
		n.Time = t
	default:
		return fmt.Errorf("db: cannot scan %T as a time", v)
	}
	n.Valid = true
	return nil
}

// Ptr is the time, or nil when NULL.
func (n NullTime) Ptr() *time.Time {
	if !n.Valid {
		return nil
	}
	t := n.Time
	return &t
}

// RetryOnDeadlock runs a write that a deadlock or lock timeout may have rolled back again,
// up to three times. Only for work that is the same operation when it runs again.
func RetryOnDeadlock[T any](ctx context.Context, op func() (T, error)) (T, error) {
	for attempt := 1; ; attempt++ {
		v, err := op()
		if err == nil || !IsDeadlock(err) || attempt >= 3 {
			return v, err
		}
		// Jittered: the two that just collided must not line up again.
		select {
		case <-ctx.Done():
			return v, err
		case <-time.After(time.Duration(1000+rand.IntN(4000)) * time.Microsecond * time.Duration(attempt)):
		}
	}
}
