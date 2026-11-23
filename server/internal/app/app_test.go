package app

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestMessageIDBytesSortByTime(t *testing.T) {
	a := MessageIDAt(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	b := MessageIDAt(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	c := NewMessageID()
	if bytes.Compare(a.Bytes(), b.Bytes()) >= 0 || bytes.Compare(b.Bytes(), c.Bytes()) >= 0 {
		t.Fatal("ordered bytes do not sort by time")
	}
	back, err := MessageIDFromBytes(c.Bytes())
	if err != nil || back != c {
		t.Fatalf("round trip: %v", err)
	}
	if _, ok := ParseMessageID(c.String()); !ok || !Matches(UUIDv1Pattern, c.String()) {
		t.Fatalf("not a v1 uuid: %s", c)
	}
	// ramsey's layout: time_hi, time_mid, time_low, then the rest.
	raw := c.Bytes()
	if !bytes.Equal(raw[0:2], c[6:8]) || !bytes.Equal(raw[2:4], c[4:6]) || !bytes.Equal(raw[4:8], c[0:4]) {
		t.Fatal("not the ordered-time layout")
	}
}

func TestGlob(t *testing.T) {
	cases := map[[2]string]bool{
		{"import-*", "import-a"}: true, {"import-*", "export-a"}: false, {"a?c", "abc"}: true,
		{"a?c", "ac"}: false, {"*", "x"}: true, {"*.mail", "x.mail"}: true, {"mail", "mail"}: true,
	}
	for c, want := range cases {
		if got := globMatch(c[0], c[1]); got != want {
			t.Errorf("%s ~ %s: got %v", c[0], c[1], got)
		}
	}
}

func TestSettingsPatch(t *testing.T) {
	var patch map[string]any
	dec := json.NewDecoder(bytes.NewReader([]byte(`{"lease_seconds": 5000, "backoff": {"jitter": 2}, "nope": 1}`)))
	dec.UseNumber()
	dec.Decode(&patch)
	errs := ValidateSettingsPatch(patch, nil, "")
	if len(errs) != 2 {
		t.Fatalf("shape errors: %v", errs)
	}
	delete(patch, "nope")
	delete(patch, "backoff")
	errs = ValidateSettingsPatch(patch, nil, "")
	if len(errs) != 1 || errs[0].Field != "lease_seconds" {
		t.Fatalf("relation errors: %v", errs)
	}
	s := ResolveSettings(map[string]any{"backoff": map[string]any{"initial_seconds": json.Number("1")}})
	if s.Int("backoff.initial_seconds") != 1 || s.Float("backoff.multiplier") != 2 {
		t.Fatalf("merge: %v", s)
	}
}

func TestSearchNeedles(t *testing.T) {
	n := searchNeedles("Café/1")
	want := []string{"café/1", `caf\u00e9\/1`}
	if len(n) != 2 || n[0] != want[0] || n[1] != want[1] {
		t.Fatalf("needles %q", n)
	}
}

func TestDurations(t *testing.T) {
	var d Durations
	d.Add(0, 9, 0, 0)
	d.Add(10, 1, 7, 7)
	v := d.View()
	if v["avg"] != 0.7 || v["max"] != 7.0 {
		t.Fatalf("view %v", v)
	}
}

// Fast work is told apart to the millisecond: 90 runs of 3 ms and 10 of 40 ms.
func TestDurationsBelowASecond(t *testing.T) {
	var d Durations
	d.Add(0, 90, 0.27, 0.003)
	d.Add(4, 10, 0.4, 0.04)
	v := d.View()
	if v["avg"] != 0.007 || v["max"] != 0.04 {
		t.Fatalf("view %v", v)
	}
	if p50 := d.Percentile(0.5); p50 <= 0 || p50 > 0.005 {
		t.Fatalf("p50 %v", p50)
	}
	if p95 := d.Percentile(0.95); p95 < 0.025 || p95 > 0.04 {
		t.Fatalf("p95 %v", p95)
	}
}
