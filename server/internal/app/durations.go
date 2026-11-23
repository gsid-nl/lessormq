package app

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// DurationBounds are the upper bounds (exclusive) of the classes of a Durations, in
// seconds; the last class has none.
var DurationBounds = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600, 86400,
}

// Durations is how long something took for a set of messages (waiting for a worker, or
// running), kept as a histogram over fixed classes, so the database can group them and
// hand over a few rows instead of a row per message. The average and the maximum are
// exact; the median and the 95th percentile are interpolated within their class.
type Durations struct {
	counts map[int]int64
	count  int64
	sum    float64
	max    *float64
}

// DurationClassSQL is the class of the seconds expression, as SQL.
func DurationClassSQL(seconds string) string {
	var b strings.Builder
	b.WriteString("CASE")
	for class, bound := range DurationBounds {
		fmt.Fprintf(&b, " WHEN %s < %s THEN %d", seconds, strconv.FormatFloat(bound, 'f', -1, 64), class)
	}
	fmt.Fprintf(&b, " ELSE %d END", len(DurationBounds))
	return b.String()
}

// Add takes in count durations of one class, with their sum and their maximum.
func (d *Durations) Add(class int, count int64, sum, maximum float64) {
	if count <= 0 {
		return
	}
	class = max(0, min(len(DurationBounds), class))
	if d.counts == nil {
		d.counts = map[int]int64{}
	}
	d.counts[class] += count
	d.count += count
	d.sum += sum
	if d.max == nil || maximum > *d.max {
		m := maximum
		d.max = &m
	}
}

// Merge adds the durations of another.
func (d *Durations) Merge(o *Durations) {
	for class, c := range o.counts {
		if d.counts == nil {
			d.counts = map[int]int64{}
		}
		d.counts[class] += c
	}
	d.count += o.count
	d.sum += o.sum
	if o.max != nil && (d.max == nil || *o.max > *d.max) {
		m := *o.max
		d.max = &m
	}
}

// Percentile is the seconds at which a share q of the durations is done.
func (d *Durations) Percentile(q float64) float64 {
	target := q * float64(d.count)
	var before int64
	classes := make([]int, 0, len(d.counts))
	for c := range d.counts {
		classes = append(classes, c)
	}
	sort.Ints(classes)
	maximum := *d.max
	for _, class := range classes {
		count := d.counts[class]
		if float64(before+count) >= target {
			var lower float64
			if class > 0 {
				lower = DurationBounds[class-1]
			}
			upper := math.Max(lower, maximum)
			if class < len(DurationBounds) {
				upper = DurationBounds[class]
			}
			value := lower + (upper-lower)*((target-float64(before))/float64(count))
			return round(math.Max(0, math.Min(value, maximum)), 3)
		}
		before += count
	}
	return maximum
}

// View is {avg, p50, p95, max} in seconds to the millisecond, or nil without durations.
func (d *Durations) View() map[string]any {
	if d.count == 0 || d.max == nil {
		return nil
	}
	return map[string]any{
		"avg": round(math.Max(0, d.sum/float64(d.count)), 3),
		"p50": d.Percentile(0.5),
		"p95": d.Percentile(0.95),
		"max": round(math.Max(0, *d.max), 3),
	}
}

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}
