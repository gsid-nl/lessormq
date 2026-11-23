package app

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"time"
)

// Settings are a queue's settings as JSON-like values: map[string]any with json.Number
// for numbers, nested objects as map[string]any.
type Settings map[string]any

// DefaultSettings are the product defaults of the spec (docs/openapi.yaml, QueueSettings).
func DefaultSettings() Settings {
	return Settings{
		"lease_seconds":      json.Number("300"),
		"max_lease_seconds":  json.Number("3600"),
		"max_attempts":       json.Number("7"),
		"max_attempts_limit": json.Number("25"),
		"backoff": map[string]any{
			"initial_seconds": json.Number("10"),
			"multiplier":      json.Number("2"),
			"max_seconds":     json.Number("3600"),
			"jitter":          json.Number("0.1"),
		},
		"on_exhausted": "dead_letter",
		"on_permanent": "dead_letter",
		"route_policy": map[string]any{
			"mode":             "none",
			"cooldown_seconds": json.Number("3600"),
		},
		"retention": map[string]any{
			"completed_seconds": json.Number("86400"),
			"failed_seconds":    json.Number("604800"),
		},
		"max_message_bytes": json.Number("1048576"),
		"paused":            false,
	}
}

var settingEnums = map[string][]string{
	"on_exhausted":      {"dead_letter", "discard"},
	"on_permanent":      {"dead_letter", "retry"},
	"route_policy.mode": {"none", "avoid_failed"},
}

// field => minimum, for integer fields
var settingIntegers = map[string]int64{
	"lease_seconds":                 1,
	"max_lease_seconds":             1,
	"max_attempts":                  1,
	"max_attempts_limit":            1,
	"backoff.initial_seconds":       0,
	"backoff.max_seconds":           0,
	"route_policy.cooldown_seconds": 0,
	"retention.completed_seconds":   0,
	"retention.failed_seconds":      0,
	"max_message_bytes":             1,
}

// ResolveSettings applies patches in order over the defaults; nested objects are merged.
func ResolveSettings(patches ...map[string]any) Settings {
	s := map[string]any(DefaultSettings())
	for _, p := range patches {
		s = MergeSettings(s, p)
	}
	return s
}

// MergeSettings returns base with patch merged in, nested objects merged, not replaced.
func MergeSettings(base, patch map[string]any) map[string]any {
	out := deepCopy(base)
	for k, v := range patch {
		if pv, ok := AsObject(v); ok {
			if bv, ok := AsObject(out[k]); ok {
				out[k] = MergeSettings(bv, pv)
				continue
			}
		}
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopy(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return deepCopy(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = deepCopyValue(e)
		}
		return out
	}
	return v
}

// AsObject is v as a JSON object; an empty JSON array counts as an empty object, as the
// PHP server read it.
func AsObject(v any) (map[string]any, bool) {
	switch x := v.(type) {
	case map[string]any:
		return x, true
	case Settings:
		return x, true
	case []any:
		if len(x) == 0 {
			return map[string]any{}, true
		}
	}
	return nil, false
}

// ValidateSettingsPatch checks a patch on its own (shape, types, ranges), then the settings
// it produces over base (rules between fields).
func ValidateSettingsPatch(patch any, base map[string]any, prefix string) []FieldError {
	obj, ok := AsObject(patch)
	if !ok {
		field := strings.TrimSuffix(prefix, ".")
		if field == "" {
			field = "(body)"
		}
		return []FieldError{{Field: field, Message: "must be an object"}}
	}
	if base == nil {
		base = DefaultSettings()
	}
	if errs := validateShape(obj, DefaultSettings(), prefix, ""); len(errs) > 0 {
		return errs
	}
	merged := MergeSettings(base, obj)
	var errs []FieldError
	if num(merged["lease_seconds"]) > num(merged["max_lease_seconds"]) {
		errs = append(errs, FieldError{Field: prefix + "lease_seconds", Message: "must not exceed max_lease_seconds"})
	}
	if num(merged["max_attempts"]) > num(merged["max_attempts_limit"]) {
		errs = append(errs, FieldError{Field: prefix + "max_attempts", Message: "must not exceed max_attempts_limit"})
	}
	return errs
}

func validateShape(patch, template map[string]any, prefix, path string) []FieldError {
	var errs []FieldError
	keys := make([]string, 0, len(patch))
	for k := range patch {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := patch[key]
		name := key
		if path != "" {
			name = path + "." + key
		}
		field := prefix + name
		expected, known := template[key]
		if !known {
			errs = append(errs, FieldError{Field: field, Message: "unknown setting"})
			continue
		}
		if sub, isObject := expected.(map[string]any); isObject {
			obj, ok := AsObject(value)
			if !ok {
				errs = append(errs, FieldError{Field: field, Message: "must be an object"})
				continue
			}
			errs = append(errs, validateShape(obj, sub, prefix, name)...)
			continue
		}
		if msg := checkSetting(name, value); msg != "" {
			errs = append(errs, FieldError{Field: field, Message: msg})
		}
	}
	return errs
}

func checkSetting(name string, value any) string {
	if min, ok := settingIntegers[name]; ok {
		n, isInt := AsInt(value)
		if !isInt {
			return "must be an integer"
		}
		if n < min {
			return fmt.Sprintf("must be at least %d", min)
		}
		return ""
	}
	if allowed, ok := settingEnums[name]; ok {
		if s, isString := value.(string); isString {
			for _, a := range allowed {
				if s == a {
					return ""
				}
			}
		}
		return "must be one of: " + strings.Join(allowed, ", ")
	}
	switch name {
	case "paused":
		if _, ok := value.(bool); ok {
			return ""
		}
		return "must be a boolean"
	case "backoff.multiplier":
		if f, ok := AsNumber(value); ok && f >= 1 {
			return ""
		}
		return "must be a number of at least 1"
	case "backoff.jitter":
		if f, ok := AsNumber(value); ok && f >= 0 && f <= 1 {
			return ""
		}
		return "must be a number from 0 to 1"
	}
	return "unknown setting"
}

func num(v any) float64 {
	f, _ := AsNumber(v)
	return f
}

// Int reads an integer setting, a.b for a nested one.
func (s Settings) Int(path string) int64 {
	return int64(s.Float(path))
}

// Float reads a number setting, a.b for a nested one.
func (s Settings) Float(path string) float64 {
	return num(s.get(path))
}

// Str reads a string setting.
func (s Settings) Str(path string) string {
	v, _ := s.get(path).(string)
	return v
}

func (s Settings) get(path string) any {
	var cur any = map[string]any(s)
	for _, part := range strings.Split(path, ".") {
		m, ok := AsObject(cur)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	return cur
}

func (s Settings) set(path string, v any) {
	parts := strings.Split(path, ".")
	m := map[string]any(s)
	for _, p := range parts[:len(parts)-1] {
		sub, ok := AsObject(m[p])
		if !ok {
			sub = map[string]any{}
		}
		m[p] = sub
		m = sub
	}
	m[parts[len(parts)-1]] = v
}

// QueueConfig is a queue's effective settings at this moment: product defaults, then the
// tenant's defaults, then the queue's own settings, held to the tenant's quotas. Pausing
// is a state with a time, not a plain setting, so it lives beside them.
type QueueConfig struct {
	Settings Settings
	PausedAt *time.Time
	Exists   bool
	Quotas   Quotas
}

func (c *QueueConfig) Paused() bool { return c.PausedAt != nil }

// Quota is one of the tenant's quotas, nil when unlimited.
func (c *QueueConfig) Quota(name string) *int64 { return c.Quotas[name] }

// View is the settings as the API shows them.
func (c *QueueConfig) View() map[string]any {
	v := deepCopy(c.Settings)
	v["paused"] = c.Paused()
	return v
}

// MaxAttemptsFor is the attempts a message gets: what the publisher asked, or the
// queue's, never above max_attempts_limit.
func (c *QueueConfig) MaxAttemptsFor(asked *int64) int64 {
	n := c.Settings.Int("max_attempts")
	if asked != nil {
		n = *asked
	}
	return min(n, c.Settings.Int("max_attempts_limit"))
}

// Outcomes of a failed attempt.
const (
	Retrying     = "retrying"
	DeadLettered = "dead_lettered"
	Discarded    = "discarded"
)

// RetryLimit is how many retries the message gets after its first attempt.
func (c *QueueConfig) RetryLimit(asked *int64) int64 {
	return c.MaxAttemptsFor(asked) - 1
}

// Decide is what the server does after a failed attempt, from the queue's settings.
func (c *QueueConfig) Decide(retriesSoFar, retryLimit int64, permanent bool) (string, int64) {
	giveUp := (permanent && c.Settings.Str("on_permanent") == "dead_letter") || retriesSoFar >= retryLimit
	if giveUp {
		if c.Settings.Str("on_exhausted") == "discard" {
			return Discarded, 0
		}
		return DeadLettered, 0
	}
	return Retrying, c.Delay(retriesSoFar)
}

// Delay is min(initial * multiplier^retries, max), randomised by +/- jitter so that a
// batch that failed together does not come back together.
func (c *QueueConfig) Delay(retriesSoFar int64) int64 {
	initial := c.Settings.Float("backoff.initial_seconds")
	multiplier := c.Settings.Float("backoff.multiplier")
	maxSeconds := c.Settings.Float("backoff.max_seconds")
	jitter := c.Settings.Float("backoff.jitter")
	delay := math.Min(initial*math.Pow(multiplier, float64(retriesSoFar)), maxSeconds)
	if jitter > 0 {
		delay += (float64(rand.IntN(2_000_001))/1_000_000 - 1) * delay * jitter
	}
	return max(0, int64(math.Round(delay)))
}

// Avoidance is the cooldown for claims on route, or nil when nothing is to be avoided.
func (c *QueueConfig) Avoidance(route string) *int64 {
	if route == "" || c.Settings.Str("route_policy.mode") != "avoid_failed" {
		return nil
	}
	n := c.Settings.Int("route_policy.cooldown_seconds")
	return &n
}

// RoutePattern: routes are keys in a message's failed_routes JSON object, so the
// characters are limited to those that need no escaping in a JSON path.
const RoutePattern = `[A-Za-z0-9][A-Za-z0-9._:@+\/-]{0,254}`

// QueueNamePattern is what a queue may be called.
const QueueNamePattern = `[A-Za-z0-9][A-Za-z0-9._-]{0,199}`

// TenantIDPattern is what a tenant id may be.
const TenantIDPattern = `[a-z0-9][a-z0-9-]{1,62}`
