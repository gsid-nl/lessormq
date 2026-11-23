package app

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Quotas are limits per tenant, set by the operator; a nil value means unlimited.
//
//   - max_queues: a new queue beyond it is refused (403 quota_exceeded).
//   - max_open_messages: pending plus processing; a publish beyond it waits (429).
//   - publish_per_second, claim_per_second: rates (429 with Retry-After).
//   - max_message_bytes, max_retention_seconds: caps on every queue's own setting.
type Quotas map[string]*int64

// QuotaFields are the quotas with their minimum, in the order the API shows them.
var QuotaFields = []struct {
	Name string
	Min  int64
}{
	{"max_queues", 1},
	{"max_open_messages", 1},
	{"publish_per_second", 1},
	{"claim_per_second", 1},
	{"max_message_bytes", 1},
	{"max_retention_seconds", 0},
}

// ResolveQuotas reads stored quotas (only the ones that are set) into every field.
func ResolveQuotas(stored map[string]any) Quotas {
	q := Quotas{}
	for _, f := range QuotaFields {
		if n, ok := AsInt(stored[f.Name]); ok {
			v := n
			q[f.Name] = &v
		} else {
			q[f.Name] = nil
		}
	}
	return q
}

// Stored is the quotas that are set, as they are kept.
func (q Quotas) Stored() map[string]any {
	out := map[string]any{}
	for k, v := range q {
		if v != nil {
			out[k] = json.Number(fmt.Sprint(*v))
		}
	}
	return out
}

// View is every quota, null where unlimited.
func (q Quotas) View() map[string]any {
	out := map[string]any{}
	for _, f := range QuotaFields {
		if v := q[f.Name]; v != nil {
			out[f.Name] = *v
		} else {
			out[f.Name] = nil
		}
	}
	return out
}

// Patch applies a validated patch: null removes a limit.
func (q Quotas) Patch(patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			q[k] = nil
			continue
		}
		if n, ok := AsInt(v); ok {
			q[k] = &n
		}
	}
}

// CapSettings holds max_message_bytes and the retention to the tenant's caps.
func CapSettings(s Settings, q Quotas) Settings {
	if b := q["max_message_bytes"]; b != nil {
		s.set("max_message_bytes", json.Number(fmt.Sprint(min(s.Int("max_message_bytes"), *b))))
	}
	if r := q["max_retention_seconds"]; r != nil {
		for _, f := range []string{"retention.completed_seconds", "retention.failed_seconds"} {
			s.set(f, json.Number(fmt.Sprint(min(s.Int(f), *r))))
		}
	}
	return s
}

// QuotaSettingErrors are the fields of a settings patch that go beyond the quotas.
func QuotaSettingErrors(patch map[string]any, q Quotas, prefix string) []FieldError {
	var errs []FieldError
	if b := q["max_message_bytes"]; b != nil {
		if n, ok := AsInt(patch["max_message_bytes"]); ok && n > *b {
			errs = append(errs, FieldError{Field: prefix + "max_message_bytes",
				Message: fmt.Sprintf("must be at most the tenant's quota of %d", *b)})
		}
	}
	if r := q["max_retention_seconds"]; r != nil {
		if ret, ok := AsObject(patch["retention"]); ok {
			for _, f := range []string{"completed_seconds", "failed_seconds"} {
				if n, ok := AsInt(ret[f]); ok && n > *r {
					errs = append(errs, FieldError{Field: prefix + "retention." + f,
						Message: fmt.Sprintf("must be at most the tenant's quota of %d", *r)})
				}
			}
		}
	}
	return errs
}

// ValidateQuotas checks a quotas patch.
func ValidateQuotas(v any, prefix string) []FieldError {
	obj, ok := AsObject(v)
	if !ok {
		return []FieldError{{Field: strings.TrimSuffix(prefix, "."), Message: "must be an object"}}
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var errs []FieldError
	for _, k := range keys {
		var minimum int64 = -1
		for _, f := range QuotaFields {
			if f.Name == k {
				minimum = f.Min
			}
		}
		if minimum < 0 {
			errs = append(errs, FieldError{Field: prefix + k, Message: "unknown quota"})
			continue
		}
		if obj[k] == nil {
			continue
		}
		if n, ok := AsInt(obj[k]); !ok || n < minimum {
			errs = append(errs, FieldError{Field: prefix + k,
				Message: fmt.Sprintf("must be null or an integer of at least %d", minimum)})
		}
	}
	return errs
}
