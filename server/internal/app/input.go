package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"
)

// Input reads a JSON request body field by field and collects every problem, so a client
// gets all invalid fields in one 400. Accessors return their zero value (and false) for
// an absent optional field and for an invalid one; call Validate before using any value.
type Input struct {
	raw    map[string]json.RawMessage
	keys   []string
	errors []FieldError
}

// ParseInput reads a request body that must be a JSON object; an empty body is one only
// when allowEmpty.
func ParseInput(body []byte, allowEmpty bool) (*Input, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 && allowEmpty {
		return &Input{raw: map[string]json.RawMessage{}}, nil
	}
	if !json.Valid(trimmed) {
		return nil, ValidationField("(body)", "must be valid JSON")
	}
	if trimmed[0] != '{' {
		// An empty array is an empty object, as PHP clients write one.
		if arr, ok := decodeAny(trimmed).([]any); ok && len(arr) == 0 {
			return &Input{raw: map[string]json.RawMessage{}}, nil
		}
		return nil, ValidationField("(body)", "must be a JSON object")
	}
	return InputFromRaw(trimmed)
}

// InputFromRaw reads a JSON object.
func InputFromRaw(obj json.RawMessage) (*Input, error) {
	in := &Input{raw: map[string]json.RawMessage{}}
	dec := json.NewDecoder(bytes.NewReader(obj))
	if _, err := dec.Token(); err != nil {
		return nil, ValidationField("(body)", "must be a JSON object")
	}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, ValidationField("(body)", "must be valid JSON")
		}
		key := t.(string)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, ValidationField("(body)", "must be valid JSON")
		}
		if _, seen := in.raw[key]; !seen {
			in.keys = append(in.keys, key)
		}
		in.raw[key] = v
	}
	return in, nil
}

func (in *Input) Has(field string) bool { _, ok := in.raw[field]; return ok }

// Raw is the field as it came, or nil when absent.
func (in *Input) Raw(field string) json.RawMessage { return in.raw[field] }

// IsNull tells whether the field is absent or JSON null.
func (in *Input) IsNull(field string) bool {
	r, ok := in.raw[field]
	return !ok || string(r) == "null"
}

// Value is the field decoded (json.Number for numbers), nil when absent.
func (in *Input) Value(field string) any {
	r, ok := in.raw[field]
	if !ok {
		return nil
	}
	return decodeAny(r)
}

// All is the whole object decoded.
func (in *Input) All() map[string]any {
	out := make(map[string]any, len(in.raw))
	for k, r := range in.raw {
		out[k] = decodeAny(r)
	}
	return out
}

func (in *Input) AllowOnly(fields ...string) {
	for _, k := range in.keys {
		ok := false
		for _, f := range fields {
			if k == f {
				ok = true
				break
			}
		}
		if !ok {
			in.AddError(k, "unknown field")
		}
	}
}

// String reads a non-empty string of at most max characters, matching pattern if given.
func (in *Input) String(field string, required bool, max int, pattern string) (string, bool) {
	if !in.Has(field) {
		if required {
			in.AddError(field, "is required")
		}
		return "", false
	}
	s, ok := in.Value(field).(string)
	if !ok || s == "" {
		in.AddError(field, "must be a non-empty string")
		return "", false
	}
	if utf8.RuneCountInString(s) > max {
		in.AddError(field, fmt.Sprintf("must be at most %d characters", max))
		return "", false
	}
	if pattern != "" && !Matches(pattern, s) {
		in.AddError(field, "must match "+pattern)
		return "", false
	}
	return s, true
}

func (in *Input) Bool(field string) (bool, bool) {
	if !in.Has(field) {
		return false, false
	}
	b, ok := in.Value(field).(bool)
	if !ok {
		in.AddError(field, "must be a boolean")
		return false, false
	}
	return b, true
}

func (in *Input) Choice(field string, allowed []string, required bool) (string, bool) {
	if !in.Has(field) {
		if required {
			in.AddError(field, "is required")
		}
		return "", false
	}
	s, ok := in.Value(field).(string)
	if ok {
		for _, a := range allowed {
			if s == a {
				return s, true
			}
		}
	}
	in.AddError(field, "must be one of: "+strings.Join(allowed, ", "))
	return "", false
}

// Int reads an integer of at least min (and at most max when max is not nil).
func (in *Input) Int(field string, min int64, max *int64) (int64, bool) {
	n, ok := AsInt(in.Value(field))
	if !ok || n < min || (max != nil && n > *max) {
		if max == nil {
			in.AddError(field, fmt.Sprintf("must be an integer of at least %d", min))
		} else {
			in.AddError(field, fmt.Sprintf("must be an integer from %d to %d", min, *max))
		}
		return 0, false
	}
	return n, true
}

func (in *Input) StringList(field string, required bool, minItems int, maxLength int, itemPattern string) ([]string, bool) {
	if !in.Has(field) {
		if required {
			in.AddError(field, "is required")
		}
		return nil, false
	}
	list, ok := in.Value(field).([]any)
	if !ok {
		in.AddError(field, "must be an array of strings")
		return nil, false
	}
	if len(list) < minItems {
		in.AddError(field, fmt.Sprintf("must have at least %d item(s)", minItems))
		return nil, false
	}
	out := []string{}
	seen := map[string]bool{}
	for i, item := range list {
		s, ok := item.(string)
		if !ok || s == "" || utf8.RuneCountInString(s) > maxLength || (itemPattern != "" && !Matches(itemPattern, s)) {
			in.AddError(fmt.Sprintf("%s[%d]", field, i), "is not valid")
			return nil, false
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out, true
}

func (in *Input) AddErrors(errs []FieldError) { in.errors = append(in.errors, errs...) }

func (in *Input) AddError(field, message string) {
	in.errors = append(in.errors, FieldError{Field: field, Message: message})
}

func (in *Input) Errors() []FieldError { return in.errors }

func (in *Input) Validate() error {
	if len(in.errors) > 0 {
		return Validation(in.errors)
	}
	return nil
}

var patterns sync.Map

// Matches tells whether the whole of s matches the pattern.
func Matches(pattern, s string) bool {
	re, ok := patterns.Load(pattern)
	if !ok {
		re = regexp.MustCompile(`^(?:` + pattern + `)$`)
		patterns.Store(pattern, re)
	}
	return re.(*regexp.Regexp).MatchString(s)
}

// decodeAny decodes JSON with numbers as json.Number, so an integer is told from a float.
func decodeAny(r json.RawMessage) any {
	dec := json.NewDecoder(bytes.NewReader(r))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil
	}
	return v
}

// DecodeAny is decodeAny for other packages.
func DecodeAny(r []byte) any { return decodeAny(r) }

// AsInt is a JSON integer (no fraction, no exponent) as an int64.
func AsInt(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		s := string(n)
		if strings.ContainsAny(s, ".eE") {
			return 0, false
		}
		i, err := n.Int64()
		return i, err == nil
	case int:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}

// AsNumber is any JSON number as a float64.
func AsNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// IsObject tells whether v is a JSON object.
func IsObject(v any) bool { _, ok := v.(map[string]any); return ok }
