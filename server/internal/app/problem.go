package app

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Problem is an error for an API client, rendered as RFC 9457 problem details with a
// stable code (docs/openapi.yaml, Problem).
type Problem struct {
	Status  int
	Code    string
	Detail  string
	Errors  []FieldError
	Headers map[string]string
}

// FieldError is one invalid field of a request.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (p *Problem) Error() string { return p.Code + ": " + p.Detail }

var problemTitles = map[string]string{
	"validation_failed": "The request is not valid",
	"unauthorized":      "Not authenticated",
	"forbidden":         "Not allowed",
	"tenant_suspended":  "Tenant suspended",
	"not_found":         "Not found",
	"lease_lost":        "Lease lost",
	"invalid_state":     "Invalid state",
	"payload_too_large": "Payload too large",
	"rate_limited":      "Rate limited",
	"quota_exceeded":    "Quota exceeded",
	"unavailable":       "Service unavailable",
	"internal":          "Internal error",
}

// Body is the problem as JSON.
func (p *Problem) Body() []byte {
	title, ok := problemTitles[p.Code]
	if !ok {
		title = "Error"
	}
	body := map[string]any{
		"type":   "urn:lessormq:problem:" + p.Code,
		"title":  title,
		"status": p.Status,
		"detail": p.Detail,
		"code":   p.Code,
	}
	if len(p.Errors) > 0 {
		body["errors"] = p.Errors
	}
	out, _ := json.Marshal(body)
	return out
}

func Validation(errors []FieldError) *Problem {
	return &Problem{Status: 400, Code: "validation_failed", Detail: "One or more fields are not valid.", Errors: errors}
}

func ValidationField(field, message string) *Problem {
	return Validation([]FieldError{{Field: field, Message: message}})
}

func Unauthorized(detail string) *Problem {
	return &Problem{Status: 401, Code: "unauthorized", Detail: detail, Headers: map[string]string{"WWW-Authenticate": "Bearer"}}
}

func Forbidden(detail string) *Problem {
	return &Problem{Status: 403, Code: "forbidden", Detail: detail}
}

func TenantSuspended() *Problem {
	return &Problem{Status: 403, Code: "tenant_suspended", Detail: "The tenant of this key is suspended."}
}

func NotFound(detail string) *Problem {
	return &Problem{Status: 404, Code: "not_found", Detail: detail}
}

func InvalidState(detail string) *Problem {
	return &Problem{Status: 409, Code: "invalid_state", Detail: detail}
}

func LeaseLost() *Problem {
	return &Problem{Status: 409, Code: "lease_lost", Detail: "This receipt is not the current lease of the message: " +
		"it expired, was released, or the message was claimed again. Stop reporting on it."}
}

func PayloadTooLarge(limit int64) *Problem {
	return &Problem{Status: 413, Code: "payload_too_large",
		Detail: fmt.Sprintf("Body and headers exceed the queue limit of %d bytes.", limit)}
}

// RateLimited is a limit that lifts by itself: a rate, or open messages being worked off.
func RateLimited(retryAfter int, detail string) *Problem {
	if retryAfter < 1 {
		retryAfter = 1
	}
	return &Problem{Status: 429, Code: "rate_limited", Detail: detail,
		Headers: map[string]string{"Retry-After": strconv.Itoa(retryAfter)}}
}

// QuotaExceeded is a limit that waiting does not lift.
func QuotaExceeded(detail string) *Problem {
	return &Problem{Status: 403, Code: "quota_exceeded", Detail: detail}
}

func Unavailable(detail string) *Problem {
	return &Problem{Status: 503, Code: "unavailable", Detail: detail}
}

func Internal() *Problem {
	return &Problem{Status: 500, Code: "internal", Detail: "An unexpected error occurred."}
}
