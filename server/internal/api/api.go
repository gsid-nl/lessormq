// Package api is LessorMQ's HTTP interface: /v1, /healthz and /metrics.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/app"
)

// maxBody is the largest request body taken: above the default max_message_bytes (1 MiB)
// with room for a queue that raises it; past a queue's own limit the answer is 413 too.
const maxBody = 16 << 20

// Server serves the API.
type Server struct {
	App  *app.App
	Log  *slog.Logger
	CORS *regexp.Regexp
}

type handler func(ctx context.Context, r *request) (int, any, error)

type request struct {
	http      *http.Request
	params    map[string]string
	principal *app.Principal
	body      []byte
}

func (r *request) param(name string) string { return r.params[name] }

func (r *request) input(allowEmpty bool) (*app.Input, error) {
	return app.ParseInput(r.body, allowEmpty)
}

func (r *request) query(name string) string { return r.http.URL.Query().Get(name) }

func (r *request) queryPtr(name string) *string {
	q := r.http.URL.Query()
	if !q.Has(name) {
		return nil
	}
	v := q.Get(name)
	return &v
}

// auth says what a route needs of the caller.
type auth int

const (
	public   auth = iota // no key
	anyKey               // a key; the handler checks the rest
	operator             // an operator key
	scoped               // a key acting in a tenant with one of the route's scopes
)

type route struct {
	method   string
	segments []string // literal, or {name}
	auth     auth
	scopes   []string
	handle   handler
}

var requirements = map[string]string{
	"queue":    app.QueueNamePattern,
	"tenantId": app.TenantIDPattern,
}

func (s *Server) routes() []route {
	r := func(method, path string, a auth, h handler, scopes ...string) route {
		return route{method: method, segments: strings.Split(strings.Trim(path, "/"), "/"), auth: a, scopes: scopes, handle: h}
	}
	consume := []string{app.ScopeConsume}
	return []route{
		r("GET", "/healthz", public, s.health),
		r("GET", "/metrics", public, s.metricsAll),
		r("GET", "/v1/metrics", anyKey, s.metrics),
		r("GET", "/v1/history", anyKey, s.history),

		r("GET", "/v1/tenant", scoped, s.ownTenant, app.ScopeAdmin),
		r("PATCH", "/v1/tenant", scoped, s.updateOwnTenant, app.ScopeAdmin),
		r("GET", "/v1/api-keys", scoped, s.listKeys, app.ScopeAdmin),
		r("POST", "/v1/api-keys", scoped, s.createKey, app.ScopeAdmin),
		r("DELETE", "/v1/api-keys/{keyId}", scoped, s.revokeKey, app.ScopeAdmin),

		r("GET", "/v1/tenants", operator, s.listTenants),
		r("POST", "/v1/tenants", operator, s.createTenant),
		r("GET", "/v1/tenants/{tenantId}", operator, s.getTenant),
		r("PATCH", "/v1/tenants/{tenantId}", operator, s.updateTenant),
		r("DELETE", "/v1/tenants/{tenantId}", operator, s.deleteTenant),
		r("GET", "/v1/tenants/{tenantId}/api-keys", operator, s.listTenantKeys),
		r("POST", "/v1/tenants/{tenantId}/api-keys", operator, s.createTenantKey),
		r("DELETE", "/v1/tenants/{tenantId}/api-keys/{keyId}", operator, s.revokeTenantKey),

		r("GET", "/v1/queues", scoped, s.listQueues, app.ScopeAdmin),
		r("GET", "/v1/queues/{queue}", scoped, s.getQueue, app.ScopeConsume, app.ScopeMetrics, app.ScopeAdmin),
		r("PATCH", "/v1/queues/{queue}", scoped, s.patchQueue, app.ScopeAdmin),
		r("GET", "/v1/queues/{queue}/workers", scoped, s.workers, app.ScopeConsume, app.ScopeMetrics, app.ScopeAdmin),
		r("GET", "/v1/queues/{queue}/history", scoped, s.queueHistory, app.ScopeMetrics, app.ScopeAdmin),
		r("GET", "/v1/queues/{queue}/messages", scoped, s.listMessages, app.ScopeAdmin),
		r("POST", "/v1/queues/{queue}/messages", scoped, s.publish, app.ScopePublish),
		r("POST", "/v1/queues/{queue}/claims", scoped, s.claim, consume...),
		r("POST", "/v1/queues/{queue}/replay", scoped, s.replayMany, app.ScopeAdmin),
		r("POST", "/v1/queues/{queue}/purge", scoped, s.purge, app.ScopeAdmin),

		r("POST", "/v1/acks", scoped, s.ackMany, consume...),

		r("GET", "/v1/messages/{id}", scoped, s.getMessage, app.ScopeAdmin),
		r("DELETE", "/v1/messages/{id}", scoped, s.deleteMessage, app.ScopeAdmin),
		r("POST", "/v1/messages/{id}/replay", scoped, s.replay, app.ScopeAdmin),
		r("POST", "/v1/messages/{id}/extend", scoped, s.extend, consume...),
		r("POST", "/v1/messages/{id}/ack", scoped, s.ack, consume...),
		r("POST", "/v1/messages/{id}/fail", scoped, s.fail, consume...),
		r("POST", "/v1/messages/{id}/release", scoped, s.release, consume...),
	}
}

// Handler is the API as an http.Handler.
func (s *Server) Handler() http.Handler {
	routes := s.routes()
	return http.HandlerFunc(func(w http.ResponseWriter, hr *http.Request) {
		start := time.Now()
		isAPI := strings.HasPrefix(hr.URL.Path, "/v1/")
		if isAPI && s.cors(w, hr) {
			return
		}
		segments := strings.Split(strings.Trim(hr.URL.Path, "/"), "/")
		var match *route
		var params map[string]string
		var allowed []string
		for i := range routes {
			rt := &routes[i]
			p, ok := matchPath(rt.segments, segments)
			if !ok {
				continue
			}
			if rt.method != hr.Method && !(rt.method == "GET" && hr.Method == "HEAD") {
				allowed = append(allowed, rt.method)
				continue
			}
			match, params = rt, p
			break
		}
		if match == nil {
			if len(allowed) > 0 {
				w.Header().Set("Allow", strings.Join(allowed, ", "))
				s.problem(w, &app.Problem{Status: 405, Code: "validation_failed",
					Detail: "Method Not Allowed (Allow: " + strings.Join(allowed, ", ") + ")."})
				return
			}
			s.problem(w, app.NotFound("No such endpoint."))
			return
		}

		req := &request{http: hr, params: params}
		status, out, err := s.serve(hr.Context(), match, req)
		if err != nil {
			s.error(w, hr, err)
		} else {
			writeResult(w, status, out)
		}
		if d := time.Since(start); d > 5*time.Second && match.segments[len(match.segments)-1] != "claims" {
			s.Log.Warn("Slow request", "method", hr.Method, "path", hr.URL.Path, "ms", d.Milliseconds())
		}
	})
}

func (s *Server) serve(ctx context.Context, rt *route, req *request) (int, any, error) {
	if rt.auth != public {
		p, err := s.App.Authenticate(ctx, req.http.Header.Get("Authorization"), req.http.Header.Get(app.TenantHeader))
		if err != nil {
			return 0, nil, err
		}
		req.principal = p
		switch rt.auth {
		case operator:
			if !p.Operator {
				return 0, nil, app.Forbidden("Only an operator key may manage tenants.")
			}
		case scoped:
			if err := p.RequireScopes(rt.scopes...); err != nil {
				return 0, nil, err
			}
		}
	}
	if req.http.Body != nil {
		body, err := io.ReadAll(io.LimitReader(req.http.Body, maxBody+1))
		if err != nil {
			return 0, nil, app.ValidationField("(body)", "could not be read")
		}
		if len(body) > maxBody {
			return 0, nil, &app.Problem{Status: 413, Code: "payload_too_large", Detail: "The request body is too large."}
		}
		req.body = body
	}
	return rt.handle(ctx, req)
}

func matchPath(pattern, path []string) (map[string]string, bool) {
	if len(pattern) != len(path) {
		return nil, false
	}
	var params map[string]string
	for i, seg := range pattern {
		if strings.HasPrefix(seg, "{") {
			name := seg[1 : len(seg)-1]
			if path[i] == "" {
				return nil, false
			}
			if req, ok := requirements[name]; ok && !app.Matches(req, path[i]) {
				return nil, false
			}
			if params == nil {
				params = map[string]string{}
			}
			params[name] = path[i]
			continue
		}
		if seg != path[i] {
			return nil, false
		}
	}
	return params, true
}

// text is a response body that is not JSON.
type text struct {
	contentType string
	body        string
}

func writeResult(w http.ResponseWriter, status int, out any) {
	if status == http.StatusNoContent || out == nil {
		w.WriteHeader(status)
		return
	}
	if t, ok := out.(text); ok {
		w.Header().Set("Content-Type", t.contentType)
		w.WriteHeader(status)
		io.WriteString(w, t.body)
		return
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(bytes.TrimRight(buf.Bytes(), "\n"))
}

func (s *Server) error(w http.ResponseWriter, r *http.Request, err error) {
	p := app.AsProblem(err)
	if p == nil {
		s.Log.Error("Unhandled error in API request", "method", r.Method, "path", r.URL.Path, "error", err.Error())
		p = app.Internal()
		var netErr net.Error
		if errors.As(err, &netErr) {
			p = app.Unavailable("The database or Redis is unreachable.")
		}
	}
	s.problem(w, p)
}

func (s *Server) problem(w http.ResponseWriter, p *app.Problem) {
	for k, v := range p.Headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	w.Write(p.Body())
}

// cors answers for the v1 API to a browser app on another origin (CORS_ALLOW_ORIGIN).
// Requests carry an API key in the Authorization header, never cookies, so credentials
// stay off. It reports true when the request was a preflight it answered.
func (s *Server) cors(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || s.CORS == nil || !s.CORS.MatchString(origin) {
		return false
	}
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Add("Vary", "Origin")
	if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
		h.Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept, LessorMQ-Tenant")
		h.Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	h.Set("Access-Control-Expose-Headers", "Retry-After, Deprecation, Link")
	return false
}

func clientAddress(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
