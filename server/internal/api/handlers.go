package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/app"
)

const metricsContentType = "text/plain; version=0.0.4; charset=utf-8"

func (s *Server) health(ctx context.Context, _ *request) (int, any, error) {
	if err := s.App.Health(ctx); err != nil {
		s.Log.Warn("Health check failed", "error", err.Error())
		return 0, nil, app.Unavailable("The database or Redis is unreachable.")
	}
	return 200, map[string]any{"status": "ok"}, nil
}

func (s *Server) metricsAll(ctx context.Context, _ *request) (int, any, error) {
	tenants, err := s.App.TenantIDs(ctx)
	if err != nil {
		return 0, nil, err
	}
	body, err := s.App.Metrics(ctx, tenants, nil)
	if err != nil {
		return 0, nil, err
	}
	return 200, text{metricsContentType, body}, nil
}

func (s *Server) metrics(ctx context.Context, r *request) (int, any, error) {
	p := r.principal
	var tenants []string
	var mayUse func(string) bool
	switch {
	case p.Operator && p.TenantID == "":
		var err error
		if tenants, err = s.App.TenantIDs(ctx); err != nil {
			return 0, nil, err
		}
	case p.Has(app.ScopeMetrics):
		tenants = []string{p.TenantID}
		mayUse = p.MayUseQueue
	default:
		return 0, nil, app.Forbidden("This key needs the metrics scope.")
	}
	body, err := s.App.Metrics(ctx, tenants, mayUse)
	if err != nil {
		return 0, nil, err
	}
	return 200, text{metricsContentType, body}, nil
}

func period(r *request) (string, error) {
	p := r.query("period")
	if !r.http.URL.Query().Has("period") {
		p = app.DefaultPeriod
	}
	return p, app.CheckPeriod(p)
}

func (s *Server) history(ctx context.Context, r *request) (int, any, error) {
	p := r.principal
	queues := map[string][]string{}
	if p.Operator && p.TenantID == "" {
		per, err := period(r)
		if err != nil {
			return 0, nil, err
		}
		tenants, err := s.App.TenantIDs(ctx)
		if err != nil {
			return 0, nil, err
		}
		if only := r.queryPtr("tenants"); only != nil {
			tenants = onlyTenants(tenants, *only)
		}
		for _, t := range tenants {
			if queues[t], err = s.App.QueueNames(ctx, t); err != nil {
				return 0, nil, err
			}
		}
		return s.historyResult(ctx, nil, queues, per)
	}
	if !p.Has(app.ScopeMetrics) {
		return 0, nil, app.Forbidden("This key needs the metrics or admin scope.")
	}
	per, err := period(r)
	if err != nil {
		return 0, nil, err
	}
	tenant, err := p.Tenant()
	if err != nil {
		return 0, nil, err
	}
	all, err := s.App.QueueNames(ctx, tenant)
	if err != nil {
		return 0, nil, err
	}
	names := []string{}
	for _, n := range all {
		if p.MayUseQueue(n) {
			names = append(names, n)
		}
	}
	queues[tenant] = names
	return s.historyResult(ctx, nil, queues, per)
}

// onlyTenants keeps the tenants named in a comma-separated list, in their own order. An
// unknown name is left out, as is a tenant being deleted; an empty list keeps none.
func onlyTenants(tenants []string, list string) []string {
	named := map[string]bool{}
	for _, t := range strings.Split(list, ",") {
		if t = strings.TrimSpace(t); t != "" {
			named[t] = true
		}
	}
	kept := []string{}
	for _, t := range tenants {
		if named[t] {
			kept = append(kept, t)
		}
	}
	return kept
}

func (s *Server) historyResult(ctx context.Context, queue any, queues map[string][]string, per string) (int, any, error) {
	h, err := s.App.History(ctx, queues, per, nowUTC())
	if err != nil {
		return 0, nil, err
	}
	out := map[string]any{"queue": queue}
	for k, v := range h {
		out[k] = v
	}
	return 200, out, nil
}

func (s *Server) queueHistory(ctx context.Context, r *request) (int, any, error) {
	per, err := period(r)
	if err != nil {
		return 0, nil, err
	}
	queue := r.param("queue")
	tenant, err := r.principal.Authorise(queue)
	if err != nil {
		return 0, nil, err
	}
	c, err := s.App.ForQueue(ctx, tenant, queue)
	if err != nil {
		return 0, nil, err
	}
	if !c.Exists {
		return 0, nil, app.NotFound("No queue \"" + queue + "\".")
	}
	return s.historyResult(ctx, queue, map[string][]string{tenant: {queue}}, per)
}

func (s *Server) ownTenant(ctx context.Context, r *request) (int, any, error) {
	t, err := s.App.GetTenant(ctx, r.principal.TenantID)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.TenantView(ctx, t)
	return 200, v, err
}

func (s *Server) updateOwnTenant(ctx context.Context, r *request) (int, any, error) {
	t, err := s.App.GetTenant(ctx, r.principal.TenantID)
	if err != nil {
		return 0, nil, err
	}
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	if err := s.App.UpdateOwnTenant(ctx, t, in); err != nil {
		return 0, nil, err
	}
	v, err := s.App.TenantView(ctx, t)
	return 200, v, err
}

func (s *Server) listKeys(ctx context.Context, r *request) (int, any, error) {
	v, err := s.App.ListKeys(ctx, r.principal.TenantID)
	return 200, v, err
}

func (s *Server) createKey(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.CreateKey(ctx, r.principal.TenantID, in)
	return http.StatusCreated, v, err
}

func (s *Server) revokeKey(ctx context.Context, r *request) (int, any, error) {
	return http.StatusNoContent, nil, s.App.RevokeKey(ctx, r.principal.TenantID, r.param("keyId"), r.principal.KeyID)
}

func (s *Server) listTenants(ctx context.Context, r *request) (int, any, error) {
	v, err := s.App.ListTenants(ctx, r.query("status"), r.queryPtr("limit"), r.query("cursor"))
	return 200, v, err
}

func (s *Server) createTenant(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.CreateTenant(ctx, in)
	return http.StatusCreated, v, err
}

func (s *Server) tenant(ctx context.Context, r *request) (*app.Tenant, error) {
	return s.App.GetTenant(ctx, r.param("tenantId"))
}

func (s *Server) getTenant(ctx context.Context, r *request) (int, any, error) {
	t, err := s.tenant(ctx, r)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.TenantView(ctx, t)
	return 200, v, err
}

func (s *Server) updateTenant(ctx context.Context, r *request) (int, any, error) {
	t, err := s.tenant(ctx, r)
	if err != nil {
		return 0, nil, err
	}
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	if err := s.App.UpdateTenant(ctx, t, in); err != nil {
		return 0, nil, err
	}
	v, err := s.App.TenantView(ctx, t)
	return 200, v, err
}

func (s *Server) deleteTenant(ctx context.Context, r *request) (int, any, error) {
	t, err := s.tenant(ctx, r)
	if err != nil {
		return 0, nil, err
	}
	if err := s.App.RequestTenantDeletion(ctx, t); err != nil {
		return 0, nil, err
	}
	v, err := s.App.TenantView(ctx, t)
	return http.StatusAccepted, v, err
}

func (s *Server) listTenantKeys(ctx context.Context, r *request) (int, any, error) {
	t, err := s.tenant(ctx, r)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.ListKeys(ctx, t.ID)
	return 200, v, err
}

func (s *Server) createTenantKey(ctx context.Context, r *request) (int, any, error) {
	t, err := s.tenant(ctx, r)
	if err != nil {
		return 0, nil, err
	}
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.CreateKey(ctx, t.ID, in)
	return http.StatusCreated, v, err
}

func (s *Server) revokeTenantKey(ctx context.Context, r *request) (int, any, error) {
	t, err := s.tenant(ctx, r)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusNoContent, nil, s.App.RevokeKey(ctx, t.ID, r.param("keyId"), r.principal.KeyID)
}

func (s *Server) listQueues(ctx context.Context, r *request) (int, any, error) {
	v, err := s.App.ListQueues(ctx, r.principal)
	return 200, v, err
}

func (s *Server) getQueue(ctx context.Context, r *request) (int, any, error) {
	v, err := s.App.GetQueue(ctx, r.principal, r.param("queue"))
	return 200, v, err
}

func (s *Server) patchQueue(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.PatchQueue(ctx, r.principal, r.param("queue"), in)
	return 200, v, err
}

func (s *Server) workers(ctx context.Context, r *request) (int, any, error) {
	v, err := s.App.Workers(ctx, r.principal, r.param("queue"))
	return 200, v, err
}

func (s *Server) listMessages(ctx context.Context, r *request) (int, any, error) {
	v, err := s.App.ListMessages(ctx, r.principal, r.param("queue"), app.ListQuery{
		Status: r.query("status"), ErrorType: r.query("error_type"), Search: r.query("search"),
		Cursor: r.query("cursor"), Limit: r.queryPtr("limit"),
	})
	return 200, v, err
}

func (s *Server) publish(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, duplicate, err := s.App.Publish(ctx, r.principal, r.param("queue"), in)
	if duplicate {
		return http.StatusOK, v, err
	}
	return http.StatusCreated, v, err
}

func (s *Server) claim(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(true)
	if err != nil {
		return 0, nil, err
	}
	lease, err := s.App.Claim(ctx, r.principal, r.param("queue"), in, app.ClientInfo{
		UserAgent: r.http.UserAgent(), Address: clientAddress(r.http),
	})
	if err != nil {
		return 0, nil, err
	}
	if lease == nil {
		return http.StatusNoContent, nil, nil
	}
	return 200, lease, nil
}

func (s *Server) replayMany(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.Bulk(ctx, r.principal, r.param("queue"), "replay", in)
	return 200, v, err
}

func (s *Server) purge(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.Bulk(ctx, r.principal, r.param("queue"), "purge", in)
	return 200, v, err
}

func (s *Server) getMessage(ctx context.Context, r *request) (int, any, error) {
	v, err := s.App.GetMessage(ctx, r.principal, r.param("id"))
	return 200, v, err
}

func (s *Server) deleteMessage(ctx context.Context, r *request) (int, any, error) {
	return http.StatusNoContent, nil, s.App.DeleteMessage(ctx, r.principal, r.param("id"))
}

func (s *Server) replay(ctx context.Context, r *request) (int, any, error) {
	v, err := s.App.ReplayMessage(ctx, r.principal, r.param("id"))
	return 200, v, err
}

func (s *Server) extend(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.Extend(ctx, r.principal, r.param("id"), in)
	return 200, v, err
}

func (s *Server) ack(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusNoContent, nil, s.App.Ack(ctx, r.principal, r.param("id"), in)
}

func (s *Server) fail(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.Fail(ctx, r.principal, r.param("id"), in)
	return 200, v, err
}

func (s *Server) release(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusNoContent, nil, s.App.Release(ctx, r.principal, r.param("id"), in)
}

func nowUTC() time.Time { return time.Now().UTC() }

func (s *Server) ackMany(ctx context.Context, r *request) (int, any, error) {
	in, err := r.input(false)
	if err != nil {
		return 0, nil, err
	}
	v, err := s.App.AckMany(ctx, r.principal, in)
	return 200, v, err
}
