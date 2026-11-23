package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/db"
)

// Scopes of a tenant key. admin includes the others; metrics only reads figures.
const (
	ScopePublish = "publish"
	ScopeConsume = "consume"
	ScopeMetrics = "metrics"
	ScopeAdmin   = "admin"
)

// TenantHeader lets an operator key act in a tenant as its admin.
const TenantHeader = "LessorMQ-Tenant"

// Principal is who is calling: the key, the tenant it acts in and what it may do there.
// A tenant key acts in its own tenant with its own scopes. An operator key acts in no
// tenant, or, with the LessorMQ-Tenant header, in that tenant as an admin.
type Principal struct {
	KeyID         string
	TenantID      string
	Scopes        []string
	QueuePatterns []string
	Operator      bool
}

// Has tells whether the key has the scope; admin satisfies any.
func (p *Principal) Has(scope string) bool {
	for _, s := range p.Scopes {
		if s == scope || s == ScopeAdmin {
			return true
		}
	}
	return false
}

// HasAny tells whether the key has one of the scopes.
func (p *Principal) HasAny(scopes ...string) bool {
	for _, s := range scopes {
		if p.Has(s) {
			return true
		}
	}
	return false
}

// MayUseQueue tells whether the key's queue patterns allow the queue; none allow all.
func (p *Principal) MayUseQueue(queue string) bool {
	if len(p.QueuePatterns) == 0 {
		return true
	}
	for _, pattern := range p.QueuePatterns {
		if globMatch(pattern, queue) {
			return true
		}
	}
	return false
}

// Tenant is the tenant to act in; an operator key without the header has none.
func (p *Principal) Tenant() (string, error) {
	if p.TenantID == "" {
		return "", Forbidden("An operator key acts in a tenant only with the LessorMQ-Tenant header.")
	}
	return p.TenantID, nil
}

// Authorise checks that the key may use the queue, and returns the tenant to act in.
func (p *Principal) Authorise(queue string) (string, error) {
	if !p.MayUseQueue(queue) {
		return "", Forbidden(fmt.Sprintf("This key may not use queue %q.", queue))
	}
	return p.Tenant()
}

// RequireScopes checks the key has one of the scopes, in a tenant.
func (p *Principal) RequireScopes(scopes ...string) error {
	if _, err := p.Tenant(); err != nil {
		return err
	}
	if !p.HasAny(scopes...) {
		return Forbidden(fmt.Sprintf("This key needs the %s scope.", strings.Join(scopes, " or ")))
	}
	return nil
}

// globMatch is fnmatch without flags for patterns of [A-Za-z0-9._*?-]: * is any run of
// characters, ? any one.
func globMatch(pattern, s string) bool {
	p := []rune(pattern)
	r := []rune(s)
	var match func(i, j int) bool
	match = func(i, j int) bool {
		for i < len(p) {
			switch p[i] {
			case '*':
				for i < len(p) && p[i] == '*' {
					i++
				}
				if i == len(p) {
					return true
				}
				for k := j; k <= len(r); k++ {
					if match(i, k) {
						return true
					}
				}
				return false
			case '?':
				if j >= len(r) {
					return false
				}
			default:
				if j >= len(r) || r[j] != p[i] {
					return false
				}
			}
			i++
			j++
		}
		return j == len(r)
	}
	return match(0, 0)
}

var bearer = regexp.MustCompile(`(?i)^Bearer\s+(\S+)$`)

// touchInterval: last_used_at is written at most this often per key.
const touchInterval = 60 * time.Second

// Authenticate turns the bearer key of a request into a Principal.
func (a *App) Authenticate(ctx context.Context, authorization, tenantHeader string) (*Principal, error) {
	m := bearer.FindStringSubmatch(authorization)
	if m == nil {
		return nil, Unauthorized(`Send the API key as "Authorization: Bearer <key>".`)
	}
	var (
		id                   string
		tenant, tenantStatus sql.NullString
		scopes, queues       string
		lastUsed             db.NullTime
	)
	err := a.DB.Row(ctx, a.DB, `SELECT k.id, k.tenant_id, k.scopes, k.queues, k.last_used_at, t.status
		FROM api_keys k LEFT JOIN tenants t ON t.id = k.tenant_id
		WHERE k.secret_hash = :hash AND k.revoked_at IS NULL`, db.Args{"hash": SecretHash(m[1])}).
		Scan(&id, &tenant, &scopes, &queues, &lastUsed, &tenantStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, Unauthorized("Unknown or revoked API key.")
	}
	if err != nil {
		return nil, err
	}
	var p *Principal
	if !tenant.Valid {
		p = &Principal{KeyID: id, Operator: true}
		if tenantHeader != "" {
			status, err := a.tenantStatus(ctx, tenantHeader)
			if err != nil {
				return nil, err
			}
			// An operator may look into a suspended tenant; that is what suspending is for.
			if status == "" {
				return nil, NotFound(fmt.Sprintf("No tenant %q.", tenantHeader))
			}
			p.TenantID = tenantHeader
			p.Scopes = []string{ScopeAdmin}
		}
	} else {
		if !tenantStatus.Valid {
			return nil, Unauthorized("Unknown or revoked API key.")
		}
		if tenantStatus.String != "active" {
			return nil, TenantSuspended()
		}
		p = &Principal{KeyID: id, TenantID: tenant.String}
		json.Unmarshal([]byte(scopes), &p.Scopes)
		json.Unmarshal([]byte(queues), &p.QueuePatterns)
	}
	t := now()
	if !lastUsed.Valid || lastUsed.Time.Before(t.Add(-touchInterval)) {
		if _, err := a.DB.Exec(ctx, a.DB, `UPDATE api_keys SET last_used_at = :now
			WHERE id = :id AND (last_used_at IS NULL OR last_used_at < :stale)`,
			db.Args{"now": db.T(t), "id": id, "stale": db.T(t.Add(-touchInterval))}); err != nil {
			a.Log.Warn("Could not record a key's use", "error", err.Error())
		}
	}
	return p, nil
}

func (a *App) tenantStatus(ctx context.Context, id string) (string, error) {
	var status string
	err := a.DB.Row(ctx, a.DB, "SELECT status FROM tenants WHERE id = :id", db.Args{"id": id}).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return status, err
}
