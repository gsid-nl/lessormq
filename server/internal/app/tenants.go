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

// Tenant is a row of tenants.
type Tenant struct {
	ID          string
	Name        string
	Status      string
	Quotas      Quotas
	Defaults    map[string]any // a patch over the product defaults
	CreatedAt   time.Time
	UpdatedAt   time.Time
	SuspendedAt *time.Time
}

const tenantColumns = "id, name, status, quotas, queue_defaults, created_at, updated_at, suspended_at"

func scanTenant(s scanner) (*Tenant, error) {
	var (
		t                Tenant
		quotas, defaults string
		created, updated db.NullTime
		suspended        db.NullTime
	)
	if err := s.Scan(&t.ID, &t.Name, &t.Status, &quotas, &defaults, &created, &updated, &suspended); err != nil {
		return nil, err
	}
	t.Quotas = ResolveQuotas(decodeObject(quotas))
	t.Defaults = decodeObject(defaults)
	t.CreatedAt, t.UpdatedAt, t.SuspendedAt = created.Time, updated.Time, suspended.Ptr()
	return &t, nil
}

func (a *App) findTenant(ctx context.Context, id string) (*Tenant, error) {
	t, err := scanTenant(a.DB.Row(ctx, a.DB, "SELECT "+tenantColumns+" FROM tenants WHERE id = :id", db.Args{"id": id}))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// GetTenant is a tenant, or 404.
func (a *App) GetTenant(ctx context.Context, id string) (*Tenant, error) {
	t, err := a.findTenant(ctx, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, NotFound(fmt.Sprintf("No tenant %q.", id))
	}
	return t, nil
}

func (a *App) saveTenant(ctx context.Context, t *Tenant, insert bool) error {
	quotas, _ := json.Marshal(t.Quotas.Stored())
	defaults, _ := json.Marshal(t.Defaults)
	args := db.Args{
		"id": t.ID, "name": t.Name, "status": t.Status, "quotas": string(quotas), "queue_defaults": string(defaults),
		"created_at": db.T(t.CreatedAt), "updated_at": db.T(t.UpdatedAt), "suspended_at": db.TPtr(t.SuspendedAt),
	}
	if insert {
		_, err := a.DB.Exec(ctx, a.DB, `INSERT INTO tenants (`+tenantColumns+`)
			VALUES (:id, :name, :status, :quotas, :queue_defaults, :created_at, :updated_at, :suspended_at)`, args)
		return err
	}
	_, err := a.DB.Exec(ctx, a.DB, `UPDATE tenants SET name = :name, status = :status, quotas = :quotas,
		queue_defaults = :queue_defaults, updated_at = :updated_at, suspended_at = :suspended_at WHERE id = :id`, args)
	return err
}

// TenantView is a tenant as the API shows it, with its usage.
func (a *App) TenantView(ctx context.Context, t *Tenant) (map[string]any, error) {
	queues, err := a.QueueNames(ctx, t.ID)
	if err != nil {
		return nil, err
	}
	usage, err := a.tenantUsage(ctx, t.ID, queues)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"id":             t.ID,
		"name":           t.Name,
		"status":         t.Status,
		"quotas":         t.Quotas.View(),
		"queue_defaults": map[string]any(ResolveSettings(t.Defaults)),
		"usage":          usage,
		"created_at":     formatTimeV(t.CreatedAt),
		"suspended_at":   formatTime(t.SuspendedAt),
	}, nil
}

// ListTenants is a page of tenants, by id.
func (a *App) ListTenants(ctx context.Context, status string, limitRaw *string, cursor string) (map[string]any, error) {
	if status != "" && status != "active" && status != "suspended" && status != "deleting" {
		return nil, ValidationField("status", "unknown status")
	}
	limit, err := PageLimit(limitRaw)
	if err != nil {
		return nil, err
	}
	after, err := PageAfter(cursor, TenantIDPattern)
	if err != nil {
		return nil, err
	}
	where := "1 = 1"
	args := db.Args{}
	if status != "" {
		where += " AND status = :status"
		args["status"] = status
	}
	if after != "" {
		where += " AND id > :after"
		args["after"] = after
	}
	rows, err := a.DB.Query(ctx, a.DB, fmt.Sprintf("SELECT %s FROM tenants WHERE %s ORDER BY id ASC LIMIT %d",
		tenantColumns, where, limit+1), args)
	if err != nil {
		return nil, err
	}
	var page []*Tenant
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		page = append(page, t)
	}
	rows.Close()
	var next any
	if len(page) > limit {
		page = page[:limit]
		next = Cursor(page[len(page)-1].ID)
	}
	items := make([]any, len(page))
	for i, t := range page {
		if items[i], err = a.TenantView(ctx, t); err != nil {
			return nil, err
		}
	}
	return map[string]any{"items": items, "next_cursor": next}, nil
}

// CreateTenant makes a tenant, with its first admin key unless asked not to.
func (a *App) CreateTenant(ctx context.Context, in *Input) (map[string]any, error) {
	in.AllowOnly("id", "name", "quotas", "queue_defaults", "create_admin_key")
	name, _ := in.String("name", true, 255, "")
	id, hasID := in.String("id", false, 63, TenantIDPattern)
	createKey := true
	if b, ok := in.Bool("create_admin_key"); ok {
		createKey = b
	}
	if in.Has("quotas") {
		in.AddErrors(ValidateQuotas(in.Value("quotas"), "quotas."))
	}
	if in.Has("queue_defaults") {
		in.AddErrors(ValidateSettingsPatch(in.Value("queue_defaults"), nil, "queue_defaults."))
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}
	if hasID {
		existing, err := a.findTenant(ctx, id)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return nil, InvalidState(fmt.Sprintf("Tenant %q already exists.", id))
		}
	} else {
		var err error
		if id, err = a.generateTenantID(ctx, name); err != nil {
			return nil, err
		}
	}
	t := now()
	tenant := &Tenant{ID: id, Name: name, Status: "active", Quotas: ResolveQuotas(nil), Defaults: map[string]any{},
		CreatedAt: t, UpdatedAt: t}
	if in.Has("quotas") {
		q, _ := AsObject(in.Value("quotas"))
		tenant.Quotas.Patch(q)
	}
	if in.Has("queue_defaults") {
		d, _ := AsObject(in.Value("queue_defaults"))
		tenant.Defaults = MergeSettings(tenant.Defaults, d)
	}
	if err := a.saveTenant(ctx, tenant, true); err != nil {
		if db.IsUniqueViolation(err) {
			return nil, InvalidState(fmt.Sprintf("Tenant %q already exists.", id))
		}
		return nil, err
	}
	view, err := a.TenantView(ctx, tenant)
	if err != nil {
		return nil, err
	}
	view["admin_key"] = nil
	if createKey {
		key, secret, err := a.createKey(ctx, &tenant.ID, "admin", []string{ScopePublish, ScopeConsume, ScopeAdmin}, []string{})
		if err != nil {
			return nil, err
		}
		kv := keyView(key)
		kv["secret"] = secret
		view["admin_key"] = kv
	}
	return view, nil
}

var notIDChars = regexp.MustCompile(`[^a-z0-9]+`)

func (a *App) generateTenantID(ctx context.Context, name string) (string, error) {
	base := strings.Trim(notIDChars.ReplaceAllString(strings.ToLower(name), "-"), "-")
	if len(base) > 54 {
		base = base[:54]
	}
	if Matches(TenantIDPattern, base) {
		existing, err := a.findTenant(ctx, base)
		if err != nil {
			return "", err
		}
		if existing == nil {
			return base, nil
		}
	}
	for {
		candidate := strings.TrimLeft(base+"-"+RandomHex(4), "-")
		existing, err := a.findTenant(ctx, candidate)
		if err != nil {
			return "", err
		}
		if existing == nil {
			return candidate, nil
		}
	}
}

// UpdateTenant is the operator's change of a tenant: name, status, quotas, defaults.
func (a *App) UpdateTenant(ctx context.Context, t *Tenant, in *Input) error {
	in.AllowOnly("name", "status", "quotas", "queue_defaults")
	name, hasName := in.String("name", false, 255, "")
	status, hasStatus := in.Choice("status", []string{"active", "suspended"}, false)
	if in.Has("quotas") {
		in.AddErrors(ValidateQuotas(in.Value("quotas"), "quotas."))
	}
	if in.Has("queue_defaults") {
		in.AddErrors(ValidateSettingsPatch(in.Value("queue_defaults"), ResolveSettings(t.Defaults), "queue_defaults."))
	}
	if err := in.Validate(); err != nil {
		return err
	}
	if t.Status == "deleting" {
		return InvalidState("The tenant is being deleted.")
	}
	n := now()
	if hasName {
		t.Name = name
	}
	if hasStatus && status == "suspended" && t.Status != "suspended" {
		t.Status = "suspended"
		t.SuspendedAt = &n
	} else if hasStatus && status == "active" {
		t.Status = "active"
		t.SuspendedAt = nil
	}
	if in.Has("quotas") {
		q, _ := AsObject(in.Value("quotas"))
		t.Quotas.Patch(q)
	}
	if in.Has("queue_defaults") {
		d, _ := AsObject(in.Value("queue_defaults"))
		t.Defaults = MergeSettings(t.Defaults, d)
	}
	t.UpdatedAt = n
	if err := a.saveTenant(ctx, t, false); err != nil {
		return err
	}
	// Cached queue settings carry the tenant's defaults and quotas.
	if in.Has("queue_defaults") || in.Has("quotas") {
		a.InvalidateTenant(ctx, t.ID)
	}
	return nil
}

// UpdateOwnTenant is a tenant's change of its own defaults, within its quotas.
func (a *App) UpdateOwnTenant(ctx context.Context, t *Tenant, in *Input) error {
	in.AllowOnly("queue_defaults")
	if in.Has("queue_defaults") {
		errs := ValidateSettingsPatch(in.Value("queue_defaults"), ResolveSettings(t.Defaults), "queue_defaults.")
		if len(errs) == 0 {
			d, _ := AsObject(in.Value("queue_defaults"))
			errs = QuotaSettingErrors(d, t.Quotas, "queue_defaults.")
		}
		in.AddErrors(errs)
	}
	if err := in.Validate(); err != nil {
		return err
	}
	if in.Has("queue_defaults") {
		d, _ := AsObject(in.Value("queue_defaults"))
		t.Defaults = MergeSettings(t.Defaults, d)
		t.UpdatedAt = now()
		if err := a.saveTenant(ctx, t, false); err != nil {
			return err
		}
		a.InvalidateTenant(ctx, t.ID)
	}
	return nil
}

// RequestTenantDeletion marks a suspended tenant for deletion; the reaper deletes its data.
func (a *App) RequestTenantDeletion(ctx context.Context, t *Tenant) error {
	if t.Status == "active" {
		return InvalidState("Suspend the tenant before deleting it.")
	}
	t.Status = "deleting"
	t.UpdatedAt = now()
	return a.saveTenant(ctx, t, false)
}

// ReapTenants deletes the data of tenants marked for deletion.
func (a *App) ReapTenants(ctx context.Context, batch int, out func(string)) error {
	rows, err := a.DB.Query(ctx, a.DB, "SELECT id FROM tenants WHERE status = 'deleting' ORDER BY id", db.Args{})
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		var deleted int64
		for {
			n, err := a.DB.Exec(ctx, a.DB, a.DB.D.DeleteMessages("tenant_id = :tenant", ""),
				db.Args{"tenant": id, "limit": batch})
			if err != nil {
				return err
			}
			deleted += n
			if n < int64(batch) {
				break
			}
		}
		keys, err := a.DB.Exec(ctx, a.DB, "DELETE FROM api_keys WHERE tenant_id = :id", db.Args{"id": id})
		if err != nil {
			return err
		}
		if _, err := a.DB.Exec(ctx, a.DB, "DELETE FROM queues WHERE tenant_id = :id", db.Args{"id": id}); err != nil {
			return err
		}
		a.InvalidateTenant(ctx, id)
		if _, err := a.DB.Exec(ctx, a.DB, "DELETE FROM tenants WHERE id = :id", db.Args{"id": id}); err != nil {
			return err
		}
		a.Log.Info("Tenant deleted", "tenant", id, "messages", deleted, "keys", keys)
		out(fmt.Sprintf("Deleted tenant %s: %d messages, %d keys.", id, deleted, keys))
	}
	return nil
}

// PurgeOld deletes the completed and failed messages past their queue's retention (or
// older than hours, when given).
func (a *App) PurgeOld(ctx context.Context, hours *int64, batch int, tenant, queue string, out func(string)) (int64, error) {
	where := []string{}
	args := db.Args{}
	if tenant != "" {
		where = append(where, "tenant_id = :tenant")
		args["tenant"] = tenant
	}
	if queue != "" {
		where = append(where, "queue = :queue")
		args["queue"] = queue
	}
	q := "SELECT DISTINCT tenant_id, queue FROM messages"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	rows, err := a.DB.Query(ctx, a.DB, q+" ORDER BY tenant_id, queue", args)
	if err != nil {
		return 0, err
	}
	type pair struct{ tenant, queue string }
	var pairs []pair
	for rows.Next() {
		var p pair
		if err := rows.Scan(&p.tenant, &p.queue); err != nil {
			rows.Close()
			return 0, err
		}
		pairs = append(pairs, p)
	}
	rows.Close()
	var total int64
	for _, p := range pairs {
		c, err := a.ForQueue(ctx, p.tenant, p.queue)
		if err != nil {
			return total, err
		}
		for _, st := range []struct {
			status string
			keep   int64
		}{
			{Completed, c.Settings.Int("retention.completed_seconds")},
			{Failed, c.Settings.Int("retention.failed_seconds")},
		} {
			keep := st.keep
			if hours != nil {
				keep = *hours * 3600
			}
			before := db.T(now().Add(-time.Duration(keep) * time.Second))
			var deleted int64
			for {
				n, err := a.DB.Exec(ctx, a.DB, a.DB.D.DeleteMessages(
					"tenant_id = :tenant AND queue = :queue AND status = :status AND updated_at < :before", ""),
					db.Args{"tenant": p.tenant, "queue": p.queue, "status": st.status, "before": before, "limit": batch})
				if err != nil {
					return total, err
				}
				deleted += n
				if n < int64(batch) {
					break
				}
			}
			if deleted > 0 && st.status == Completed {
				a.forgetCompleted(ctx, p.tenant, p.queue)
			}
			if deleted > 0 {
				out(fmt.Sprintf("%s/%s: deleted %d %s messages", p.tenant, p.queue, deleted, st.status))
			}
			total += deleted
		}
	}
	a.Log.Info("Purged old messages", "total_deleted", total)
	return total, nil
}

// API keys. A key without a tenant is an operator key: it manages tenants and is never
// created over HTTP.

type apiKey struct {
	ID        string
	TenantID  *string
	Name      string
	Prefix    string
	Scopes    []string
	Queues    []string
	CreatedAt time.Time
	LastUsed  *time.Time
}

const keyQueuePattern = `[A-Za-z0-9._*?-]{1,200}`

func keyView(k *apiKey) map[string]any {
	scopes := k.Scopes
	if scopes == nil {
		scopes = []string{}
	}
	queues := k.Queues
	if queues == nil {
		queues = []string{}
	}
	return map[string]any{
		"id":           k.ID,
		"name":         k.Name,
		"prefix":       k.Prefix,
		"scopes":       scopes,
		"queues":       queues,
		"created_at":   formatTimeV(k.CreatedAt),
		"last_used_at": formatTime(k.LastUsed),
	}
}

func (a *App) createKey(ctx context.Context, tenant *string, name string, scopes, queues []string) (*apiKey, string, error) {
	secret := "lmq_" + RandomHex(24)
	k := &apiKey{ID: RandomHex(8), TenantID: tenant, Name: name, Prefix: secret[:12], Scopes: scopes,
		Queues: queues, CreatedAt: now()}
	if k.Scopes == nil {
		k.Scopes = []string{}
	}
	if k.Queues == nil {
		k.Queues = []string{}
	}
	scopesJSON, _ := json.Marshal(k.Scopes)
	queuesJSON, _ := json.Marshal(k.Queues)
	var tenantArg any
	if tenant != nil {
		tenantArg = *tenant
	}
	_, err := a.DB.Exec(ctx, a.DB, `INSERT INTO api_keys (id, tenant_id, name, prefix, secret_hash, scopes, queues, created_at)
		VALUES (:id, :tenant, :name, :prefix, :hash, :scopes, :queues, :created_at)`, db.Args{
		"id": k.ID, "tenant": tenantArg, "name": name, "prefix": k.Prefix, "hash": SecretHash(secret),
		"scopes": string(scopesJSON), "queues": string(queuesJSON), "created_at": db.T(k.CreatedAt),
	})
	return k, secret, err
}

// CreateOperatorKey makes an operator key; the secret is shown once.
func (a *App) CreateOperatorKey(ctx context.Context, name string) (string, string, error) {
	k, secret, err := a.createKey(ctx, nil, name, []string{}, []string{})
	if err != nil {
		return "", "", err
	}
	return k.ID, secret, nil
}

// CreateKey makes a key of a tenant from a request.
func (a *App) CreateKey(ctx context.Context, tenant string, in *Input) (map[string]any, error) {
	in.AllowOnly("name", "scopes", "queues")
	name, _ := in.String("name", true, 255, "")
	scopes, _ := in.StringList("scopes", true, 1, 255, "publish|consume|metrics|admin")
	queues, _ := in.StringList("queues", false, 0, 255, keyQueuePattern)
	if err := in.Validate(); err != nil {
		return nil, err
	}
	k, secret, err := a.createKey(ctx, &tenant, name, scopes, queues)
	if err != nil {
		return nil, err
	}
	v := keyView(k)
	v["secret"] = secret
	return v, nil
}

// ListKeys lists a tenant's keys that are not revoked, oldest first.
func (a *App) ListKeys(ctx context.Context, tenant string) (map[string]any, error) {
	rows, err := a.DB.Query(ctx, a.DB, `SELECT id, name, prefix, scopes, queues, created_at, last_used_at
		FROM api_keys WHERE tenant_id = :tenant AND revoked_at IS NULL ORDER BY created_at ASC, id ASC`,
		db.Args{"tenant": tenant})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []any{}
	for rows.Next() {
		var k apiKey
		var scopes, queues string
		var created, used db.NullTime
		if err := rows.Scan(&k.ID, &k.Name, &k.Prefix, &scopes, &queues, &created, &used); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(scopes), &k.Scopes)
		json.Unmarshal([]byte(queues), &k.Queues)
		k.CreatedAt, k.LastUsed = created.Time, used.Ptr()
		items = append(items, keyView(&k))
	}
	return map[string]any{"items": items}, rows.Err()
}

// RevokeKey revokes a key of a tenant; a key cannot revoke itself.
func (a *App) RevokeKey(ctx context.Context, tenant, keyID, callerKeyID string) error {
	var id string
	err := a.DB.Row(ctx, a.DB, "SELECT id FROM api_keys WHERE id = :id AND tenant_id = :tenant AND revoked_at IS NULL",
		db.Args{"id": keyID, "tenant": tenant}).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return NotFound(fmt.Sprintf("No API key %q.", keyID))
	}
	if err != nil {
		return err
	}
	if id == callerKeyID {
		return InvalidState("A key cannot revoke itself; use another admin key.")
	}
	_, err = a.DB.Exec(ctx, a.DB, "UPDATE api_keys SET revoked_at = :now WHERE id = :id AND revoked_at IS NULL",
		db.Args{"now": db.T(now()), "id": id})
	return err
}
