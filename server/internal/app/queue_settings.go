package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/db"
)

// The effective settings of a queue are read on every claim, so every node keeps them in
// memory for a minute; every change goes through here and drops them on every node.
const settingsTTL = time.Minute

func settingsKey(tenant, queue string) string { return "queue_settings:" + tenant + ":" + queue }

// ForQueue is a queue's effective settings.
func (a *App) ForQueue(ctx context.Context, tenant, queue string) (*QueueConfig, error) {
	key := settingsKey(tenant, queue)
	if v, ok := a.Coord.Local().Get(key); ok {
		return v.(*QueueConfig), nil
	}
	c, err := a.resolveSettings(ctx, tenant, queue)
	if err != nil {
		return nil, err
	}
	a.Coord.Local().Set(key, c, settingsTTL)
	return c, nil
}

// ForQueues is ForQueue for several queues.
func (a *App) ForQueues(ctx context.Context, tenant string, queues []string) (map[string]*QueueConfig, error) {
	out := make(map[string]*QueueConfig, len(queues))
	for _, q := range queues {
		c, err := a.ForQueue(ctx, tenant, q)
		if err != nil {
			return nil, err
		}
		out[q] = c
	}
	return out, nil
}

// reloadSettings is a queue's settings as they are now, not as cached: for a claim that
// waited and was woken, perhaps by a resume.
func (a *App) reloadSettings(ctx context.Context, tenant, queue string) (*QueueConfig, error) {
	a.Coord.Local().Del(settingsKey(tenant, queue))
	return a.ForQueue(ctx, tenant, queue)
}

type storedQueue struct {
	defaults map[string]any
	quotas   map[string]any
	settings map[string]any
	pausedAt *time.Time
	exists   bool
}

func decodeObject(s string) map[string]any {
	obj, ok := AsObject(DecodeAny([]byte(s)))
	if !ok {
		return map[string]any{}
	}
	return obj
}

func (a *App) loadQueue(ctx context.Context, tenant, queue string) (*storedQueue, error) {
	var (
		defaults, quotas string
		settings, name   sql.NullString
		paused           db.NullTime
	)
	err := a.DB.Row(ctx, a.DB, `SELECT t.queue_defaults, t.quotas, q.settings, q.paused_at, q.name
		FROM tenants t LEFT JOIN queues q ON q.tenant_id = t.id AND q.name = :queue
		WHERE t.id = :tenant`, db.Args{"tenant": tenant, "queue": queue}).
		Scan(&defaults, &quotas, &settings, &paused, &name)
	if errors.Is(err, sql.ErrNoRows) {
		return &storedQueue{defaults: map[string]any{}, quotas: map[string]any{}, settings: map[string]any{}}, nil
	}
	if err != nil {
		return nil, err
	}
	s := &storedQueue{
		defaults: decodeObject(defaults),
		quotas:   decodeObject(quotas),
		settings: map[string]any{},
		pausedAt: paused.Ptr(),
		exists:   name.Valid,
	}
	if settings.Valid {
		s.settings = decodeObject(settings.String)
	}
	return s, nil
}

func (a *App) resolveSettings(ctx context.Context, tenant, queue string) (*QueueConfig, error) {
	s, err := a.loadQueue(ctx, tenant, queue)
	if err != nil {
		return nil, err
	}
	quotas := ResolveQuotas(s.quotas)
	return &QueueConfig{
		Settings: CapSettings(ResolveSettings(s.defaults, s.settings), quotas),
		PausedAt: s.pausedAt,
		Exists:   s.exists,
		Quotas:   quotas,
	}, nil
}

// QueueNames are a tenant's queues, by name.
func (a *App) QueueNames(ctx context.Context, tenant string) ([]string, error) {
	rows, err := a.DB.Query(ctx, a.DB, "SELECT name FROM queues WHERE tenant_id = :tenant ORDER BY name",
		db.Args{"tenant": tenant})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := []string{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

func (a *App) invalidateQueue(ctx context.Context, tenant, queue string) {
	a.Coord.Forget(ctx, settingsKey(tenant, queue))
}

// InvalidateTenant drops every cached queue of a tenant, after its defaults changed.
func (a *App) InvalidateTenant(ctx context.Context, tenant string) {
	a.Coord.ForgetPrefix(ctx, "queue_settings:"+tenant+":")
}

// ensureQueue is called on publish: a queue exists from its first message on, with the
// tenant's defaults (and paused, if those say new queues start paused).
func (a *App) ensureQueue(ctx context.Context, tenant, queue string, c *QueueConfig) error {
	if c.Exists {
		return nil
	}
	if err := a.assertRoomForQueue(ctx, tenant, c); err != nil {
		return err
	}
	var paused any
	if b, _ := c.Settings["paused"].(bool); b {
		paused = db.T(now())
	}
	t := db.T(now())
	cols := []string{"tenant_id", "name", "settings", "paused_at", "created_at", "updated_at"}
	if _, err := a.DB.Exec(ctx, a.DB, a.DB.D.InsertOrIgnore("queues", cols, []string{"tenant_id", "name"}), db.Args{
		"tenant_id": tenant, "name": queue, "settings": "{}", "paused_at": paused, "created_at": t, "updated_at": t,
	}); err != nil {
		return err
	}
	a.invalidateQueue(ctx, tenant, queue)
	return nil
}

// assertRoomForQueue allows a new queue only while the tenant has fewer than max_queues.
func (a *App) assertRoomForQueue(ctx context.Context, tenant string, c *QueueConfig) error {
	limit := c.Quota("max_queues")
	if limit == nil {
		return nil
	}
	var n int64
	if err := a.DB.Row(ctx, a.DB, "SELECT COUNT(*) FROM queues WHERE tenant_id = :tenant",
		db.Args{"tenant": tenant}).Scan(&n); err != nil {
		return err
	}
	if n >= *limit {
		return QuotaExceeded(fmt.Sprintf("This tenant may have at most %d queues.", *limit))
	}
	return nil
}

// UpdateQueue is PATCH /v1/queues/{queue}: any subset of the settings, paused included.
// It creates the queue when it does not exist yet.
func (a *App) UpdateQueue(ctx context.Context, tenant, queue string, patch map[string]any) (*QueueConfig, error) {
	current, err := a.ForQueue(ctx, tenant, queue)
	if err != nil {
		return nil, err
	}
	errs := ValidateSettingsPatch(patch, current.Settings, "")
	if len(errs) == 0 {
		errs = QuotaSettingErrors(patch, current.Quotas, "")
	}
	if len(errs) > 0 {
		return nil, Validation(errs)
	}
	if !current.Exists {
		if err := a.assertRoomForQueue(ctx, tenant, current); err != nil {
			return nil, err
		}
	}
	var paused *bool
	if v, ok := patch["paused"].(bool); ok {
		paused = &v
	}
	rest := map[string]any{}
	for k, v := range patch {
		if k != "paused" {
			rest[k] = v
		}
	}
	return a.applyQueue(ctx, tenant, queue, rest, paused)
}

// PauseQueue and ResumeQueue change only the pause state.
func (a *App) PauseQueue(ctx context.Context, tenant, queue string) (*QueueConfig, error) {
	t := true
	return a.applyQueue(ctx, tenant, queue, map[string]any{}, &t)
}

func (a *App) applyQueue(ctx context.Context, tenant, queue string, patch map[string]any, paused *bool) (*QueueConfig, error) {
	stored, err := a.loadQueue(ctx, tenant, queue)
	if err != nil {
		return nil, err
	}
	current, err := a.ForQueue(ctx, tenant, queue)
	if err != nil {
		return nil, err
	}
	var pausedAt any
	switch {
	case paused != nil && *paused:
		if current.PausedAt == nil {
			pausedAt = db.T(now())
		} else {
			pausedAt = db.T(*current.PausedAt)
		}
	case paused != nil:
		pausedAt = nil
	default:
		pausedAt = db.TPtr(current.PausedAt)
	}
	settings, _ := json.Marshal(MergeSettings(stored.settings, patch))
	t := db.T(now())
	cols := []string{"tenant_id", "name", "settings", "paused_at", "created_at", "updated_at"}
	if _, err := a.DB.Exec(ctx, a.DB, a.DB.D.Upsert("queues", cols, []string{"tenant_id", "name"},
		[]string{"settings", "paused_at", "updated_at"}), db.Args{
		"tenant_id": tenant, "name": queue, "settings": string(settings), "paused_at": pausedAt,
		"created_at": t, "updated_at": t,
	}); err != nil {
		return nil, err
	}
	a.invalidateQueue(ctx, tenant, queue)
	changed := make([]string, 0, len(patch))
	for k := range patch {
		changed = append(changed, k)
	}
	a.Log.Info("Queue settings changed", "tenant", tenant, "queue", queue, "changed", changed, "paused", pausedAt != nil)
	return a.reloadSettings(ctx, tenant, queue)
}
