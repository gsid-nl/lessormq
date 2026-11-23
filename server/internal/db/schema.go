package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// The schema is the one the PHP server made, so the Go server runs on the same data. Its
// migrations were recorded in doctrine_migration_versions, one series per database; the
// Go server keeps that table, so an installation moves from one server to the other
// without a step of its own.
//
// A new database gets the schema at once and every migration of its series recorded as
// done. A database that has only part of its series was not upgraded far enough by the
// PHP server and is left alone.

const migrationsTable = "doctrine_migration_versions"

// series is what the PHP server recorded per database, oldest first.
var series = map[string][]string{
	"mariadb": {
		`DoctrineMigrations\Version20260319151724`,
		`DoctrineMigrations\Version20260723090000`,
		`DoctrineMigrations\Version20261007120000`,
		`DoctrineMigrations\Version20261119120000`,
		`DoctrineMigrations\Version20261119150000`,
		`DoctrineMigrations\Version20261119180000`,
		`DoctrineMigrations\Version20261119210000`,
		`DoctrineMigrations\Version20261120090000`,
		`DoctrineMigrations\Version20261120120000`,
		`DoctrineMigrations\Version20261120180000`,
	},
	"mysql":      {`LessorMQ\Migrations\MySQL\Version20261120200000`},
	"postgresql": {`LessorMQ\Migrations\PostgreSQL\Version20261120200000`},
	"sqlite":     {`LessorMQ\Migrations\SQLite\Version20261120200000`},
}

func mysqlSchema() []string {
	table := "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB"
	json := "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
	return []string{
		`CREATE TABLE IF NOT EXISTS tenants (
			id VARCHAR(63) NOT NULL,
			name VARCHAR(255) NOT NULL,
			status VARCHAR(20) NOT NULL,
			quotas ` + json + ` NOT NULL CHECK (JSON_VALID(quotas)),
			queue_defaults ` + json + ` NOT NULL CHECK (JSON_VALID(queue_defaults)),
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			suspended_at DATETIME DEFAULT NULL,
			PRIMARY KEY (id)
		) ` + table,
		`CREATE TABLE IF NOT EXISTS api_keys (
			id VARCHAR(32) NOT NULL,
			tenant_id VARCHAR(63) DEFAULT NULL,
			name VARCHAR(255) NOT NULL,
			prefix VARCHAR(16) NOT NULL,
			secret_hash CHAR(64) NOT NULL,
			scopes ` + json + ` NOT NULL CHECK (JSON_VALID(scopes)),
			queues ` + json + ` NOT NULL CHECK (JSON_VALID(queues)),
			created_at DATETIME NOT NULL,
			last_used_at DATETIME DEFAULT NULL,
			revoked_at DATETIME DEFAULT NULL,
			PRIMARY KEY (id),
			UNIQUE INDEX uniq_api_keys_secret_hash (secret_hash),
			INDEX idx_api_keys_tenant (tenant_id)
		) ` + table,
		`CREATE TABLE IF NOT EXISTS queues (
			tenant_id VARCHAR(63) NOT NULL,
			name VARCHAR(255) NOT NULL,
			settings ` + json + ` NOT NULL CHECK (JSON_VALID(settings)),
			paused_at DATETIME DEFAULT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			PRIMARY KEY (tenant_id, name)
		) ` + table,
		`CREATE TABLE IF NOT EXISTS messages (
			id BINARY(16) NOT NULL,
			tenant_id VARCHAR(63) NOT NULL,
			queue VARCHAR(255) NOT NULL,
			body ` + json + ` NOT NULL CHECK (JSON_VALID(body)),
			headers ` + json + ` DEFAULT NULL CHECK (JSON_VALID(headers)),
			status VARCHAR(20) NOT NULL DEFAULT 'pending',
			processing_started_at DATETIME DEFAULT NULL,
			processing_after DATETIME DEFAULT NULL,
			processed_at DATETIME DEFAULT NULL,
			failed_at DATETIME DEFAULT NULL,
			retry_count INT NOT NULL,
			max_attempts INT DEFAULT NULL,
			consumed_by VARCHAR(255) DEFAULT NULL,
			receipt VARCHAR(64) DEFAULT NULL,
			lease_until DATETIME DEFAULT NULL,
			route VARCHAR(255) DEFAULT NULL,
			failed_routes ` + json + ` DEFAULT NULL CHECK (JSON_VALID(failed_routes)),
			dedup_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
			dedup_open VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
			error_message LONGTEXT DEFAULT NULL,
			error_details LONGTEXT DEFAULT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL,
			PRIMARY KEY (id),
			UNIQUE INDEX uniq_open_dedup (tenant_id, queue, dedup_open),
			INDEX idx_tenant_claim (tenant_id, queue, status, id, processing_after),
			INDEX idx_tenant_stale (tenant_id, queue, status, processing_started_at),
			INDEX idx_tenant_purge (tenant_id, queue, status, updated_at)
		) ` + table,
	}
}

func portableSchema(time, json, blob, integer string) []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS tenants (
			id VARCHAR(63) NOT NULL,
			name VARCHAR(255) NOT NULL,
			status VARCHAR(20) NOT NULL,
			quotas ` + json + ` NOT NULL,
			queue_defaults ` + json + ` NOT NULL,
			created_at ` + time + ` NOT NULL,
			updated_at ` + time + ` NOT NULL,
			suspended_at ` + time + ` DEFAULT NULL,
			PRIMARY KEY (id)
		)`,
		`CREATE TABLE IF NOT EXISTS api_keys (
			id VARCHAR(32) NOT NULL,
			tenant_id VARCHAR(63) DEFAULT NULL,
			name VARCHAR(255) NOT NULL,
			prefix VARCHAR(16) NOT NULL,
			secret_hash CHAR(64) NOT NULL,
			scopes ` + json + ` NOT NULL,
			queues ` + json + ` NOT NULL,
			created_at ` + time + ` NOT NULL,
			last_used_at ` + time + ` DEFAULT NULL,
			revoked_at ` + time + ` DEFAULT NULL,
			PRIMARY KEY (id)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uniq_api_keys_secret_hash ON api_keys (secret_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys (tenant_id)`,
		`CREATE TABLE IF NOT EXISTS queues (
			tenant_id VARCHAR(63) NOT NULL,
			name VARCHAR(255) NOT NULL,
			settings ` + json + ` NOT NULL,
			paused_at ` + time + ` DEFAULT NULL,
			created_at ` + time + ` NOT NULL,
			updated_at ` + time + ` NOT NULL,
			PRIMARY KEY (tenant_id, name)
		)`,
		`CREATE TABLE IF NOT EXISTS messages (
			id ` + blob + ` NOT NULL,
			tenant_id VARCHAR(63) NOT NULL,
			queue VARCHAR(255) NOT NULL,
			body ` + json + ` NOT NULL,
			headers ` + json + ` DEFAULT NULL,
			status VARCHAR(20) NOT NULL DEFAULT 'pending',
			processing_started_at ` + time + ` DEFAULT NULL,
			processing_after ` + time + ` DEFAULT NULL,
			processed_at ` + time + ` DEFAULT NULL,
			failed_at ` + time + ` DEFAULT NULL,
			retry_count ` + integer + ` NOT NULL,
			max_attempts ` + integer + ` DEFAULT NULL,
			consumed_by VARCHAR(255) DEFAULT NULL,
			receipt VARCHAR(64) DEFAULT NULL,
			lease_until ` + time + ` DEFAULT NULL,
			route VARCHAR(255) DEFAULT NULL,
			failed_routes ` + json + ` DEFAULT NULL,
			dedup_key VARCHAR(255) DEFAULT NULL,
			dedup_open VARCHAR(255) DEFAULT NULL,
			error_message TEXT DEFAULT NULL,
			error_details TEXT DEFAULT NULL,
			created_at ` + time + ` NOT NULL,
			updated_at ` + time + ` NOT NULL,
			PRIMARY KEY (id)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uniq_open_dedup ON messages (tenant_id, queue, dedup_open)`,
		`CREATE INDEX IF NOT EXISTS idx_tenant_claim ON messages (tenant_id, queue, status, id, processing_after)`,
		`CREATE INDEX IF NOT EXISTS idx_tenant_stale ON messages (tenant_id, queue, status, processing_started_at)`,
		`CREATE INDEX IF NOT EXISTS idx_tenant_purge ON messages (tenant_id, queue, status, updated_at)`,
	}
}

func (d *DB) baseline() []string {
	switch d.D.Name() {
	case "postgresql":
		// json rather than jsonb keeps a body exactly as it was published.
		return portableSchema("TIMESTAMP(0) WITHOUT TIME ZONE", "JSON", "BYTEA", "INT")
	case "sqlite":
		return append([]string{"PRAGMA journal_mode = WAL"}, portableSchema("DATETIME", "CLOB", "BLOB", "INTEGER")...)
	default:
		return mysqlSchema()
	}
}

// Migrate brings the schema up to date: on a new database, all of it.
func (d *DB) Migrate(ctx context.Context) error {
	conn, err := d.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	unlock, err := d.lock(ctx, conn)
	if err != nil {
		return err
	}
	defer unlock()

	timeType := "DATETIME"
	if d.D.Name() == "postgresql" {
		timeType = "TIMESTAMP(0) WITHOUT TIME ZONE"
	}
	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+migrationsTable+` (
		version VARCHAR(191) NOT NULL,
		executed_at `+timeType+` DEFAULT NULL,
		execution_time INT DEFAULT NULL,
		PRIMARY KEY (version)
	)`); err != nil {
		return fmt.Errorf("migrations: %w", err)
	}

	done := map[string]bool{}
	rows, err := conn.QueryContext(ctx, "SELECT version FROM "+migrationsTable)
	if err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		done[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	versions := series[d.D.Name()]
	missing := 0
	for _, v := range versions {
		if !done[v] {
			missing++
		}
	}
	switch {
	case missing == 0:
		return nil
	case missing < len(versions):
		return fmt.Errorf("this database stopped at an older schema (%d of %d migrations of LessorMQ 0.3): "+
			"upgrade it with the PHP server of LessorMQ 0.3 first", len(versions)-missing, len(versions))
	}

	start := time.Now()
	for _, stmt := range d.baseline() {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w\n%s", err, strings.TrimSpace(stmt))
		}
	}
	elapsed := int(time.Since(start).Milliseconds())
	for _, v := range versions {
		q, a := d.Bind(d.D.InsertOrIgnore(migrationsTable, []string{"version", "executed_at", "execution_time"},
			[]string{"version"}), Args{"version": v, "executed_at": T(time.Now()), "execution_time": elapsed})
		if _, err := conn.ExecContext(ctx, q, a...); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
	}
	return nil
}

// lock keeps two servers that start at once from migrating at once.
func (d *DB) lock(ctx context.Context, conn *sql.Conn) (func(), error) {
	switch d.D.Name() {
	case "mariadb", "mysql":
		var got sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK('lessormq_migrations', 60)").Scan(&got); err != nil {
			return nil, fmt.Errorf("migrations: %w", err)
		}
		if got.Int64 != 1 {
			return nil, fmt.Errorf("migrations: another server holds the migration lock")
		}
		return func() { conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK('lessormq_migrations')") }, nil
	case "postgresql":
		if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(7105441)"); err != nil {
			return nil, fmt.Errorf("migrations: %w", err)
		}
		return func() { conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(7105441)") }, nil
	}
	return func() {}, nil
}
