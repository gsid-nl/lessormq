// Package db holds what differs between the databases LessorMQ runs on (MariaDB, MySQL 8,
// PostgreSQL and SQLite), the connection with its named parameters, and the schema.
//
// Fragments take column names and SQL expressions (with :named parameters) as they come;
// none of them takes a value from a request.
package db

import (
	"fmt"
	"strings"
)

// Dialect is the SQL that differs per database. Everything else is written once, in SQL
// all four read alike.
type Dialect interface {
	// Name is mariadb, mysql, postgresql or sqlite.
	Name() string
	// SkipLocked is what a claim's candidate SELECT ends with, to walk past rows other
	// claims hold.
	SkipLocked() string
	// ClaimsInOneStatement tells whether a claim is a single UPDATE ... RETURNING rather
	// than a locking SELECT and an UPDATE: for a database without row locks.
	ClaimsInOneStatement() bool
	// ReplaysInOneStatement tells whether a bulk replay is one UPDATE IGNORE that skips
	// the rows whose dedup_key an open message holds.
	ReplaysInOneStatement() bool
	// JSONValue is a top-level scalar of a JSON column as text; key is a plain name.
	JSONValue(column, key string) string
	// JSONMember is the member of a JSON object column named by the parameter, as text.
	JSONMember(column, param string) string
	// JSONMemberArgument is what the parameter of JSONMember is bound to, for member name.
	JSONMemberArgument(name string) string
	// JSONText is a JSON column as text, for LIKE.
	JSONText(column string) string
	// JSONObject is a JSON object with these members (name, SQL expression), as text.
	JSONObject(members [][2]string) string
	// JSONBool is the JSON true or false, as a value of JSONObject.
	JSONBool(value bool) string
	// JSONString is a string parameter, as a value of JSONObject or JSONWith.
	JSONString(param string) string
	// JSONWith is the JSON object in column (an empty one when NULL) with the member
	// named by the expression name set to value.
	JSONWith(column, name, value string) string
	// Least is the smaller of two numbers.
	Least(a, b string) string
	// Power is base to the power exponent.
	Power(base, exponent string) string
	// PlusSeconds is the time (a UTC datetime parameter) plus seconds, a whole number.
	PlusSeconds(time, seconds string) string
	// EpochSeconds is the datetime expression as whole seconds since 1970 UTC.
	EpochSeconds(time string) string
	// SecondsBetween is how long from one datetime expression to another, in seconds to
	// the millisecond.
	SecondsBetween(from, to string) string
	// DeleteMessages deletes the rows of messages matching where, at most :limit of them,
	// in orderBy order when one is given.
	DeleteMessages(where, orderBy string) string
	// InsertOrIgnore inserts a row (columns bound as :column) unless one with key exists.
	InsertOrIgnore(table string, columns, key []string) string
	// Upsert inserts a row, or sets the update columns of the one with the same key.
	Upsert(table string, columns, key, update []string) string
	// Placeholder is the n-th (1-based) positional parameter.
	Placeholder(n int) string
}

func values(columns []string) string {
	return "(" + strings.Join(columns, ", ") + ") VALUES (:" + strings.Join(columns, ", :") + ")"
}

func jsonObjectParts(members [][2]string) string {
	parts := make([]string, len(members))
	for i, m := range members {
		parts[i] = fmt.Sprintf("'%s', %s", m[0], m[1])
	}
	return strings.Join(parts, ", ")
}

// MariaDB 10.6 and newer: the SQL LessorMQ was written in.
type mariaDB struct{}

func (mariaDB) Name() string                { return "mariadb" }
func (mariaDB) SkipLocked() string          { return " FOR UPDATE SKIP LOCKED" }
func (mariaDB) ClaimsInOneStatement() bool  { return false }
func (mariaDB) ReplaysInOneStatement() bool { return true }
func (mariaDB) JSONValue(c, k string) string {
	return fmt.Sprintf("JSON_VALUE(%s, '$.%s')", c, k)
}
func (mariaDB) JSONMember(c, p string) string      { return fmt.Sprintf("JSON_VALUE(%s, %s)", c, p) }
func (mariaDB) JSONMemberArgument(n string) string { return `$."` + n + `"` }
func (mariaDB) JSONText(c string) string           { return c }
func (mariaDB) JSONObject(m [][2]string) string    { return "JSON_OBJECT(" + jsonObjectParts(m) + ")" }
func (mariaDB) JSONString(p string) string         { return p }
func (mariaDB) Least(a, b string) string           { return fmt.Sprintf("LEAST(%s, %s)", a, b) }
func (mariaDB) Power(b, e string) string           { return fmt.Sprintf("POW(%s, %s)", b, e) }
func (mariaDB) Placeholder(int) string             { return "?" }
func (mariaDB) PlusSeconds(t, s string) string {
	return fmt.Sprintf("CAST(%s AS DATETIME(3)) + INTERVAL %s SECOND", t, s)
}
func (mariaDB) JSONBool(v bool) string {
	if v {
		return "JSON_EXTRACT('true', '$')"
	}
	return "JSON_EXTRACT('false', '$')"
}
func (mariaDB) JSONWith(c, n, v string) string {
	return fmt.Sprintf(`JSON_SET(COALESCE(%s, JSON_OBJECT()), CONCAT('$."', %s, '"'), %s)`, c, n, v)
}

// EpochSeconds is not UNIX_TIMESTAMP(), which reads the time in the session's zone.
func (mariaDB) EpochSeconds(t string) string {
	return fmt.Sprintf("TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', %s)", t)
}
func (mariaDB) SecondsBetween(from, to string) string {
	return fmt.Sprintf("TIMESTAMPDIFF(MICROSECOND, %s, %s) / 1000000", from, to)
}
func (mariaDB) DeleteMessages(where, orderBy string) string {
	order := ""
	if orderBy != "" {
		order = " ORDER BY " + orderBy
	}
	return "DELETE FROM messages WHERE " + where + order + " LIMIT :limit"
}
func (mariaDB) InsertOrIgnore(table string, columns, _ []string) string {
	return "INSERT IGNORE INTO " + table + " " + values(columns)
}
func (mariaDB) Upsert(table string, columns, _ []string, update []string) string {
	set := make([]string, len(update))
	for i, c := range update {
		set[i] = c + " = VALUES(" + c + ")"
	}
	return "INSERT INTO " + table + " " + values(columns) + " ON DUPLICATE KEY UPDATE " + strings.Join(set, ", ")
}

// MySQL 8.0.21 and newer: MariaDB's SQL, but MySQL deprecates VALUES() in an upsert.
type mySQL struct{ mariaDB }

func (mySQL) Name() string { return "mysql" }
func (mySQL) Upsert(table string, columns, _ []string, update []string) string {
	set := make([]string, len(update))
	for i, c := range update {
		set[i] = c + " = new." + c
	}
	return "INSERT INTO " + table + " " + values(columns) + " AS new ON DUPLICATE KEY UPDATE " + strings.Join(set, ", ")
}

// PostgreSQL 13 and newer. A parameter inside a json builder has no type PostgreSQL can
// infer, so it is cast to text.
type postgreSQL struct{}

func (postgreSQL) Name() string                { return "postgresql" }
func (postgreSQL) SkipLocked() string          { return " FOR UPDATE SKIP LOCKED" }
func (postgreSQL) ClaimsInOneStatement() bool  { return false }
func (postgreSQL) ReplaysInOneStatement() bool { return false }
func (postgreSQL) JSONValue(c, k string) string {
	return fmt.Sprintf("(CAST(%s AS jsonb) ->> '%s')", c, k)
}
func (postgreSQL) JSONMember(c, p string) string {
	return fmt.Sprintf("(CAST(%s AS jsonb) ->> CAST(%s AS TEXT))", c, p)
}
func (postgreSQL) JSONMemberArgument(n string) string { return n }
func (postgreSQL) JSONText(c string) string           { return "CAST(" + c + " AS TEXT)" }
func (postgreSQL) JSONObject(m [][2]string) string {
	return "CAST(json_build_object(" + jsonObjectParts(m) + ") AS TEXT)"
}
func (postgreSQL) JSONBool(v bool) string {
	if v {
		return "TRUE"
	}
	return "FALSE"
}
func (postgreSQL) JSONString(p string) string { return "CAST(" + p + " AS TEXT)" }
func (postgreSQL) JSONWith(c, n, v string) string {
	return fmt.Sprintf("CAST(COALESCE(CAST(%s AS jsonb), '{}'::jsonb) || jsonb_build_object(%s, %s) AS json)", c, n, v)
}
func (postgreSQL) Least(a, b string) string { return fmt.Sprintf("LEAST(%s, %s)", a, b) }
func (postgreSQL) Power(b, e string) string {
	return fmt.Sprintf("POWER(CAST(%s AS DOUBLE PRECISION), %s)", b, e)
}
func (postgreSQL) PlusSeconds(t, s string) string {
	return fmt.Sprintf("CAST(%s AS TIMESTAMP(3)) + (%s) * INTERVAL '1 second'", t, s)
}

// EpochSeconds: of a timestamp without time zone, the nominal seconds, whatever the zone.
func (postgreSQL) EpochSeconds(t string) string {
	return fmt.Sprintf("CAST(EXTRACT(EPOCH FROM %s) AS BIGINT)", t)
}
func (postgreSQL) SecondsBetween(from, to string) string {
	return fmt.Sprintf("CAST(EXTRACT(EPOCH FROM (%s) - (%s)) AS DOUBLE PRECISION)", to, from)
}
func (postgreSQL) DeleteMessages(where, orderBy string) string { return deleteByID(where, orderBy) }
func (postgreSQL) InsertOrIgnore(table string, columns, key []string) string {
	return "INSERT INTO " + table + " " + values(columns) + " ON CONFLICT (" + strings.Join(key, ", ") + ") DO NOTHING"
}
func (postgreSQL) Upsert(table string, columns, key, update []string) string {
	return upsertOnConflict(table, columns, key, update, "EXCLUDED")
}
func (postgreSQL) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

// SQLite 3.35 and newer, with its math functions: for one machine. It has no row locks,
// only one writer at a time, so a claim is one UPDATE ... RETURNING.
type sqlite struct{}

func (sqlite) Name() string                { return "sqlite" }
func (sqlite) SkipLocked() string          { return "" }
func (sqlite) ClaimsInOneStatement() bool  { return true }
func (sqlite) ReplaysInOneStatement() bool { return false }
func (sqlite) JSONValue(c, k string) string {
	return fmt.Sprintf("json_extract(%s, '$.%s')", c, k)
}
func (sqlite) JSONMember(c, p string) string      { return fmt.Sprintf("json_extract(%s, %s)", c, p) }
func (sqlite) JSONMemberArgument(n string) string { return `$."` + n + `"` }
func (sqlite) JSONText(c string) string           { return c }
func (sqlite) JSONObject(m [][2]string) string    { return "json_object(" + jsonObjectParts(m) + ")" }
func (sqlite) JSONBool(v bool) string {
	if v {
		return "json('true')"
	}
	return "json('false')"
}
func (sqlite) JSONString(p string) string { return p }
func (sqlite) JSONWith(c, n, v string) string {
	return fmt.Sprintf(`json_set(COALESCE(%s, '{}'), '$."' || %s || '"', %s)`, c, n, v)
}
func (sqlite) Least(a, b string) string { return fmt.Sprintf("MIN(%s, %s)", a, b) }
func (sqlite) Power(b, e string) string { return fmt.Sprintf("pow(%s, %s)", b, e) }
func (sqlite) PlusSeconds(t, s string) string {
	return fmt.Sprintf("strftime('%%Y-%%m-%%d %%H:%%M:%%f', %s, '+' || CAST(%s AS INTEGER) || ' seconds')", t, s)
}
func (sqlite) EpochSeconds(t string) string {
	return fmt.Sprintf("CAST(strftime('%%s', %s) AS INTEGER)", t)
}
func (sqlite) SecondsBetween(from, to string) string {
	return fmt.Sprintf("ROUND((julianday(%s) - julianday(%s)) * 86400.0, 3)", to, from)
}
func (sqlite) DeleteMessages(where, orderBy string) string { return deleteByID(where, orderBy) }
func (sqlite) InsertOrIgnore(table string, columns, key []string) string {
	return "INSERT INTO " + table + " " + values(columns) + " ON CONFLICT (" + strings.Join(key, ", ") + ") DO NOTHING"
}
func (sqlite) Upsert(table string, columns, key, update []string) string {
	return upsertOnConflict(table, columns, key, update, "excluded")
}
func (sqlite) Placeholder(int) string { return "?" }

func deleteByID(where, orderBy string) string {
	order := ""
	if orderBy != "" {
		order = " ORDER BY " + orderBy
	}
	return "DELETE FROM messages WHERE id IN (SELECT id FROM messages WHERE " + where + order + " LIMIT :limit)"
}

func upsertOnConflict(table string, columns, key, update []string, excluded string) string {
	set := make([]string, len(update))
	for i, c := range update {
		set[i] = c + " = " + excluded + "." + c
	}
	return "INSERT INTO " + table + " " + values(columns) + " ON CONFLICT (" + strings.Join(key, ", ") +
		") DO UPDATE SET " + strings.Join(set, ", ")
}
