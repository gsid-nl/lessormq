package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers "pgx"
	sqlite3 "modernc.org/sqlite"
	sqlite3lib "modernc.org/sqlite/lib"
)

// DB is the database with its dialect. Every query takes :named parameters, which Bind
// turns into the driver's placeholders; a []string value becomes a list for IN (...).
type DB struct {
	*sql.DB
	D Dialect
	// Observe, when set, sees every query as written, before its parameters are bound:
	// for tests that check what is asked of the database.
	Observe func(query string)
}

// Args are the values of a query's :named parameters.
type Args map[string]any

// Querier is a *sql.DB or a *sql.Tx.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Options size the connection pool.
type Options struct {
	MaxOpen int
}

// Open connects to the database a Doctrine-style URL names, as LessorMQ has always read
// DATABASE_QUEUE_URL: mysql://, postgresql:// (or postgres://, pgsql://) and sqlite:///.
func Open(ctx context.Context, rawURL string, opts Options) (*DB, error) {
	if opts.MaxOpen <= 0 {
		opts.MaxOpen = 32
	}
	switch {
	case strings.HasPrefix(rawURL, "sqlite:"):
		return openSQLite(rawURL)
	case strings.HasPrefix(rawURL, "postgres://"), strings.HasPrefix(rawURL, "postgresql://"),
		strings.HasPrefix(rawURL, "pgsql://"):
		return openPostgreSQL(ctx, rawURL, opts)
	case strings.HasPrefix(rawURL, "mysql://"), strings.HasPrefix(rawURL, "mariadb://"):
		return openMySQL(ctx, rawURL, opts)
	}
	return nil, fmt.Errorf("DATABASE_QUEUE_URL must start with mysql://, postgresql:// or sqlite:///")
}

func openMySQL(ctx context.Context, rawURL string, opts Options) (*DB, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_QUEUE_URL: %w", err)
	}
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = u.Host
	if u.Port() == "" {
		cfg.Addr = u.Hostname() + ":3306"
	}
	cfg.User = u.User.Username()
	cfg.Passwd, _ = u.User.Password()
	cfg.DBName = strings.TrimPrefix(u.Path, "/")
	// One round trip per query instead of prepare, execute and close.
	cfg.InterpolateParams = true
	cfg.ParseTime = false
	cfg.Params = map[string]string{}
	// utf8mb4, as the schema is; the collation names the connection's character set.
	cfg.Collation = "utf8mb4_unicode_ci"
	version := strings.ToLower(u.Query().Get("serverVersion"))

	maria := strings.Contains(version, "mariadb") || strings.HasPrefix(rawURL, "mariadb://")
	if version == "" {
		probe, err := connector(cfg)
		if err != nil {
			return nil, err
		}
		var v string
		err = probe.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v)
		probe.Close()
		if err != nil {
			return nil, fmt.Errorf("database: %w", err)
		}
		maria = strings.Contains(strings.ToLower(v), "mariadb")
	}
	// InnoDB defaults to REPEATABLE READ, whose gap locks made claims and the range writers
	// deadlock. Every connection is READ COMMITTED instead, without a statement per
	// transaction to say so.
	var d Dialect = mySQL{}
	if maria {
		d = mariaDB{}
		cfg.Params["tx_isolation"] = "'READ-COMMITTED'"
	} else {
		cfg.Params["transaction_isolation"] = "'READ-COMMITTED'"
	}
	pool, err := connector(cfg)
	if err != nil {
		return nil, err
	}
	pool.SetMaxOpenConns(opts.MaxOpen)
	pool.SetMaxIdleConns(opts.MaxOpen)
	pool.SetConnMaxLifetime(5 * time.Minute)
	return &DB{DB: pool, D: d}, nil
}

func connector(cfg *mysql.Config) (*sql.DB, error) {
	c, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_QUEUE_URL: %w", err)
	}
	return sql.OpenDB(c), nil
}

func openPostgreSQL(_ context.Context, rawURL string, opts Options) (*DB, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_QUEUE_URL: %w", err)
	}
	u.Scheme = "postgres"
	// Doctrine's parameters, which PostgreSQL would take for settings of its own.
	q := u.Query()
	q.Del("serverVersion")
	q.Del("charset")
	if q.Get("sslmode") == "" {
		q.Set("sslmode", "prefer")
	}
	u.RawQuery = q.Encode()
	pool, err := sql.Open("pgx", u.String())
	if err != nil {
		return nil, fmt.Errorf("DATABASE_QUEUE_URL: %w", err)
	}
	pool.SetMaxOpenConns(opts.MaxOpen)
	pool.SetMaxIdleConns(opts.MaxOpen)
	pool.SetConnMaxLifetime(5 * time.Minute)
	return &DB{DB: pool, D: postgreSQL{}}, nil
}

func openSQLite(rawURL string) (*DB, error) {
	// sqlite:///relative.db and sqlite:////absolute/path.db, as Doctrine reads them.
	path := strings.TrimPrefix(rawURL, "sqlite://")
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return nil, fmt.Errorf("DATABASE_QUEUE_URL: sqlite:/// needs a file")
	}
	dsn := "file:" + path + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	pool, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_QUEUE_URL: %w", err)
	}
	// One writer at a time: one connection, so no statement ever waits on a lock.
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	return &DB{DB: pool, D: sqlite{}}, nil
}

// Exec runs a statement and returns the rows it changed.
func (d *DB) Exec(ctx context.Context, q Querier, query string, args Args) (int64, error) {
	s, a := d.Bind(query, args)
	res, err := q.ExecContext(ctx, s, a...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Query runs a query.
func (d *DB) Query(ctx context.Context, q Querier, query string, args Args) (*sql.Rows, error) {
	s, a := d.Bind(query, args)
	return q.QueryContext(ctx, s, a...)
}

// Row runs a query for one row.
func (d *DB) Row(ctx context.Context, q Querier, query string, args Args) *sql.Row {
	s, a := d.Bind(query, args)
	return q.QueryRowContext(ctx, s, a...)
}

type segment struct {
	text  string
	param string
}

var templates sync.Map // query => []segment

var paramName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*`)

func parse(query string) []segment {
	if t, ok := templates.Load(query); ok {
		return t.([]segment)
	}
	var out []segment
	var b strings.Builder
	for i := 0; i < len(query); i++ {
		c := query[i]
		if c == '\'' {
			// A string literal is copied as it is: no parameters in there.
			j := i + 1
			for j < len(query) {
				if query[j] == '\'' {
					if j+1 < len(query) && query[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			if j >= len(query) {
				j = len(query) - 1
			}
			b.WriteString(query[i : j+1])
			i = j
			continue
		}
		if c == ':' && i+1 < len(query) && query[i+1] == ':' {
			b.WriteString("::")
			i++
			continue
		}
		if c == ':' && (i == 0 || query[i-1] != ':') {
			if m := paramName.FindString(query[i+1:]); m != "" {
				out = append(out, segment{text: b.String(), param: m})
				b.Reset()
				i += len(m)
				continue
			}
		}
		b.WriteByte(c)
	}
	out = append(out, segment{text: b.String()})
	templates.Store(query, out)
	return out
}

// Bind turns :named parameters into the driver's placeholders.
func (d *DB) Bind(query string, args Args) (string, []any) {
	if d.Observe != nil {
		d.Observe(query)
	}
	segs := parse(query)
	var b strings.Builder
	out := make([]any, 0, len(args))
	for _, s := range segs {
		b.WriteString(s.text)
		if s.param == "" {
			continue
		}
		v, ok := args[s.param]
		if !ok {
			panic("db: no value for :" + s.param + " in " + query)
		}
		if list, ok := v.([][]byte); ok {
			if len(list) == 0 {
				b.WriteString("NULL")
				continue
			}
			for i, item := range list {
				if i > 0 {
					b.WriteString(", ")
				}
				out = append(out, item)
				b.WriteString(d.D.Placeholder(len(out)))
			}
			continue
		}
		if list, ok := v.([]string); ok {
			if len(list) == 0 {
				// IN () is not SQL; a list that matches nothing is.
				b.WriteString("NULL")
				continue
			}
			for i, item := range list {
				if i > 0 {
					b.WriteString(", ")
				}
				out = append(out, item)
				b.WriteString(d.D.Placeholder(len(out)))
			}
			continue
		}
		out = append(out, v)
		b.WriteString(d.D.Placeholder(len(out)))
	}
	return b.String(), out
}

// IsUniqueViolation tells whether an insert or update ran into a unique index.
func IsUniqueViolation(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1062
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code == "23505"
	}
	var se *sqlite3.Error
	if errors.As(err, &se) {
		return se.Code() == sqlite3lib.SQLITE_CONSTRAINT_UNIQUE || se.Code() == sqlite3lib.SQLITE_CONSTRAINT_PRIMARYKEY
	}
	return false
}

// IsDeadlock tells whether a statement was picked as a deadlock victim or timed out on a
// lock: it changed nothing and may run again.
func IsDeadlock(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1213 || me.Number == 1205
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code == "40P01" || pe.Code == "40001" || pe.Code == "55P03"
	}
	var se *sqlite3.Error
	if errors.As(err, &se) {
		return se.Code()&0xff == sqlite3lib.SQLITE_BUSY
	}
	return false
}
