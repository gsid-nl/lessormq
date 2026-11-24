// Command lessormq is the LessorMQ server and its maintenance commands.
//
//	lessormq [serve]                     the HTTP server (migrates the schema first)
//	lessormq migrate                     only bring the schema up to date
//	lessormq operator-key:create         make an operator key; prints the secret last
//	lessormq purge-old-messages          delete finished messages past their retention
//	lessormq tenants:reap                delete the data of tenants marked for deletion
//	lessormq healthcheck                 exit 0 when this node's /healthz answers 200
//
// Configuration comes from the environment: DATABASE_QUEUE_URL (required), REDIS_URL
// (only for several nodes), LISTEN (default :8080), LEASE_SWEEP_SECONDS, MAX_WAITING_CLAIMS,
// CORS_ALLOW_ORIGIN, DB_MAX_CONNECTIONS, MIGRATE (0 to skip migrating on start), LOG_LEVEL.
// A file of the same NAME=value lines can set them too: --config FILE, or
// /etc/lessormq/lessormq.conf when it exists; the environment overrides the file.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/api"
	"github.com/gsid-nl/lessormq/server/internal/app"
	"github.com/gsid-nl/lessormq/server/internal/coord"
	"github.com/gsid-nl/lessormq/server/internal/db"
)

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	args, err := configure(os.Args[1:])
	log := logger()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
	command := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}
	switch command {
	case "serve":
		err = serve(log)
	case "migrate", "doctrine:migrations:migrate":
		err = withApp(log, false, func(ctx context.Context, a *app.App) error { return a.DB.Migrate(ctx) })
	case "operator-key:create", "lessormq:operator-key:create":
		err = createOperatorKey(log, args)
	case "purge-old-messages", "app:purge-old-messages":
		err = purge(log, args)
	case "tenants:reap", "lessormq:tenants:reap":
		err = reap(log, args)
	case "healthcheck":
		err = healthcheck()
	case "version", "--version":
		fmt.Println(version)
	case "help", "-h", "--help":
		fmt.Println("lessormq [--config FILE] [serve|migrate|operator-key:create|purge-old-messages|tenants:reap|healthcheck|version]")
	default:
		err = fmt.Errorf("unknown command %q", command)
	}
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}

// healthcheck asks this node's /healthz, for a container's health check (the image has no
// shell or curl).
func healthcheck() error {
	listen := os.Getenv("LISTEN")
	if listen == "" {
		listen = ":8080"
	}
	host, port, _ := strings.Cut(listen, ":")
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get("http://" + host + ":" + port + "/healthz")
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz answered %d", res.StatusCode)
	}
	return nil
}

func logger() *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func open(ctx context.Context, log *slog.Logger) (*app.App, error) {
	url := os.Getenv("DATABASE_QUEUE_URL")
	if url == "" {
		return nil, errors.New("set DATABASE_QUEUE_URL")
	}
	database, err := db.Open(ctx, url, db.Options{MaxOpen: envInt("DB_MAX_CONNECTIONS", 32)})
	if err != nil {
		return nil, err
	}
	var c coord.Coord = coord.NewMemory()
	if r := os.Getenv("REDIS_URL"); r != "" {
		rc, err := coord.NewRedis(ctx, r, log)
		if err != nil {
			return nil, fmt.Errorf("REDIS_URL: %w", err)
		}
		c = rc
	}
	return app.New(database, c, log, app.Config{
		LeaseSweepSeconds: envInt("LEASE_SWEEP_SECONDS", 60),
		MaxWaitingClaims:  envInt("MAX_WAITING_CLAIMS", 0),
	}), nil
}

func withApp(log *slog.Logger, _ bool, fn func(context.Context, *app.App) error) error {
	ctx := context.Background()
	a, err := open(ctx, log)
	if err != nil {
		return err
	}
	defer a.DB.Close()
	defer a.Coord.Close()
	return fn(ctx, a)
}

func serve(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	a, err := open(ctx, log)
	if err != nil {
		return err
	}
	defer a.DB.Close()
	defer a.Coord.Close()
	if os.Getenv("MIGRATE") != "0" {
		if err := migrateWithRetry(ctx, a, log); err != nil {
			return err
		}
	}
	origin := os.Getenv("CORS_ALLOW_ORIGIN")
	if origin == "" {
		origin = `^https?://(localhost|127\.0\.0\.1)(:[0-9]+)?$`
	}
	cors, err := regexp.Compile("(?i)" + origin)
	if err != nil {
		return fmt.Errorf("CORS_ALLOW_ORIGIN: %w", err)
	}
	listen := os.Getenv("LISTEN")
	if listen == "" {
		listen = ":8080"
	}
	srv := &http.Server{
		Addr:              listen,
		Handler:           (&api.Server{App: a, Log: log, CORS: cors}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errs := make(chan error, 1)
	go func() { errs <- srv.ListenAndServe() }()
	log.Info("LessorMQ serving", "listen", listen, "version", version, "database", a.DB.D.Name(),
		"shared_state", a.Coord.Shared())
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	log.Info("Shutting down")
	// Waiting claims answer 204 at once; everything else finishes.
	a.Stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}

// migrateWithRetry waits for a database that is still starting.
func migrateWithRetry(ctx context.Context, a *app.App, log *slog.Logger) error {
	deadline := time.Now().Add(time.Minute)
	for {
		err := a.DB.Migrate(ctx)
		if err == nil || time.Now().After(deadline) || strings.Contains(err.Error(), "older schema") {
			return err
		}
		log.Warn("Database not ready, retrying", "error", err.Error())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func createOperatorKey(log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("operator-key:create", flag.ContinueOnError)
	name := fs.String("name", "operator", "what the key is for")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return withApp(log, true, func(ctx context.Context, a *app.App) error {
		if err := a.DB.Migrate(ctx); err != nil {
			return err
		}
		id, secret, err := a.CreateOperatorKey(ctx, *name)
		if err != nil {
			return err
		}
		fmt.Printf("Operator key %s (%s) created. Store the secret now; it is not shown again:\n", id, *name)
		fmt.Println(secret)
		return nil
	})
}

func purge(log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("purge-old-messages", flag.ContinueOnError)
	hours := fs.Int64("hours", -1, "ignore the queue settings and purge everything finished more than this many hours ago")
	batch := fs.Int("batch-size", 1000, "rows deleted per statement")
	tenant := fs.String("tenant", "", "only purge this tenant")
	queue := fs.String("queue", "", "only purge this queue")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var h *int64
	if *hours >= 0 {
		h = hours
	}
	return withApp(log, true, func(ctx context.Context, a *app.App) error {
		total, err := a.PurgeOld(ctx, h, max(1, *batch), *tenant, *queue, func(s string) { fmt.Println(s) })
		if err != nil {
			return err
		}
		fmt.Printf("Done. Purged %d messages.\n", total)
		return nil
	})
}

func reap(log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("tenants:reap", flag.ContinueOnError)
	batch := fs.Int("batch-size", 1000, "messages deleted per statement")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return withApp(log, true, func(ctx context.Context, a *app.App) error {
		return a.ReapTenants(ctx, max(1, *batch), func(s string) { fmt.Println(s) })
	})
}
