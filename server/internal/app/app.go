// Package app is the LessorMQ server: the message protocol, the queues and their
// settings, tenants and keys, and the figures, over the database and the coordination
// state.
package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/gsid-nl/lessormq/server/internal/coord"
	"github.com/gsid-nl/lessormq/server/internal/db"
)

// Config is what an installation sets.
type Config struct {
	// LeaseSweepSeconds is how often, at most, a queue's expired leases are ended.
	LeaseSweepSeconds int
	// MaxWaitingClaims is how many claims may wait their full wait_seconds at once on this
	// node (0: no limit).
	MaxWaitingClaims int
}

// App is the server.
type App struct {
	DB    *db.DB
	Coord coord.Coord
	Log   *slog.Logger
	Cfg   Config

	waiting  waitingClaims
	stopping chan struct{}
	stopOnce sync.Once
}

func New(database *db.DB, c coord.Coord, log *slog.Logger, cfg Config) *App {
	if cfg.LeaseSweepSeconds <= 0 {
		cfg.LeaseSweepSeconds = 60
	}
	return &App{DB: database, Coord: c, Log: log, Cfg: cfg, stopping: make(chan struct{})}
}

// Stop ends the waits of waiting claims, which then answer that there is nothing: for a
// node that shuts down.
func (a *App) Stop() { a.stopOnce.Do(func() { close(a.stopping) }) }

func (a *App) stopped() bool {
	select {
	case <-a.stopping:
		return true
	default:
		return false
	}
}

// waitContext is ctx, also done when the node stops.
func (a *App) waitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-a.stopping:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func now() time.Time { return time.Now().UTC() }

// Health checks the database and, when there is one, Redis.
func (a *App) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var one int
	if err := a.DB.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}
	return a.Coord.Ping(ctx)
}

// AsProblem is the problem an error is for the client: itself when it is one, 503 when
// the database could not be reached, 500 otherwise.
func AsProblem(err error) *Problem {
	var p *Problem
	if errors.As(err, &p) {
		return p
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Unavailable("The request was cancelled or took too long.")
	}
	return nil
}
