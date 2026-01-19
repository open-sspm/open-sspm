package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	httpapp "github.com/open-sspm/open-sspm/internal/http"
	"github.com/open-sspm/open-sspm/internal/http/handlers"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run the HTTP server.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServe()
	},
}

func runServe() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	locks, err := sync.NewLockManager(pool, sync.LockManagerConfig{
		Mode:              cfg.SyncLockMode,
		InstanceID:        cfg.SyncLockInstanceID,
		TTL:               cfg.SyncLockTTL,
		HeartbeatInterval: cfg.SyncLockHeartbeatInterval,
		HeartbeatTimeout:  cfg.SyncLockHeartbeatTimeout,
	})
	if err != nil {
		return err
	}

	queries := gen.New(pool)

	if cfg.DevSeedAdmin {
		if err := maybeSeedDevAdmin(ctx, queries); err != nil {
			return err
		}
	}

	reg, err := buildConnectorRegistry(cfg)
	if err != nil {
		return err
	}

	dbRunner := sync.NewDBRunner(pool, reg)
	dbRunner.SetLockManager(locks)
	dbRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)

	var syncer handlers.SyncRunner
	if cfg.ResyncEnabled {
		switch strings.ToLower(strings.TrimSpace(cfg.ResyncMode)) {
		case "signal":
			syncer = sync.NewResyncSignalRunner(pool, locks)
		default:
			syncer = sync.NewTryRunOnceLockRunner(locks, dbRunner)
		}
	} else {
		syncer = nil
	}

	srv, err := httpapp.NewEchoServer(cfg, pool, queries, syncer, reg)
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	metricsServer, metricsErrCh := metrics.StartServer(ctx, cfg.MetricsAddr)
	go func() {
		slog.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.StartServer(httpServer); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	shutdown := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if metricsServer != nil {
			_ = metricsServer.Shutdown(shutdownCtx)
		}
		_ = srv.Shutdown(shutdownCtx)
	}

	if metricsErrCh == nil {
		select {
		case <-ctx.Done():
			shutdown()
			return nil
		case err := <-errCh:
			shutdown()
			return err
		}
	}

	select {
	case <-ctx.Done():
		shutdown()
		return nil
	case err := <-errCh:
		shutdown()
		return err
	case err := <-metricsErrCh:
		shutdown()
		return err
	}
}

func maybeSeedDevAdmin(ctx context.Context, q *gen.Queries) error {
	count, err := q.CountAuthUsers(ctx)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	hash, err := auth.HashPassword("admin")
	if err != nil {
		return err
	}

	_, err = q.CreateAuthUser(ctx, gen.CreateAuthUserParams{
		Email:        "admin@admin.com",
		PasswordHash: hash,
		Role:         auth.RoleAdmin,
		IsActive:     true,
	})
	if err != nil {
		return err
	}
	slog.Warn("seeded dev admin user (DEV_SEED_ADMIN=1)", "email", "admin@admin.com")
	return nil
}
