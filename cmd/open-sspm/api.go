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

	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	httpapp "github.com/open-sspm/open-sspm/internal/http"
	"github.com/open-sspm/open-sspm/internal/http/handlers"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/readmodels"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "Run the API and web UI HTTP server.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runAPI()
	},
}

func runAPI() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runtimeDeps, err := openRuntimeDependencies(ctx, cfg)
	if err != nil {
		return err
	}
	defer runtimeDeps.pool.Close()

	locks, err := sync.NewLockManager(runtimeDeps.pool, sync.LockManagerConfig{
		Mode:              cfg.SyncLockMode,
		InstanceID:        cfg.SyncLockInstanceID,
		TTL:               cfg.SyncLockTTL,
		HeartbeatInterval: cfg.SyncLockHeartbeatInterval,
		HeartbeatTimeout:  cfg.SyncLockHeartbeatTimeout,
	})
	if err != nil {
		return err
	}

	queries := runtimeDeps.queries
	jobStore := sync.NewSyncJobStore(runtimeDeps.pool)

	if cfg.DevSeedAdmin {
		if err := maybeSeedDevAdmin(ctx, queries); err != nil {
			return err
		}
	}

	reg, err := buildConnectorRegistry(cfg)
	if err != nil {
		return err
	}
	if err := rebuildStoredReadModels(ctx, runtimeDeps.pool, queries, cfg); err != nil {
		return err
	}

	fullDBRunner := sync.NewDBRunner(runtimeDeps.pool, reg)
	fullDBRunner.SetLockManager(locks)
	fullDBRunner.SetRunMode(registry.RunModeFull)
	fullDBRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	fullDBRunner.SetReadModelConfig(readmodels.RefreshConfigFromConfig(cfg))

	discoveryDBRunner := sync.NewDBRunner(runtimeDeps.pool, reg)
	discoveryDBRunner.SetLockManager(locks)
	discoveryDBRunner.SetRunMode(registry.RunModeDiscovery)
	discoveryDBRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	discoveryDBRunner.SetReadModelConfig(readmodels.RefreshConfigFromConfig(cfg))
	discoveryDBRunner.EnableDiscoveryMetricsRefresh()

	var syncer handlers.SyncRunner
	if cfg.ResyncEnabled {
		runners := []sync.Runner{}
		switch strings.ToLower(strings.TrimSpace(cfg.ResyncMode)) {
		case "signal":
			for _, mode := range apiResyncModes(cfg) {
				switch mode {
				case registry.RunModeFull:
					runners = append(runners, sync.NewResyncQueueRunnerWithPlanner(jobStore, fullDBRunner, registry.RunModeFull))
				case registry.RunModeDiscovery:
					runners = append(runners, sync.NewResyncQueueRunnerWithPlanner(jobStore, discoveryDBRunner, registry.RunModeDiscovery))
				}
			}
		default:
			for _, mode := range apiResyncModes(cfg) {
				switch mode {
				case registry.RunModeFull:
					runners = append(runners, sync.NewTryRunOnceLockRunnerWithScope(locks, fullDBRunner, sync.RunOnceScopeNameFull))
				case registry.RunModeDiscovery:
					runners = append(runners, sync.NewTryRunOnceLockRunnerWithScope(locks, discoveryDBRunner, sync.RunOnceScopeNameDiscovery))
				}
			}
		}
		if len(runners) > 0 {
			syncer = sync.NewCompositeRunner(runners...)
		}
	} else {
		syncer = nil
	}

	srv, err := httpapp.NewEchoServer(cfg, runtimeDeps.pool, queries, syncer, reg, runtimeDeps.mailer)
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	metricsServer, metricsErrCh := metrics.StartServer(ctx, cfg.MetricsAddr, backgroundMetricsRefresh(queries))
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

func apiResyncModes(cfg config.Config) []registry.RunMode {
	if !cfg.ResyncEnabled {
		return nil
	}
	modes := make([]registry.RunMode, 0, 2)
	if cfg.SyncFullEnabled {
		modes = append(modes, registry.RunModeFull)
	}
	if cfg.SyncDiscoveryEnabled {
		modes = append(modes, registry.RunModeDiscovery)
	}
	if len(modes) == 0 {
		return nil
	}
	return modes
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
