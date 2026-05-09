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
	oktaingest "github.com/open-sspm/open-sspm/internal/ingest/okta"
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

var serveCmd = &cobra.Command{
	Use:        "serve",
	Short:      "Run the API and web UI HTTP server.",
	Deprecated: "use api instead",
	Args:       cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runAPIWithOptions(apiRunOptions{StartIngestWorker: true})
	},
}

type apiRunOptions struct {
	StartIngestWorker bool
}

func runAPI() error {
	return runAPIWithOptions(apiRunOptions{})
}

func runAPIWithOptions(opts apiRunOptions) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if opts.StartIngestWorker {
		slog.Warn("open-sspm serve is deprecated; use open-sspm api plus open-sspm worker-ingest")
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
			runners = append(runners, sync.NewResyncQueueRunnerWithPlanner(jobStore, fullDBRunner, registry.RunModeFull))
			if cfg.SyncDiscoveryEnabled {
				runners = append(runners, sync.NewResyncQueueRunnerWithPlanner(jobStore, discoveryDBRunner, registry.RunModeDiscovery))
			}
		default:
			runners = append(runners, sync.NewTryRunOnceLockRunnerWithScope(locks, fullDBRunner, sync.RunOnceScopeNameFull))
			if cfg.SyncDiscoveryEnabled {
				runners = append(runners, sync.NewTryRunOnceLockRunnerWithScope(locks, discoveryDBRunner, sync.RunOnceScopeNameDiscovery))
			}
		}
		syncer = sync.NewCompositeRunner(runners...)
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
	metricsServer, metricsErrCh := metrics.StartServer(ctx, cfg.MetricsAddr, discoveryMetricsRefresh(queries))
	if opts.StartIngestWorker && cfg.SyncDiscoveryEnabled {
		go func() {
			if err := oktaingest.RunLoop(ctx, queries, runtimeDeps.pool, oktaingest.DefaultConfig()); err != nil {
				errCh <- err
			}
		}()
	}
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
