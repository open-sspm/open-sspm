package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run the background sync loop.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorker()
	},
}

func runWorker() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.SyncInterval <= 0 {
		return errors.New("SYNC_INTERVAL must be > 0 to run the worker")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	reg, err := buildConnectorRegistry(cfg)
	if err != nil {
		return err
	}

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

	dbRunner := sync.NewDBRunner(pool, reg)
	dbRunner.SetReporter(&sync.LogReporter{})
	dbRunner.SetLockManager(locks)
	dbRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	backoffMax := cfg.SyncFailureBackoffMax
	if backoffMax <= 0 {
		backoffMax = cfg.SyncInterval * 10
	}
	dbRunner.SetRunPolicy(sync.RunPolicy{
		IntervalByKind: map[string]time.Duration{
			"okta":    cfg.SyncOktaInterval,
			"entra":   cfg.SyncEntraInterval,
			"github":  cfg.SyncGitHubInterval,
			"datadog": cfg.SyncDatadogInterval,
			"aws":     cfg.SyncAWSInterval,
		},
		FailureBackoffBase:   cfg.SyncInterval,
		FailureBackoffMax:    backoffMax,
		RecentFinishedRunCap: 10,
	})
	runner := sync.NewBlockingRunOnceLockRunner(locks, dbRunner)

	slog.Info("sync worker started", "interval", cfg.SyncInterval)
	triggers := make(chan struct{}, 1)
	go func() {
		if err := sync.ListenForResyncRequests(ctx, pool, triggers); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("resync listener failed", "err", err)
		}
	}()
	scheduler := sync.Scheduler{Runner: runner, Interval: cfg.SyncInterval, Trigger: triggers}
	metricsServer, metricsErrCh := metrics.StartServer(ctx, cfg.MetricsAddr)
	doneCh := make(chan struct{})
	go func() {
		scheduler.Run(ctx)
		close(doneCh)
	}()

	var metricsErr error
	schedulerDone := false
	if metricsErrCh == nil {
		select {
		case <-ctx.Done():
		case <-doneCh:
			schedulerDone = true
		}
	} else {
		select {
		case <-ctx.Done():
		case err := <-metricsErrCh:
			if err != nil {
				metricsErr = err
				slog.Error("metrics server failed", "err", err)
				stop()
			}
		case <-doneCh:
			schedulerDone = true
		}
	}

	if !schedulerDone {
		<-doneCh
	}
	if metricsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}
	if metricsErr != nil {
		return metricsErr
	}
	return nil
}
