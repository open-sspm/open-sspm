package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/readmodels"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var workerDiscoveryCmd = &cobra.Command{
	Use:   "worker-discovery",
	Short: "Run the background SaaS discovery sync loop.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorkerDiscovery()
	},
}

func runWorkerDiscovery() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !cfg.SyncDiscoveryEnabled {
		return errors.New("SYNC_DISCOVERY_ENABLED must be true to run the discovery worker")
	}
	if cfg.SyncDiscoveryInterval <= 0 {
		return errors.New("SYNC_DISCOVERY_INTERVAL must be > 0 to run the discovery worker")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runtimeDeps, err := openRuntimeDependencies(ctx, cfg)
	if err != nil {
		return err
	}
	defer runtimeDeps.pool.Close()
	queries := runtimeDeps.queries

	reg, err := buildConnectorRegistry(cfg)
	if err != nil {
		return err
	}
	if err := rebuildStoredReadModels(ctx, runtimeDeps.pool, queries, cfg); err != nil {
		return err
	}

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
	// Queue-consumer coordination is long-lived; force lease locks here so
	// advisory mode does not pin an extra pool connection for the worker lifetime.
	consumerLocks, err := sync.NewLockManager(runtimeDeps.pool, sync.LockManagerConfig{
		Mode:              sync.LockModeLease,
		InstanceID:        cfg.SyncLockInstanceID,
		TTL:               cfg.SyncLockTTL,
		HeartbeatInterval: cfg.SyncLockHeartbeatInterval,
		HeartbeatTimeout:  cfg.SyncLockHeartbeatTimeout,
	})
	if err != nil {
		return err
	}

	dbRunner := sync.NewDBRunner(runtimeDeps.pool, reg)
	dbRunner.SetReporter(&sync.LogReporter{})
	dbRunner.SetLockManager(locks)
	dbRunner.SetRunMode(registry.RunModeDiscovery)
	dbRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	dbRunner.SetReadModelConfig(readmodels.RefreshConfigFromConfig(cfg))
	dbRunner.EnableDiscoveryMetricsRefresh()
	backoffMax := cfg.SyncFailureBackoffMax
	if backoffMax <= 0 {
		backoffMax = cfg.SyncDiscoveryInterval * 10
	}
	dbRunner.SetRunPolicy(sync.RunPolicy{
		IntervalByKind: map[string]time.Duration{
			"okta_discovery":             cfg.SyncDiscoveryInterval,
			"entra_discovery":            cfg.SyncDiscoveryInterval,
			"google_workspace_discovery": cfg.SyncDiscoveryInterval,
		},
		FailureBackoffBase:   cfg.SyncDiscoveryInterval,
		FailureBackoffMax:    backoffMax,
		RecentFinishedRunCap: 10,
	})
	executionRunner := sync.NewBlockingRunOnceLockRunnerWithScope(locks, dbRunner, sync.RunOnceScopeNameDiscovery)
	jobStore := sync.NewSyncJobStore(runtimeDeps.pool)
	wakeups := make(chan struct{}, 1)
	jobConsumer := sync.NewSyncJobConsumer(jobStore, consumerLocks, executionRunner, sync.SyncJobConsumerConfig{
		Mode:              registry.RunModeDiscovery,
		PollInterval:      30 * time.Second,
		LeaseTTL:          cfg.SyncLockTTL,
		HeartbeatInterval: cfg.SyncLockHeartbeatInterval,
		RetryBaseDelay:    cfg.SyncDiscoveryInterval,
		RetryMaxDelay:     backoffMax,
		Wakeups:           wakeups,
	})

	slog.Info("discovery sync worker started", "interval", cfg.SyncDiscoveryInterval)
	go func() {
		if err := jobConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("discovery sync job consumer failed", "err", err)
		}
	}()
	go func() {
		if err := sync.ListenForSyncJobSignals(ctx, runtimeDeps.pool, sync.SyncJobNotifyChannelForMode(registry.RunModeDiscovery), wakeups); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("discovery sync job listener failed", "err", err)
		}
	}()

	scheduler := sync.Scheduler{Runner: executionRunner, Interval: cfg.SyncDiscoveryInterval}
	metricsServer, metricsErrCh := metrics.StartServer(ctx, cfg.MetricsAddr, discoveryMetricsRefresh(queries))
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
