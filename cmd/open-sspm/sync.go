package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/readmodels"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Run one-off full sync followed by SaaS discovery sync (if configured).",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSync()
	},
}

func runSync() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runtimeDeps, err := openRuntimeDependencies(ctx, cfg)
	if err != nil {
		return err
	}
	defer runtimeDeps.pool.Close()

	reg, err := buildConnectorRegistry(cfg)
	if err != nil {
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

	fullDBRunner := sync.NewDBRunner(runtimeDeps.pool, reg)
	fullDBRunner.SetReporter(&sync.LogReporter{})
	fullDBRunner.SetLockManager(locks)
	fullDBRunner.SetRunMode(registry.RunModeFull)
	fullDBRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	fullDBRunner.SetReadModelConfig(readmodels.RefreshConfigFromConfig(cfg))
	fullRunner := sync.NewBlockingRunOnceLockRunnerWithScope(locks, fullDBRunner, sync.RunOnceScopeNameFull)

	discoveryDBRunner := sync.NewDBRunner(runtimeDeps.pool, reg)
	discoveryDBRunner.SetReporter(&sync.LogReporter{})
	discoveryDBRunner.SetLockManager(locks)
	discoveryDBRunner.SetRunMode(registry.RunModeDiscovery)
	discoveryDBRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	discoveryDBRunner.SetReadModelConfig(readmodels.RefreshConfigFromConfig(cfg))
	discoveryDBRunner.EnableDiscoveryMetricsRefresh()
	runners := []sync.Runner{fullRunner}
	if cfg.SyncDiscoveryEnabled {
		discoveryRunner := sync.NewBlockingRunOnceLockRunnerWithScope(locks, discoveryDBRunner, sync.RunOnceScopeNameDiscovery)
		runners = append(runners, discoveryRunner)
	}

	runner := sync.NewCompositeRunner(runners...)

	syncErr := runner.RunOnce(ctx)
	if syncErr == nil {
		return nil
	}
	if errors.Is(syncErr, context.Canceled) {
		return &exitError{code: 130, err: syncErr, silent: true}
	}
	return &exitError{code: 1, err: syncErr, silent: false}
}
