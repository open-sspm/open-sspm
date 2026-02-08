package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var syncDiscoveryCmd = &cobra.Command{
	Use:   "sync-discovery",
	Short: "Run a one-off SaaS discovery sync against configured Okta/Entra connectors.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSyncDiscovery()
	},
}

func runSyncDiscovery() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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
	dbRunner.SetRunMode(registry.RunModeDiscovery)
	dbRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	runner := sync.NewBlockingRunOnceLockRunnerWithScope(locks, dbRunner, sync.RunOnceScopeNameDiscovery)

	syncErr := runner.RunOnce(ctx)
	if syncErr == nil {
		return nil
	}
	if errors.Is(syncErr, context.Canceled) {
		return &exitError{code: 130, err: syncErr, silent: true}
	}
	return &exitError{code: 1, err: syncErr, silent: false}
}
