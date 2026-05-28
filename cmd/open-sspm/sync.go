package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/readmodels"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var syncLaneFlag string

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Run one-off sync lanes.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSync(syncLaneFlag)
	},
}

func init() {
	syncCmd.Flags().StringVar(&syncLaneFlag, "lane", "all", "sync lane to run: all, full, discovery")
}

func runSync(laneName string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	modes, err := syncRunModesForLane(laneName, cfg)
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

	runners := make([]sync.Runner, 0, len(modes))
	for _, mode := range modes {
		runner, err := oneOffSyncRunnerForMode(runtimeDeps, cfg, reg, locks, mode)
		if err != nil {
			return err
		}
		runners = append(runners, runner)
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

func syncRunModesForLane(laneName string, cfg config.Config) ([]registry.RunMode, error) {
	switch strings.ToLower(strings.TrimSpace(laneName)) {
	case "", "all":
		modes := []registry.RunMode{registry.RunModeFull}
		if cfg.SyncDiscoveryEnabled {
			modes = append(modes, registry.RunModeDiscovery)
		}
		return modes, nil
	case "full":
		return []registry.RunMode{registry.RunModeFull}, nil
	case "discovery":
		if !cfg.SyncDiscoveryEnabled {
			return nil, errors.New("SYNC_DISCOVERY_ENABLED must be true to run discovery sync")
		}
		return []registry.RunMode{registry.RunModeDiscovery}, nil
	default:
		return nil, fmt.Errorf("unknown sync lane %q", laneName)
	}
}

func oneOffSyncRunnerForMode(deps *runtimeDependencies, cfg config.Config, reg *registry.ConnectorRegistry, locks sync.LockManager, mode registry.RunMode) (sync.Runner, error) {
	dbRunner := sync.NewDBRunner(deps.pool, reg)
	dbRunner.SetReporter(&sync.LogReporter{})
	dbRunner.SetLockManager(locks)
	dbRunner.SetRunMode(mode)
	dbRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	dbRunner.SetReadModelConfig(readmodels.RefreshConfigFromConfig(cfg))

	switch mode {
	case registry.RunModeFull:
		return sync.NewBlockingRunOnceLockRunnerWithScope(locks, dbRunner, sync.RunOnceScopeNameFull), nil
	case registry.RunModeDiscovery:
		dbRunner.EnableDiscoveryMetricsRefresh()
		return sync.NewBlockingRunOnceLockRunnerWithScope(locks, dbRunner, sync.RunOnceScopeNameDiscovery), nil
	default:
		return nil, fmt.Errorf("unsupported one-off sync mode %q", mode)
	}
}
