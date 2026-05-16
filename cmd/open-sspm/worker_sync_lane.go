package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/readmodels"
	"github.com/open-sspm/open-sspm/internal/sync"
)

const defaultSyncWorkerConsumerPollInterval = 30 * time.Second

type syncWorkerLane struct {
	name                   string
	mode                   registry.RunMode
	interval               func(config.Config) time.Duration
	scopeName              string
	consumerPollInterval   time.Duration
	intervalByKind         func(config.Config) map[string]time.Duration
	enableDiscoveryMetrics bool
	notifyChannel          string
	requireEnabled         func(config.Config) error
}

func fullSyncLane() syncWorkerLane {
	return syncWorkerLane{
		name:                 "full",
		mode:                 registry.RunModeFull,
		interval:             func(cfg config.Config) time.Duration { return cfg.SyncInterval },
		scopeName:            sync.RunOnceScopeNameFull,
		consumerPollInterval: defaultSyncWorkerConsumerPollInterval,
		intervalByKind: func(cfg config.Config) map[string]time.Duration {
			return map[string]time.Duration{
				"okta":             effectiveConnectorInterval(cfg.SyncOktaInterval, cfg.SyncInterval),
				"entra":            effectiveConnectorInterval(cfg.SyncEntraInterval, cfg.SyncInterval),
				"google_workspace": effectiveConnectorInterval(cfg.SyncGoogleWorkspaceInterval, cfg.SyncInterval),
				"github":           effectiveConnectorInterval(cfg.SyncGitHubInterval, cfg.SyncInterval),
				"datadog":          effectiveConnectorInterval(cfg.SyncDatadogInterval, cfg.SyncInterval),
				"aws":              effectiveConnectorInterval(cfg.SyncAWSInterval, cfg.SyncInterval),
			}
		},
		notifyChannel: sync.SyncJobNotifyChannelForMode(registry.RunModeFull),
		requireEnabled: func(cfg config.Config) error {
			if !cfg.SyncFullEnabled {
				return errors.New("SYNC_FULL_ENABLED must be true to run the full sync worker")
			}
			if cfg.SyncInterval <= 0 {
				return errors.New("SYNC_INTERVAL must be > 0 to run the worker")
			}
			return nil
		},
	}
}

func discoverySyncLane() syncWorkerLane {
	return syncWorkerLane{
		name:                   "discovery",
		mode:                   registry.RunModeDiscovery,
		interval:               func(cfg config.Config) time.Duration { return cfg.SyncDiscoveryInterval },
		scopeName:              sync.RunOnceScopeNameDiscovery,
		consumerPollInterval:   defaultSyncWorkerConsumerPollInterval,
		enableDiscoveryMetrics: true,
		intervalByKind: func(cfg config.Config) map[string]time.Duration {
			return map[string]time.Duration{
				"okta_discovery":             cfg.SyncDiscoveryInterval,
				"entra_discovery":            cfg.SyncDiscoveryInterval,
				"google_workspace_discovery": cfg.SyncDiscoveryInterval,
			}
		},
		notifyChannel: sync.SyncJobNotifyChannelForMode(registry.RunModeDiscovery),
		requireEnabled: func(cfg config.Config) error {
			if !cfg.SyncDiscoveryEnabled {
				return errors.New("SYNC_DISCOVERY_ENABLED must be true to run the discovery worker")
			}
			if cfg.SyncDiscoveryInterval <= 0 {
				return errors.New("SYNC_DISCOVERY_INTERVAL must be > 0 to run the discovery worker")
			}
			return nil
		},
	}
}

func (l syncWorkerLane) Name() string {
	return l.name
}

func (l syncWorkerLane) ValidateConfig(cfg config.Config) error {
	if l.requireEnabled != nil {
		return l.requireEnabled(cfg)
	}
	if l.interval(cfg) <= 0 {
		return errors.New("worker interval must be > 0")
	}
	return nil
}

func (l syncWorkerLane) MetricsRefresh(deps *runtimeDependencies, _ config.Config) metrics.RefreshFunc {
	if deps == nil {
		return nil
	}
	return backgroundMetricsRefresh(deps.queries)
}

func (l syncWorkerLane) Start(ctx context.Context, host *workerHostRun, deps *runtimeDependencies, cfg config.Config) error {
	if deps == nil {
		return errors.New("runtime dependencies are not configured")
	}
	reg, err := buildConnectorRegistry(cfg)
	if err != nil {
		return err
	}
	if err := rebuildStoredReadModels(ctx, deps.pool, deps.queries, cfg); err != nil {
		return err
	}

	locks, err := sync.NewLockManager(deps.pool, sync.LockManagerConfig{
		Mode:              cfg.SyncLockMode,
		InstanceID:        cfg.SyncLockInstanceID,
		TTL:               cfg.SyncLockTTL,
		HeartbeatInterval: cfg.SyncLockHeartbeatInterval,
		HeartbeatTimeout:  cfg.SyncLockHeartbeatTimeout,
	})
	if err != nil {
		return err
	}
	consumerLocks, err := sync.NewLockManager(deps.pool, sync.LockManagerConfig{
		Mode:              sync.LockModeLease,
		InstanceID:        cfg.SyncLockInstanceID,
		TTL:               cfg.SyncLockTTL,
		HeartbeatInterval: cfg.SyncLockHeartbeatInterval,
		HeartbeatTimeout:  cfg.SyncLockHeartbeatTimeout,
	})
	if err != nil {
		return err
	}

	dbRunner := sync.NewDBRunner(deps.pool, reg)
	dbRunner.SetReporter(&sync.LogReporter{})
	dbRunner.SetLockManager(locks)
	dbRunner.SetRunMode(l.mode)
	dbRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	dbRunner.SetReadModelConfig(readmodels.RefreshConfigFromConfig(cfg))
	if l.enableDiscoveryMetrics {
		dbRunner.EnableDiscoveryMetricsRefresh()
	}

	interval := l.interval(cfg)
	backoffMax := cfg.SyncFailureBackoffMax
	if backoffMax <= 0 {
		backoffMax = interval * 10
	}
	dbRunner.SetRunPolicy(sync.RunPolicy{
		IntervalByKind:       l.intervalByKind(cfg),
		FailureBackoffBase:   interval,
		FailureBackoffMax:    backoffMax,
		RecentFinishedRunCap: 10,
	})

	executionRunner := sync.NewBlockingRunOnceLockRunnerWithScope(locks, dbRunner, l.scopeName)
	jobStore := sync.NewSyncJobStore(deps.pool)
	wakeups := make(chan struct{}, 1)
	jobConsumer := sync.NewSyncJobConsumer(jobStore, consumerLocks, executionRunner, sync.SyncJobConsumerConfig{
		Mode:              l.mode,
		PollInterval:      l.consumerPollInterval,
		LeaseTTL:          cfg.SyncLockTTL,
		HeartbeatInterval: cfg.SyncLockHeartbeatInterval,
		RetryBaseDelay:    interval,
		RetryMaxDelay:     backoffMax,
		Wakeups:           wakeups,
		OnLoopTick:        func(string) { host.ObserveLoopTick() },
		OnClaimAttempt:    func(string) { host.ObserveClaimAttempt() },
	})

	slog.Info("sync worker lane configured", "lane", l.name, "interval", interval, "mode", string(l.mode))
	host.Go("sync-job-consumer", true, func(ctx context.Context) error {
		return jobConsumer.Run(ctx)
	})
	host.Go("sync-job-listener", true, func(ctx context.Context) error {
		return sync.ListenForSyncJobSignalsWithObserver(ctx, deps.pool, l.notifyChannel, wakeups, func(string) {
			host.ObserveListenerConnect()
		})
	})

	scheduler := sync.Scheduler{
		Runner:   sync.NewScheduledSyncJobRunner(jobStore, l.mode),
		Interval: interval,
	}
	host.Go("scheduler", true, func(ctx context.Context) error {
		scheduler.Run(ctx)
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("scheduler stopped unexpectedly")
	})
	return nil
}

func effectiveConnectorInterval(connectorInterval, laneInterval time.Duration) time.Duration {
	if connectorInterval > 0 {
		return connectorInterval
	}
	return laneInterval
}
