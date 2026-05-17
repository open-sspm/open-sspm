package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/timing"
)

func startEventPartitionMaintenance(run *workerHostRun, deps *runtimeDependencies, cfg config.Config) {
	if run == nil || deps == nil || deps.pool == nil {
		return
	}
	interval := cfg.EventPartitions.MaintenanceInterval
	if interval <= 0 {
		return
	}
	manager := events.NewPartitionManager(deps.pool)
	run.Go("event-partition-maintenance", false, func(ctx context.Context) error {
		runEventPartitionMaintenance(ctx, manager, cfg)
		for timing.SleepContext(ctx, interval) {
			runEventPartitionMaintenance(ctx, manager, cfg)
		}
		return nil
	})
}

func runEventPartitionMaintenance(ctx context.Context, manager *events.PartitionManager, cfg config.Config) {
	result, err := manager.MaintainDailyPartitions(ctx, events.PartitionMaintenanceConfig{
		FutureDays:    cfg.EventPartitions.FutureDays,
		RetentionDays: cfg.EventPartitions.RetentionDays,
	})
	if err != nil {
		metrics.EventPartitionMaintenanceRunsTotal.WithLabelValues("failure").Inc()
		slog.Warn("event partition maintenance failed", "err", err)
		return
	}
	metrics.EventPartitionMaintenanceRunsTotal.WithLabelValues("success").Inc()
	metrics.EventPartitionEnsuredUntilTimestamp.Set(float64(result.EnsuredEnd.Unix()))
	if result.Dropped > 0 {
		metrics.EventPartitionsDroppedTotal.Add(float64(result.Dropped))
	}
	slog.Info(
		"event partition maintenance complete",
		"ensured_start", result.EnsuredStart.Format(time.DateOnly),
		"ensured_end", result.EnsuredEnd.Format(time.DateOnly),
		"dropped", result.Dropped,
	)
}
