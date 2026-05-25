package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/db/gen"
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
		runEventPartitionMaintenance(ctx, manager, deps.queries, cfg)
		for timing.SleepContext(ctx, interval) {
			runEventPartitionMaintenance(ctx, manager, deps.queries, cfg)
		}
		return nil
	})
}

func runEventPartitionMaintenance(ctx context.Context, manager *events.PartitionManager, q *gen.Queries, cfg config.Config) {
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
	runEventRetentionCleanup(ctx, q, cfg)
}

func runEventRetentionCleanup(ctx context.Context, q *gen.Queries, cfg config.Config) {
	if q == nil || cfg.EventPartitions.RetentionDays <= 0 {
		return
	}
	cutoffValue := eventRetentionCleanupCutoff(time.Now(), cfg.EventPartitions.RetentionDays)
	if _, err := q.DeleteEventDedupeKeysBefore(ctx, cutoffValue); err != nil {
		slog.Warn("event dedupe retention cleanup failed", "err", err)
	}
	if _, err := q.DeleteEventProjectionDiffRunsBefore(ctx, cutoffValue); err != nil {
		slog.Warn("event projection diff retention cleanup failed", "err", err)
	}
	if _, err := q.DeleteFinishedRiskpolicyEventQueueBefore(ctx, cutoffValue); err != nil {
		slog.Warn("riskpolicy event queue retention cleanup failed", "err", err)
	}
	if _, err := q.DeleteRiskpolicyEventShadowSignalsBefore(ctx, cutoffValue); err != nil {
		slog.Warn("riskpolicy shadow signal retention cleanup failed", "err", err)
	}
}

func eventRetentionCleanupCutoff(now time.Time, retentionDays int32) pgtype.Timestamptz {
	cutoff := now.UTC().AddDate(0, 0, -int(retentionDays))
	year, month, day := cutoff.Date()
	return pgtype.Timestamptz{
		Time:  time.Date(year, month, day, 0, 0, 0, 0, time.UTC),
		Valid: true,
	}
}
