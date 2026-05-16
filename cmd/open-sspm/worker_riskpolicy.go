package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/findings"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/riskpolicy"
	"github.com/open-sspm/open-sspm/internal/timing"
	"github.com/spf13/cobra"
)

const riskpolicyEventStaleRecoveryInterval = time.Minute

var workerRiskpolicyCmd = &cobra.Command{
	Use:   "worker-riskpolicy",
	Short: "Run the background shadow riskpolicy event evaluation loop.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorkerRiskpolicy()
	},
}

type riskpolicyEventLane struct{}

func runWorkerRiskpolicy() error {
	return runWorkerHost(riskpolicyEventLane{})
}

func (riskpolicyEventLane) Name() string { return "riskpolicy-event" }

func (riskpolicyEventLane) ValidateConfig(cfg config.Config) error {
	if cfg.RiskpolicyEventWorker.PollInterval <= 0 {
		return errors.New("RISKPOLICY_EVENT_WORKER_POLL_INTERVAL must be > 0 to run the riskpolicy worker")
	}
	if cfg.RiskpolicyEventWorker.BatchSize <= 0 {
		return errors.New("RISKPOLICY_EVENT_WORKER_BATCH_SIZE must be > 0 to run the riskpolicy worker")
	}
	if cfg.RiskpolicyEventWorker.MaxAttempts <= 0 {
		return errors.New("RISKPOLICY_EVENT_WORKER_MAX_ATTEMPTS must be > 0 to run the riskpolicy worker")
	}
	return nil
}

func (riskpolicyEventLane) MetricsRefresh(*runtimeDependencies, config.Config) metrics.RefreshFunc {
	return nil
}

func (riskpolicyEventLane) Start(ctx context.Context, host *workerHostRun, deps *runtimeDependencies, cfg config.Config) error {
	if deps == nil || deps.queries == nil {
		return errors.New("runtime dependencies are not configured")
	}
	processor, err := riskpolicy.NewEventQueueProcessor(deps.queries, riskpolicy.EventQueueProcessorConfig{
		ClaimedBy:   workerClaimedBy(cfg, "riskpolicy-event"),
		MaxAttempts: cfg.RiskpolicyEventWorker.MaxAttempts,
	})
	if err != nil {
		return err
	}
	host.Go("riskpolicy-event-processor", true, func(ctx context.Context) error {
		return runRiskpolicyEventLoop(ctx, processor, deps, cfg.RiskpolicyEventWorker.BatchSize, cfg.RiskpolicyEventWorker.PollInterval)
	})
	return nil
}

func runRiskpolicyEventLoop(ctx context.Context, processor *riskpolicy.EventQueueProcessor, deps *runtimeDependencies, batchSize int32, pollInterval time.Duration) error {
	if batchSize <= 0 {
		batchSize = 100
	}
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	nextStaleRecovery := time.Now().Add(riskpolicyEventStaleRecoveryInterval)
	findingsProjector := findings.NewRiskpolicyProjector(deps.queries)

	for {
		if ctx.Err() != nil {
			return nil
		}
		processRiskpolicyEventBatch(ctx, processor, findingsProjector, batchSize)

		now := time.Now()
		if !now.Before(nextStaleRecovery) {
			requeueStaleRiskpolicyEventEvaluations(ctx, deps)
			nextStaleRecovery = now.Add(riskpolicyEventStaleRecoveryInterval)
		}

		if !timing.SleepContext(ctx, pollInterval) {
			return nil
		}
	}
}

func processRiskpolicyEventBatch(ctx context.Context, processor *riskpolicy.EventQueueProcessor, findingsProjector *findings.RiskpolicyProjector, batchSize int32) {
	result, err := processor.ProcessQueued(ctx, batchSize)
	if err != nil {
		slog.Warn("riskpolicy event queue processing failed", "err", err)
		return
	}
	if result.Claimed == 0 {
		return
	}
	slog.Info("riskpolicy event queue processed", "claimed", result.Claimed, "processed", result.Processed, "signals", result.Signals, "retried", result.Retried, "dead", result.Dead, "lease_lost", result.LeaseLost)
	if result.Signals == 0 {
		return
	}
	projected, err := findingsProjector.ProjectEventShadowFindings(ctx, findings.RiskpolicyProjectionParams{Limit: int32(result.Signals)})
	if err != nil {
		slog.Warn("riskpolicy shadow finding projection failed", "err", err)
		return
	}
	slog.Info("riskpolicy shadow findings projected", "projected", projected.Projected)
}

func requeueStaleRiskpolicyEventEvaluations(ctx context.Context, deps *runtimeDependencies) {
	if deps == nil || deps.queries == nil {
		return
	}
	if _, err := deps.queries.RequeueStaleRiskpolicyEventEvaluations(ctx); err != nil {
		slog.Warn("riskpolicy event stale queue recovery failed", "err", err)
	}
}
