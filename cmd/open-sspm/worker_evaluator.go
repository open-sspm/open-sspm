package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/evaluator"
	"github.com/open-sspm/open-sspm/internal/findings"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/timing"
)

const eventEvaluatorStaleRecoveryInterval = time.Minute

type eventEvaluatorLane struct{}

func (eventEvaluatorLane) Name() string { return "evaluator" }

func (eventEvaluatorLane) ValidateConfig(cfg config.Config) error {
	if cfg.EventEvaluatorWorker.PollInterval <= 0 {
		return errors.New("EVENT_EVALUATOR_WORKER_POLL_INTERVAL must be > 0 to run the evaluator worker")
	}
	if cfg.EventEvaluatorWorker.BatchSize <= 0 {
		return errors.New("EVENT_EVALUATOR_WORKER_BATCH_SIZE must be > 0 to run the evaluator worker")
	}
	if cfg.EventEvaluatorWorker.MaxAttempts <= 0 {
		return errors.New("EVENT_EVALUATOR_WORKER_MAX_ATTEMPTS must be > 0 to run the evaluator worker")
	}
	return nil
}

func (eventEvaluatorLane) MetricsRefresh(*runtimeDependencies, config.Config) metrics.RefreshFunc {
	return nil
}

func (eventEvaluatorLane) Start(ctx context.Context, host *workerHostRun, deps *runtimeDependencies, cfg config.Config) error {
	if deps == nil || deps.queries == nil {
		return errors.New("runtime dependencies are not configured")
	}
	processor, err := evaluator.NewEventQueueProcessor(deps.queries, evaluator.EventQueueProcessorConfig{
		ClaimedBy:   workerClaimedBy(cfg, "evaluator"),
		MaxAttempts: cfg.EventEvaluatorWorker.MaxAttempts,
		SignalSink:  findings.NewEventEvaluationFindingSink(deps.queries),
	})
	if err != nil {
		return err
	}
	host.Go("event-evaluator", true, func(ctx context.Context) error {
		return runEventEvaluatorLoop(ctx, processor, deps, cfg.EventEvaluatorWorker.BatchSize, cfg.EventEvaluatorWorker.PollInterval)
	})
	return nil
}

func runEventEvaluatorLoop(ctx context.Context, processor *evaluator.EventQueueProcessor, deps *runtimeDependencies, batchSize int32, pollInterval time.Duration) error {
	if batchSize <= 0 {
		batchSize = 100
	}
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	nextStaleRecovery := time.Now().Add(eventEvaluatorStaleRecoveryInterval)

	for {
		if ctx.Err() != nil {
			return nil
		}
		processEventEvaluatorBatch(ctx, processor, batchSize)

		now := time.Now()
		if !now.Before(nextStaleRecovery) {
			requeueStaleEventEvaluations(ctx, deps)
			nextStaleRecovery = now.Add(eventEvaluatorStaleRecoveryInterval)
		}

		if !timing.SleepContext(ctx, pollInterval) {
			return nil
		}
	}
}

func processEventEvaluatorBatch(ctx context.Context, processor *evaluator.EventQueueProcessor, batchSize int32) {
	result, err := processor.ProcessQueued(ctx, batchSize)
	if err != nil {
		slog.Warn("event evaluator queue processing failed", "err", err)
		return
	}
	if result.Claimed == 0 {
		return
	}
	slog.Info("event evaluator queue processed", "claimed", result.Claimed, "processed", result.Processed, "signals", result.Signals, "findings", result.Findings, "retried", result.Retried, "dead", result.Dead, "lease_lost", result.LeaseLost)
}

func requeueStaleEventEvaluations(ctx context.Context, deps *runtimeDependencies) {
	if deps == nil || deps.queries == nil {
		return
	}
	if _, err := deps.queries.RequeueStaleEventEvaluations(ctx); err != nil {
		slog.Warn("event evaluator stale queue recovery failed", "err", err)
	}
}
