package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/open-sspm/open-sspm/internal/config"
	oktaingest "github.com/open-sspm/open-sspm/internal/ingest/okta"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

type oktaPushIngestLane struct{}

func (oktaPushIngestLane) Name() string {
	return "okta-push-ingest"
}

func (oktaPushIngestLane) ValidateConfig(cfg config.Config) error {
	if !cfg.OktaPushIngestEnabled {
		if cfg.OktaPushIngestEnabledSet {
			return errors.New("OKTA_PUSH_INGEST_ENABLED must be true to run the ingest worker")
		}
		return errors.New("OKTA_PUSH_INGEST_ENABLED or SYNC_DISCOVERY_ENABLED must be true to run the ingest worker")
	}
	return nil
}

func (oktaPushIngestLane) MetricsRefresh(deps *runtimeDependencies, _ config.Config) metrics.RefreshFunc {
	if deps == nil {
		return nil
	}
	return oktaPushMetricsRefresh(deps.queries)
}

func (oktaPushIngestLane) Start(ctx context.Context, host *workerHostRun, deps *runtimeDependencies, cfg config.Config) error {
	if deps == nil {
		return errors.New("runtime dependencies are not configured")
	}
	oktaPushInboxQueue, err := openOktaPushInboxQueue(ctx, cfg)
	if err != nil {
		return err
	}
	if oktaPushInboxQueue != nil {
		host.OnShutdown("okta-push-inbox-queue", func(context.Context) error {
			return oktaPushInboxQueue.Close()
		})
	}

	ingestCfg := oktaPushIngestConfigFromConfig(cfg, host.ObserveLoopTick, host.ObserveClaimAttempt, host.ObserveLeaseLost)
	host.Go("okta-push-processor", true, func(ctx context.Context) error {
		return oktaingest.RunLoopWithQueue(ctx, deps.queries, deps.pool, ingestCfg, oktaPushInboxQueue)
	})
	return nil
}

func oktaPushIngestConfigFromConfig(cfg config.Config, onLoopTick, onClaimAttempt, onLeaseLost func()) oktaingest.Config {
	ingest := cfg.OktaPushIngest
	return oktaingest.Config{
		BatchSize:               ingest.BatchSize,
		PollInterval:            ingest.PollInterval,
		CleanupInterval:         ingest.CleanupInterval,
		RetryDelay:              ingest.RetryDelay,
		RetryDelayMax:           ingest.RetryDelayMax,
		StaleProcessingAfter:    ingest.StaleProcessingTimeout,
		MaxAttempts:             ingest.MaxAttempts,
		ProcessedRetentionDays:  ingest.ProcessedRetentionDays,
		DeadLetterRetentionDays: ingest.DeadLetterRetentionDays,
		LeaseTTL:                cfg.SyncLockTTL,
		HeartbeatInterval:       cfg.SyncLockHeartbeatInterval,
		ClaimedBy:               workerClaimedBy(cfg, "okta-push-ingest"),
		OnLoopTick:              onLoopTick,
		OnClaimAttempt:          onClaimAttempt,
		OnLeaseLost:             onLeaseLost,
	}
}

func workerClaimedBy(cfg config.Config, lane string) string {
	instanceID := strings.TrimSpace(cfg.SyncLockInstanceID)
	if instanceID == "" {
		instanceID = strings.TrimSpace(os.Getenv("HOSTNAME"))
	}
	if instanceID == "" {
		if host, err := os.Hostname(); err == nil {
			instanceID = strings.TrimSpace(host)
		}
	}
	if instanceID == "" {
		instanceID = "pid"
	}
	return fmt.Sprintf("%s/%s/%d", instanceID, stringsTrimDefault(lane, "worker"), os.Getpid())
}
