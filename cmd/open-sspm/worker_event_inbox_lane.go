package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/ingest/inbox"
	oktaingest "github.com/open-sspm/open-sspm/internal/ingest/okta"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

type eventInboxLane struct{}

func (eventInboxLane) Name() string {
	return "event-inbox"
}

func (eventInboxLane) ValidateConfig(cfg config.Config) error {
	if !cfg.EventInboxEnabled {
		return errors.New("EVENT_INBOX_ENABLED must be true to run the event-inbox worker")
	}
	return nil
}

func (eventInboxLane) MetricsRefresh(deps *runtimeDependencies, _ config.Config) metrics.RefreshFunc {
	if deps == nil {
		return nil
	}
	return eventInboxMetricsRefresh(deps.queries)
}

func (eventInboxLane) Start(ctx context.Context, host *workerHostRun, deps *runtimeDependencies, cfg config.Config) error {
	if deps == nil {
		return errors.New("runtime dependencies are not configured")
	}
	ingestCfg := eventInboxConfigFromConfig(cfg, host.ObserveLoopTick, host.ObserveClaimAttempt)
	handler := oktaingest.NewEventInboxHandler(deps.queries, deps.pool)
	host.Go("event-inbox-processor", true, func(ctx context.Context) error {
		return inbox.RunLoop(ctx, deps.queries, handler, ingestCfg)
	})
	return nil
}

func eventInboxConfigFromConfig(cfg config.Config, onLoopTick, onClaimAttempt func()) inbox.Config {
	eventInbox := cfg.EventInbox
	return inbox.Config{
		BatchSize:               eventInbox.BatchSize,
		PollInterval:            eventInbox.PollInterval,
		CleanupInterval:         eventInbox.CleanupInterval,
		RetryDelay:              eventInbox.RetryDelay,
		RetryDelayMax:           eventInbox.RetryDelayMax,
		LeaseTTL:                eventInbox.StaleProcessingTimeout,
		MaxAttempts:             eventInbox.MaxAttempts,
		ProcessedRetentionDays:  eventInbox.ProcessedRetentionDays,
		DeadLetterRetentionDays: eventInbox.DeadLetterRetentionDays,
		ClaimedBy:               workerClaimedBy(cfg, "event-inbox"),
		OnLoopTick:              onLoopTick,
		OnClaimAttempt:          onClaimAttempt,
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
