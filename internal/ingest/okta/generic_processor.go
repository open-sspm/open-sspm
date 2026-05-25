package oktaingest

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	genericinbox "github.com/open-sspm/open-sspm/internal/ingest/inbox"
	"github.com/open-sspm/open-sspm/internal/ingest/recorddispatch"
)

func ProcessGenericQueued(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, limit int32) (genericinbox.RunOnceResult, error) {
	return ProcessGenericQueuedWithConfig(ctx, q, pool, limit, DefaultConfig())
}

func ProcessGenericQueuedWithConfig(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, limit int32, cfg Config) (genericinbox.RunOnceResult, error) {
	cfg = cfg.normalized()
	if limit > 0 {
		cfg.BatchSize = limit
	}
	return processGenericEventInboxOnce(ctx, q, pool, cfg)
}

func processGenericEventInboxOnce(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, cfg Config) (genericinbox.RunOnceResult, error) {
	store := genericinbox.NewStore(q)
	processor := genericinbox.NewProcessor(store, oktaEventInboxHandler{q: q, pool: pool}, genericinbox.ProcessorConfig{
		BatchSize:     cfg.BatchSize,
		LeaseOwner:    cfg.ClaimedBy,
		LeaseTTL:      cfg.LeaseTTL,
		RetryDelay:    cfg.RetryDelay,
		MaxRetryDelay: cfg.RetryDelayMax,
	})
	return processor.RunOnce(ctx)
}

func runGenericEventInboxIteration(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, cfg Config) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		result, err := ProcessGenericQueuedWithConfig(ctx, q, pool, cfg.BatchSize, cfg)
		if err != nil {
			slog.Warn("generic event inbox processing failed", "error", err, "claimed", result.Claimed, "processed", result.Processed, "ignored", result.Ignored, "dead", result.Dead, "retried", result.Retried)
		}
		if result.Claimed == 0 || int32(result.Claimed) < cfg.BatchSize {
			return
		}
	}
}

func requeueExpiredEventInboxLeases(ctx context.Context, q *gen.Queries) error {
	if q == nil {
		return nil
	}
	_, err := q.RequeueExpiredEventInboxLeases(ctx)
	return err
}

type oktaEventInboxHandler struct {
	q    *gen.Queries
	pool *pgxpool.Pool
}

func (h oktaEventInboxHandler) ProcessInboxDelivery(ctx context.Context, delivery genericinbox.Delivery) (genericinbox.ProcessResult, error) {
	sourceKind := strings.TrimSpace(delivery.Source.Kind)
	sourceName := strings.TrimSpace(delivery.Source.Name)
	channel := strings.TrimSpace(delivery.Channel)
	if !strings.EqualFold(sourceKind, configstore.KindOkta) {
		return genericinbox.ProcessResult{
			Status:       genericinbox.ProcessStatusIgnored,
			IgnoreReason: "delivery source is not Okta",
		}, nil
	}
	if sourceName == "" {
		return genericinbox.ProcessResult{
			Status:    genericinbox.ProcessStatusDead,
			LastError: "Okta delivery source name is required",
		}, nil
	}
	if !oktaEventInboxKnownChannel(channel) {
		return genericinbox.ProcessResult{
			Status:    genericinbox.ProcessStatusDead,
			LastError: "unknown Okta push channel",
		}, nil
	}

	event, err := oktaconnector.MapSystemLogEventJSON(delivery.RawBody)
	if err != nil {
		return genericinbox.ProcessResult{
			Status:    genericinbox.ProcessStatusDead,
			LastError: "invalid Okta System Log event JSON",
		}, nil
	}
	if !oktaconnector.ShouldIngestPushEvent(event) {
		return genericinbox.ProcessResult{
			Status:       genericinbox.ProcessStatusIgnored,
			IgnoreReason: "event is not discovery or state-refresh evidence",
			DecodedSummary: map[string]any{
				"event_id":   strings.TrimSpace(event.ID),
				"event_type": strings.TrimSpace(event.EventType),
			},
		}, nil
	}

	discoveryEvents := []oktaconnector.SystemLogEvent{}
	if _, ok := oktaconnector.DiscoverySignalKind(event); ok {
		discoveryEvents = append(discoveryEvents, event)
	}
	refreshCounts := make(map[string]int64)
	if kind, ok := oktaconnector.StateRefreshSignalKind(event); ok {
		refreshCounts[kind] = 1
	}

	sources, normalizedEvents := oktaconnector.NormalizeDiscoveryEvents(discoveryEvents, sourceName, time.Now().UTC())
	hasDiscoveryRows := len(sources) > 0 || len(normalizedEvents) > 0
	hasStateRefresh := len(refreshCounts) > 0
	if !hasDiscoveryRows && !hasStateRefresh {
		return genericinbox.ProcessResult{
			Status:       genericinbox.ProcessStatusIgnored,
			IgnoreReason: "event did not normalize to discovery or state-refresh evidence",
			DecodedSummary: map[string]any{
				"event_id":   strings.TrimSpace(event.ID),
				"event_type": strings.TrimSpace(event.EventType),
			},
		}, nil
	}

	runStarted := time.Now()
	runID, err := startOktaPushSyncRun(ctx, h.q, sourceName)
	if err != nil {
		return genericinbox.ProcessResult{}, err
	}
	failRun := func(cause error) {
		_ = registry.FailSyncRun(ctx, h.q, runID, cause, registry.SyncErrorKindDB)
	}

	if err := dispatchGenericOktaPushRecord(ctx, h.pool, sourceName, channel, event); err != nil {
		failRun(err)
		return genericinbox.ProcessResult{}, err
	}
	if err := enqueueOktaSystemLogTail(ctx, h.q, sourceName, 1); err != nil {
		failRun(err)
		return genericinbox.ProcessResult{}, err
	}
	if hasDiscoveryRows {
		if err := dispatchOktaDiscoveryEvidenceRecords(ctx, h.q, sourceName, runID, sources, normalizedEvents); err != nil {
			failRun(err)
			return genericinbox.ProcessResult{}, err
		}
	}
	if hasStateRefresh {
		if err := enqueueOktaFullSync(ctx, h.pool, sourceName); err != nil {
			failRun(err)
			return genericinbox.ProcessResult{}, err
		}
	}
	if err := finalizeOktaPushRun(ctx, h.q, h.pool, runID, sourceName, time.Since(runStarted), hasDiscoveryRows, refreshCounts); err != nil {
		failRun(err)
		return genericinbox.ProcessResult{}, err
	}

	return genericinbox.ProcessResult{
		Status: genericinbox.ProcessStatusProcessed,
		DecodedSummary: map[string]any{
			"sync_run_id":         runID,
			"event_id":            strings.TrimSpace(event.ID),
			"event_type":          strings.TrimSpace(event.EventType),
			"discovery_sources":   len(sources),
			"discovery_events":    len(normalizedEvents),
			"state_refresh":       oktaStateRefreshRunCounts(refreshCounts),
			"source_kind":         configstore.KindOkta,
			"source_name":         sourceName,
			"channel":             channel,
			"external_event_id":   strings.TrimSpace(delivery.ExternalEventID),
			"delivery_dedupe_key": strings.TrimSpace(delivery.DedupeKey),
		},
	}, nil
}

func dispatchGenericOktaPushRecord(ctx context.Context, pool *pgxpool.Pool, sourceName, channel string, event oktaconnector.SystemLogEvent) error {
	dispatcher := recorddispatch.NewEventDispatcher(pool)
	record, err := oktaconnector.CanonicalEventRecord(sourceName, channel, event)
	if err != nil {
		return err
	}
	if _, err := dispatcher.DispatchEvent(ctx, record); err != nil {
		return fmt.Errorf("dispatch canonical Okta push event %s: %w", event.ID, err)
	}
	return nil
}

func oktaEventInboxKnownChannel(channel string) bool {
	switch strings.TrimSpace(channel) {
	case "event_hook", "eventbridge":
		return true
	default:
		return false
	}
}
