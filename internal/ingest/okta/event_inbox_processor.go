package oktaingest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	genericinbox "github.com/open-sspm/open-sspm/internal/ingest/inbox"
	"github.com/open-sspm/open-sspm/internal/ingest/recorddispatch"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

type EventInboxHandler struct {
	q    *gen.Queries
	pool *pgxpool.Pool
}

func NewEventInboxHandler(q *gen.Queries, pool *pgxpool.Pool) EventInboxHandler {
	return EventInboxHandler{q: q, pool: pool}
}

func (h EventInboxHandler) ProcessInboxDelivery(ctx context.Context, delivery genericinbox.Delivery) (genericinbox.ProcessResult, error) {
	sourceKind := strings.TrimSpace(delivery.Source.Kind)
	sourceName := strings.TrimSpace(delivery.Source.Name)
	channel := strings.TrimSpace(delivery.Channel)
	if !strings.EqualFold(sourceKind, configstore.KindOkta) {
		return genericinbox.ProcessResult{
			Status:       genericinbox.ProcessStatusIgnored,
			IgnoreReason: "delivery source is not Okta",
		}, nil
	}
	started := time.Now()
	finish := func(result genericinbox.ProcessResult) (genericinbox.ProcessResult, error) {
		status := genericinbox.ProcessStatusProcessed.String()
		switch result.Status {
		case genericinbox.ProcessStatusIgnored:
			status = genericinbox.ProcessStatusIgnored.String()
		case genericinbox.ProcessStatusDead:
			status = genericinbox.ProcessStatusDead.String()
		}
		observeEventInboxDeliveryProcessed(sourceName, channel, status, started)
		return result, nil
	}
	if sourceName == "" {
		return finish(genericinbox.ProcessResult{
			Status:    genericinbox.ProcessStatusDead,
			LastError: "Okta delivery source name is required",
		})
	}
	if !oktaconnector.IsPushChannel(channel) {
		return finish(genericinbox.ProcessResult{
			Status:    genericinbox.ProcessStatusDead,
			LastError: "unknown Okta event inbox channel",
		})
	}

	event, err := oktaconnector.MapSystemLogEventJSON(delivery.RawBody)
	if err != nil {
		return finish(genericinbox.ProcessResult{
			Status:    genericinbox.ProcessStatusDead,
			LastError: "invalid Okta System Log event JSON",
		})
	}
	if !oktaconnector.ShouldIngestEventInboxEvent(event) {
		return finish(genericinbox.ProcessResult{
			Status:       genericinbox.ProcessStatusIgnored,
			IgnoreReason: "event is not discovery or state-refresh evidence",
			DecodedSummary: map[string]any{
				"event_id":   strings.TrimSpace(event.ID),
				"event_type": strings.TrimSpace(event.EventType),
			},
		})
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
		return finish(genericinbox.ProcessResult{
			Status:       genericinbox.ProcessStatusIgnored,
			IgnoreReason: "event did not normalize to discovery or state-refresh evidence",
			DecodedSummary: map[string]any{
				"event_id":   strings.TrimSpace(event.ID),
				"event_type": strings.TrimSpace(event.EventType),
			},
		})
	}

	runStarted := time.Now()
	runID, err := startOktaEventInboxSyncRun(ctx, h.q, sourceName)
	if err != nil {
		return genericinbox.ProcessResult{}, err
	}
	failRun := func(cause error) {
		_ = registry.FailSyncRun(ctx, h.q, runID, cause, registry.SyncErrorKindDB)
	}

	if err := dispatchOktaEventInboxRecord(ctx, h.pool, sourceName, channel, event); err != nil {
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
	if err := finalizeOktaEventInboxRun(ctx, h.q, h.pool, runID, sourceName, time.Since(runStarted), hasDiscoveryRows, refreshCounts); err != nil {
		failRun(err)
		return genericinbox.ProcessResult{}, err
	}

	return finish(genericinbox.ProcessResult{
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
	})
}

func dispatchOktaEventInboxRecord(ctx context.Context, pool *pgxpool.Pool, sourceName, channel string, event oktaconnector.SystemLogEvent) error {
	dispatcher := recorddispatch.NewEventDispatcher(pool)
	record, err := oktaconnector.CanonicalEventRecord(sourceName, channel, event)
	if err != nil {
		return err
	}
	if _, err := dispatcher.DispatchEvent(ctx, record); err != nil {
		return fmt.Errorf("dispatch canonical Okta event inbox event %s: %w", event.ID, err)
	}
	return nil
}

func observeEventInboxDeliveryProcessed(sourceName, channel, status string, started time.Time) {
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" {
		sourceName = "unknown"
	}
	channel = strings.TrimSpace(channel)
	if channel == "" {
		channel = "unknown"
	}
	metrics.EventInboxDeliveriesProcessedTotal.WithLabelValues(configstore.KindOkta, sourceName, channel, status).Inc()
	if !started.IsZero() {
		metrics.EventInboxProcessingDuration.WithLabelValues(configstore.KindOkta, sourceName, channel).Observe(time.Since(started).Seconds())
	}
}
