package oktaingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/ingest/recorddispatch"
	osspmsync "github.com/open-sspm/open-sspm/internal/sync"
	"github.com/open-sspm/open-sspm/internal/tail"
)

func startOktaEventInboxSyncRun(ctx context.Context, q *gen.Queries, sourceName string) (int64, error) {
	if q == nil {
		return 0, fmt.Errorf("sync run start could not be persisted: queries is nil")
	}
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" {
		return 0, fmt.Errorf("sync run start requires source name")
	}
	runID, err := q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: configstore.KindOkta,
		SourceName: sourceName,
		RunMode:    string(registry.RunModeEventInbox),
	})
	if err != nil {
		return 0, fmt.Errorf("create sync run for okta event inbox/%s: %w", sourceName, err)
	}
	return runID, nil
}

func finalizeOktaEventInboxRun(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, runID int64, sourceName string, duration time.Duration, hasDiscoveryRows bool, refreshCounts map[string]int64) error {
	counts := oktaStateRefreshRunCounts(refreshCounts)
	if hasDiscoveryRows {
		return registry.FinalizeDiscoveryRunWithCounts(ctx, q, pool, runID, configstore.KindOkta, sourceName, duration, counts)
	}
	stats := registry.MarshalJSON(map[string]any{
		"counts":      counts,
		"duration_ms": duration.Milliseconds(),
	})
	return q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: stats})
}

func oktaStateRefreshRunCounts(refreshCounts map[string]int64) map[string]int64 {
	counts := map[string]int64{}
	var total int64
	for kind, count := range refreshCounts {
		counts["state_refresh_"+kind] = count
		total += count
	}
	if total > 0 {
		counts["state_refresh_events"] = total
	}
	return counts
}

func dispatchOktaDiscoveryEvidenceRecords(ctx context.Context, q *gen.Queries, sourceName string, runID int64, sources []discovery.SourceRow, events []discovery.EventRow) error {
	dispatcher := recorddispatch.NewDispatcher(nil, recorddispatch.NewOktaStateProjector(q, runID))
	for _, record := range oktaconnector.DiscoveryEvidenceRecordsFromRows(sourceName, sources, events) {
		if err := dispatcher.UpsertState(ctx, record); err != nil {
			return fmt.Errorf("dispatch okta discovery evidence %s: %w", record.Key, err)
		}
	}
	return nil
}

func enqueueOktaFullSync(ctx context.Context, pool *pgxpool.Pool, sourceName string) error {
	if pool == nil {
		return fmt.Errorf("queue okta full sync: pool is nil")
	}
	store := osspmsync.NewSyncJobStore(pool)
	runner := osspmsync.NewResyncQueueRunnerWithPlanner(store, nil, registry.RunModeFull)
	err := runner.RunOnce(osspmsync.WithConnectorScope(ctx, configstore.KindOkta, sourceName))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, osspmsync.ErrSyncQueued), errors.Is(err, osspmsync.ErrSyncAlreadyRunning):
		return nil
	default:
		return fmt.Errorf("queue okta full sync for %s: %w", sourceName, err)
	}
}

func enqueueOktaSystemLogTail(ctx context.Context, q *gen.Queries, sourceName string, eventCount int) error {
	scheduler := tail.NewScheduler(q)
	_, err := scheduler.Wake(ctx, tail.Wakeup{
		SourceKind: configstore.KindOkta,
		SourceName: sourceName,
		Resource:   oktaconnector.SystemLogTailResource,
		Reason:     "event_inbox_wakeup",
		Priority:   10,
		Payload: map[string]any{
			"channel":     "event_inbox",
			"event_count": eventCount,
		},
	})
	if err != nil {
		return fmt.Errorf("queue okta system log tail for %s: %w", sourceName, err)
	}
	return nil
}
