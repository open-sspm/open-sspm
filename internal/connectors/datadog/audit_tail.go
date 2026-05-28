package datadog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	canonevents "github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/tail"
)

const (
	DatadogAuditTailResource        = "audit_logs"
	datadogAuditTailChannel         = "audit_logs_tail"
	datadogAuditTailCursorKind      = "watermark_overlap"
	datadogAuditTailDefaultLookback = 15 * time.Minute
	datadogAuditTailOverlap         = 5 * time.Minute
)

type datadogAuditTailStats struct {
	Fetched    int
	Written    int
	Duplicates int
	Since      time.Time
	Watermark  time.Time
}

func (i *DatadogIntegration) runAuditTail(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	if q == nil {
		return errors.New("datadog audit tail requires queries")
	}
	if pool == nil {
		return errors.New("datadog audit tail requires database pool")
	}
	if i == nil || i.adapter == nil {
		return nil
	}

	resource := DatadogAuditTailResource
	if scopedResource, ok := registry.ResourceScopeFromContext(ctx); ok {
		if scopedResource != DatadogAuditTailResource {
			return fmt.Errorf("datadog tail resource %q is not supported", scopedResource)
		}
		resource = scopedResource
	}

	started := time.Now()
	runID, err := registry.StartSyncRunWithMode(ctx, q, configstore.KindDatadog, i.site, registry.RunModeTail)
	if err != nil {
		return err
	}

	report(registry.Event{Source: configstore.KindDatadog, Stage: "tail-audit-logs", Current: 0, Total: 1, Message: "tailing Datadog Audit Logs"})
	stats, err := i.tailAuditEventsWithCursor(ctx, pool, runID, resource)
	if err != nil {
		report(registry.Event{Source: configstore.KindDatadog, Stage: "tail-audit-logs", Current: 1, Total: 1, Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}

	duration := time.Since(started)
	statsPayload := registry.MarshalJSON(map[string]any{
		"counts": map[string]int{
			"fetched":    stats.Fetched,
			"written":    stats.Written,
			"duplicates": stats.Duplicates,
		},
		"duration_ms": duration.Milliseconds(),
		"resource":    resource,
		"since":       formatDatadogTailTime(stats.Since),
		"watermark":   formatDatadogTailTime(stats.Watermark),
	})
	if err := q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: statsPayload}); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	report(registry.Event{
		Source:  configstore.KindDatadog,
		Stage:   "tail-audit-logs",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("tailed %d Audit Logs events since %s", stats.Fetched, stats.Since.Format(time.RFC3339)),
	})
	slog.Info("datadog audit tail complete", "source", i.site, "fetched", stats.Fetched, "written", stats.Written, "duplicates", stats.Duplicates)
	return nil
}

func (i *DatadogIntegration) tailAuditEventsWithCursor(ctx context.Context, pool *pgxpool.Pool, runID int64, resource string) (datadogAuditTailStats, error) {
	var (
		stats   datadogAuditTailStats
		tailErr error
	)
	store := tail.NewCursorStore(pool)
	err := store.WithLockedCursor(ctx, tail.CursorKey{
		SourceKind: configstore.KindDatadog,
		SourceName: i.site,
		Resource:   resource,
		CursorKind: datadogAuditTailCursorKind,
	}, func(lockCtx context.Context, qtx *gen.Queries, state gen.ConnectorCursorState) error {
		attemptedAt := time.Now().UTC()
		since := datadogAuditTailSince(state, attemptedAt)
		stats.Since = since

		events, err := i.adapter.ListAuditEvents(lockCtx, since)
		if err != nil {
			tailErr = fmt.Errorf("datadog list audit events since %s: %w", since.Format(time.RFC3339), err)
			return updateDatadogAuditTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
		}

		writer := canonevents.NewWriter(pool)
		stats.Fetched = len(events)
		watermark := attemptedAt
		for _, event := range events {
			if event.Timestamp.IsZero() {
				tailErr = fmt.Errorf("datadog audit event %q is missing timestamp", event.ID)
				return updateDatadogAuditTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			occurredAt := event.Timestamp.UTC()
			if occurredAt.After(watermark) || watermark.Equal(attemptedAt) {
				watermark = occurredAt
			}
			record, err := datadogCanonicalAuditEventRecord(i.site, datadogAuditTailChannel, event)
			if err != nil {
				tailErr = err
				return updateDatadogAuditTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			result, err := writer.WriteEvent(lockCtx, record, canonevents.WriteOptions{})
			if err != nil {
				tailErr = fmt.Errorf("write canonical Datadog audit event %s: %w", event.ID, err)
				return updateDatadogAuditTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			if result.Inserted {
				stats.Written++
			} else {
				stats.Duplicates++
			}
		}
		stats.Watermark = watermark

		return updateDatadogAuditTailCursorSuccess(lockCtx, qtx, state, runID, attemptedAt, watermark, lastDatadogAuditEventID(events), stats)
	})
	if err != nil {
		return stats, err
	}
	if tailErr != nil {
		return stats, tailErr
	}
	return stats, nil
}

func datadogAuditTailSince(state gen.ConnectorCursorState, now time.Time) time.Time {
	now = now.UTC()
	if state.Watermark.Valid && !state.Watermark.Time.IsZero() {
		return state.Watermark.Time.UTC().Add(-datadogAuditTailOverlap)
	}
	return now.Add(-datadogAuditTailDefaultLookback)
}

func updateDatadogAuditTailCursorSuccess(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState, runID int64, attemptedAt, watermark time.Time, lastProviderEventID string, stats datadogAuditTailStats) error {
	params := datadogAuditTailCursorParams(state)
	params.LastAttemptAt = pgtype.Timestamptz{Time: attemptedAt.UTC(), Valid: true}
	params.LastSuccessAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	params.LastErrorAt = pgtype.Timestamptz{}
	params.LastError = ""
	params.LastRunID = pgtype.Int8{Int64: runID, Valid: runID != 0}
	params.LastProviderEventID = strings.TrimSpace(lastProviderEventID)
	params.Watermark = pgtype.Timestamptz{Time: watermark.UTC(), Valid: !watermark.IsZero()}
	params.CursorJson = registry.MarshalJSON(map[string]any{
		"channel":         datadogAuditTailChannel,
		"overlap_seconds": int(datadogAuditTailOverlap.Seconds()),
		"fetched":         stats.Fetched,
		"written":         stats.Written,
		"duplicates":      stats.Duplicates,
		"since":           formatDatadogTailTime(stats.Since),
		"watermark":       formatDatadogTailTime(watermark),
	})
	params.NeedsFullResync = false
	return q.UpsertConnectorCursorState(ctx, params)
}

func updateDatadogAuditTailCursorError(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState, runID int64, attemptedAt time.Time, err error) error {
	params := datadogAuditTailCursorParams(state)
	params.LastAttemptAt = pgtype.Timestamptz{Time: attemptedAt.UTC(), Valid: true}
	params.LastErrorAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	params.LastError = truncateDatadogTailMessage(err)
	params.LastRunID = pgtype.Int8{Int64: runID, Valid: runID != 0}
	return q.UpsertConnectorCursorState(ctx, params)
}

func datadogAuditTailCursorParams(state gen.ConnectorCursorState) gen.UpsertConnectorCursorStateParams {
	cursorJSON := state.CursorJson
	if len(cursorJSON) == 0 {
		cursorJSON = []byte(`{}`)
	}
	return gen.UpsertConnectorCursorStateParams{
		SourceKind:          configstore.KindDatadog,
		SourceID:            state.SourceID,
		SourceName:          strings.TrimSpace(state.SourceName),
		Resource:            strings.TrimSpace(state.Resource),
		CursorKind:          datadogAuditTailCursorKind,
		CursorJson:          cursorJSON,
		Watermark:           state.Watermark,
		CursorExpiresAt:     state.CursorExpiresAt,
		LastSuccessAt:       state.LastSuccessAt,
		LastAttemptAt:       state.LastAttemptAt,
		LastErrorAt:         state.LastErrorAt,
		LastError:           strings.TrimSpace(state.LastError),
		LastRunID:           state.LastRunID,
		LastProviderEventID: strings.TrimSpace(state.LastProviderEventID),
		NeedsFullResync:     state.NeedsFullResync,
	}
}

func datadogCanonicalAuditEventRecord(sourceName, channel string, event AuditEvent) (records.EventRecord, error) {
	raw := map[string]any{}
	if len(event.RawJSON) > 0 {
		if err := json.Unmarshal(event.RawJSON, &raw); err != nil {
			return records.EventRecord{}, fmt.Errorf("decode Datadog audit event %s: %w", event.ID, err)
		}
	}
	action := strings.TrimSpace(event.Action)
	if action == "" {
		action = "audit_event"
	}
	targets := make([]records.TargetRef, 0, 1)
	if strings.TrimSpace(event.TargetID) != "" || strings.TrimSpace(event.TargetName) != "" {
		targets = append(targets, records.TargetRef{
			Kind: "datadog_resource",
			ID:   strings.TrimSpace(event.TargetID),
			Name: strings.TrimSpace(event.TargetName),
			Role: "resource",
		})
	}

	return records.EventRecord{
		Source: records.SourceRef{
			Kind: configstore.KindDatadog,
			Name: sourceName,
		},
		Channel:         strings.TrimSpace(channel),
		ProviderEventID: strings.TrimSpace(event.ID),
		DedupeKeyValue:  "provider:" + strings.TrimSpace(event.ID),
		EventType:       "datadog.audit." + strings.ToLower(action),
		Category:        "datadog.audit",
		Action:          action,
		OccurredAt:      event.Timestamp,
		ObservedAt:      event.Timestamp,
		Actor: records.ActorRef{
			Kind:        "datadog_actor",
			ID:          strings.TrimSpace(event.ActorID),
			Email:       strings.ToLower(strings.TrimSpace(event.ActorEmail)),
			DisplayName: strings.TrimSpace(event.ActorName),
		},
		Targets: targets,
		Envelope: map[string]any{
			"service":    strings.TrimSpace(event.Service),
			"message":    strings.TrimSpace(event.Message),
			"attributes": event.Attributes,
		},
		Raw: raw,
	}, nil
}

func lastDatadogAuditEventID(events []AuditEvent) string {
	var (
		lastID string
		lastAt time.Time
	)
	for _, event := range events {
		id := strings.TrimSpace(event.ID)
		if id == "" {
			continue
		}
		occurredAt := event.Timestamp.UTC()
		if lastID == "" || occurredAt.After(lastAt) || occurredAt.Equal(lastAt) {
			lastID = id
			lastAt = occurredAt
		}
	}
	return lastID
}

func formatDatadogTailTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func truncateDatadogTailMessage(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	const limit = 2048
	if len(msg) <= limit {
		return msg
	}
	return msg[:limit]
}
