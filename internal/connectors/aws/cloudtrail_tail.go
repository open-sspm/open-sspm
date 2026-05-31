package aws

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
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/ingest/recorddispatch"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/tail"
)

const (
	AWSCloudTrailTailResource        = "cloudtrail_lookup"
	awsCloudTrailTailChannel         = "cloudtrail_tail"
	awsCloudTrailTailCursorKind      = "watermark_overlap"
	awsCloudTrailTailDefaultLookback = 15 * time.Minute
	awsCloudTrailTailOverlap         = 5 * time.Minute
)

type awsCloudTrailTailStats struct {
	Fetched    int
	Written    int
	Duplicates int
	Since      time.Time
	Watermark  time.Time
}

func (i *AWSIntegration) runCloudTrailTail(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	if q == nil {
		return errors.New("aws cloudtrail tail requires queries")
	}
	if pool == nil {
		return errors.New("aws cloudtrail tail requires database pool")
	}
	if i == nil || i.client == nil || i.client.cloudtrail == nil {
		return nil
	}

	resource := AWSCloudTrailTailResource
	if scopedResource, ok := registry.ResourceScopeFromContext(ctx); ok {
		if scopedResource != AWSCloudTrailTailResource {
			return fmt.Errorf("aws tail resource %q is not supported", scopedResource)
		}
		resource = scopedResource
	}

	started := time.Now()
	runID, err := registry.StartSyncRunWithMode(ctx, q, "aws", i.sourceName, registry.RunModeTail)
	if err != nil {
		return err
	}

	report(registry.Event{Source: "aws", Stage: "tail-cloudtrail", Current: 0, Total: 1, Message: "tailing AWS CloudTrail events"})
	stats, err := i.tailCloudTrailWithCursor(ctx, pool, runID, resource)
	if err != nil {
		report(registry.Event{Source: "aws", Stage: "tail-cloudtrail", Current: 1, Total: 1, Message: err.Error(), Err: err})
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
		"since":       formatAWSTailTime(stats.Since),
		"watermark":   formatAWSTailTime(stats.Watermark),
	})
	if err := q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: statsPayload}); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	report(registry.Event{
		Source:  "aws",
		Stage:   "tail-cloudtrail",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("tailed %d CloudTrail events since %s", stats.Fetched, stats.Since.Format(time.RFC3339)),
	})
	slog.Info("aws cloudtrail tail complete", "source", i.sourceName, "fetched", stats.Fetched, "written", stats.Written, "duplicates", stats.Duplicates)
	return nil
}

func (i *AWSIntegration) tailCloudTrailWithCursor(ctx context.Context, pool *pgxpool.Pool, runID int64, resource string) (awsCloudTrailTailStats, error) {
	var (
		stats   awsCloudTrailTailStats
		tailErr error
	)
	store := tail.NewCursorStore(pool)
	err := store.WithLockedCursor(ctx, tail.CursorKey{
		SourceKind: "aws",
		SourceName: i.sourceName,
		Resource:   resource,
		CursorKind: awsCloudTrailTailCursorKind,
	}, func(lockCtx context.Context, qtx *gen.Queries, state gen.ConnectorCursorState) error {
		attemptedAt := time.Now().UTC()
		since := awsCloudTrailTailSince(state, attemptedAt)
		stats.Since = since

		events, err := i.client.ListCloudTrailEvents(lockCtx, since)
		if err != nil {
			tailErr = fmt.Errorf("aws list CloudTrail events since %s: %w", since.Format(time.RFC3339), err)
			return updateAWSCloudTrailTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
		}

		dispatcher := recorddispatch.NewEventDispatcher(pool)
		stats.Fetched = len(events)
		watermark := attemptedAt
		for _, event := range events {
			if event.Timestamp.IsZero() {
				tailErr = fmt.Errorf("aws CloudTrail event %q is missing timestamp", event.ID)
				return updateAWSCloudTrailTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			occurredAt := event.Timestamp.UTC()
			if occurredAt.After(watermark) || watermark.Equal(attemptedAt) {
				watermark = occurredAt
			}
			record, err := awsCanonicalCloudTrailEventRecord(i.sourceName, awsCloudTrailTailChannel, event)
			if err != nil {
				tailErr = err
				return updateAWSCloudTrailTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			result, err := dispatcher.DispatchEvent(lockCtx, record)
			if err != nil {
				tailErr = fmt.Errorf("write canonical AWS CloudTrail event %s: %w", event.ID, err)
				return updateAWSCloudTrailTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			if result.Inserted {
				stats.Written++
			} else {
				stats.Duplicates++
			}
		}
		stats.Watermark = watermark

		return updateAWSCloudTrailTailCursorSuccess(lockCtx, qtx, state, runID, attemptedAt, watermark, lastAWSCloudTrailEventID(events), stats)
	})
	if err != nil {
		return stats, err
	}
	if tailErr != nil {
		return stats, tailErr
	}
	return stats, nil
}

func awsCloudTrailTailSince(state gen.ConnectorCursorState, now time.Time) time.Time {
	now = now.UTC()
	if state.Watermark.Valid && !state.Watermark.Time.IsZero() {
		return state.Watermark.Time.UTC().Add(-awsCloudTrailTailOverlap)
	}
	return now.Add(-awsCloudTrailTailDefaultLookback)
}

func updateAWSCloudTrailTailCursorSuccess(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState, runID int64, attemptedAt, watermark time.Time, lastProviderEventID string, stats awsCloudTrailTailStats) error {
	params := awsCloudTrailTailCursorParams(state)
	params.LastAttemptAt = pgtype.Timestamptz{Time: attemptedAt.UTC(), Valid: true}
	params.LastSuccessAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	params.LastErrorAt = pgtype.Timestamptz{}
	params.LastError = ""
	params.LastRunID = pgtype.Int8{Int64: runID, Valid: runID != 0}
	params.LastProviderEventID = strings.TrimSpace(lastProviderEventID)
	params.Watermark = pgtype.Timestamptz{Time: watermark.UTC(), Valid: !watermark.IsZero()}
	params.CursorJson = registry.MarshalJSON(map[string]any{
		"channel":         awsCloudTrailTailChannel,
		"overlap_seconds": int(awsCloudTrailTailOverlap.Seconds()),
		"fetched":         stats.Fetched,
		"written":         stats.Written,
		"duplicates":      stats.Duplicates,
		"since":           formatAWSTailTime(stats.Since),
		"watermark":       formatAWSTailTime(watermark),
	})
	params.NeedsFullResync = false
	return q.UpsertConnectorCursorState(ctx, params)
}

func updateAWSCloudTrailTailCursorError(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState, runID int64, attemptedAt time.Time, err error) error {
	params := awsCloudTrailTailCursorParams(state)
	params.LastAttemptAt = pgtype.Timestamptz{Time: attemptedAt.UTC(), Valid: true}
	params.LastErrorAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	params.LastError = truncateAWSTailMessage(err)
	params.LastRunID = pgtype.Int8{Int64: runID, Valid: runID != 0}
	return q.UpsertConnectorCursorState(ctx, params)
}

func awsCloudTrailTailCursorParams(state gen.ConnectorCursorState) gen.UpsertConnectorCursorStateParams {
	cursorJSON := state.CursorJson
	if len(cursorJSON) == 0 {
		cursorJSON = []byte(`{}`)
	}
	return gen.UpsertConnectorCursorStateParams{
		SourceKind:          "aws",
		SourceID:            state.SourceID,
		SourceName:          strings.TrimSpace(state.SourceName),
		Resource:            strings.TrimSpace(state.Resource),
		CursorKind:          awsCloudTrailTailCursorKind,
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

func awsCanonicalCloudTrailEventRecord(sourceName, channel string, event CloudTrailEvent) (records.EventRecord, error) {
	raw := map[string]any{}
	if len(event.RawJSON) > 0 {
		if err := json.Unmarshal(event.RawJSON, &raw); err != nil {
			return records.EventRecord{}, fmt.Errorf("decode AWS CloudTrail event %s: %w", event.ID, err)
		}
	}
	targets := []records.TargetRef{
		{
			Kind: "aws_service",
			ID:   strings.TrimSpace(event.Source),
			Name: strings.TrimSpace(event.Name),
			Role: "resource",
		},
	}
	return records.EventRecord{
		Source: records.SourceRef{
			Kind: "aws",
			Name: sourceName,
		},
		Channel:         strings.TrimSpace(channel),
		ProviderEventID: strings.TrimSpace(event.ID),
		DedupeKeyValue:  "provider:" + strings.TrimSpace(event.ID),
		EventType:       "aws.cloudtrail." + strings.ToLower(strings.TrimSpace(event.Name)),
		Category:        "aws.cloudtrail",
		Action:          strings.TrimSpace(event.Name),
		OccurredAt:      event.Timestamp,
		ObservedAt:      event.Timestamp,
		Actor: records.ActorRef{
			Kind:        "aws_principal",
			DisplayName: strings.TrimSpace(event.Username),
		},
		Targets: targets,
		Envelope: map[string]any{
			"event_source":  strings.TrimSpace(event.Source),
			"access_key_id": strings.TrimSpace(event.AccessKeyID),
		},
		Raw: raw,
	}, nil
}

func lastAWSCloudTrailEventID(events []CloudTrailEvent) string {
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

func formatAWSTailTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func truncateAWSTailMessage(err error) string {
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
