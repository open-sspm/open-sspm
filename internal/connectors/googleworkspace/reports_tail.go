package googleworkspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	GoogleWorkspaceReportsTailResource        = "reports_activities"
	googleWorkspaceReportsTailChannel         = "reports_tail"
	googleWorkspaceReportsTailCursorKind      = "watermark_overlap"
	googleWorkspaceReportsTailDefaultLookback = 15 * time.Minute
	googleWorkspaceReportsTailOverlap         = 5 * time.Minute
)

type googleWorkspaceReportsTailStats struct {
	Fetched    int
	Written    int
	Duplicates int
	Since      time.Time
	Watermark  time.Time
}

func (i *GoogleWorkspaceIntegration) runReportsTail(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	if q == nil {
		return errors.New("google workspace reports tail requires queries")
	}
	if pool == nil {
		return errors.New("google workspace reports tail requires database pool")
	}
	if i == nil || !i.SupportsRunMode(registry.RunModeTail) {
		return nil
	}

	resource := GoogleWorkspaceReportsTailResource
	if scopedResource, ok := registry.ResourceScopeFromContext(ctx); ok {
		if scopedResource != GoogleWorkspaceReportsTailResource {
			return fmt.Errorf("google workspace tail resource %q is not supported", scopedResource)
		}
		resource = scopedResource
	}

	started := time.Now()
	runID, err := registry.StartSyncRunWithMode(ctx, q, configstore.KindGoogleWorkspace, i.customerID, registry.RunModeTail)
	if err != nil {
		return err
	}

	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "tail-reports", Current: 0, Total: 1, Message: "tailing Google Workspace Reports activities"})
	stats, err := i.tailReportsWithCursor(ctx, pool, runID, resource)
	if err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "tail-reports", Current: 1, Total: 1, Message: err.Error(), Err: err})
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
		"since":       formatGoogleWorkspaceTailTime(stats.Since),
		"watermark":   formatGoogleWorkspaceTailTime(stats.Watermark),
	})
	if err := q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: statsPayload}); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	report(registry.Event{
		Source:  configstore.KindGoogleWorkspace,
		Stage:   "tail-reports",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("tailed %d Reports activities since %s", stats.Fetched, stats.Since.Format(time.RFC3339)),
	})
	slog.Info("google workspace reports tail complete", "source", i.customerID, "fetched", stats.Fetched, "written", stats.Written, "duplicates", stats.Duplicates)
	return nil
}

func (i *GoogleWorkspaceIntegration) tailReportsWithCursor(ctx context.Context, pool *pgxpool.Pool, runID int64, resource string) (googleWorkspaceReportsTailStats, error) {
	var (
		stats   googleWorkspaceReportsTailStats
		tailErr error
	)
	store := tail.NewCursorStore(pool)
	err := store.WithLockedCursor(ctx, tail.CursorKey{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: i.customerID,
		Resource:   resource,
		CursorKind: googleWorkspaceReportsTailCursorKind,
	}, func(lockCtx context.Context, qtx *gen.Queries, state gen.ConnectorCursorState) error {
		attemptedAt := time.Now().UTC()
		since := googleWorkspaceReportsTailSince(state, attemptedAt)
		stats.Since = since

		activities, err := i.listReportsTailActivities(lockCtx, since)
		if err != nil {
			tailErr = fmt.Errorf("google workspace list Reports activities since %s: %w", since.Format(time.RFC3339), err)
			return updateGoogleWorkspaceReportsTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
		}

		writer := canonevents.NewWriter(pool)
		stats.Fetched = len(activities)
		watermark := attemptedAt
		for _, activity := range activities {
			occurredAt, err := googleWorkspaceActivityOccurredAt(activity)
			if err != nil {
				tailErr = err
				return updateGoogleWorkspaceReportsTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			if occurredAt.After(watermark) || watermark.Equal(attemptedAt) {
				watermark = occurredAt
			}
			record, err := googleWorkspaceCanonicalActivityRecord(i.customerID, googleWorkspaceReportsTailChannel, activity, occurredAt)
			if err != nil {
				tailErr = err
				return updateGoogleWorkspaceReportsTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			result, err := writer.WriteEvent(lockCtx, record, canonevents.WriteOptions{})
			if err != nil {
				tailErr = fmt.Errorf("write canonical Google Workspace Reports event %s: %w", record.ProviderEventID, err)
				return updateGoogleWorkspaceReportsTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			if result.Inserted {
				stats.Written++
			} else {
				stats.Duplicates++
			}
		}
		stats.Watermark = watermark

		return updateGoogleWorkspaceReportsTailCursorSuccess(lockCtx, qtx, state, runID, attemptedAt, watermark, lastGoogleWorkspaceActivityID(activities), stats)
	})
	if err != nil {
		return stats, err
	}
	if tailErr != nil {
		return stats, tailErr
	}
	return stats, nil
}

func (i *GoogleWorkspaceIntegration) listReportsTailActivities(ctx context.Context, since time.Time) ([]WorkspaceActivity, error) {
	if i == nil {
		return nil, errors.New("google workspace integration is nil")
	}
	if i.reportsActivityLister != nil {
		return i.reportsActivityLister(ctx, since)
	}
	if i.client == nil {
		return nil, errors.New("google workspace client is required for Reports tail")
	}
	loginActivities, err := i.client.ListLoginActivities(ctx, &since)
	if err != nil {
		return nil, err
	}
	tokenActivities, err := i.client.ListTokenActivities(ctx, &since)
	if err != nil {
		return nil, err
	}
	activities := make([]WorkspaceActivity, 0, len(loginActivities)+len(tokenActivities))
	activities = append(activities, loginActivities...)
	activities = append(activities, tokenActivities...)
	return activities, nil
}

func googleWorkspaceReportsTailSince(state gen.ConnectorCursorState, now time.Time) time.Time {
	now = now.UTC()
	if state.Watermark.Valid && !state.Watermark.Time.IsZero() {
		return state.Watermark.Time.UTC().Add(-googleWorkspaceReportsTailOverlap)
	}
	return now.Add(-googleWorkspaceReportsTailDefaultLookback)
}

func updateGoogleWorkspaceReportsTailCursorSuccess(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState, runID int64, attemptedAt, watermark time.Time, lastProviderEventID string, stats googleWorkspaceReportsTailStats) error {
	params := googleWorkspaceReportsTailCursorParams(state)
	params.LastAttemptAt = pgtype.Timestamptz{Time: attemptedAt.UTC(), Valid: true}
	params.LastSuccessAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	params.LastErrorAt = pgtype.Timestamptz{}
	params.LastError = ""
	params.LastRunID = pgtype.Int8{Int64: runID, Valid: runID != 0}
	params.LastProviderEventID = strings.TrimSpace(lastProviderEventID)
	params.Watermark = pgtype.Timestamptz{Time: watermark.UTC(), Valid: !watermark.IsZero()}
	params.CursorJson = registry.MarshalJSON(map[string]any{
		"channel":         googleWorkspaceReportsTailChannel,
		"overlap_seconds": int(googleWorkspaceReportsTailOverlap.Seconds()),
		"fetched":         stats.Fetched,
		"written":         stats.Written,
		"duplicates":      stats.Duplicates,
		"since":           formatGoogleWorkspaceTailTime(stats.Since),
		"watermark":       formatGoogleWorkspaceTailTime(watermark),
	})
	params.NeedsFullResync = false
	return q.UpsertConnectorCursorState(ctx, params)
}

func updateGoogleWorkspaceReportsTailCursorError(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState, runID int64, attemptedAt time.Time, err error) error {
	params := googleWorkspaceReportsTailCursorParams(state)
	params.LastAttemptAt = pgtype.Timestamptz{Time: attemptedAt.UTC(), Valid: true}
	params.LastErrorAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	params.LastError = truncateGoogleWorkspaceTailMessage(err)
	params.LastRunID = pgtype.Int8{Int64: runID, Valid: runID != 0}
	return q.UpsertConnectorCursorState(ctx, params)
}

func googleWorkspaceReportsTailCursorParams(state gen.ConnectorCursorState) gen.UpsertConnectorCursorStateParams {
	cursorJSON := state.CursorJson
	if len(cursorJSON) == 0 {
		cursorJSON = []byte(`{}`)
	}
	return gen.UpsertConnectorCursorStateParams{
		SourceKind:          configstore.KindGoogleWorkspace,
		SourceID:            state.SourceID,
		SourceName:          strings.TrimSpace(state.SourceName),
		Resource:            strings.TrimSpace(state.Resource),
		CursorKind:          googleWorkspaceReportsTailCursorKind,
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

func googleWorkspaceCanonicalActivityRecord(sourceName, channel string, activity WorkspaceActivity, occurredAt time.Time) (records.EventRecord, error) {
	raw, err := googleWorkspaceActivityRawMap(activity)
	if err != nil {
		return records.EventRecord{}, err
	}
	applicationName := strings.TrimSpace(activity.ID.ApplicationName)
	if applicationName == "" {
		applicationName = "reports"
	}
	eventName := strings.TrimSpace(activity.EventName())
	if eventName == "" {
		eventName = "activity"
	}
	eventType := "google_workspace." + strings.ToLower(applicationName) + "." + strings.ToLower(eventName)
	providerEventID := googleWorkspaceActivityProviderID(activity)
	targets := googleWorkspaceActivityTargets(activity)

	return records.EventRecord{
		Source: records.SourceRef{
			Kind: configstore.KindGoogleWorkspace,
			Name: sourceName,
		},
		Channel:         strings.TrimSpace(channel),
		ProviderEventID: providerEventID,
		DedupeKeyValue:  googleWorkspaceActivityDedupeKey(activity, providerEventID),
		EventType:       eventType,
		Category:        "google_workspace.audit",
		Action:          strings.TrimSpace(eventName),
		OccurredAt:      occurredAt,
		ObservedAt:      occurredAt,
		Actor: records.ActorRef{
			Kind:  "google_workspace_actor",
			ID:    strings.TrimSpace(activity.Actor.ProfileID),
			Email: strings.ToLower(strings.TrimSpace(activity.Actor.Email)),
		},
		Targets: targets,
		Outcome: googleWorkspaceActivityOutcome(activity),
		Client: records.ClientRef{
			IP: strings.TrimSpace(activity.IPAddress),
		},
		Envelope: map[string]any{
			"application_name": applicationName,
			"customer_id":      strings.TrimSpace(activity.ID.CustomerID),
			"event_type":       strings.TrimSpace(activity.EventType()),
		},
		Raw: raw,
	}, nil
}

func googleWorkspaceActivityTargets(activity WorkspaceActivity) []records.TargetRef {
	targets := make([]records.TargetRef, 0, 2)
	actorID := strings.TrimSpace(activity.Actor.ProfileID)
	actorEmail := strings.ToLower(strings.TrimSpace(activity.Actor.Email))
	if actorID != "" || actorEmail != "" {
		targets = append(targets, records.TargetRef{
			Kind:  "google_workspace_user",
			ID:    actorID,
			Email: actorEmail,
			Role:  "actor",
		})
	}

	clientID := firstGoogleWorkspaceActivityParam(activity, "client_id", "oauth_client_id", "app_id")
	clientName := firstGoogleWorkspaceActivityParam(activity, "app_name", "client_name", "application_name", "display_name")
	if clientID != "" || clientName != "" {
		targets = append(targets, records.TargetRef{
			Kind: "google_workspace_app",
			ID:   clientID,
			Name: clientName,
			Role: "resource",
		})
	}
	return targets
}

func googleWorkspaceActivityOutcome(activity WorkspaceActivity) records.Outcome {
	value := strings.ToLower(strings.TrimSpace(activity.EventName() + " " + activity.EventType()))
	switch {
	case strings.Contains(value, "fail"), strings.Contains(value, "denied"), strings.Contains(value, "error"):
		return records.OutcomeFailure
	case strings.Contains(value, "success"), strings.Contains(value, "login"):
		return records.OutcomeSuccess
	default:
		return records.OutcomeUnknown
	}
}

func googleWorkspaceActivityRawMap(activity WorkspaceActivity) (map[string]any, error) {
	raw := map[string]any{}
	if len(activity.RawJSON) > 0 {
		if err := json.Unmarshal(activity.RawJSON, &raw); err != nil {
			return nil, fmt.Errorf("decode Google Workspace Reports activity %s: %w", googleWorkspaceActivityProviderID(activity), err)
		}
		return raw, nil
	}
	b, err := json.Marshal(activity)
	if err != nil {
		return nil, fmt.Errorf("encode Google Workspace Reports activity %s: %w", googleWorkspaceActivityProviderID(activity), err)
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("decode Google Workspace Reports activity %s: %w", googleWorkspaceActivityProviderID(activity), err)
	}
	return raw, nil
}

func googleWorkspaceActivityOccurredAt(activity WorkspaceActivity) (time.Time, error) {
	rawTime := strings.TrimSpace(activity.ID.Time)
	if rawTime == "" {
		return time.Time{}, fmt.Errorf("google workspace Reports activity %s is missing activity time", googleWorkspaceActivityProviderID(activity))
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, rawTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse google workspace Reports activity time %q: %w", rawTime, err)
	}
	return occurredAt.UTC(), nil
}

func googleWorkspaceActivityProviderID(activity WorkspaceActivity) string {
	parts := []string{
		strings.TrimSpace(activity.ID.UniqueQualifier),
		strings.TrimSpace(activity.ID.Time),
		strings.TrimSpace(activity.ID.ApplicationName),
		strings.TrimSpace(activity.EventName()),
		strings.TrimSpace(activity.Actor.ProfileID),
		strings.ToLower(strings.TrimSpace(activity.Actor.Email)),
	}
	nonEmpty := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			nonEmpty = append(nonEmpty, part)
		}
	}
	if len(nonEmpty) == 0 {
		return ""
	}
	return strings.Join(nonEmpty, ":")
}

func googleWorkspaceActivityDedupeKey(activity WorkspaceActivity, providerEventID string) string {
	if strings.TrimSpace(providerEventID) != "" {
		return "provider:" + strings.TrimSpace(providerEventID)
	}
	raw := activity.RawJSON
	if len(raw) == 0 {
		raw = registry.MarshalJSON(activity)
	}
	sum := sha256.Sum256(raw)
	return "hash:" + hex.EncodeToString(sum[:])
}

func lastGoogleWorkspaceActivityID(activities []WorkspaceActivity) string {
	var (
		lastID string
		lastAt time.Time
	)
	for _, activity := range activities {
		id := googleWorkspaceActivityProviderID(activity)
		if id == "" {
			continue
		}
		occurredAt, err := googleWorkspaceActivityOccurredAt(activity)
		if err != nil {
			continue
		}
		if lastID == "" || occurredAt.After(lastAt) || occurredAt.Equal(lastAt) {
			lastID = id
			lastAt = occurredAt
		}
	}
	return lastID
}

func firstGoogleWorkspaceActivityParam(activity WorkspaceActivity, names ...string) string {
	for _, name := range names {
		values := activity.ParameterValues(name)
		for _, value := range values {
			if value = strings.TrimSpace(value); value != "" {
				return value
			}
		}
	}
	return ""
}

func formatGoogleWorkspaceTailTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func truncateGoogleWorkspaceTailMessage(err error) string {
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
