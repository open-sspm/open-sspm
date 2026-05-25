package okta

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/ingest/recorddispatch"
	"github.com/open-sspm/open-sspm/internal/matching"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/rules/datasets"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
	"github.com/open-sspm/open-sspm/internal/tail"
)

type OktaIntegration struct {
	client                 *Client
	sourceName             string
	workers                int
	discoveryEnabled       bool
	discoveryPollerEnabled bool
	systemLogLister        func(context.Context, time.Time) ([]SystemLogEvent, error)
	lastRunID              int64
}

func NewOktaIntegration(client *Client, sourceName string, workers int, discoveryEnabled bool) *OktaIntegration {
	return NewOktaIntegrationWithDiscoveryPolling(client, sourceName, workers, discoveryEnabled, true)
}

func NewOktaIntegrationWithDiscoveryPolling(client *Client, sourceName string, workers int, discoveryEnabled, discoveryPollerEnabled bool) *OktaIntegration {
	if workers < 1 {
		workers = 3
	}
	return &OktaIntegration{
		client:                 client,
		sourceName:             strings.TrimSpace(sourceName),
		workers:                workers,
		discoveryEnabled:       discoveryEnabled,
		discoveryPollerEnabled: discoveryPollerEnabled,
	}
}

func (i *OktaIntegration) Kind() string { return "okta" }
func (i *OktaIntegration) Name() string { return i.sourceName }
func (i *OktaIntegration) Role() registry.IntegrationRole {
	return registry.RoleIdP
}

func (i *OktaIntegration) SupportsRunMode(mode registry.RunMode) bool {
	if i == nil {
		return false
	}
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return i.client != nil && i.discoveryEnabled && i.discoveryPollerEnabled
	case registry.RunModeTail:
		return i.client != nil || i.systemLogLister != nil
	default:
		return i.client != nil
	}
}

func (i *OktaIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "okta", Stage: "list-users", Current: 0, Total: 1, Message: "listing users"},
		{Source: "okta", Stage: "sync-users", Current: 0, Total: registry.UnknownTotal, Message: "syncing users"},
		{Source: "okta", Stage: "sync-groups", Current: 0, Total: registry.UnknownTotal, Message: "syncing groups"},
		{Source: "okta", Stage: "sync-app-assignments", Current: 0, Total: registry.UnknownTotal, Message: "syncing app assignments"},
		{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: registry.UnknownTotal, Message: "syncing app group assignments"},
		{Source: "okta", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing discovery events"},
		{Source: "okta", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery events"},
		{Source: "okta", Stage: "write-discovery", Current: 0, Total: registry.UnknownTotal, Message: "writing discovery data"},
		{Source: "okta", Stage: "evaluate-rules", Current: 0, Total: 1, Message: "evaluating rulesets"},
	}
}

func (i *OktaIntegration) Run(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event), mode registry.RunMode) error {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		if !i.SupportsRunMode(registry.RunModeDiscovery) {
			return nil
		}
		return i.runDiscovery(ctx, q, pool, report)
	case registry.RunModeTail:
		if !i.SupportsRunMode(registry.RunModeTail) {
			return nil
		}
		return i.runSystemLogTail(ctx, q, pool, report)
	default:
		return i.runFull(ctx, q, pool, report)
	}
}

func (i *OktaIntegration) runFull(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	if i.client == nil {
		return fmt.Errorf("okta API token is required for full sync")
	}
	started := time.Now()
	runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind("okta", registry.RunModeFull), i.sourceName)
	if err != nil {
		return err
	}
	i.lastRunID = runID

	emitter := recorddispatch.NewDispatcher(nil, recorddispatch.NewOktaStateProjector(q, runID))
	if err := beginOktaSnapshot(ctx, emitter, i.sourceName, records.ResourceIdentity, runID); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	users, err := i.client.ListUsers(ctx)
	if err != nil {
		err = fmt.Errorf("okta list users (/api/v1/users): %w", err)
		report(registry.Event{Source: "okta", Stage: "list-users", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: "okta", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})
	report(registry.Event{Source: "okta", Stage: "sync-users", Current: 0, Total: int64(len(users)), Message: fmt.Sprintf("syncing %d users", len(users))})

	if err := i.syncOktaAccountsRecords(ctx, emitter, report, runID, users); err != nil {
		report(registry.Event{Source: "okta", Stage: "sync-users", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	if err := beginOktaSnapshot(ctx, emitter, i.sourceName, records.ResourceGroup, runID); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	if err := i.syncOktaGroupsRecords(ctx, emitter, report, runID); err != nil {
		report(registry.Event{Source: "okta", Stage: "sync-groups", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	if err := beginOktaSnapshot(ctx, emitter, i.sourceName, records.ResourceApplication, runID); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	if err := beginOktaSnapshot(ctx, emitter, i.sourceName, records.ResourceEntitlement, runID); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	appIDs, err := i.syncOktaAppAssignmentsRecords(ctx, emitter, report, runID)
	if err != nil {
		report(registry.Event{Source: "okta", Stage: "sync-app-assignments", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	if err := i.syncOktaAppGroupAssignmentsRecords(ctx, emitter, report, runID, appIDs); err != nil {
		report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	if err := registry.FinalizeOktaRun(ctx, q, pool, runID, i.sourceName, time.Since(started), false); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	slog.Info("okta sync complete", "users", len(users))
	return nil
}

func (i *OktaIntegration) runDiscovery(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	if i.client == nil {
		return fmt.Errorf("okta API token is required for discovery polling")
	}
	started := time.Now()
	runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind("okta", registry.RunModeDiscovery), i.sourceName)
	if err != nil {
		return err
	}
	emitter := recorddispatch.NewDispatcher(nil, recorddispatch.NewOktaStateProjector(q, runID))
	if err := i.syncDiscoveryRecords(ctx, q, emitter, report, runID); err != nil {
		report(registry.Event{Source: "okta", Stage: "write-discovery", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}
	if err := registry.FinalizeDiscoveryRun(ctx, q, pool, runID, "okta", i.sourceName, time.Since(started)); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	if err := i.seedOktaAutoBindings(ctx, q); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	slog.Info("okta discovery sync complete", "source", i.sourceName)
	return nil
}

const (
	SystemLogTailResource            = "system_log"
	SystemLogTailChannel             = "system_log_tail"
	oktaSystemLogTailCursorKind      = "watermark_overlap"
	oktaSystemLogTailDefaultLookback = 15 * time.Minute
	oktaSystemLogTailOverlap         = 2 * time.Minute
)

type oktaSystemLogTailStats struct {
	Fetched    int
	Written    int
	Duplicates int
	Ignored    int
	Since      time.Time
	Watermark  time.Time
}

func (i *OktaIntegration) runSystemLogTail(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	if q == nil {
		return errors.New("okta system log tail requires queries")
	}
	if pool == nil {
		return errors.New("okta system log tail requires database pool")
	}
	if i == nil || !i.SupportsRunMode(registry.RunModeTail) {
		return nil
	}

	resource := SystemLogTailResource
	if scopedResource, ok := registry.ResourceScopeFromContext(ctx); ok {
		if scopedResource != SystemLogTailResource {
			return fmt.Errorf("okta tail resource %q is not supported", scopedResource)
		}
		resource = scopedResource
	}

	started := time.Now()
	runKind := registry.SyncRunSourceKind("okta", registry.RunModeTail)
	runID, err := registry.StartSyncRun(ctx, q, runKind, i.sourceName)
	if err != nil {
		return err
	}

	report(registry.Event{Source: "okta", Stage: "tail-system-log", Current: 0, Total: 1, Message: "tailing Okta System Log"})
	stats, err := i.tailSystemLogWithCursor(ctx, pool, runID, resource)
	if err != nil {
		report(registry.Event{Source: "okta", Stage: "tail-system-log", Current: 1, Total: 1, Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}

	duration := time.Since(started)
	statsPayload := registry.MarshalJSON(map[string]any{
		"counts": map[string]int{
			"fetched":    stats.Fetched,
			"written":    stats.Written,
			"duplicates": stats.Duplicates,
			"ignored":    stats.Ignored,
		},
		"duration_ms": duration.Milliseconds(),
		"resource":    resource,
		"since":       formatOptionalTime(stats.Since),
		"watermark":   formatOptionalTime(stats.Watermark),
	})
	if err := q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: statsPayload}); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	report(registry.Event{
		Source:  "okta",
		Stage:   "tail-system-log",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("tailed %d events since %s", stats.Fetched, stats.Since.Format(time.RFC3339)),
	})
	slog.Info("okta system log tail complete", "source", i.sourceName, "fetched", stats.Fetched, "written", stats.Written, "duplicates", stats.Duplicates, "ignored", stats.Ignored)
	return nil
}

func (i *OktaIntegration) tailSystemLogWithCursor(ctx context.Context, pool *pgxpool.Pool, runID int64, resource string) (oktaSystemLogTailStats, error) {
	var (
		stats   oktaSystemLogTailStats
		tailErr error
	)
	store := tail.NewCursorStore(pool)
	err := store.WithLockedCursor(ctx, tail.CursorKey{
		SourceKind: "okta",
		SourceName: i.sourceName,
		Resource:   resource,
		CursorKind: oktaSystemLogTailCursorKind,
	}, func(lockCtx context.Context, qtx *gen.Queries, state gen.ConnectorCursorState) error {
		attemptedAt := time.Now().UTC()
		since := oktaSystemLogTailSince(state, attemptedAt)
		stats.Since = since

		events, err := i.listSystemLogEventsSince(lockCtx, since)
		if err != nil {
			tailErr = fmt.Errorf("okta list system log events since %s: %w", since.Format(time.RFC3339), err)
			return updateOktaTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
		}

		dispatcher := recorddispatch.NewEventDispatcher(pool)
		stats.Fetched = len(events)
		watermark := maxOktaSystemLogWatermark(events)
		if watermark.IsZero() {
			watermark = attemptedAt
		}
		stats.Watermark = watermark
		for _, event := range events {
			if !oktaTailShouldWriteEvent(event) {
				stats.Ignored++
				continue
			}
			record, err := CanonicalEventRecord(i.sourceName, SystemLogTailChannel, event)
			if err != nil {
				tailErr = err
				return updateOktaTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			result, err := dispatcher.DispatchEvent(lockCtx, record)
			if err != nil {
				tailErr = fmt.Errorf("write canonical Okta tail event %s: %w", event.ID, err)
				return updateOktaTailCursorError(lockCtx, qtx, state, runID, attemptedAt, tailErr)
			}
			if result.Inserted {
				stats.Written++
			} else {
				stats.Duplicates++
			}
		}

		lastProviderEventID := lastOktaSystemLogEventID(events)
		return updateOktaTailCursorSuccess(lockCtx, qtx, state, runID, attemptedAt, watermark, lastProviderEventID, stats)
	})
	if err != nil {
		return stats, err
	}
	if tailErr != nil {
		return stats, tailErr
	}
	return stats, nil
}

func (i *OktaIntegration) listSystemLogEventsSince(ctx context.Context, since time.Time) ([]SystemLogEvent, error) {
	if i == nil {
		return nil, errors.New("okta integration is nil")
	}
	if i.systemLogLister != nil {
		return i.systemLogLister(ctx, since)
	}
	if i.client == nil {
		return nil, errors.New("okta API token is required for system log tail")
	}
	return i.client.ListSystemLogEventsSince(ctx, since)
}

func oktaSystemLogTailSince(state gen.ConnectorCursorState, now time.Time) time.Time {
	now = now.UTC()
	if state.Watermark.Valid && !state.Watermark.Time.IsZero() {
		return state.Watermark.Time.UTC().Add(-oktaSystemLogTailOverlap)
	}
	return now.Add(-oktaSystemLogTailDefaultLookback)
}

func updateOktaTailCursorSuccess(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState, runID int64, attemptedAt, watermark time.Time, lastProviderEventID string, stats oktaSystemLogTailStats) error {
	params := oktaTailCursorParams(state)
	params.LastAttemptAt = pgtype.Timestamptz{Time: attemptedAt.UTC(), Valid: true}
	params.LastSuccessAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	params.LastErrorAt = pgtype.Timestamptz{}
	params.LastError = ""
	params.LastRunID = pgtype.Int8{Int64: runID, Valid: runID != 0}
	params.LastProviderEventID = strings.TrimSpace(lastProviderEventID)
	params.Watermark = pgtype.Timestamptz{Time: watermark.UTC(), Valid: !watermark.IsZero()}
	params.CursorJson = registry.MarshalJSON(map[string]any{
		"channel":         SystemLogTailChannel,
		"overlap_seconds": int(oktaSystemLogTailOverlap.Seconds()),
		"fetched":         stats.Fetched,
		"written":         stats.Written,
		"duplicates":      stats.Duplicates,
		"ignored":         stats.Ignored,
		"since":           formatOptionalTime(stats.Since),
		"watermark":       formatOptionalTime(watermark),
	})
	params.NeedsFullResync = false
	return q.UpsertConnectorCursorState(ctx, params)
}

func updateOktaTailCursorError(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState, runID int64, attemptedAt time.Time, err error) error {
	params := oktaTailCursorParams(state)
	params.LastAttemptAt = pgtype.Timestamptz{Time: attemptedAt.UTC(), Valid: true}
	params.LastErrorAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	params.LastError = truncateSyncMessage(err)
	params.LastRunID = pgtype.Int8{Int64: runID, Valid: runID != 0}
	return q.UpsertConnectorCursorState(ctx, params)
}

func oktaTailCursorParams(state gen.ConnectorCursorState) gen.UpsertConnectorCursorStateParams {
	cursorJSON := state.CursorJson
	if len(cursorJSON) == 0 {
		cursorJSON = []byte(`{}`)
	}
	return gen.UpsertConnectorCursorStateParams{
		SourceKind:          "okta",
		SourceID:            state.SourceID,
		SourceName:          strings.TrimSpace(state.SourceName),
		Resource:            strings.TrimSpace(state.Resource),
		CursorKind:          oktaSystemLogTailCursorKind,
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

func maxOktaSystemLogWatermark(events []SystemLogEvent) time.Time {
	var watermark time.Time
	for _, event := range events {
		published := event.Published.UTC()
		if published.IsZero() {
			continue
		}
		if watermark.IsZero() || published.After(watermark) {
			watermark = published
		}
	}
	return watermark
}

func lastOktaSystemLogEventID(events []SystemLogEvent) string {
	var (
		lastID        string
		lastPublished time.Time
	)
	for _, event := range events {
		id := strings.TrimSpace(event.ID)
		if id == "" {
			continue
		}
		published := event.Published.UTC()
		if lastID == "" || published.After(lastPublished) || published.Equal(lastPublished) {
			lastID = id
			lastPublished = published
		}
	}
	return lastID
}

func oktaTailShouldWriteEvent(event SystemLogEvent) bool {
	if strings.TrimSpace(event.ID) == "" {
		return false
	}
	return ShouldIngestPushEvent(event)
}

func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func truncateSyncMessage(err error) string {
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

func (i *OktaIntegration) EvaluateCompliance(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	if i == nil {
		return nil
	}

	runID := i.lastRunID
	if runID == 0 {
		return nil
	}

	oktaProvider := &datasets.OktaProvider{
		Client:  i.client,
		BaseURL: i.client.BaseURL,
		Token:   i.client.Token,
	}
	router := datasets.RouterProvider{
		Okta: oktaProvider,
	}
	e := engine.Engine{
		Q:        q,
		DB:       pool,
		Datasets: router,
		Now:      time.Now,
	}
	if err := e.Run(ctx, engine.Context{
		ScopeKind:   "connector_instance",
		SourceKind:  "okta",
		SourceName:  i.sourceName,
		SyncRunID:   &runID,
		EvaluatedAt: time.Now(),
	}); err != nil {
		err = fmt.Errorf("okta ruleset evaluations: %w", err)
		slog.Error("okta ruleset evaluations failed", "err", err)
		report(registry.Event{Source: "okta", Stage: "evaluate-rules", Current: 1, Total: 1, Message: err.Error(), Err: err})
		return err
	}

	report(registry.Event{Source: "okta", Stage: "evaluate-rules", Current: 1, Total: 1, Message: "evaluations complete"})
	return nil
}

// PHASE-TWO-DELETE: legacy direct Okta account writer kept only as a parity fallback; runFull now emits identity records through recorddispatch.
func (i *OktaIntegration) syncOktaAccounts(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, users []User) error {
	if len(users) == 0 {
		report(registry.Event{Source: "okta", Stage: "sync-users", Current: 0, Total: 0, Message: "no users to sync"})
		return nil
	}

	const batchSize = 1000
	for start := 0; start < len(users); start += batchSize {
		end := min(start+batchSize, len(users))
		batch := users[start:end]

		externalIDs := make([]string, 0, len(batch))
		emails := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		accountKinds := make([]string, 0, len(batch))
		entityCategories := make([]string, 0, len(batch))
		statuses := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		lastLoginAts := make([]pgtype.Timestamptz, 0, len(batch))
		lastLoginIPs := make([]string, 0, len(batch))
		lastLoginRegions := make([]string, 0, len(batch))

		for _, user := range batch {
			id := strings.TrimSpace(user.ID)
			if id == "" {
				continue
			}
			externalIDs = append(externalIDs, id)
			emails = append(emails, matching.NormalizeEmail(user.Email))
			displayNames = append(displayNames, user.DisplayName)
			accountKinds = append(accountKinds, oktaAccountKind(user))
			entityCategories = append(entityCategories, registry.EntityCategoryUser)
			statuses = append(statuses, user.Status)
			rawJSONs = append(rawJSONs, registry.WithEntityCategory(registry.NormalizeJSON(user.RawJSON), registry.EntityCategoryUser))
			lastLoginAts = append(lastLoginAts, registry.PgTimestamptzPtr(user.LastLoginAt))
			lastLoginIPs = append(lastLoginIPs, "")
			lastLoginRegions = append(lastLoginRegions, "")
		}
		if len(externalIDs) == 0 {
			continue
		}

		if _, err := q.UpsertOktaAccountsBulk(ctx, gen.UpsertOktaAccountsBulkParams{
			SourceName:       i.sourceName,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs,
			Emails:           emails,
			DisplayNames:     displayNames,
			AccountKinds:     accountKinds,
			EntityCategories: entityCategories,
			Statuses:         statuses,
			RawJsons:         rawJSONs,
			LastLoginAts:     lastLoginAts,
			LastLoginIps:     lastLoginIPs,
			LastLoginRegions: lastLoginRegions,
		}); err != nil {
			return fmt.Errorf("upsert okta accounts: %w", err)
		}

		report(registry.Event{
			Source:  "okta",
			Stage:   "sync-users",
			Current: int64(end),
			Total:   int64(len(users)),
			Message: fmt.Sprintf("users %d/%d", end, len(users)),
		})
	}

	return nil
}

// PHASE-TWO-DELETE: legacy direct Okta group writer kept only as a parity fallback; runFull now emits group and membership records through recorddispatch.
func (i *OktaIntegration) syncOktaGroups(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64) error {
	groups, err := i.client.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("okta list groups: %w", err)
	}
	report(registry.Event{Source: "okta", Stage: "sync-groups", Current: 0, Total: int64(len(groups)), Message: fmt.Sprintf("syncing %d groups", len(groups))})

	if len(groups) == 0 {
		return nil
	}

	const batchSize = 500
	for start := 0; start < len(groups); start += batchSize {
		end := min(start+batchSize, len(groups))
		batch := groups[start:end]

		externalIDs := make([]string, 0, len(batch))
		names := make([]string, 0, len(batch))
		types := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		accountExternalIDs := make([]string, 0, len(batch))
		accountEmails := make([]string, 0, len(batch))
		accountDisplayNames := make([]string, 0, len(batch))
		accountKinds := make([]string, 0, len(batch))
		accountEntityCategories := make([]string, 0, len(batch))
		accountRawJSONs := make([][]byte, 0, len(batch))
		accountLastLoginAts := make([]pgtype.Timestamptz, 0, len(batch))
		accountLastLoginIPs := make([]string, 0, len(batch))
		accountLastLoginRegions := make([]string, 0, len(batch))
		for _, group := range batch {
			id := strings.TrimSpace(group.ID)
			if id == "" {
				continue
			}
			externalIDs = append(externalIDs, id)
			names = append(names, group.Name)
			types = append(types, group.Type)
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(group.RawJSON))

			groupExternalID := oktaGroupExternalID(id)
			if groupExternalID == "" {
				continue
			}
			display := strings.TrimSpace(group.Name)
			if display == "" {
				display = groupExternalID
			}
			accountExternalIDs = append(accountExternalIDs, groupExternalID)
			accountEmails = append(accountEmails, "")
			accountDisplayNames = append(accountDisplayNames, display)
			accountKinds = append(accountKinds, registry.AccountKindService)
			accountEntityCategories = append(accountEntityCategories, registry.EntityCategoryGroup)
			accountRawJSONs = append(accountRawJSONs, registry.WithEntityCategory(registry.NormalizeJSON(group.RawJSON), registry.EntityCategoryGroup))
			accountLastLoginAts = append(accountLastLoginAts, pgtype.Timestamptz{})
			accountLastLoginIPs = append(accountLastLoginIPs, "")
			accountLastLoginRegions = append(accountLastLoginRegions, "")
		}
		if len(externalIDs) == 0 {
			continue
		}
		if _, err := q.UpsertOktaGroupsBulk(ctx, gen.UpsertOktaGroupsBulkParams{
			SeenInRunID: runID,
			SourceKind:  "okta",
			SourceName:  i.sourceName,
			ExternalIds: externalIDs,
			Names:       names,
			Types:       types,
			RawJsons:    rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert okta groups: %w", err)
		}
		if len(accountExternalIDs) > 0 {
			if _, err := q.UpsertSourceAccountsBulkBySource(ctx, gen.UpsertSourceAccountsBulkBySourceParams{
				SourceKind:       "okta",
				SourceName:       i.sourceName,
				SeenInRunID:      runID,
				ExternalIds:      accountExternalIDs,
				Emails:           accountEmails,
				DisplayNames:     accountDisplayNames,
				AccountKinds:     accountKinds,
				EntityCategories: accountEntityCategories,
				RawJsons:         accountRawJSONs,
				LastLoginAts:     accountLastLoginAts,
				LastLoginIps:     accountLastLoginIPs,
				LastLoginRegions: accountLastLoginRegions,
			}); err != nil {
				return fmt.Errorf("upsert okta group accounts: %w", err)
			}
		}
	}

	workers := min(len(groups), i.workers)
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan Group, len(groups))
	var done int64

	worker := func() {
		defer wg.Done()
		for group := range jobs {
			if jobCtx.Err() != nil {
				return
			}
			userExternalIDs, err := i.client.ListGroupUserIDs(jobCtx, group.ID)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("okta group %s users: %w", group.ID, err)
					cancel()
				})
				return
			}
			const membershipBatchSize = 5000
			for start := 0; start < len(userExternalIDs); start += membershipBatchSize {
				end := min(start+membershipBatchSize, len(userExternalIDs))
				oktaAccountExternalIDs := make([]string, 0, end-start)
				groupExternalIDs := make([]string, 0, end-start)
				for _, userExternalID := range userExternalIDs[start:end] {
					userExternalID = strings.TrimSpace(userExternalID)
					if userExternalID == "" {
						continue
					}
					oktaAccountExternalIDs = append(oktaAccountExternalIDs, userExternalID)
					groupExternalIDs = append(groupExternalIDs, group.ID)
				}
				if len(oktaAccountExternalIDs) == 0 {
					continue
				}
				if _, err := q.UpsertOktaGroupMembershipsBulkByOktaAccountExternalIDs(jobCtx, gen.UpsertOktaGroupMembershipsBulkByOktaAccountExternalIDsParams{
					SeenInRunID:            runID,
					OktaAccountExternalIds: oktaAccountExternalIDs,
					OktaGroupExternalIds:   groupExternalIDs,
				}); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("upsert okta group memberships for group %s: %w", group.ID, err)
						cancel()
					})
					return
				}
			}
			n := atomic.AddInt64(&done, 1)
			report(registry.Event{
				Source:  "okta",
				Stage:   "sync-groups",
				Current: n,
				Total:   int64(len(groups)),
				Message: fmt.Sprintf("groups %d/%d", n, len(groups)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}

	for _, group := range groups {
		if strings.TrimSpace(group.ID) == "" {
			continue
		}
		jobs <- group
	}
	close(jobs)
	wg.Wait()

	return firstErr
}

// PHASE-TWO-DELETE: legacy direct Okta app/user assignment writer kept only as a parity fallback; runFull now emits application and entitlement records through recorddispatch.
func (i *OktaIntegration) syncOktaAppAssignments(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64) ([]string, error) {
	apps, err := i.client.ListApps(ctx)
	if err != nil {
		return nil, fmt.Errorf("okta list apps: %w", err)
	}
	validApps := make([]App, 0, len(apps))
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" {
			continue
		}
		validApps = append(validApps, app)
	}
	report(registry.Event{Source: "okta", Stage: "sync-app-assignments", Current: 0, Total: int64(len(validApps)), Message: fmt.Sprintf("syncing %d apps", len(validApps))})

	appExternalIDs := make([]string, 0, len(validApps))
	if len(validApps) == 0 {
		return appExternalIDs, nil
	}

	const batchSize = 500
	for start := 0; start < len(validApps); start += batchSize {
		end := min(start+batchSize, len(validApps))
		batch := validApps[start:end]

		externalIDs := make([]string, 0, len(batch))
		labels := make([]string, 0, len(batch))
		names := make([]string, 0, len(batch))
		statuses := make([]string, 0, len(batch))
		signOnModes := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))

		for _, app := range batch {
			id := strings.TrimSpace(app.ID)
			if id == "" {
				continue
			}
			appExternalIDs = append(appExternalIDs, id)
			externalIDs = append(externalIDs, id)
			labels = append(labels, app.Label)
			names = append(names, app.Name)
			statuses = append(statuses, app.Status)
			signOnModes = append(signOnModes, app.SignOnMode)
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(app.RawJSON))
		}
		if len(externalIDs) == 0 {
			continue
		}
		if _, err := q.UpsertOktaAppsBulk(ctx, gen.UpsertOktaAppsBulkParams{
			SeenInRunID: runID,
			SourceKind:  "okta",
			SourceName:  i.sourceName,
			ExternalIds: externalIDs,
			Labels:      labels,
			Names:       names,
			Statuses:    statuses,
			SignOnModes: signOnModes,
			RawJsons:    rawJSONs,
		}); err != nil {
			return nil, fmt.Errorf("upsert okta apps: %w", err)
		}
	}

	workers := min(len(validApps), i.workers)
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan App, len(validApps))
	var done int64

	worker := func() {
		defer wg.Done()
		for app := range jobs {
			if jobCtx.Err() != nil {
				return
			}
			assignments, err := i.client.ListApplicationAccounts(jobCtx, app.ID)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("okta app %s accounts: %w", app.ID, err)
					cancel()
				})
				return
			}
			if len(assignments) == 0 {
				n := atomic.AddInt64(&done, 1)
				report(registry.Event{
					Source:  "okta",
					Stage:   "sync-app-assignments",
					Current: n,
					Total:   int64(len(validApps)),
					Message: fmt.Sprintf("apps %d/%d", n, len(validApps)),
				})
				continue
			}

			const assignmentBatchSize = 5000
			for start := 0; start < len(assignments); start += assignmentBatchSize {
				end := min(start+assignmentBatchSize, len(assignments))
				oktaAccountExternalIDs := make([]string, 0, end-start)
				oktaAppExternalIDs := make([]string, 0, end-start)
				scopes := make([]string, 0, end-start)
				profileJSONs := make([][]byte, 0, end-start)
				rawJSONs := make([][]byte, 0, end-start)
				for _, assignment := range assignments[start:end] {
					accountID := strings.TrimSpace(assignment.AccountID)
					if accountID == "" {
						continue
					}
					oktaAccountExternalIDs = append(oktaAccountExternalIDs, accountID)
					oktaAppExternalIDs = append(oktaAppExternalIDs, app.ID)
					scopes = append(scopes, assignment.Scope)
					profileJSONs = append(profileJSONs, registry.NormalizeJSON(assignment.ProfileJSON))
					rawJSONs = append(rawJSONs, registry.NormalizeJSON(assignment.RawJSON))
				}
				if len(oktaAccountExternalIDs) == 0 {
					continue
				}
				if _, err := q.UpsertOktaAppAssignmentsBulkByOktaAccountExternalIDs(jobCtx, gen.UpsertOktaAppAssignmentsBulkByOktaAccountExternalIDsParams{
					SeenInRunID:            runID,
					OktaAccountExternalIds: oktaAccountExternalIDs,
					OktaAppExternalIds:     oktaAppExternalIDs,
					Scopes:                 scopes,
					ProfileJsons:           profileJSONs,
					RawJsons:               rawJSONs,
				}); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("upsert okta app assignments for app %s: %w", app.ID, err)
						cancel()
					})
					return
				}
			}

			n := atomic.AddInt64(&done, 1)
			report(registry.Event{
				Source:  "okta",
				Stage:   "sync-app-assignments",
				Current: n,
				Total:   int64(len(validApps)),
				Message: fmt.Sprintf("apps %d/%d", n, len(validApps)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}

	for _, app := range validApps {
		jobs <- app
	}
	close(jobs)
	wg.Wait()

	return appExternalIDs, firstErr
}

// PHASE-TWO-DELETE: legacy direct Okta app/group assignment writer kept only as a parity fallback; runFull now emits group and entitlement records through recorddispatch.
func (i *OktaIntegration) syncOktaAppGroupAssignments(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, appExternalIDs []string) error {
	if len(appExternalIDs) == 0 {
		report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: 0, Message: "no apps to sync"})
		return nil
	}
	report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: int64(len(appExternalIDs)), Message: fmt.Sprintf("syncing %d apps", len(appExternalIDs))})

	workers := min(len(appExternalIDs), i.workers)
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan string, len(appExternalIDs))
	var done int64

	worker := func() {
		defer wg.Done()
		for appExternalID := range jobs {
			if jobCtx.Err() != nil {
				return
			}
			assignments, err := i.client.ListApplicationGroupAssignments(jobCtx, appExternalID)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("okta app %s group assignments: %w", appExternalID, err)
					cancel()
				})
				return
			}
			if len(assignments) > 0 {
				externalIDs := make([]string, 0, len(assignments))
				names := make([]string, 0, len(assignments))
				types := make([]string, 0, len(assignments))
				groupRawJSONs := make([][]byte, 0, len(assignments))
				for _, assignment := range assignments {
					group := assignment.Group
					id := strings.TrimSpace(group.ID)
					if id == "" {
						continue
					}
					externalIDs = append(externalIDs, id)
					names = append(names, group.Name)
					types = append(types, group.Type)
					groupRawJSONs = append(groupRawJSONs, registry.NormalizeJSON(group.RawJSON))
				}
				if len(externalIDs) > 0 {
					if _, err := q.UpsertOktaGroupsBulk(jobCtx, gen.UpsertOktaGroupsBulkParams{
						SeenInRunID: runID,
						SourceKind:  "okta",
						SourceName:  i.sourceName,
						ExternalIds: externalIDs,
						Names:       names,
						Types:       types,
						RawJsons:    groupRawJSONs,
					}); err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("upsert okta groups for app %s: %w", appExternalID, err)
							cancel()
						})
						return
					}
				}

				const assignmentBatchSize = 5000
				for start := 0; start < len(assignments); start += assignmentBatchSize {
					end := min(start+assignmentBatchSize, len(assignments))
					oktaAppExternalIDs := make([]string, 0, end-start)
					groupExternalIDs := make([]string, 0, end-start)
					priorities := make([]int32, 0, end-start)
					profileJSONs := make([][]byte, 0, end-start)
					rawJSONs := make([][]byte, 0, end-start)
					for _, assignment := range assignments[start:end] {
						groupID := strings.TrimSpace(assignment.Group.ID)
						if groupID == "" {
							continue
						}
						oktaAppExternalIDs = append(oktaAppExternalIDs, appExternalID)
						groupExternalIDs = append(groupExternalIDs, groupID)
						priorities = append(priorities, int32(assignment.Priority))
						profileJSONs = append(profileJSONs, registry.NormalizeJSON(assignment.ProfileJSON))
						rawJSONs = append(rawJSONs, registry.NormalizeJSON(assignment.RawJSON))
					}
					if len(oktaAppExternalIDs) == 0 {
						continue
					}
					if _, err := q.UpsertOktaAppGroupAssignmentsBulkByExternalIDs(jobCtx, gen.UpsertOktaAppGroupAssignmentsBulkByExternalIDsParams{
						SeenInRunID:          runID,
						SourceKind:           "okta",
						SourceName:           i.sourceName,
						OktaAppExternalIds:   oktaAppExternalIDs,
						OktaGroupExternalIds: groupExternalIDs,
						Priorities:           priorities,
						ProfileJsons:         profileJSONs,
						RawJsons:             rawJSONs,
					}); err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("upsert okta app group assignments for app %s: %w", appExternalID, err)
							cancel()
						})
						return
					}
				}
			}
			n := atomic.AddInt64(&done, 1)
			report(registry.Event{
				Source:  "okta",
				Stage:   "sync-app-group-assignments",
				Current: n,
				Total:   int64(len(appExternalIDs)),
				Message: fmt.Sprintf("apps %d/%d", n, len(appExternalIDs)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}

	for _, externalID := range appExternalIDs {
		externalID = strings.TrimSpace(externalID)
		if externalID == "" {
			continue
		}
		jobs <- externalID
	}
	close(jobs)
	wg.Wait()

	return firstErr
}

// PHASE-TWO-DELETE: legacy direct Okta discovery writer kept only as a parity fallback; discovery polling now emits discovery evidence records through recorddispatch.
func (i *OktaIntegration) syncDiscovery(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64) error {
	report(registry.Event{Source: "okta", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing discovery events"})

	now := time.Now().UTC()
	since := now.Add(-7 * 24 * time.Hour)
	latestObservedAt, err := q.GetLatestSaaSDiscoveryObservedAtBySource(ctx, gen.GetLatestSaaSDiscoveryObservedAtBySourceParams{
		SourceKind: "okta",
		SourceName: i.sourceName,
	})
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("okta", "idp_sso", "watermark_query_error").Inc()
		return fmt.Errorf("query latest discovery watermark: %w", err)
	}
	if latestObservedAt.Valid {
		candidate := latestObservedAt.Time.UTC().Add(-15 * time.Minute)
		if candidate.After(since) {
			since = candidate
		}
	}

	events, err := i.client.ListSystemLogEventsSince(ctx, since)
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("okta", "idp_sso", "api_error").Inc()
		return fmt.Errorf("okta list system log events: %w", err)
	}
	report(registry.Event{
		Source:  "okta",
		Stage:   "list-discovery-events",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d events since %s", len(events), since.Format(time.RFC3339)),
	})

	report(registry.Event{Source: "okta", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery events"})
	sources, normalizedEvents := NormalizeDiscoveryEvents(events, i.sourceName, now)
	report(registry.Event{
		Source:  "okta",
		Stage:   "normalize-discovery",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("normalized %d source rows and %d events", len(sources), len(normalizedEvents)),
	})

	if err := i.writeDiscoveryRows(ctx, q, report, runID, sources, normalizedEvents); err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("okta", "idp_sso", "db_error").Inc()
		return err
	}
	if err := i.seedOktaAutoBindings(ctx, q); err != nil {
		return err
	}
	return nil
}

func NormalizeDiscoveryEvents(events []SystemLogEvent, sourceName string, now time.Time) ([]discovery.SourceRow, []discovery.EventRow) {
	sourceByID := make(map[string]discovery.SourceRow, len(events))
	normalizedEvents := make([]discovery.EventRow, 0, len(events))

	for _, event := range events {
		sourceAppID := strings.TrimSpace(event.AppID)
		if sourceAppID == "" {
			sourceAppID = strings.TrimSpace(event.AppName)
		}
		if sourceAppID == "" {
			continue
		}

		sourceAppName := strings.TrimSpace(event.AppName)
		if sourceAppName == "" {
			sourceAppName = sourceAppID
		}
		sourceAppDomain := strings.TrimSpace(event.AppDomain)
		signalKind, ok := DiscoverySignalKind(event)
		if !ok {
			continue
		}

		observedAt := event.Published.UTC()
		if observedAt.IsZero() {
			observedAt = now
		}

		metadata := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:    "okta",
			SourceName:    sourceName,
			SourceAppID:   sourceAppID,
			SourceAppName: sourceAppName,
			SourceDomain:  sourceAppDomain,
		})
		if metadata.VendorName == "" {
			metadata = discovery.BuildMetadata(discovery.CanonicalInput{
				SourceKind:       "okta",
				SourceName:       sourceName,
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceDomain:     sourceAppDomain,
				SourceVendorName: sourceAppName,
			})
		}

		current := sourceByID[sourceAppID]
		if current.SourceAppID == "" || observedAt.After(current.SeenAt) {
			sourceByID[sourceAppID] = discovery.SourceRow{
				CanonicalKey:     metadata.CanonicalKey,
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SourceCategory:   metadata.Category,
				SeenAt:           observedAt,
			}
		}

		normalizedEvents = append(normalizedEvents, discovery.EventRow{
			CanonicalKey:     metadata.CanonicalKey,
			SignalKind:       signalKind,
			EventExternalID:  strings.TrimSpace(event.ID),
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceAppDomain:  metadata.Domain,
			SourceVendorName: metadata.VendorName,
			SourceCategory:   metadata.Category,
			ActorExternalID:  strings.TrimSpace(event.ActorID),
			ActorEmail:       strings.ToLower(strings.TrimSpace(event.ActorEmail)),
			ActorDisplayName: strings.TrimSpace(event.ActorName),
			ObservedAt:       observedAt,
			Scopes:           discovery.NormalizeScopes(event.GrantedScopes),
			RawJSON:          registry.NormalizeJSON(event.RawJSON),
		})
	}

	sourceRows := make([]discovery.SourceRow, 0, len(sourceByID))
	for _, sourceRow := range sourceByID {
		sourceRows = append(sourceRows, sourceRow)
	}
	return sourceRows, normalizedEvents
}

// DiscoverySignalKind recognizes only the event types we treat as real access
// evidence. Other app-tagged events (e.g. policy.lifecycle.update,
// user.session.start, partner application.user_membership.* variants) are
// intentionally dropped — the prior catch-all-to-IDPSSO behavior produced too
// much incidental signal once Okta push ingestion broadened the input stream.
func DiscoverySignalKind(event SystemLogEvent) (string, bool) {
	normalizedEventType := strings.ToLower(strings.TrimSpace(event.EventType))
	hasApp := strings.TrimSpace(event.AppID) != "" || strings.TrimSpace(event.AppName) != ""
	if !hasApp {
		return "", false
	}
	if isApplicationMembershipEvent(normalizedEventType) {
		return discovery.SignalKindAssignment, true
	}
	switch normalizedEventType {
	case "user.authentication.sso", "app.oauth2.signon":
		return discovery.SignalKindIDPSSO, true
	}
	if strings.Contains(normalizedEventType, "oauth") ||
		strings.Contains(normalizedEventType, "grant") ||
		strings.Contains(normalizedEventType, "consent") {
		return discovery.SignalKindOAuth, true
	}
	return "", false
}

func StateRefreshSignalKind(event SystemLogEvent) (string, bool) {
	normalizedEventType := strings.ToLower(strings.TrimSpace(event.EventType))
	switch {
	case isApplicationMembershipEvent(normalizedEventType):
		return "app_assignment", true
	case isGroupMembershipEvent(normalizedEventType):
		return "group_membership", true
	case strings.HasPrefix(normalizedEventType, "user.lifecycle.") || strings.HasPrefix(normalizedEventType, "user.account."):
		return "user", true
	case strings.HasPrefix(normalizedEventType, "group.lifecycle."):
		return "group", true
	case strings.HasPrefix(normalizedEventType, "application.lifecycle.") || strings.HasPrefix(normalizedEventType, "app.lifecycle."):
		return "app", true
	default:
		return "", false
	}
}

func ShouldIngestPushEvent(event SystemLogEvent) bool {
	if _, ok := DiscoverySignalKind(event); ok {
		return true
	}
	_, ok := StateRefreshSignalKind(event)
	return ok
}

func isApplicationMembershipEvent(eventType string) bool {
	switch eventType {
	case "application.user_membership.add", "application.user_membership.remove", "application.user_membership.update":
		return true
	default:
		return false
	}
}

func isGroupMembershipEvent(eventType string) bool {
	switch eventType {
	case "group.user_membership.add", "group.user_membership.remove", "group.user_membership.update":
		return true
	default:
		return false
	}
}

// PHASE-TWO-DELETE: legacy helper for direct discovery writes; the active Okta discovery path projects records.DiscoveryEvidencePayload.
func (i *OktaIntegration) writeDiscoveryRows(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, sources []discovery.SourceRow, events []discovery.EventRow) error {
	return discovery.WriteRows(ctx, q, discovery.WriteRowsParams{
		SourceKind: "okta",
		SourceName: i.sourceName,
		RunID:      runID,
		Sources:    sources,
		Events:     events,
		Report:     registry.DiscoveryProgressReporter(report),
	})
}

func (i *OktaIntegration) seedOktaAutoBindings(ctx context.Context, q *gen.Queries) error {
	rows, err := q.ListMappedOktaDiscoveryAppsBySource(ctx, i.sourceName)
	if err != nil {
		return fmt.Errorf("list okta discovery auto-bind candidates: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	connectorSourceByKind := map[string]string{}

	githubConfigRow, err := q.GetConnectorConfig(ctx, configstore.KindGitHub)
	if err == nil && githubConfigRow.Enabled {
		secretRows, secretErr := q.ListConnectorSecretsByKind(ctx, configstore.KindGitHub)
		if secretErr == nil {
			secretPresence := configstore.SecretPresenceByKind(secretRows)
			resolvedRaw, resolveErr := configstore.ResolveConfigWithSecretPresence(configstore.KindGitHub, githubConfigRow.Config, secretPresence[configstore.KindGitHub])
			if resolveErr == nil {
				githubCfg, decodeErr := configstore.DecodeGitHubConfig(resolvedRaw)
				if decodeErr == nil {
					githubCfg = githubCfg.Normalized()
					if githubCfg.Validate() == nil && strings.TrimSpace(githubCfg.Org) != "" {
						connectorSourceByKind[configstore.KindGitHub] = githubCfg.Org
					}
				}
			}
		}
	}

	datadogConfigRow, err := q.GetConnectorConfig(ctx, configstore.KindDatadog)
	if err == nil && datadogConfigRow.Enabled {
		secretRows, secretErr := q.ListConnectorSecretsByKind(ctx, configstore.KindDatadog)
		if secretErr == nil {
			secretPresence := configstore.SecretPresenceByKind(secretRows)
			resolvedRaw, resolveErr := configstore.ResolveConfigWithSecretPresence(configstore.KindDatadog, datadogConfigRow.Config, secretPresence[configstore.KindDatadog])
			if resolveErr == nil {
				datadogCfg, decodeErr := configstore.DecodeDatadogConfig(resolvedRaw)
				if decodeErr == nil {
					datadogCfg = datadogCfg.Normalized()
					if datadogCfg.Validate() == nil && strings.TrimSpace(datadogCfg.Site) != "" {
						connectorSourceByKind[configstore.KindDatadog] = datadogCfg.Site
					}
				}
			}
		}
	}

	boundCount := 0
	for _, row := range rows {
		connectorKind := strings.ToLower(strings.TrimSpace(row.IntegrationKind))
		connectorSource := strings.TrimSpace(connectorSourceByKind[connectorKind])
		if connectorSource == "" {
			continue
		}
		if err := q.UpsertSaaSAppBinding(ctx, gen.UpsertSaaSAppBindingParams{
			SaasAppID:           row.SaasAppID,
			ConnectorKind:       connectorKind,
			ConnectorSourceName: connectorSource,
			BindingSource:       "auto",
			Confidence:          0.8,
			IsPrimary:           false,
			CreatedByAuthUserID: pgtype.Int8{},
		}); err != nil {
			return fmt.Errorf("upsert okta auto binding for app %d: %w", row.SaasAppID, err)
		}
		boundCount++
	}

	if boundCount > 0 {
		if _, err := q.RecomputePrimarySaaSAppBindingsForAll(ctx); err != nil {
			return fmt.Errorf("recompute primary bindings: %w", err)
		}
	}
	return nil
}
