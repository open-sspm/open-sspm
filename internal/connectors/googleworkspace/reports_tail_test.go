package googleworkspace

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestGoogleWorkspaceReportsTailWritesCanonicalEventsAndAdvancesCursor(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "google_reports_tail"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		sourceName := "C0123"
		runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind(configstore.KindGoogleWorkspace, registry.RunModeTail), sourceName)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}

		occurredAt := time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC)
		activity := googleWorkspaceReportsActivityFixture(t, occurredAt)
		var requestedSince time.Time
		integration := &GoogleWorkspaceIntegration{
			customerID: sourceName,
			reportsActivityLister: func(_ context.Context, since time.Time) ([]WorkspaceActivity, error) {
				requestedSince = since
				return []WorkspaceActivity{activity, activity}, nil
			},
		}

		stats, err := integration.tailReportsWithCursor(ctx, pool, runID, GoogleWorkspaceReportsTailResource)
		if err != nil {
			t.Fatalf("tailReportsWithCursor() err = %v", err)
		}
		if requestedSince.IsZero() {
			t.Fatal("expected lister to receive a since watermark")
		}
		if stats.Fetched != 2 || stats.Written != 1 || stats.Duplicates != 1 {
			t.Fatalf("tail stats = %+v, want fetched=2 written=1 duplicates=1", stats)
		}

		var eventCount, targetCount, queueCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE source_kind = 'google_workspace' AND source_name = $1`, sourceName).Scan(&eventCount); err != nil {
			t.Fatalf("count canonical events: %v", err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_targets`).Scan(&targetCount); err != nil {
			t.Fatalf("count event targets: %v", err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM riskpolicy_event_queue`).Scan(&queueCount); err != nil {
			t.Fatalf("count riskpolicy queue rows: %v", err)
		}
		if eventCount != 1 {
			t.Fatalf("event count = %d, want 1", eventCount)
		}
		if targetCount != 2 {
			t.Fatalf("target count = %d, want 2", targetCount)
		}
		if queueCount != 1 {
			t.Fatalf("riskpolicy queue count = %d, want 1", queueCount)
		}

		cursor, err := q.GetConnectorCursorState(ctx, gen.GetConnectorCursorStateParams{
			SourceKind: configstore.KindGoogleWorkspace,
			SourceName: sourceName,
			Resource:   GoogleWorkspaceReportsTailResource,
		})
		if err != nil {
			t.Fatalf("GetConnectorCursorState() err = %v", err)
		}
		if !cursor.Watermark.Valid || !cursor.Watermark.Time.Equal(occurredAt) {
			t.Fatalf("cursor watermark = %v, want %v", cursor.Watermark, occurredAt)
		}
		if !cursor.LastRunID.Valid || cursor.LastRunID.Int64 != runID {
			t.Fatalf("cursor last_run_id = %+v, want %d", cursor.LastRunID, runID)
		}
		if cursor.LastProviderEventID == "" {
			t.Fatalf("expected last provider event id to be recorded")
		}
	})
}

func TestGoogleWorkspaceReportsTailDoesNotAdvanceCursorAfterPartialWriteFailure(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "google_reports_tail_failure"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		sourceName := "C0123"
		runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind(configstore.KindGoogleWorkspace, registry.RunModeTail), sourceName)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}

		occurredAt := time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC)
		good := googleWorkspaceReportsActivityFixture(t, occurredAt)
		bad := googleWorkspaceReportsActivityFixture(t, occurredAt.Add(time.Minute))
		bad.ID.UniqueQualifier = "activity-bad"
		bad.RawJSON = []byte(`{`)
		integration := &GoogleWorkspaceIntegration{
			customerID: sourceName,
			reportsActivityLister: func(_ context.Context, since time.Time) ([]WorkspaceActivity, error) {
				return []WorkspaceActivity{good, bad}, nil
			},
		}

		stats, err := integration.tailReportsWithCursor(ctx, pool, runID, GoogleWorkspaceReportsTailResource)
		if err == nil {
			t.Fatalf("tailReportsWithCursor() err = nil, want bad raw JSON error")
		}
		if stats.Fetched != 2 || stats.Written != 1 {
			t.Fatalf("tail stats = %+v, want fetched=2 written=1 before failure", stats)
		}

		var eventCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE source_kind = 'google_workspace' AND source_name = $1`, sourceName).Scan(&eventCount); err != nil {
			t.Fatalf("count canonical events: %v", err)
		}
		if eventCount != 1 {
			t.Fatalf("event count = %d, want first event to remain durable", eventCount)
		}

		cursor, err := q.GetConnectorCursorState(ctx, gen.GetConnectorCursorStateParams{
			SourceKind: configstore.KindGoogleWorkspace,
			SourceName: sourceName,
			Resource:   GoogleWorkspaceReportsTailResource,
		})
		if err != nil {
			t.Fatalf("GetConnectorCursorState() err = %v", err)
		}
		if cursor.Watermark.Valid {
			t.Fatalf("cursor watermark = %+v, want no success watermark after failure", cursor.Watermark)
		}
		if cursor.LastSuccessAt.Valid {
			t.Fatalf("cursor last_success_at = %+v, want invalid after failure", cursor.LastSuccessAt)
		}
		if cursor.LastError == "" {
			t.Fatalf("cursor last_error is empty, want failure recorded")
		}
	})
}

func googleWorkspaceReportsActivityFixture(t *testing.T, occurredAt time.Time) WorkspaceActivity {
	t.Helper()

	raw := []byte(`{
		"id": {
			"time": "` + occurredAt.Format(time.RFC3339Nano) + `",
			"uniqueQualifier": "activity-1",
			"applicationName": "token",
			"customerId": "C0123"
		},
		"actor": {
			"email": "Alice@Example.com",
			"profileId": "u-1"
		},
		"ipAddress": "203.0.113.10",
		"events": [
			{
				"name": "authorize",
				"type": "access",
				"parameters": [
					{"name": "client_id", "value": "client-1"},
					{"name": "app_name", "value": "Payroll"}
				]
			}
		]
	}`)
	var activity WorkspaceActivity
	if err := json.Unmarshal(raw, &activity); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	activity.RawJSON = raw
	return activity
}
