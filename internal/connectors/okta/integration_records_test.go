package okta

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/ingest/recorddispatch"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestFinalizeOktaRecordSnapshotsCompletesAndExpiresThroughProjector(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_record_finalize"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		sourceName := "example.okta.com"
		source := records.SourceRef{Kind: "okta", Name: sourceName}

		runID1, err := registry.StartSyncRun(ctx, q, "okta", sourceName)
		if err != nil {
			t.Fatalf("StartSyncRun(run1) err = %v", err)
		}
		dispatcher1 := recorddispatch.NewDispatcher(nil, recorddispatch.NewOktaStateProjector(q, runID1))
		for _, record := range []records.StateUpsert{
			{
				Source:         source,
				Resource:       records.ResourceIdentity,
				Key:            "00u-old",
				DedupeKeyValue: "state:identity:00u-old",
				Payload: records.IdentityPayload{
					ExternalID:  "00u-old",
					Email:       "old@example.com",
					DisplayName: "Old User",
					Status:      "ACTIVE",
					ProviderAttrs: map[string]any{
						"account_kind":    registry.AccountKindHuman,
						"entity_category": registry.EntityCategoryUser,
					},
				},
			},
			groupStateRecord(sourceName, Group{ID: "00g-old", Name: "Old Group", Type: "OKTA_GROUP"}),
			appStateRecord(sourceName, App{ID: "0oa-old", Label: "Old App", Status: "ACTIVE"}),
			appUserAssignmentRecord(sourceName, App{ID: "0oa-old", Label: "Old App"}, AppAccountAssignment{AccountID: "00u-old", Scope: "USER"}),
		} {
			if err := dispatcher1.UpsertState(ctx, record); err != nil {
				t.Fatalf("run1 UpsertState(%s/%s) err = %v", record.Resource, record.Key, err)
			}
		}
		if err := finalizeOktaRecordSnapshots(ctx, q, pool, runID1, sourceName, time.Second); err != nil {
			t.Fatalf("finalizeOktaRecordSnapshots(run1) err = %v", err)
		}
		assertRecordFinalizeCounts(t, ctx, pool, sourceName, 2, 1, 1, 1, 1)

		runID2, err := registry.StartSyncRun(ctx, q, "okta", sourceName)
		if err != nil {
			t.Fatalf("StartSyncRun(run2) err = %v", err)
		}
		dispatcher2 := recorddispatch.NewDispatcher(nil, recorddispatch.NewOktaStateProjector(q, runID2))
		if err := dispatcher2.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceIdentity,
			Key:            "00u-new",
			DedupeKeyValue: "state:identity:00u-new",
			Payload: records.IdentityPayload{
				ExternalID:  "00u-new",
				Email:       "new@example.com",
				DisplayName: "New User",
				Status:      "ACTIVE",
				ProviderAttrs: map[string]any{
					"account_kind":    registry.AccountKindHuman,
					"entity_category": registry.EntityCategoryUser,
				},
			},
		}); err != nil {
			t.Fatalf("run2 UpsertState(identity) err = %v", err)
		}
		if err := finalizeOktaRecordSnapshots(ctx, q, pool, runID2, sourceName, time.Second); err != nil {
			t.Fatalf("finalizeOktaRecordSnapshots(run2) err = %v", err)
		}
		assertRecordFinalizeCounts(t, ctx, pool, sourceName, 1, 0, 0, 0, 0)

		var status string
		var stats []byte
		if err := pool.QueryRow(ctx, `SELECT status, stats FROM sync_runs WHERE id = $1`, runID2).Scan(&status, &stats); err != nil {
			t.Fatalf("query sync run err = %v", err)
		}
		if status != "success" {
			t.Fatalf("sync run status = %q, want success", status)
		}
		var payload struct {
			Counts map[string]int64 `json:"counts"`
		}
		if err := json.Unmarshal(stats, &payload); err != nil {
			t.Fatalf("unmarshal sync run stats err = %v", err)
		}
		if payload.Counts["okta_accounts_observed"] != 1 || payload.Counts["okta_accounts_expired"] != 2 {
			t.Fatalf("account counts = %+v, want observed=1 expired=2", payload.Counts)
		}
		if payload.Counts["okta_groups_expired"] != 1 || payload.Counts["okta_apps_expired"] != 1 || payload.Counts["okta_app_assignments_expired"] != 1 {
			t.Fatalf("expiration counts = %+v, want group/app/assignment expiration through snapshot complete", payload.Counts)
		}
		if payload.Counts["entitlements_observed"] != 0 || payload.Counts["entitlements_expired"] != 1 {
			t.Fatalf("entitlement counts = %+v, want observed=0 expired=1", payload.Counts)
		}

		if err := finalizeOktaRecordSnapshots(ctx, q, pool, runID2, sourceName, time.Second); err != nil {
			t.Fatalf("finalizeOktaRecordSnapshots(run2 repeat) err = %v", err)
		}
		assertRecordFinalizeCounts(t, ctx, pool, sourceName, 1, 0, 0, 0, 0)
	})
}

func assertRecordFinalizeCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceName string, accounts, groups, apps, assignments, entitlements int) {
	t.Helper()
	assertRecordFinalizeCount(t, ctx, pool, "active okta accounts", `
		SELECT count(*)
		FROM accounts
		WHERE source_kind = 'okta'
		  AND source_name = $1
		  AND expired_at IS NULL
		  AND last_observed_run_id IS NOT NULL
	`, accounts, sourceName)
	assertRecordFinalizeCount(t, ctx, pool, "active okta groups", `
		SELECT count(*)
		FROM okta_groups
		WHERE source_kind = 'okta'
		  AND source_name = $1
		  AND expired_at IS NULL
		  AND last_observed_run_id IS NOT NULL
	`, groups, sourceName)
	assertRecordFinalizeCount(t, ctx, pool, "active okta apps", `
		SELECT count(*)
		FROM okta_apps
		WHERE source_kind = 'okta'
		  AND source_name = $1
		  AND expired_at IS NULL
		  AND last_observed_run_id IS NOT NULL
	`, apps, sourceName)
	assertRecordFinalizeCount(t, ctx, pool, "active okta app assignments", `
		SELECT count(*)
		FROM okta_user_app_assignments ua
		JOIN accounts a ON a.id = ua.okta_user_account_id
		WHERE a.source_kind = 'okta'
		  AND a.source_name = $1
		  AND ua.expired_at IS NULL
		  AND ua.last_observed_run_id IS NOT NULL
	`, assignments, sourceName)
	assertRecordFinalizeCount(t, ctx, pool, "active entitlements", `
		SELECT count(*)
		FROM entitlements e
		JOIN accounts a ON a.id = e.app_user_id
		WHERE a.source_kind = 'okta'
		  AND a.source_name = $1
		  AND e.expired_at IS NULL
		  AND e.last_observed_run_id IS NOT NULL
	`, entitlements, sourceName)
}

func assertRecordFinalizeCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label, query string, want int, args ...any) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("%s query err = %v", label, err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", label, got, want)
	}
}
