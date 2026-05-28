package recorddispatch

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestOktaStateProjectorProjectsNamedOktaTargets(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_state_projector"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		source := records.SourceRef{Kind: "okta", Name: "example.okta.com"}
		runID, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}
		dispatcher := NewDispatcher(nil, NewOktaStateProjector(q, runID))

		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceIdentity,
			Key:            "00u1",
			DedupeKeyValue: "state:identity:00u1",
			Payload: records.IdentityPayload{
				ExternalID:  "00u1",
				Email:       "alice@example.com",
				DisplayName: "Alice",
				Status:      "ACTIVE",
				ProviderAttrs: map[string]any{
					"account_kind":    registry.AccountKindHuman,
					"entity_category": registry.EntityCategoryUser,
				},
			},
		}); err != nil {
			t.Fatalf("identity UpsertState() err = %v", err)
		}
		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceGroup,
			Key:            "00g1",
			DedupeKeyValue: "state:group:00g1",
			Payload: records.GroupPayload{
				ExternalID:  "00g1",
				DisplayName: "Engineering",
				Type:        "OKTA_GROUP",
			},
		}); err != nil {
			t.Fatalf("group UpsertState() err = %v", err)
		}
		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceApplication,
			Key:            "0oa1",
			DedupeKeyValue: "state:app:0oa1",
			Payload: records.ApplicationPayload{
				ExternalID:  "0oa1",
				DisplayName: "Payroll",
				Name:        "payroll",
				Status:      "ACTIVE",
				SignOnMode:  "SAML_2_0",
			},
		}); err != nil {
			t.Fatalf("application UpsertState() err = %v", err)
		}
		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceEntitlement,
			Key:            "group_membership:00u1:00g1",
			DedupeKeyValue: "state:entitlement:group_membership:00u1:00g1",
			Payload: records.EntitlementPayload{
				ExternalID: "group_membership:00u1:00g1",
				Kind:       records.EntitlementKindOktaGroupMembership,
				Subject:    records.ResourceRef{Resource: records.ResourceIdentity, ExternalID: "00u1"},
				Target:     records.ResourceRef{Resource: records.ResourceGroup, ExternalID: "00g1", DisplayName: "Engineering"},
				Permission: "member",
			},
		}); err != nil {
			t.Fatalf("group membership UpsertState() err = %v", err)
		}
		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceEntitlement,
			Key:            "00u1:0oa1",
			DedupeKeyValue: "state:entitlement:00u1:0oa1",
			Payload: records.EntitlementPayload{
				ExternalID: "00u1:0oa1",
				Kind:       records.EntitlementKindOktaAppUserAssignment,
				Subject:    records.ResourceRef{Resource: records.ResourceIdentity, ExternalID: "00u1"},
				Target:     records.ResourceRef{Resource: records.ResourceApplication, ExternalID: "0oa1", DisplayName: "Payroll"},
				Scope:      "USER",
				Profile:    map[string]any{"appUserName": "alice.payroll"},
			},
		}); err != nil {
			t.Fatalf("entitlement UpsertState() err = %v", err)
		}
		observedAt := time.Date(2026, time.May, 20, 12, 0, 0, 0, time.UTC)
		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceDiscoveryEvidence,
			Key:            "source:0oa1",
			DedupeKeyValue: "state:discovery:source:0oa1",
			ObservedAt:     observedAt,
			Payload: records.DiscoveryEvidencePayload{
				ExternalID:       "source:0oa1",
				Kind:             records.DiscoveryEvidenceKindSource,
				CanonicalKey:     "okta_app:example.okta.com:0oa1",
				SourceAppID:      "0oa1",
				SourceAppName:    "Payroll",
				SourceAppDomain:  "payroll.example.com",
				SourceVendorName: "Payroll",
				SourceCategory:   "finance",
				ObservedAt:       observedAt,
			},
		}); err != nil {
			t.Fatalf("discovery source UpsertState() err = %v", err)
		}
		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceDiscoveryEvidence,
			Key:            "event:idp_sso:evt1",
			DedupeKeyValue: "state:discovery:event:evt1",
			ObservedAt:     observedAt,
			Payload: records.DiscoveryEvidencePayload{
				ExternalID:       "event:idp_sso:evt1",
				Kind:             records.DiscoveryEvidenceKindEvent,
				CanonicalKey:     "okta_app:example.okta.com:0oa1",
				SignalKind:       "idp_sso",
				EventExternalID:  "evt1",
				SourceAppID:      "0oa1",
				SourceAppName:    "Payroll",
				SourceAppDomain:  "payroll.example.com",
				ActorExternalID:  "00u1",
				ActorEmail:       "alice@example.com",
				ActorDisplayName: "Alice",
				ObservedAt:       observedAt,
				Raw:              map[string]any{"uuid": "evt1"},
			},
		}); err != nil {
			t.Fatalf("discovery event UpsertState() err = %v", err)
		}

		for _, resource := range []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourceApplication,
			records.ResourceEntitlement,
			records.ResourceDiscoveryEvidence,
		} {
			if err := dispatcher.CompleteSnapshot(ctx, records.SnapshotComplete{
				Source:         source,
				Resource:       resource,
				Scope:          records.FullScope{Resource: resource},
				Complete:       true,
				ExpireAbsent:   true,
				DedupeKeyValue: "snapshot_complete:" + string(resource),
			}); err != nil {
				t.Fatalf("CompleteSnapshot(%s) err = %v", resource, err)
			}
		}

		assertCount(t, ctx, pool, "accounts", `SELECT count(*) FROM accounts WHERE source_kind = 'okta' AND source_name = $1 AND expired_at IS NULL AND last_observed_run_id IS NOT NULL`, 2, source.Name)
		assertCount(t, ctx, pool, "okta_groups", `SELECT count(*) FROM okta_groups WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL`, 1)
		assertCount(t, ctx, pool, "okta_apps", `SELECT count(*) FROM okta_apps WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL`, 1)
		assertCount(t, ctx, pool, "entitlements", `SELECT count(*) FROM entitlements e JOIN accounts a ON a.id = e.app_user_id WHERE a.source_kind = 'okta' AND a.source_name = $1 AND e.expired_at IS NULL AND e.last_observed_run_id IS NOT NULL`, 2, source.Name)
		assertCount(t, ctx, pool, "group membership entitlements", `
			SELECT count(*)
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'okta'
			  AND a.source_name = $1
			  AND a.external_id = '00u1'
			  AND e.kind = 'group_membership'
			  AND e.resource = 'group:00g1'
			  AND e.permission = 'member'
			  AND e.expired_at IS NULL
			  AND e.last_observed_run_id IS NOT NULL
		`, 1, source.Name)
		assertCount(t, ctx, pool, "app assignment entitlements", `
			SELECT count(*)
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'okta'
			  AND a.source_name = $1
			  AND a.external_id = '00u1'
			  AND e.kind = 'application_assignment'
			  AND e.resource = '0oa1'
			  AND e.permission = 'USER'
			  AND e.raw_json #>> '{attributes,target,display_name}' = 'Payroll'
			  AND e.raw_json #>> '{attributes,profile,appUserName}' = 'alice.payroll'
			  AND e.expired_at IS NULL
			  AND e.last_observed_run_id IS NOT NULL
		`, 1, source.Name)
		assertCount(t, ctx, pool, "saas_app_sources", `SELECT count(*) FROM saas_app_sources WHERE source_kind = 'okta' AND source_name = $1 AND expired_at IS NULL AND last_observed_run_id IS NOT NULL`, 1, source.Name)
		assertCount(t, ctx, pool, "saas_app_events", `SELECT count(*) FROM saas_app_events WHERE source_kind = 'okta' AND source_name = $1 AND expired_at IS NULL AND last_observed_run_id IS NOT NULL`, 1, source.Name)
	})
}

func TestOktaStateProjectorSnapshotExpirationRequiresCompleteExpireAbsent(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_snapshot_contract"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		source := records.SourceRef{Kind: "okta", Name: "example.okta.com"}

		runID1, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
		if err != nil {
			t.Fatalf("StartSyncRun(run1) err = %v", err)
		}
		dispatcher1 := NewDispatcher(nil, NewOktaStateProjector(q, runID1))
		if err := dispatcher1.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceIdentity,
			Key:            "00u-old",
			DedupeKeyValue: "state:identity:00u-old",
			Payload:        records.IdentityPayload{ExternalID: "00u-old", Email: "old@example.com"},
		}); err != nil {
			t.Fatalf("seed UpsertState() err = %v", err)
		}
		if err := dispatcher1.CompleteSnapshot(ctx, records.SnapshotComplete{
			Source:         source,
			Resource:       records.ResourceIdentity,
			Scope:          records.FullScope{Resource: records.ResourceIdentity},
			Complete:       true,
			ExpireAbsent:   true,
			DedupeKeyValue: "snapshot_complete:identity:run1",
		}); err != nil {
			t.Fatalf("seed CompleteSnapshot() err = %v", err)
		}

		runID2, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
		if err != nil {
			t.Fatalf("StartSyncRun(run2) err = %v", err)
		}
		dispatcher2 := NewDispatcher(nil, NewOktaStateProjector(q, runID2))
		scope := records.FullScope{Resource: records.ResourceIdentity}
		if err := dispatcher2.BeginSnapshot(ctx, records.SnapshotBegin{
			Source:         source,
			Resource:       records.ResourceIdentity,
			Scope:          scope,
			StartedAt:      time.Now(),
			DedupeKeyValue: "snapshot_begin:identity:run2",
		}); err != nil {
			t.Fatalf("BeginSnapshot() err = %v", err)
		}
		assertActiveOktaAccounts(t, ctx, pool, source.Name, 1)

		if err := dispatcher2.CompleteSnapshot(ctx, records.SnapshotComplete{
			Source:         source,
			Resource:       records.ResourceIdentity,
			Scope:          scope,
			Complete:       false,
			ExpireAbsent:   true,
			DedupeKeyValue: "snapshot_complete:false",
		}); err != nil {
			t.Fatalf("CompleteSnapshot(false) err = %v", err)
		}
		assertActiveOktaAccounts(t, ctx, pool, source.Name, 1)

		if err := dispatcher2.CompleteSnapshot(ctx, records.SnapshotComplete{
			Source:         source,
			Resource:       records.ResourceIdentity,
			Scope:          scope,
			Complete:       true,
			ExpireAbsent:   false,
			DedupeKeyValue: "snapshot_complete:no_expire",
		}); err != nil {
			t.Fatalf("CompleteSnapshot(no expire) err = %v", err)
		}
		assertActiveOktaAccounts(t, ctx, pool, source.Name, 1)

		if err := dispatcher2.CompleteSnapshot(ctx, records.SnapshotComplete{
			Source:         source,
			Resource:       records.ResourceIdentity,
			Scope:          scope,
			Complete:       true,
			ExpireAbsent:   true,
			DedupeKeyValue: "snapshot_complete:expire",
		}); err != nil {
			t.Fatalf("CompleteSnapshot(expire) err = %v", err)
		}
		assertActiveOktaAccounts(t, ctx, pool, source.Name, 0)
	})
}

func TestOktaStateProjectorExpiresStaleDiscoveryEvidenceOnCompleteExpireAbsent(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_state_expire"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		source := records.SourceRef{Kind: "okta", Name: "example.okta.com"}
		staleObservedAt := time.Now().Add(-45 * 24 * time.Hour).UTC()

		runID1, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
		if err != nil {
			t.Fatalf("StartSyncRun(run1) err = %v", err)
		}
		dispatcher1 := NewDispatcher(nil, NewOktaStateProjector(q, runID1))
		if err := dispatcher1.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceDiscoveryEvidence,
			Key:            "source:0oa-stale",
			DedupeKeyValue: "state:discovery:source:0oa-stale",
			ObservedAt:     staleObservedAt,
			Payload: records.DiscoveryEvidencePayload{
				ExternalID:      "source:0oa-stale",
				Kind:            records.DiscoveryEvidenceKindSource,
				CanonicalKey:    "okta_app:example.okta.com:0oa-stale",
				SourceAppID:     "0oa-stale",
				SourceAppName:   "Stale app",
				SourceAppDomain: "stale.example.com",
				ObservedAt:      staleObservedAt,
			},
		}); err != nil {
			t.Fatalf("source discovery UpsertState() err = %v", err)
		}
		if err := dispatcher1.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceDiscoveryEvidence,
			Key:            "event:idp_sso:evt-stale",
			DedupeKeyValue: "state:discovery:event:evt-stale",
			ObservedAt:     staleObservedAt,
			Payload: records.DiscoveryEvidencePayload{
				ExternalID:      "event:idp_sso:evt-stale",
				Kind:            records.DiscoveryEvidenceKindEvent,
				CanonicalKey:    "okta_app:example.okta.com:0oa-stale",
				SignalKind:      "idp_sso",
				EventExternalID: "evt-stale",
				SourceAppID:     "0oa-stale",
				SourceAppName:   "Stale app",
				ObservedAt:      staleObservedAt,
			},
		}); err != nil {
			t.Fatalf("event discovery UpsertState() err = %v", err)
		}
		if err := dispatcher1.CompleteSnapshot(ctx, records.SnapshotComplete{
			Source:         source,
			Resource:       records.ResourceDiscoveryEvidence,
			Scope:          records.FullScope{Resource: records.ResourceDiscoveryEvidence},
			Complete:       true,
			ExpireAbsent:   false,
			DedupeKeyValue: "snapshot_complete:discovery:seed",
		}); err != nil {
			t.Fatalf("seed discovery CompleteSnapshot() err = %v", err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE saas_app_sources
			SET last_observed_at = $1
			WHERE source_kind = 'okta' AND source_name = $2
		`, staleObservedAt, source.Name); err != nil {
			t.Fatalf("age saas_app_sources err = %v", err)
		}
		assertCount(t, ctx, pool, "active discovery sources before expiration", `
			SELECT count(*)
			FROM saas_app_sources
			WHERE source_kind = 'okta'
			  AND source_name = $1
			  AND expired_at IS NULL
			  AND last_observed_run_id IS NOT NULL
		`, 1, source.Name)
		assertCount(t, ctx, pool, "active discovery events before expiration", `
			SELECT count(*)
			FROM saas_app_events
			WHERE source_kind = 'okta'
			  AND source_name = $1
			  AND expired_at IS NULL
			  AND last_observed_run_id IS NOT NULL
		`, 1, source.Name)

		runID2, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
		if err != nil {
			t.Fatalf("StartSyncRun(run2) err = %v", err)
		}
		dispatcher2 := NewDispatcher(nil, NewOktaStateProjector(q, runID2))
		if err := dispatcher2.CompleteSnapshot(ctx, records.SnapshotComplete{
			Source:         source,
			Resource:       records.ResourceDiscoveryEvidence,
			Scope:          records.FullScope{Resource: records.ResourceDiscoveryEvidence},
			Complete:       true,
			ExpireAbsent:   true,
			DedupeKeyValue: "snapshot_complete:discovery:expire",
		}); err != nil {
			t.Fatalf("expire discovery CompleteSnapshot() err = %v", err)
		}
		assertCount(t, ctx, pool, "active discovery sources after expiration", `
			SELECT count(*)
			FROM saas_app_sources
			WHERE source_kind = 'okta'
			  AND source_name = $1
			  AND expired_at IS NULL
			  AND last_observed_run_id IS NOT NULL
		`, 0, source.Name)
		assertCount(t, ctx, pool, "active discovery events after expiration", `
			SELECT count(*)
			FROM saas_app_events
			WHERE source_kind = 'okta'
			  AND source_name = $1
			  AND expired_at IS NULL
			  AND last_observed_run_id IS NOT NULL
		`, 0, source.Name)
	})
}

func TestOktaStateProjectorSnapshotExpirationIsScopedToSource(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_snapshot_source_scope"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		sourceA := records.SourceRef{Kind: "okta", Name: "a.okta.com"}
		sourceB := records.SourceRef{Kind: "okta", Name: "b.okta.com"}

		seedOktaStateSlice(t, ctx, q, sourceA, "a")
		seedOktaStateSlice(t, ctx, q, sourceB, "b")

		runID, err := registry.StartSyncRun(ctx, q, "okta", sourceA.Name)
		if err != nil {
			t.Fatalf("StartSyncRun(sourceA refresh) err = %v", err)
		}
		dispatcher := NewDispatcher(nil, NewOktaStateProjector(q, runID))
		if err := dispatcher.UpsertState(ctx, identityState(sourceA, "00u-a-new")); err != nil {
			t.Fatalf("refresh sourceA identity UpsertState() err = %v", err)
		}
		for _, resource := range []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourceApplication,
			records.ResourceEntitlement,
		} {
			if err := dispatcher.CompleteSnapshot(ctx, records.SnapshotComplete{
				Source:         sourceA,
				Resource:       resource,
				Scope:          records.FullScope{Resource: resource},
				Complete:       true,
				ExpireAbsent:   true,
				DedupeKeyValue: "snapshot_complete:source_a:" + string(resource),
			}); err != nil {
				t.Fatalf("CompleteSnapshot(%s) err = %v", resource, err)
			}
		}

		assertActiveOktaAccounts(t, ctx, pool, sourceA.Name, 1)
		assertActiveOktaAccounts(t, ctx, pool, sourceB.Name, 2)
		assertCount(t, ctx, pool, "sourceA generic entitlements", `
			SELECT count(*)
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'okta'
			  AND a.source_name = $1
			  AND e.expired_at IS NULL
		`, 0, sourceA.Name)
		assertCount(t, ctx, pool, "sourceB generic entitlements", `
			SELECT count(*)
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'okta'
			  AND a.source_name = $1
			  AND e.expired_at IS NULL
		`, 1, sourceB.Name)
		assertCount(t, ctx, pool, "active okta apps", `SELECT count(*) FROM okta_apps WHERE expired_at IS NULL`, 2)
	})
}

func TestOktaStateProjectorScopesGroupsAndAppsBySource(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_source_scope"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		sourceA := records.SourceRef{Kind: "okta", Name: "a.okta.com"}
		sourceB := records.SourceRef{Kind: "okta", Name: "b.okta.com"}

		seedSharedOktaApp(t, ctx, q, sourceA, "shared-app")
		seedSharedOktaApp(t, ctx, q, sourceB, "shared-app")

		assertCount(t, ctx, pool, "shared apps across sources", `
			SELECT count(*)
			FROM okta_apps
			WHERE external_id = 'shared-app'
			  AND expired_at IS NULL
			  AND last_observed_run_id IS NOT NULL
		`, 2)
		assertCount(t, ctx, pool, "source A shared app", `
			SELECT count(*)
			FROM okta_apps
			WHERE source_kind = 'okta'
			  AND source_name = $1
			  AND external_id = 'shared-app'
			  AND expired_at IS NULL
			  AND last_observed_run_id IS NOT NULL
		`, 1, sourceA.Name)
		assertCount(t, ctx, pool, "source B shared app", `
			SELECT count(*)
			FROM okta_apps
			WHERE source_kind = 'okta'
			  AND source_name = $1
			  AND external_id = 'shared-app'
			  AND expired_at IS NULL
			  AND last_observed_run_id IS NOT NULL
		`, 1, sourceB.Name)
	})
}

func TestOktaStateProjectorAppGroupAssignmentDoesNotClobberGroup(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_app_group_assignment_group"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		source := records.SourceRef{Kind: "okta", Name: "example.okta.com"}
		runID, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}
		dispatcher := NewDispatcher(nil, NewOktaStateProjector(q, runID))

		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceGroup,
			Key:            "00g1",
			DedupeKeyValue: "state:group:00g1",
			Payload: records.GroupPayload{
				ExternalID:  "00g1",
				DisplayName: "Engineering",
				Type:        "OKTA_GROUP",
				Raw:         map[string]any{"kind": "group"},
			},
		}); err != nil {
			t.Fatalf("group UpsertState() err = %v", err)
		}
		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceApplication,
			Key:            "0oa1",
			DedupeKeyValue: "state:app:0oa1",
			Payload: records.ApplicationPayload{
				ExternalID:  "0oa1",
				DisplayName: "Payroll",
				Status:      "ACTIVE",
			},
		}); err != nil {
			t.Fatalf("app UpsertState() err = %v", err)
		}
		if err := dispatcher.UpsertState(ctx, records.StateUpsert{
			Source:         source,
			Resource:       records.ResourceEntitlement,
			Key:            "00g1:0oa1",
			DedupeKeyValue: "state:entitlement:00g1:0oa1",
			Payload: records.EntitlementPayload{
				ExternalID: "00g1:0oa1",
				Kind:       records.EntitlementKindOktaAppGroupAssignment,
				Subject:    records.ResourceRef{Resource: records.ResourceGroup, ExternalID: "00g1", DisplayName: "Engineering"},
				Target:     records.ResourceRef{Resource: records.ResourceApplication, ExternalID: "0oa1", DisplayName: "Payroll"},
				Priority:   1,
				Raw:        map[string]any{"kind": "assignment"},
			},
		}); err != nil {
			t.Fatalf("app group assignment UpsertState() err = %v", err)
		}

		var groupType, rawKind string
		if err := pool.QueryRow(ctx, `
			SELECT type, raw_json->>'kind'
			FROM okta_groups
			WHERE source_kind = 'okta'
			  AND source_name = $1
			  AND external_id = '00g1'
		`, source.Name).Scan(&groupType, &rawKind); err != nil {
			t.Fatalf("query group err = %v", err)
		}
		if groupType != "OKTA_GROUP" || rawKind != "group" {
			t.Fatalf("group type/raw = %q/%q, want OKTA_GROUP/group", groupType, rawKind)
		}
	})
}

func TestOktaStateProjectorDeleteStateRequiresIdentifier(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_delete_state_guard"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		source := records.SourceRef{Kind: "okta", Name: "example.okta.com"}
		runID, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}

		err = NewOktaStateProjector(q, runID).DeleteState(ctx, records.StateDelete{
			Source:   source,
			Resource: records.ResourceIdentity,
		})
		if err == nil {
			t.Fatalf("DeleteState() err = nil, want missing identifier error")
		}
	})
}

func seedOktaStateSlice(t *testing.T, ctx context.Context, q *gen.Queries, source records.SourceRef, suffix string) {
	t.Helper()
	runID, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
	if err != nil {
		t.Fatalf("StartSyncRun(%s) err = %v", source.Name, err)
	}
	dispatcher := NewDispatcher(nil, NewOktaStateProjector(q, runID))
	groupID := "00g-" + suffix
	appID := "0oa-" + suffix
	userID := "00u-" + suffix

	for _, record := range []records.StateUpsert{
		identityState(source, userID),
		groupState(source, groupID),
		appState(source, appID),
		appAssignmentState(source, userID, appID),
	} {
		if err := dispatcher.UpsertState(ctx, record); err != nil {
			t.Fatalf("seed %s %s UpsertState() err = %v", source.Name, record.Key, err)
		}
	}
	for _, resource := range []records.ResourceName{
		records.ResourceIdentity,
		records.ResourceGroup,
		records.ResourceApplication,
		records.ResourceEntitlement,
	} {
		if err := dispatcher.CompleteSnapshot(ctx, records.SnapshotComplete{
			Source:         source,
			Resource:       resource,
			Scope:          records.FullScope{Resource: resource},
			Complete:       true,
			ExpireAbsent:   true,
			DedupeKeyValue: "snapshot_complete:seed:" + string(resource) + ":" + suffix,
		}); err != nil {
			t.Fatalf("seed CompleteSnapshot(%s) err = %v", resource, err)
		}
	}
}

func seedSharedOktaApp(t *testing.T, ctx context.Context, q *gen.Queries, source records.SourceRef, appID string) {
	t.Helper()
	runID, err := registry.StartSyncRun(ctx, q, "okta", source.Name)
	if err != nil {
		t.Fatalf("StartSyncRun(%s) err = %v", source.Name, err)
	}
	dispatcher := NewDispatcher(nil, NewOktaStateProjector(q, runID))
	if err := dispatcher.UpsertState(ctx, appState(source, appID)); err != nil {
		t.Fatalf("UpsertState(%s/%s) err = %v", source.Name, appID, err)
	}
	if err := dispatcher.CompleteSnapshot(ctx, records.SnapshotComplete{
		Source:         source,
		Resource:       records.ResourceApplication,
		Scope:          records.FullScope{Resource: records.ResourceApplication},
		Complete:       true,
		ExpireAbsent:   true,
		DedupeKeyValue: "snapshot_complete:shared_app:" + source.Name,
	}); err != nil {
		t.Fatalf("CompleteSnapshot(%s) err = %v", source.Name, err)
	}
}

func identityState(source records.SourceRef, externalID string) records.StateUpsert {
	return records.StateUpsert{
		Source:         source,
		Resource:       records.ResourceIdentity,
		Key:            externalID,
		DedupeKeyValue: "state:identity:" + externalID,
		Payload: records.IdentityPayload{
			ExternalID: externalID,
			Email:      externalID + "@example.com",
		},
	}
}

func groupState(source records.SourceRef, externalID string) records.StateUpsert {
	return records.StateUpsert{
		Source:         source,
		Resource:       records.ResourceGroup,
		Key:            externalID,
		DedupeKeyValue: "state:group:" + externalID,
		Payload: records.GroupPayload{
			ExternalID:  externalID,
			DisplayName: "Group " + externalID,
		},
	}
}

func appState(source records.SourceRef, externalID string) records.StateUpsert {
	return records.StateUpsert{
		Source:         source,
		Resource:       records.ResourceApplication,
		Key:            externalID,
		DedupeKeyValue: "state:app:" + externalID,
		Payload: records.ApplicationPayload{
			ExternalID:  externalID,
			DisplayName: "App " + externalID,
			Status:      "ACTIVE",
		},
	}
}

func appAssignmentState(source records.SourceRef, userID, appID string) records.StateUpsert {
	return records.StateUpsert{
		Source:         source,
		Resource:       records.ResourceEntitlement,
		Key:            userID + ":" + appID,
		DedupeKeyValue: "state:entitlement:" + userID + ":" + appID,
		Payload: records.EntitlementPayload{
			ExternalID: userID + ":" + appID,
			Kind:       records.EntitlementKindOktaAppUserAssignment,
			Subject:    records.ResourceRef{Resource: records.ResourceIdentity, ExternalID: userID},
			Target:     records.ResourceRef{Resource: records.ResourceApplication, ExternalID: appID, DisplayName: "App " + appID},
			Scope:      "USER",
		},
	}
}

func assertActiveOktaAccounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceName string, want int) {
	t.Helper()
	assertCount(t, ctx, pool, "active okta accounts", `SELECT count(*) FROM accounts WHERE source_kind = 'okta' AND source_name = $1 AND expired_at IS NULL AND last_observed_run_id IS NOT NULL`, want, sourceName)
}

func assertCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label, query string, want int, args ...any) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("%s count query err = %v", label, err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", label, got, want)
	}
}
