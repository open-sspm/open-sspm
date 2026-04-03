package gen

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGenericAccountQueriesFilterEntityCategory(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "seed", "seed")

		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "entra", SourceName: "tenant-1", ExternalID: "entra-user-1", Email: "entra.user@example.com", DisplayName: "Entra User", Status: "active", AccountKind: "human", EntityCategory: "user", RawJSON: `{"status":"active"}`})
		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "entra", SourceName: "tenant-1", ExternalID: "sp:svc-1", DisplayName: "Entra SP", Status: "active", AccountKind: "service", EntityCategory: "service_principal", RawJSON: `{"status":"active"}`})
		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "aws", SourceName: "directory-1", ExternalID: "aws-user-1", Email: "aws.user@example.com", DisplayName: "AWS User", Status: "active", AccountKind: "human", EntityCategory: "user", RawJSON: `{"status":"active"}`})
		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "aws", SourceName: "directory-1", ExternalID: "group:g-1", DisplayName: "AWS Group", Status: "active", AccountKind: "service", EntityCategory: "group", RawJSON: `{"status":"active"}`})
		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "github", SourceName: "acme", ExternalID: "github-user-1", Email: "github.user@example.com", DisplayName: "GitHub User", Status: "active", AccountKind: "human", EntityCategory: "user", RawJSON: `{"status":"active"}`})
		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "github", SourceName: "acme", ExternalID: "team:platform", DisplayName: "Platform", Status: "active", AccountKind: "service", EntityCategory: "team", RawJSON: `{"status":"active"}`})

		datadogLinkedID := insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "datadog", SourceName: "datadoghq.com", ExternalID: "dd-user-1", Email: "dd.user1@example.com", DisplayName: "Datadog User 1", Status: "active", AccountKind: "human", EntityCategory: "user", RawJSON: `{"status":"active"}`})
		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "datadog", SourceName: "datadoghq.com", ExternalID: "dd-user-2", Email: "dd.user2@example.com", DisplayName: "Datadog User 2", Status: "inactive", AccountKind: "human", EntityCategory: "user", RawJSON: `{"status":"inactive"}`})
		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "datadog", SourceName: "datadoghq.com", ExternalID: "service_account:sa-1", DisplayName: "DD Service", Status: "active", AccountKind: "service", EntityCategory: "service_account", RawJSON: `{"status":"active"}`})
		insertAccount(t, ctx, pool, runID, accountSeed{SourceKind: "datadog", SourceName: "datadoghq.com", ExternalID: "role:admin", DisplayName: "Admin Role", Status: "active", AccountKind: "service", EntityCategory: "role", RawJSON: `{"status":"active"}`})

		insertIdentitySourceSetting(t, ctx, pool, "datadog", "datadoghq.com", true)
		identityID := insertIdentity(t, ctx, pool, "human", "dd.user1@example.com", "Datadog User 1")
		insertIdentityAccountLink(t, ctx, pool, identityID, datadogLinkedID)

		userCounts := []struct {
			sourceKind string
			sourceName string
			wantCount  int64
		}{
			{sourceKind: "entra", sourceName: "tenant-1", wantCount: 1},
			{sourceKind: "aws", sourceName: "directory-1", wantCount: 1},
			{sourceKind: "github", sourceName: "acme", wantCount: 1},
			{sourceKind: "datadog", sourceName: "datadoghq.com", wantCount: 2},
		}

		for _, tc := range userCounts {
			count, err := q.CountSourceAccountsBySourceAndQuery(ctx, CountSourceAccountsBySourceAndQueryParams{
				SourceKind:     tc.sourceKind,
				SourceName:     tc.sourceName,
				EntityCategory: "user",
			})
			if err != nil {
				t.Fatalf("CountSourceAccountsBySourceAndQuery(%s): %v", tc.sourceKind, err)
			}
			if count != tc.wantCount {
				t.Fatalf("CountSourceAccountsBySourceAndQuery(%s)=%d want %d", tc.sourceKind, count, tc.wantCount)
			}

			rows, err := q.ListSourceAccountsPageBySourceAndQuery(ctx, ListSourceAccountsPageBySourceAndQueryParams{
				SourceKind:     tc.sourceKind,
				SourceName:     tc.sourceName,
				EntityCategory: "user",
				PageLimit:      20,
			})
			if err != nil {
				t.Fatalf("ListSourceAccountsPageBySourceAndQuery(%s): %v", tc.sourceKind, err)
			}
			if len(rows) != int(tc.wantCount) {
				t.Fatalf("ListSourceAccountsPageBySourceAndQuery(%s) len=%d want %d", tc.sourceKind, len(rows), tc.wantCount)
			}
			for _, row := range rows {
				if row.EntityCategory != "user" {
					t.Fatalf("ListSourceAccountsPageBySourceAndQuery(%s) included %s entity_category=%q", tc.sourceKind, row.ExternalID, row.EntityCategory)
				}
			}
		}

		allDatadogCount, err := q.CountSourceAccountsBySourceAndQuery(ctx, CountSourceAccountsBySourceAndQueryParams{
			SourceKind: "datadog",
			SourceName: "datadoghq.com",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsBySourceAndQuery(all datadog): %v", err)
		}
		if allDatadogCount != 4 {
			t.Fatalf("CountSourceAccountsBySourceAndQuery(all datadog)=%d want 4", allDatadogCount)
		}

		unmatchedCount, err := q.CountUnlinkedSourceAccountsBySourceAndQuery(ctx, CountUnlinkedSourceAccountsBySourceAndQueryParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountUnlinkedSourceAccountsBySourceAndQuery(user): %v", err)
		}
		if unmatchedCount != 1 {
			t.Fatalf("CountUnlinkedSourceAccountsBySourceAndQuery(user)=%d want 1", unmatchedCount)
		}

		unmatchedRows, err := q.ListUnlinkedSourceAccountsPageBySourceAndQuery(ctx, ListUnlinkedSourceAccountsPageBySourceAndQueryParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListUnlinkedSourceAccountsPageBySourceAndQuery(user): %v", err)
		}
		if len(unmatchedRows) != 1 || unmatchedRows[0].ExternalID != "dd-user-2" {
			t.Fatalf("ListUnlinkedSourceAccountsPageBySourceAndQuery(user)=%v want [dd-user-2]", accountExternalIDs(unmatchedRows))
		}

		activeUserCount, err := q.CountSourceAccountsBySourceAndQueryAndState(ctx, CountSourceAccountsBySourceAndQueryAndStateParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
			State:          "active",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsBySourceAndQueryAndState(user active): %v", err)
		}
		if activeUserCount != 1 {
			t.Fatalf("CountSourceAccountsBySourceAndQueryAndState(user active)=%d want 1", activeUserCount)
		}

		activeUserRows, err := q.ListSourceAccountsPageBySourceAndQueryAndState(ctx, ListSourceAccountsPageBySourceAndQueryAndStateParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
			State:          "active",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryAndState(user active): %v", err)
		}
		if len(activeUserRows) != 1 || activeUserRows[0].ExternalID != "dd-user-1" {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryAndState(user active)=%v want [dd-user-1]", accountExternalIDs(activeUserRows))
		}

		activeAllCount, err := q.CountSourceAccountsBySourceAndQueryAndState(ctx, CountSourceAccountsBySourceAndQueryAndStateParams{
			SourceKind: "datadog",
			SourceName: "datadoghq.com",
			State:      "active",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsBySourceAndQueryAndState(all active): %v", err)
		}
		if activeAllCount != 3 {
			t.Fatalf("CountSourceAccountsBySourceAndQueryAndState(all active)=%d want 3", activeAllCount)
		}
	})
}

func TestGoogleWorkspaceQueriesUseEntityCategoryColumn(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "seed", "seed")
		userID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			ExternalID:     "gw-user-1",
			Email:          "user@example.com",
			DisplayName:    "Workspace User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"entity_category":"group","status":"active"}`,
		})
		insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			ExternalID:     "gw-group-1",
			Email:          "team@example.com",
			DisplayName:    "Workspace Group",
			Status:         "active",
			AccountKind:    "unknown",
			EntityCategory: "group",
			RawJSON:        `{"entity_category":"user","status":"active"}`,
		})

		insertIdentitySourceSetting(t, ctx, pool, "google_workspace", "C0123", true)
		identityID := insertIdentity(t, ctx, pool, "human", "user@example.com", "Workspace User")
		insertIdentityAccountLink(t, ctx, pool, identityID, userID)

		userCount, err := q.CountSourceAccountsBySourceAndQuery(ctx, CountSourceAccountsBySourceAndQueryParams{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsBySourceAndQuery(google workspace users): %v", err)
		}
		if userCount != 1 {
			t.Fatalf("CountSourceAccountsBySourceAndQuery(google workspace users)=%d want 1", userCount)
		}

		userRows, err := q.ListSourceAccountsPageBySourceAndQuery(ctx, ListSourceAccountsPageBySourceAndQueryParams{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			EntityCategory: "user",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(google workspace users): %v", err)
		}
		if len(userRows) != 1 || userRows[0].ExternalID != "gw-user-1" {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(google workspace users)=%v want [gw-user-1]", linkedExternalIDs(userRows))
		}
		if userRows[0].EntityCategory != "user" {
			t.Fatalf("google workspace user row entity_category=%q want user", userRows[0].EntityCategory)
		}

		groupCount, err := q.CountGoogleWorkspaceGroupsBySourceAndQuery(ctx, CountGoogleWorkspaceGroupsBySourceAndQueryParams{
			SourceKind: "google_workspace",
			SourceName: "C0123",
		})
		if err != nil {
			t.Fatalf("CountGoogleWorkspaceGroupsBySourceAndQuery: %v", err)
		}
		if groupCount != 1 {
			t.Fatalf("CountGoogleWorkspaceGroupsBySourceAndQuery=%d want 1", groupCount)
		}

		groupRows, err := q.ListGoogleWorkspaceGroupsPageBySourceAndQuery(ctx, ListGoogleWorkspaceGroupsPageBySourceAndQueryParams{
			SourceKind: "google_workspace",
			SourceName: "C0123",
			PageLimit:  20,
		})
		if err != nil {
			t.Fatalf("ListGoogleWorkspaceGroupsPageBySourceAndQuery: %v", err)
		}
		if len(groupRows) != 1 || groupRows[0].ExternalID != "gw-group-1" {
			t.Fatalf("ListGoogleWorkspaceGroupsPageBySourceAndQuery=%v want [gw-group-1]", accountExternalIDs(groupRows))
		}
		if groupRows[0].EntityCategory != "group" {
			t.Fatalf("google workspace group row entity_category=%q want group", groupRows[0].EntityCategory)
		}

		unmatchedCount, err := q.CountUnlinkedSourceAccountsBySourceAndQuery(ctx, CountUnlinkedSourceAccountsBySourceAndQueryParams{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountUnlinkedSourceAccountsBySourceAndQuery(google workspace users): %v", err)
		}
		if unmatchedCount != 0 {
			t.Fatalf("CountUnlinkedSourceAccountsBySourceAndQuery(google workspace users)=%d want 0", unmatchedCount)
		}

		unmatchedRows, err := q.ListUnlinkedSourceAccountsPageBySourceAndQuery(ctx, ListUnlinkedSourceAccountsPageBySourceAndQueryParams{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			EntityCategory: "user",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListUnlinkedSourceAccountsPageBySourceAndQuery(google workspace users): %v", err)
		}
		if len(unmatchedRows) != 0 {
			t.Fatalf("ListUnlinkedSourceAccountsPageBySourceAndQuery(google workspace users)=%v want []", accountExternalIDs(unmatchedRows))
		}
	})
}

func TestSourceAccountInventoryQueriesProjectEntitlementCounts(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "seed", "seed")

		entraUserID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "entra",
			SourceName:     "tenant-1",
			ExternalID:     "entra-user-1",
			Email:          "entra.user@example.com",
			DisplayName:    "Entra User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		insertEntitlement(t, ctx, pool, runID, entraUserID, "entra_directory_role", "Directory Role A", "member")
		insertEntitlement(t, ctx, pool, runID, entraUserID, "entra_directory_role", "Directory Role A", "eligible")
		insertEntitlement(t, ctx, pool, runID, entraUserID, "entra_directory_role", "Directory Role B", "member")
		insertEntitlement(t, ctx, pool, runID, entraUserID, "entra_app_role", "Enterprise App A", "Reader")
		insertEntitlement(t, ctx, pool, runID, entraUserID, "entra_app_role", "Enterprise App A", "Writer")
		insertEntitlement(t, ctx, pool, runID, entraUserID, "entra_app_role", "Enterprise App B", "Reader")
		insertEntitlement(t, ctx, pool, runID, entraUserID, "entra_app_role", "   ", "ignored")

		awsUserID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "aws",
			SourceName:     "directory-1",
			ExternalID:     "aws-user-1",
			Email:          "aws.user@example.com",
			DisplayName:    "AWS User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		insertEntitlement(t, ctx, pool, runID, awsUserID, "aws_permission_set", "aws_account:111111111111", "Admin")
		insertEntitlement(t, ctx, pool, runID, awsUserID, "aws_permission_set", "aws_account:111111111111", "ReadOnly")
		insertEntitlement(t, ctx, pool, runID, awsUserID, "aws_permission_set", "aws_account:222222222222", "Admin")
		insertEntitlement(t, ctx, pool, runID, awsUserID, "aws_permission_set", "   ", "ignored")

		googleUserID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			ExternalID:     "gw-user-1",
			Email:          "user@example.com",
			DisplayName:    "Workspace User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		insertEntitlement(t, ctx, pool, runID, googleUserID, "google_group_member", "google_group:eng", "member")
		insertEntitlement(t, ctx, pool, runID, googleUserID, "google_group_member", "google_group:eng", "owner")
		insertEntitlement(t, ctx, pool, runID, googleUserID, "google_group_member", "google_group:sec", "member")
		insertEntitlement(t, ctx, pool, runID, googleUserID, "google_admin_role", "admin_role:User Management", "assigned")
		insertEntitlement(t, ctx, pool, runID, googleUserID, "google_admin_role", "admin_role:User Management", "delegated")
		insertEntitlement(t, ctx, pool, runID, googleUserID, "google_admin_role", "admin_role:Groups", "assigned")
		insertEntitlement(t, ctx, pool, runID, googleUserID, "google_admin_role", "", "ignored")

		entraRows, err := q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
			SourceKind:            "entra",
			SourceName:            "tenant-1",
			EntityCategory:        "user",
			PageLimit:             20,
			DistinctResourceKind1: "entra_directory_role",
			DistinctResourceKind2: "entra_app_role",
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(entra): %v", err)
		}
		if len(entraRows) != 1 {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(entra) len=%d want 1", len(entraRows))
		}
		if entraRows[0].DistinctResourceCount1 != 2 || entraRows[0].DistinctResourceCount2 != 2 || entraRows[0].EntitlementCount1 != 0 {
			t.Fatalf("entra summary counts=(%d,%d,%d) want (2,2,0)", entraRows[0].DistinctResourceCount1, entraRows[0].DistinctResourceCount2, entraRows[0].EntitlementCount1)
		}

		awsRows, err := q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
			SourceKind:            "aws",
			SourceName:            "directory-1",
			EntityCategory:        "user",
			PageLimit:             20,
			DistinctResourceKind1: "aws_permission_set",
			EntitlementKind1:      "aws_permission_set",
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(aws): %v", err)
		}
		if len(awsRows) != 1 {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(aws) len=%d want 1", len(awsRows))
		}
		if awsRows[0].DistinctResourceCount1 != 2 || awsRows[0].DistinctResourceCount2 != 0 || awsRows[0].EntitlementCount1 != 3 {
			t.Fatalf("aws summary counts=(%d,%d,%d) want (2,0,3)", awsRows[0].DistinctResourceCount1, awsRows[0].DistinctResourceCount2, awsRows[0].EntitlementCount1)
		}

		googleRows, err := q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
			SourceKind:            "google_workspace",
			SourceName:            "C0123",
			EntityCategory:        "user",
			PageLimit:             20,
			DistinctResourceKind1: "google_group_member",
			DistinctResourceKind2: "google_admin_role",
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(google workspace): %v", err)
		}
		if len(googleRows) != 1 {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(google workspace) len=%d want 1", len(googleRows))
		}
		if googleRows[0].DistinctResourceCount1 != 2 || googleRows[0].DistinctResourceCount2 != 2 || googleRows[0].EntitlementCount1 != 0 {
			t.Fatalf("google workspace summary counts=(%d,%d,%d) want (2,2,0)", googleRows[0].DistinctResourceCount1, googleRows[0].DistinctResourceCount2, googleRows[0].EntitlementCount1)
		}
	})
}

func TestEntityCategoryMigrationBackfillsExistingRows(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateToVersion(t, migrator, 28)

		runID := insertSyncRun(t, ctx, pool, "seed", "seed")
		insertLegacyAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			ExternalID:  "github-user-legacy",
			Email:       "legacy.user@example.com",
			DisplayName: "Legacy GitHub User",
			Status:      "active",
			AccountKind: "human",
			RawJSON:     `{"entity_category":"user","status":"active"}`,
		})
		insertLegacyAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:  "google_workspace",
			SourceName:  "C0123",
			ExternalID:  "gw-group-legacy",
			Email:       "team@example.com",
			DisplayName: "Legacy Workspace Group",
			Status:      "active",
			AccountKind: "unknown",
			RawJSON:     `{"entity_category":"group","status":"active"}`,
		})

		migrateUp(t, migrator)

		var githubCategory string
		if err := pool.QueryRow(ctx, `SELECT entity_category FROM accounts WHERE external_id = $1`, "github-user-legacy").Scan(&githubCategory); err != nil {
			t.Fatalf("select github entity_category: %v", err)
		}
		if githubCategory != "user" {
			t.Fatalf("github entity_category=%q want user", githubCategory)
		}

		var googleCategory string
		if err := pool.QueryRow(ctx, `SELECT entity_category FROM accounts WHERE external_id = $1`, "gw-group-legacy").Scan(&googleCategory); err != nil {
			t.Fatalf("select google entity_category: %v", err)
		}
		if googleCategory != "group" {
			t.Fatalf("google entity_category=%q want group", googleCategory)
		}

		githubCount, err := q.CountSourceAccountsBySourceAndQuery(ctx, CountSourceAccountsBySourceAndQueryParams{
			SourceKind:     "github",
			SourceName:     "acme",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsBySourceAndQuery after backfill: %v", err)
		}
		if githubCount != 1 {
			t.Fatalf("CountSourceAccountsBySourceAndQuery after backfill=%d want 1", githubCount)
		}

		googleGroupCount, err := q.CountGoogleWorkspaceGroupsBySourceAndQuery(ctx, CountGoogleWorkspaceGroupsBySourceAndQueryParams{
			SourceKind: "google_workspace",
			SourceName: "C0123",
		})
		if err != nil {
			t.Fatalf("CountGoogleWorkspaceGroupsBySourceAndQuery after backfill: %v", err)
		}
		if googleGroupCount != 1 {
			t.Fatalf("CountGoogleWorkspaceGroupsBySourceAndQuery after backfill=%d want 1", googleGroupCount)
		}
	})
}

func TestGitHubEntityCategoryRepairMigrationBackfillsLegacyRows(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateToVersion(t, migrator, 28)

		runID := insertSyncRun(t, ctx, pool, "seed", "seed")
		insertLegacyAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			ExternalID:  "github-member-legacy",
			Email:       "legacy.member@example.com",
			DisplayName: "Legacy GitHub Member",
			Status:      "active",
			AccountKind: "human",
			RawJSON:     `{"login":"github-member-legacy","type":"User","status":"active"}`,
		})
		insertLegacyAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			ExternalID:  "github-bot-legacy",
			DisplayName: "Legacy GitHub Bot",
			Status:      "active",
			AccountKind: "bot",
			RawJSON:     `{"login":"github-bot-legacy","type":"Bot","status":"active"}`,
		})
		insertLegacyAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			ExternalID:  "team:platform",
			DisplayName: "Platform",
			Status:      "active",
			AccountKind: "service",
			RawJSON:     `{"id":42,"name":"Platform","slug":"platform"}`,
		})
		insertLegacyAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			ExternalID:  "acme",
			DisplayName: "Acme",
			Status:      "active",
			AccountKind: "service",
			RawJSON:     `{"id":7,"login":"acme","type":"Organization","slug":"acme"}`,
		})

		migrateToVersion(t, migrator, 29)

		for _, externalID := range []string{"github-member-legacy", "github-bot-legacy", "team:platform", "acme"} {
			var category string
			if err := pool.QueryRow(ctx, `SELECT entity_category FROM accounts WHERE external_id = $1`, externalID).Scan(&category); err != nil {
				t.Fatalf("select legacy github entity_category for %s: %v", externalID, err)
			}
			if category != "unknown" {
				t.Fatalf("github entity_category after v29 for %s = %q want unknown", externalID, category)
			}
		}

		migrateUp(t, migrator)

		wantCategories := map[string]string{
			"github-member-legacy": "user",
			"github-bot-legacy":    "user",
			"team:platform":        "team",
			"acme":                 "user",
		}
		for externalID, want := range wantCategories {
			var got string
			if err := pool.QueryRow(ctx, `SELECT entity_category FROM accounts WHERE external_id = $1`, externalID).Scan(&got); err != nil {
				t.Fatalf("select repaired github entity_category for %s: %v", externalID, err)
			}
			if got != want {
				t.Fatalf("github entity_category after repair for %s = %q want %q", externalID, got, want)
			}
		}

		githubUserCount, err := q.CountSourceAccountsBySourceAndQuery(ctx, CountSourceAccountsBySourceAndQueryParams{
			SourceKind:     "github",
			SourceName:     "acme",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsBySourceAndQuery(github users after repair): %v", err)
		}
		if githubUserCount != 3 {
			t.Fatalf("CountSourceAccountsBySourceAndQuery(github users after repair)=%d want 3", githubUserCount)
		}

		githubUserRows, err := q.ListSourceAccountsPageBySourceAndQuery(ctx, ListSourceAccountsPageBySourceAndQueryParams{
			SourceKind:     "github",
			SourceName:     "acme",
			EntityCategory: "user",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(github users after repair): %v", err)
		}
		if got := linkedExternalIDs(githubUserRows); !slices.Equal(got, []string{"acme", "github-bot-legacy", "github-member-legacy"}) {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(github users after repair)=%v want [acme github-bot-legacy github-member-legacy]", got)
		}

		githubTeamCount, err := q.CountSourceAccountsBySourceAndQuery(ctx, CountSourceAccountsBySourceAndQueryParams{
			SourceKind:     "github",
			SourceName:     "acme",
			EntityCategory: "team",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsBySourceAndQuery(github teams after repair): %v", err)
		}
		if githubTeamCount != 1 {
			t.Fatalf("CountSourceAccountsBySourceAndQuery(github teams after repair)=%d want 1", githubTeamCount)
		}
	})
}

type accountSeed struct {
	SourceKind     string
	SourceName     string
	ExternalID     string
	Email          string
	DisplayName    string
	Status         string
	AccountKind    string
	EntityCategory string
	RawJSON        string
}

func withEntityCategoryTestDatabase(t *testing.T, fn func(context.Context, *pgxpool.Pool, *Queries, *migrate.Migrate)) {
	t.Helper()

	baseURL := strings.TrimSpace(os.Getenv("OPENSSPM_TEST_DATABASE_URL"))
	if baseURL == "" {
		t.Skip("OPENSSPM_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminURL, err := testDatabaseAdminURL(baseURL)
	if err != nil {
		t.Fatalf("testDatabaseAdminURL() err = %v", err)
	}

	adminConn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgx.Connect(admin) err = %v", err)
	}
	defer adminConn.Close(ctx)

	dbName := "opensspm_entity_category_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminConn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatalf("CREATE DATABASE err = %v", err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		_, _ = adminConn.Exec(dropCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	}()

	testURL, err := testDatabaseURLWithName(baseURL, dbName)
	if err != nil {
		t.Fatalf("testDatabaseURLWithName() err = %v", err)
	}

	migrator, err := migrate.New("file://"+testMigrationsDir(t), testURL)
	if err != nil {
		t.Fatalf("migrate.New() err = %v", err)
	}
	defer func() {
		_, _ = migrator.Close()
	}()

	pool, err := pgxpool.New(ctx, testURL)
	if err != nil {
		t.Fatalf("pgxpool.New() err = %v", err)
	}
	defer pool.Close()

	fn(ctx, pool, New(pool), migrator)
}

func migrateUp(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
}

func migrateToVersion(t *testing.T, migrator *migrate.Migrate, version uint) {
	t.Helper()

	if err := migrator.Migrate(version); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate to version %d: %v", version, err)
	}
}

func insertSyncRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName string) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message)
		VALUES ($1, $2, 'success', now(), now(), '')
		RETURNING id
	`, sourceKind, sourceName).Scan(&id)
	if err != nil {
		t.Fatalf("insert sync run: %v", err)
	}
	return id
}

func insertAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, seed accountSeed) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO accounts (
			source_kind,
			source_name,
			external_id,
			email,
			display_name,
			status,
			account_kind,
			entity_category,
			raw_json,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, now(), $10, now(), now())
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.ExternalID, seed.Email, seed.DisplayName, seed.Status, seed.AccountKind, seed.EntityCategory, seed.RawJSON, runID).Scan(&id)
	if err != nil {
		t.Fatalf("insert account %s/%s: %v", seed.SourceKind, seed.ExternalID, err)
	}
	return id
}

func insertLegacyAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, seed accountSeed) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO accounts (
			source_kind,
			source_name,
			external_id,
			email,
			display_name,
			status,
			account_kind,
			raw_json,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, now(), $9, now(), now())
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.ExternalID, seed.Email, seed.DisplayName, seed.Status, seed.AccountKind, seed.RawJSON, runID).Scan(&id)
	if err != nil {
		t.Fatalf("insert legacy account %s/%s: %v", seed.SourceKind, seed.ExternalID, err)
	}
	return id
}

func insertIdentitySourceSetting(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName string, authoritative bool) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative, created_at, updated_at)
		VALUES ($1, $2, $3, now(), now())
		ON CONFLICT (source_kind, source_name) DO UPDATE SET
			is_authoritative = EXCLUDED.is_authoritative,
			updated_at = EXCLUDED.updated_at
	`, sourceKind, sourceName, authoritative); err != nil {
		t.Fatalf("insert identity source setting %s/%s: %v", sourceKind, sourceName, err)
	}
}

func insertIdentity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind, email, displayName string) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO identities (kind, display_name, primary_email, created_at, updated_at)
		VALUES ($1, $2, $3, now(), now())
		RETURNING id
	`, kind, displayName, email).Scan(&id)
	if err != nil {
		t.Fatalf("insert identity %s: %v", email, err)
	}
	return id
}

func insertIdentityAccountLink(t *testing.T, ctx context.Context, pool *pgxpool.Pool, identityID, accountID int64) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, created_at, updated_at)
		VALUES ($1, $2, 'seed', 1.0, now(), now())
	`, identityID, accountID); err != nil {
		t.Fatalf("insert identity account link %d/%d: %v", identityID, accountID, err)
	}
}

func insertEntitlement(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, accountID int64, kind, resource, permission string) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		INSERT INTO entitlements (
			app_user_id,
			kind,
			resource,
			permission,
			raw_json,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, '{}'::jsonb, $5, now(), $5, now(), now())
	`, accountID, kind, resource, permission, runID); err != nil {
		t.Fatalf("insert entitlement %s/%s for account %d: %v", kind, resource, accountID, err)
	}
}

func linkedExternalIDs(rows []ListSourceAccountsPageBySourceAndQueryRow) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ExternalID)
	}
	slices.Sort(ids)
	return ids
}

func accountExternalIDs(rows []Account) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ExternalID)
	}
	slices.Sort(ids)
	return ids
}

func testMigrationsDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "db", "migrations")
}

func testDatabaseAdminURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/postgres"
	return parsed.String(), nil
}

func testDatabaseURLWithName(raw, dbName string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + dbName
	return parsed.String(), nil
}
