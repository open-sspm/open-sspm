package gen

import (
	"context"
	"slices"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/testdb"
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

		metricUserTotal, err := q.CountSourceAccountsBySource(ctx, CountSourceAccountsBySourceParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsBySource(datadog user metrics): %v", err)
		}
		if metricUserTotal != 2 {
			t.Fatalf("CountSourceAccountsBySource(datadog user metrics)=%d want 2", metricUserTotal)
		}

		metricUserAnchored, err := q.CountAnchoredSourceAccountsBySource(ctx, CountAnchoredSourceAccountsBySourceParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountAnchoredSourceAccountsBySource(datadog user metrics): %v", err)
		}
		if metricUserAnchored != 1 {
			t.Fatalf("CountAnchoredSourceAccountsBySource(datadog user metrics)=%d want 1", metricUserAnchored)
		}

		metricUserNeedsAnchor, err := q.CountSourceAccountsNeedingAnchorBySource(ctx, CountSourceAccountsNeedingAnchorBySourceParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySource(datadog user metrics): %v", err)
		}
		if metricUserNeedsAnchor != 1 {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySource(datadog user metrics)=%d want 1", metricUserNeedsAnchor)
		}

		metricAllNeedsAnchor, err := q.CountSourceAccountsNeedingAnchorBySource(ctx, CountSourceAccountsNeedingAnchorBySourceParams{
			SourceKind: "datadog",
			SourceName: "datadoghq.com",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySource(datadog all metrics): %v", err)
		}
		if metricAllNeedsAnchor != 3 {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySource(datadog all metrics)=%d want 3", metricAllNeedsAnchor)
		}

		needsAnchorCount, err := q.CountSourceAccountsNeedingAnchorBySourceAndQuery(ctx, CountSourceAccountsNeedingAnchorBySourceAndQueryParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySourceAndQuery(user): %v", err)
		}
		if needsAnchorCount != 1 {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySourceAndQuery(user)=%d want 1", needsAnchorCount)
		}

		needsAnchorRows, err := q.ListSourceAccountsNeedingAnchorPageBySourceAndQuery(ctx, ListSourceAccountsNeedingAnchorPageBySourceAndQueryParams{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			EntityCategory: "user",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsNeedingAnchorPageBySourceAndQuery(user): %v", err)
		}
		if len(needsAnchorRows) != 1 || needsAnchorRows[0].ExternalID != "dd-user-2" {
			t.Fatalf("ListSourceAccountsNeedingAnchorPageBySourceAndQuery(user)=%v want [dd-user-2]", accountExternalIDs(needsAnchorRows))
		}

		githubProvisionalID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "github",
			SourceName:     "acme",
			ExternalID:     "github-provisional",
			Email:          "provisional@example.com",
			DisplayName:    "GitHub Provisional",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		provisionalIdentityID := insertIdentity(t, ctx, pool, "human", "provisional@example.com", "GitHub Provisional")
		insertIdentityAccountLink(t, ctx, pool, provisionalIdentityID, githubProvisionalID)

		provisionalCount, err := q.CountSourceAccountsNeedingAnchorBySourceAndQuery(ctx, CountSourceAccountsNeedingAnchorBySourceAndQueryParams{
			SourceKind: "github",
			SourceName: "acme",
			Query:      "github-provisional",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySourceAndQuery(provisional github): %v", err)
		}
		if provisionalCount != 1 {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySourceAndQuery(provisional github)=%d want 1", provisionalCount)
		}

		oktaRunID := insertSyncRun(t, ctx, pool, "okta", "acme.okta.com")
		oktaAnchorID := insertAccount(t, ctx, pool, oktaRunID, accountSeed{
			SourceKind:     "okta",
			SourceName:     "acme.okta.com",
			ExternalID:     "okta-anchor",
			Email:          "provisional@example.com",
			DisplayName:    "GitHub Provisional",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		insertIdentitySourceSetting(t, ctx, pool, "okta", "acme.okta.com", true)
		insertIdentityAccountLink(t, ctx, pool, provisionalIdentityID, oktaAnchorID)

		anchoredProvisionalCount, err := q.CountSourceAccountsNeedingAnchorBySourceAndQuery(ctx, CountSourceAccountsNeedingAnchorBySourceAndQueryParams{
			SourceKind: "github",
			SourceName: "acme",
			Query:      "github-provisional",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySourceAndQuery(anchored github): %v", err)
		}
		if anchoredProvisionalCount != 0 {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySourceAndQuery(anchored github)=%d want 0", anchoredProvisionalCount)
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

		needsAnchorCount, err := q.CountSourceAccountsNeedingAnchorBySourceAndQuery(ctx, CountSourceAccountsNeedingAnchorBySourceAndQueryParams{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			EntityCategory: "user",
		})
		if err != nil {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySourceAndQuery(google workspace users): %v", err)
		}
		if needsAnchorCount != 0 {
			t.Fatalf("CountSourceAccountsNeedingAnchorBySourceAndQuery(google workspace users)=%d want 0", needsAnchorCount)
		}

		needsAnchorRows, err := q.ListSourceAccountsNeedingAnchorPageBySourceAndQuery(ctx, ListSourceAccountsNeedingAnchorPageBySourceAndQueryParams{
			SourceKind:     "google_workspace",
			SourceName:     "C0123",
			EntityCategory: "user",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsNeedingAnchorPageBySourceAndQuery(google workspace users): %v", err)
		}
		if len(needsAnchorRows) != 0 {
			t.Fatalf("ListSourceAccountsNeedingAnchorPageBySourceAndQuery(google workspace users)=%v want []", accountExternalIDs(needsAnchorRows))
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

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_entity_category"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		fn(ctx, pool, New(pool), migrator)
	})
}

func migrateUp(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	testdb.MigrateUp(t, migrator)
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
