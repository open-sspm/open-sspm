package gen

import (
	"context"
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIdentityGraphCoreMappingInvariants(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "seed", "identity-graph")
		identityA := insertIdentity(t, ctx, pool, "human", "owner@example.com", "Owner Example")
		identityB := insertIdentity(t, ctx, pool, "human", "other@example.com", "Other Example")

		githubAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "github",
			SourceName:     "acme",
			ExternalID:     "owner-gh",
			Email:          "owner@example.com",
			DisplayName:    "Owner GitHub",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		datadogAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "datadog",
			SourceName:     "datadoghq.com",
			ExternalID:     "owner-dd",
			Email:          "owner@example.com",
			DisplayName:    "Owner Datadog",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})

		if _, err := q.UpsertIdentityAccountLink(ctx, UpsertIdentityAccountLinkParams{
			IdentityID: identityA,
			AccountID:  githubAccountID,
			LinkReason: "manual",
			Confidence: 1,
		}); err != nil {
			t.Fatalf("link github account to identity A: %v", err)
		}
		if _, err := q.UpsertIdentityAccountLink(ctx, UpsertIdentityAccountLinkParams{
			IdentityID: identityA,
			AccountID:  datadogAccountID,
			LinkReason: "manual",
			Confidence: 1,
		}); err != nil {
			t.Fatalf("link datadog account to identity A: %v", err)
		}

		linked, err := q.ListLinkedAccountsForIdentity(ctx, identityA)
		if err != nil {
			t.Fatalf("ListLinkedAccountsForIdentity(identity A): %v", err)
		}
		if len(linked) != 2 {
			t.Fatalf("identity A linked account count = %d, want 2", len(linked))
		}

		_, dupErr := pool.Exec(ctx, `
			INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, created_at, updated_at)
			VALUES ($1, $2, 'shared_attempt', 1.0, now(), now())
		`, identityB, githubAccountID)
		if dupErr == nil {
			t.Fatalf("duplicate identity_accounts row for one account succeeded; want unique constraint failure")
		}
		// Pin the test to the specific unique-violation code so that NOT NULL
		// or CHECK failures (which would also produce a non-nil err) don't
		// silently satisfy the invariant we're trying to enforce.
		var pgErr *pgconn.PgError
		if !errors.As(dupErr, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("duplicate insert err = %v, want unique_violation (23505)", dupErr)
		}

		if _, err := q.UpsertIdentityAccountLink(ctx, UpsertIdentityAccountLinkParams{
			IdentityID: identityB,
			AccountID:  githubAccountID,
			LinkReason: "manual",
			Confidence: 1,
		}); err != nil {
			t.Fatalf("relink github account to identity B: %v", err)
		}

		link, err := q.GetIdentityAccountLinkByAccountID(ctx, githubAccountID)
		if err != nil {
			t.Fatalf("GetIdentityAccountLinkByAccountID(github): %v", err)
		}
		if link.IdentityID != identityB {
			t.Fatalf("github account identity_id = %d, want %d after relink", link.IdentityID, identityB)
		}
		if count := countIdentityAccountRows(t, ctx, pool, githubAccountID); count != 1 {
			t.Fatalf("identity_accounts rows for github account = %d, want 1", count)
		}

		linkedAAfterRelink, err := q.ListLinkedAccountsForIdentity(ctx, identityA)
		if err != nil {
			t.Fatalf("ListLinkedAccountsForIdentity(identity A after relink): %v", err)
		}
		if len(linkedAAfterRelink) != 1 || linkedAAfterRelink[0].ID != datadogAccountID {
			t.Fatalf("identity A accounts after relink = %v, want only datadog account %d", accountIDs(linkedAAfterRelink), datadogAccountID)
		}

		insertEntitlement(t, ctx, pool, runID, githubAccountID, "github_team_repo_permission", "github_repo:acme/api", "admin")
		insertEntitlement(t, ctx, pool, runID, datadogAccountID, "datadog_role", "datadog_role:admin", "admin")

		assignments, err := q.ListNormalizedEntitlementAssignments(ctx)
		if err != nil {
			t.Fatalf("ListNormalizedEntitlementAssignments(): %v", err)
		}
		if len(assignments) != 2 {
			t.Fatalf("normalized entitlement assignment count = %d, want 2", len(assignments))
		}

		assignmentByAccount := make(map[string]ListNormalizedEntitlementAssignmentsRow, len(assignments))
		for _, row := range assignments {
			assignmentByAccount[row.AccountExternalID] = row
		}
		if row := assignmentByAccount["owner-gh"]; row.IdentityID != identityB {
			t.Fatalf("github entitlement identity_id = %d, want %d", row.IdentityID, identityB)
		}
		if row := assignmentByAccount["owner-dd"]; row.IdentityID != identityA {
			t.Fatalf("datadog entitlement identity_id = %d, want %d", row.IdentityID, identityA)
		}
	})
}

func countIdentityAccountRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, accountID int64) int64 {
	t.Helper()

	var count int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM identity_accounts
		WHERE account_id = $1
	`, accountID).Scan(&count); err != nil {
		t.Fatalf("count identity account rows for account %d: %v", accountID, err)
	}
	return count
}

func accountIDs(accounts []Account) []int64 {
	ids := make([]int64, 0, len(accounts))
	for _, account := range accounts {
		ids = append(ids, account.ID)
	}
	return ids
}
