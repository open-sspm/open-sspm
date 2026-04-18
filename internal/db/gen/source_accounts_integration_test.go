package gen

import (
	"context"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUpsertSourceAccountsBulkBySourcePreservesExistingEmailWhenIncomingEmailMissing(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		sourceKind := "github"
		sourceName := "acme"
		externalID := "user-1"

		initialRunID := insertSyncRun(t, ctx, pool, sourceKind, sourceName)
		upsertSourceAccountForTest(t, ctx, pool, q, initialRunID, sourceKind, sourceName, externalID, stringPtr("known@example.com"))

		cases := []struct {
			name  string
			email *string
		}{
			{name: "blank", email: stringPtr("   ")},
			{name: "null", email: nil},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				updateRunID := insertSyncRun(t, ctx, pool, sourceKind, sourceName)
				upsertSourceAccountForTest(t, ctx, pool, q, updateRunID, sourceKind, sourceName, externalID, tc.email)

				if got := fetchSourceAccountEmail(t, ctx, pool, sourceKind, sourceName, externalID); got != "known@example.com" {
					t.Fatalf("email = %q, want %q", got, "known@example.com")
				}
			})
		}
	})
}

func TestUpsertSourceAccountsBulkBySourcePopulatesBlankEmailWhenIncomingEmailPresent(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		sourceKind := "github"
		sourceName := "acme"
		externalID := "user-2"

		initialRunID := insertSyncRun(t, ctx, pool, sourceKind, sourceName)
		upsertSourceAccountForTest(t, ctx, pool, q, initialRunID, sourceKind, sourceName, externalID, stringPtr(""))

		updateRunID := insertSyncRun(t, ctx, pool, sourceKind, sourceName)
		upsertSourceAccountForTest(t, ctx, pool, q, updateRunID, sourceKind, sourceName, externalID, stringPtr("filled@example.com"))

		if got := fetchSourceAccountEmail(t, ctx, pool, sourceKind, sourceName, externalID); got != "filled@example.com" {
			t.Fatalf("email = %q, want %q", got, "filled@example.com")
		}
	})
}

func TestUpsertSourceAccountsBulkBySourceOverwritesExistingEmailWhenIncomingEmailPresent(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		sourceKind := "github"
		sourceName := "acme"
		externalID := "user-3"

		initialRunID := insertSyncRun(t, ctx, pool, sourceKind, sourceName)
		upsertSourceAccountForTest(t, ctx, pool, q, initialRunID, sourceKind, sourceName, externalID, stringPtr("old@example.com"))

		updateRunID := insertSyncRun(t, ctx, pool, sourceKind, sourceName)
		upsertSourceAccountForTest(t, ctx, pool, q, updateRunID, sourceKind, sourceName, externalID, stringPtr("new@example.com"))

		if got := fetchSourceAccountEmail(t, ctx, pool, sourceKind, sourceName, externalID); got != "new@example.com" {
			t.Fatalf("email = %q, want %q", got, "new@example.com")
		}
	})
}

func upsertSourceAccountForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, q *Queries, runID int64, sourceKind, sourceName, externalID string, email *string) {
	t.Helper()

	rawJSON := [][]byte{[]byte(`{"status":"active"}`)}
	lastLoginAts := []pgtype.Timestamptz{{}}
	lastLoginIps := []string{""}
	lastLoginRegions := []string{""}

	if email == nil {
		if _, err := pool.Exec(ctx, upsertSourceAccountsBulkBySource,
			sourceKind,
			sourceName,
			runID,
			[]string{externalID},
			[]*string{nil},
			[]string{"Example User"},
			[]string{"human"},
			[]string{"user"},
			rawJSON,
			lastLoginAts,
			lastLoginIps,
			lastLoginRegions,
		); err != nil {
			t.Fatalf("pool.Exec(upsertSourceAccountsBulkBySource): %v", err)
		}
		return
	}

	if _, err := q.UpsertSourceAccountsBulkBySource(ctx, UpsertSourceAccountsBulkBySourceParams{
		SourceKind:       sourceKind,
		SourceName:       sourceName,
		SeenInRunID:      runID,
		ExternalIds:      []string{externalID},
		Emails:           []string{*email},
		DisplayNames:     []string{"Example User"},
		AccountKinds:     []string{"human"},
		EntityCategories: []string{"user"},
		RawJsons:         rawJSON,
		LastLoginAts:     lastLoginAts,
		LastLoginIps:     lastLoginIps,
		LastLoginRegions: lastLoginRegions,
	}); err != nil {
		t.Fatalf("UpsertSourceAccountsBulkBySource(): %v", err)
	}
}

func fetchSourceAccountEmail(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName, externalID string) string {
	t.Helper()

	var email string
	if err := pool.QueryRow(ctx, `
		SELECT email
		FROM accounts
		WHERE source_kind = $1
		  AND source_name = $2
		  AND external_id = $3
	`, sourceKind, sourceName, externalID).Scan(&email); err != nil {
		t.Fatalf("select account email: %v", err)
	}
	return email
}

func stringPtr(s string) *string {
	return &s
}
