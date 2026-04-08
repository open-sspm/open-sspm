package datadog

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type stubDatadogAdapter struct {
	accounts            []Account
	roles               []Role
	roleMembersByRoleID map[string][]string
	listAccountsErr     error
	listRolesErr        error
	listRoleMembersErr  map[string]error
}

func (a stubDatadogAdapter) ListAccounts(context.Context) ([]Account, error) {
	return a.accounts, a.listAccountsErr
}

func (a stubDatadogAdapter) ListRoles(context.Context) ([]Role, error) {
	return a.roles, a.listRolesErr
}

func (a stubDatadogAdapter) ListRoleMembers(_ context.Context, roleID string) ([]string, error) {
	if err := a.listRoleMembersErr[roleID]; err != nil {
		return nil, err
	}
	return a.roleMembersByRoleID[roleID], nil
}

func TestDatadogIntegrationRunWritesAccountsAndEntitlements(t *testing.T) {
	t.Parallel()

	withDatadogTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateDatadogUp(t, migrator)

		adapter := stubDatadogAdapter{
			accounts: []Account{
				{
					ExternalID:     "u-1",
					Email:          "alice@example.com",
					DisplayName:    "alice@example.com",
					Status:         "Active",
					AccountKind:    registry.AccountKindHuman,
					EntityCategory: registry.EntityCategoryUser,
					RawJSON:        []byte(`{"user_name":"alice@example.com","status":"Active"}`),
				},
				{
					ExternalID:     "service_account:sa-1",
					Email:          "ci@example.com",
					DisplayName:    "CI Service Account",
					Status:         "Active",
					AccountKind:    registry.AccountKindService,
					EntityCategory: registry.EntityCategoryServiceAccount,
					RawJSON:        []byte(`{"name":"CI Service Account","email":"ci@example.com","status":"Active"}`),
				},
			},
			roles: []Role{
				{ID: "admin", Name: "Admin", RawJSON: []byte(`{"name":"Admin"}`)},
			},
			roleMembersByRoleID: map[string][]string{
				"admin": {"u-1", "service_account:sa-1"},
			},
			listRoleMembersErr: map[string]error{},
		}

		integration := NewDatadogIntegration(adapter, "datadoghq.com", 2)
		if err := integration.Run(ctx, q, pool, func(registry.Event) {}, registry.RunModeFull); err != nil {
			t.Fatalf("Run(): %v", err)
		}

		rows, err := q.ListSourceAccountsPageBySourceAndQuery(ctx, gen.ListSourceAccountsPageBySourceAndQueryParams{
			SourceKind: "datadog",
			SourceName: "datadoghq.com",
			PageLimit:  20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(): %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("len(rows) = %d, want 3", len(rows))
		}

		byExternalID := make(map[string]gen.ListSourceAccountsPageBySourceAndQueryRow, len(rows))
		for _, row := range rows {
			byExternalID[row.ExternalID] = row
		}

		user := byExternalID["u-1"]
		if user.EntityCategory != registry.EntityCategoryUser {
			t.Fatalf("user entity category = %q, want %q", user.EntityCategory, registry.EntityCategoryUser)
		}
		if user.Status != "Active" {
			t.Fatalf("user status = %q, want %q", user.Status, "Active")
		}

		serviceAccount := byExternalID["service_account:sa-1"]
		if serviceAccount.EntityCategory != registry.EntityCategoryServiceAccount {
			t.Fatalf("service entity category = %q, want %q", serviceAccount.EntityCategory, registry.EntityCategoryServiceAccount)
		}
		if serviceAccount.AccountKind != registry.AccountKindService {
			t.Fatalf("service account kind = %q, want %q", serviceAccount.AccountKind, registry.AccountKindService)
		}

		role := byExternalID["role:admin"]
		if role.EntityCategory != registry.EntityCategoryRole {
			t.Fatalf("role entity category = %q, want %q", role.EntityCategory, registry.EntityCategoryRole)
		}

		accountIDs := []int64{user.ID, serviceAccount.ID}
		ents, err := q.ListEntitlementsForAccountIDs(ctx, accountIDs)
		if err != nil {
			t.Fatalf("ListEntitlementsForAccountIDs(): %v", err)
		}
		if len(ents) != 2 {
			t.Fatalf("len(ents) = %d, want 2", len(ents))
		}

		entByAccountID := make(map[int64]gen.ListEntitlementsForAccountIDsRow, len(ents))
		for _, ent := range ents {
			entByAccountID[ent.AccountID] = ent
		}
		for _, accountID := range accountIDs {
			ent := entByAccountID[accountID]
			if ent.Kind != "datadog_role" {
				t.Fatalf("entitlement kind = %q, want datadog_role", ent.Kind)
			}
			if ent.Resource != "datadog_role:admin" {
				t.Fatalf("entitlement resource = %q, want datadog_role:admin", ent.Resource)
			}
			var payload map[string]string
			if err := json.Unmarshal(ent.RawJson, &payload); err != nil {
				t.Fatalf("json.Unmarshal(ent.RawJson): %v", err)
			}
			if payload["role_id"] != "admin" || payload["role_name"] != "Admin" {
				t.Fatalf("unexpected entitlement payload: %#v", payload)
			}
		}

		recentRuns, err := q.ListRecentFinishedSyncRunsBySource(ctx, gen.ListRecentFinishedSyncRunsBySourceParams{
			SourceKind: "datadog",
			SourceName: "datadoghq.com",
			Limit:      1,
		})
		if err != nil {
			t.Fatalf("ListRecentFinishedSyncRunsBySource(): %v", err)
		}
		if len(recentRuns) != 1 || recentRuns[0].Status != "success" {
			t.Fatalf("recentRuns = %#v, want one successful run", recentRuns)
		}
	})
}

func TestDatadogIntegrationRunPreservesExistingLinkedAccount(t *testing.T) {
	t.Parallel()

	withDatadogTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateDatadogUp(t, migrator)

		seedRunID, err := registry.StartSyncRun(ctx, q, "seed", "seed")
		if err != nil {
			t.Fatalf("StartSyncRun(seed): %v", err)
		}

		var existingAccountID int64
		if err := pool.QueryRow(ctx, `
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
			VALUES (
				'datadog',
				'datadoghq.com',
				'u-1',
				'legacy@example.com',
				'Legacy Name',
				'Inactive',
				'human',
				'user',
				'{"user_name":"Legacy Name","status":"Inactive"}'::jsonb,
				$1,
				now(),
				$1,
				now(),
				now()
			)
			RETURNING id
		`, seedRunID).Scan(&existingAccountID); err != nil {
			t.Fatalf("insert legacy datadog account: %v", err)
		}

		var identityID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO identities (kind, display_name, primary_email, created_at, updated_at)
			VALUES ('human', 'Alice Identity', 'alice@example.com', now(), now())
			RETURNING id
		`).Scan(&identityID); err != nil {
			t.Fatalf("insert identity: %v", err)
		}

		if _, err := pool.Exec(ctx, `
			INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, created_at, updated_at)
			VALUES ($1, $2, 'seed', 1.0, now(), now())
		`, identityID, existingAccountID); err != nil {
			t.Fatalf("insert identity account link: %v", err)
		}

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
			VALUES (
				$1,
				'datadog_role',
				'datadog_role:admin',
				'member',
				'{"role_id":"admin","role_name":"Legacy Admin"}'::jsonb,
				$2,
				now(),
				$2,
				now(),
				now()
			)
		`, existingAccountID, seedRunID); err != nil {
			t.Fatalf("insert legacy entitlement: %v", err)
		}

		adapter := stubDatadogAdapter{
			accounts: []Account{
				{
					ExternalID:     "u-1",
					Email:          "alice@example.com",
					DisplayName:    "Alice Example",
					Status:         "Active",
					AccountKind:    registry.AccountKindHuman,
					EntityCategory: registry.EntityCategoryUser,
					RawJSON:        []byte(`{"user_name":"Alice Example","status":"Active"}`),
				},
			},
			roles: []Role{
				{ID: "admin", Name: "Admin", RawJSON: []byte(`{"name":"Admin"}`)},
			},
			roleMembersByRoleID: map[string][]string{
				"admin": {"u-1"},
			},
			listRoleMembersErr: map[string]error{},
		}

		integration := NewDatadogIntegration(adapter, "datadoghq.com", 1)
		if err := integration.Run(ctx, q, pool, func(registry.Event) {}, registry.RunModeFull); err != nil {
			t.Fatalf("Run(): %v", err)
		}

		rows, err := q.ListSourceAccountsPageBySourceAndQuery(ctx, gen.ListSourceAccountsPageBySourceAndQueryParams{
			SourceKind: "datadog",
			SourceName: "datadoghq.com",
			PageLimit:  20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(): %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("len(rows) = %d, want 2", len(rows))
		}

		byExternalID := make(map[string]gen.ListSourceAccountsPageBySourceAndQueryRow, len(rows))
		for _, row := range rows {
			byExternalID[row.ExternalID] = row
		}

		user := byExternalID["u-1"]
		if user.ID != existingAccountID {
			t.Fatalf("user id = %d, want existing id %d", user.ID, existingAccountID)
		}
		if user.Email != "alice@example.com" {
			t.Fatalf("user email = %q, want %q", user.Email, "alice@example.com")
		}
		if user.DisplayName != "Alice Example" {
			t.Fatalf("user display name = %q, want %q", user.DisplayName, "Alice Example")
		}
		if user.Status != "Active" {
			t.Fatalf("user status = %q, want %q", user.Status, "Active")
		}
		if user.EntityCategory != registry.EntityCategoryUser {
			t.Fatalf("user entity category = %q, want %q", user.EntityCategory, registry.EntityCategoryUser)
		}

		link, err := q.GetIdentityAccountLinkByAccountID(ctx, existingAccountID)
		if err != nil {
			t.Fatalf("GetIdentityAccountLinkByAccountID(): %v", err)
		}
		if link.IdentityID != identityID {
			t.Fatalf("link identity id = %d, want %d", link.IdentityID, identityID)
		}
		if link.AccountID != existingAccountID {
			t.Fatalf("link account id = %d, want %d", link.AccountID, existingAccountID)
		}

		role := byExternalID["role:admin"]
		if role.EntityCategory != registry.EntityCategoryRole {
			t.Fatalf("role entity category = %q, want %q", role.EntityCategory, registry.EntityCategoryRole)
		}

		ents, err := q.ListEntitlementsForAccountIDs(ctx, []int64{existingAccountID})
		if err != nil {
			t.Fatalf("ListEntitlementsForAccountIDs(): %v", err)
		}
		if len(ents) != 1 {
			t.Fatalf("len(ents) = %d, want 1", len(ents))
		}
		if ents[0].Kind != "datadog_role" {
			t.Fatalf("entitlement kind = %q, want datadog_role", ents[0].Kind)
		}
		if ents[0].Resource != "datadog_role:admin" {
			t.Fatalf("entitlement resource = %q, want %q", ents[0].Resource, "datadog_role:admin")
		}

		var payload map[string]string
		if err := json.Unmarshal(ents[0].RawJson, &payload); err != nil {
			t.Fatalf("json.Unmarshal(ent.RawJson): %v", err)
		}
		if payload["role_id"] != "admin" || payload["role_name"] != "Admin" {
			t.Fatalf("unexpected entitlement payload: %#v", payload)
		}
	})
}

func TestDatadogIntegrationRunFailsWhenRoleMemberFetchFails(t *testing.T) {
	t.Parallel()

	withDatadogTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateDatadogUp(t, migrator)

		adapter := stubDatadogAdapter{
			accounts: []Account{
				{
					ExternalID:     "u-1",
					Email:          "alice@example.com",
					DisplayName:    "alice@example.com",
					Status:         "Active",
					AccountKind:    registry.AccountKindHuman,
					EntityCategory: registry.EntityCategoryUser,
					RawJSON:        []byte(`{"user_name":"alice@example.com","status":"Active"}`),
				},
			},
			roles: []Role{
				{ID: "admin", Name: "Admin", RawJSON: []byte(`{"name":"Admin"}`)},
			},
			roleMembersByRoleID: map[string][]string{},
			listRoleMembersErr: map[string]error{
				"admin": errors.New("boom"),
			},
		}

		integration := NewDatadogIntegration(adapter, "datadoghq.com", 1)
		if err := integration.Run(ctx, q, pool, func(registry.Event) {}, registry.RunModeFull); err == nil {
			t.Fatalf("Run() error = nil, want non-nil")
		}

		var activeAccountCount int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM accounts
			WHERE source_kind = 'datadog'
			  AND source_name = 'datadoghq.com'
			  AND expired_at IS NULL
			  AND last_observed_run_id IS NOT NULL
		`).Scan(&activeAccountCount); err != nil {
			t.Fatalf("count active datadog accounts: %v", err)
		}
		if activeAccountCount != 0 {
			t.Fatalf("active datadog accounts = %d, want 0", activeAccountCount)
		}

		recentRuns, err := q.ListRecentFinishedSyncRunsBySource(ctx, gen.ListRecentFinishedSyncRunsBySourceParams{
			SourceKind: "datadog",
			SourceName: "datadoghq.com",
			Limit:      1,
		})
		if err != nil {
			t.Fatalf("ListRecentFinishedSyncRunsBySource(): %v", err)
		}
		if len(recentRuns) != 1 {
			t.Fatalf("len(recentRuns) = %d, want 1", len(recentRuns))
		}
		if recentRuns[0].Status != registry.SyncStatusError {
			t.Fatalf("run status = %q, want %q", recentRuns[0].Status, registry.SyncStatusError)
		}
		if recentRuns[0].ErrorKind != registry.SyncErrorKindAPI {
			t.Fatalf("run error_kind = %q, want %q", recentRuns[0].ErrorKind, registry.SyncErrorKindAPI)
		}
	})
}

func withDatadogTestDatabase(t *testing.T, fn func(context.Context, *pgxpool.Pool, *gen.Queries, *migrate.Migrate)) {
	t.Helper()

	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	adminURL, err := datadogTestDatabaseAdminURL(baseURL)
	if err != nil {
		t.Fatalf("datadogTestDatabaseAdminURL() err = %v", err)
	}

	adminConn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgx.Connect() err = %v", err)
	}
	defer adminConn.Close(ctx)

	dbName := "test_datadog_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminConn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatalf("CREATE DATABASE %s: %v", dbName, err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		_, _ = adminConn.Exec(dropCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	}()

	testURL, err := datadogTestDatabaseURLWithName(baseURL, dbName)
	if err != nil {
		t.Fatalf("datadogTestDatabaseURLWithName() err = %v", err)
	}

	migrator, err := migrate.New("file://"+datadogTestMigrationsDir(t), testURL)
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

	fn(ctx, pool, gen.New(pool), migrator)
}

func migrateDatadogUp(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
}

func datadogTestMigrationsDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "db", "migrations")
}

func datadogTestDatabaseAdminURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/postgres"
	return parsed.String(), nil
}

func datadogTestDatabaseURLWithName(raw, dbName string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + dbName
	return parsed.String(), nil
}
