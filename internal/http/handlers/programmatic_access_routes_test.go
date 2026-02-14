package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	githubconnector "github.com/open-sspm/open-sspm/internal/connectors/github"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleCredentials_DBBackedRiskFilter(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	now := time.Now().UTC()
	expired := now.Add(-2 * time.Hour)
	healthyExpiry := now.Add(60 * 24 * time.Hour)
	recentUsage := now.Add(-24 * time.Hour)

	harness.insertCredential(t, credentialSeed{
		AssetRefExternalID:   "github_app_installation:install-1",
		CredentialKind:       "github_deploy_key",
		ExternalID:           "key-critical",
		DisplayName:          "critical-deploy-key",
		Status:               "active",
		CreatedByExternalID:  "owner@example.com",
		CreatedByDisplayName: "Owner One",
		ExpiresAtSource:      &expired,
	})
	harness.insertCredential(t, credentialSeed{
		AssetRefExternalID:    "github_app_installation:install-2",
		CredentialKind:        "github_pat_fine_grained",
		ExternalID:            "pat-healthy",
		DisplayName:           "healthy-pat",
		Status:                "active",
		CreatedByExternalID:   "creator@example.com",
		CreatedByDisplayName:  "Creator One",
		ApprovedByExternalID:  "approver@example.com",
		ApprovedByDisplayName: "Approver One",
		ExpiresAtSource:       &healthyExpiry,
		LastUsedAtSource:      &recentUsage,
	})

	target := "/credentials?source_kind=github&source_name=" + url.QueryEscape(harness.sourceName) + "&risk_level=critical"
	c, rec := newTestContext(http.MethodGet, target)
	if err := harness.handlers.HandleCredentials(c); err != nil {
		t.Fatalf("HandleCredentials() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "critical-deploy-key") {
		t.Fatalf("response missing critical credential row: %q", body)
	}
	if strings.Contains(body, "healthy-pat") {
		t.Fatalf("response included filtered-out credential: %q", body)
	}
}

func TestHandleCredentials_DBBackedHighRiskIncludesExpiredNonActive(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	now := time.Now().UTC()
	expired := now.Add(-2 * time.Hour)
	healthyExpiry := now.Add(60 * 24 * time.Hour)
	recentUsage := now.Add(-24 * time.Hour)

	harness.insertCredential(t, credentialSeed{
		AssetRefExternalID:    "github_app_installation:install-1",
		CredentialKind:        "github_pat_fine_grained",
		ExternalID:            "pat-critical",
		DisplayName:           "pat-critical",
		Status:                "active",
		CreatedByExternalID:   "creator@example.com",
		CreatedByDisplayName:  "Creator One",
		ApprovedByExternalID:  "approver@example.com",
		ApprovedByDisplayName: "Approver One",
		ExpiresAtSource:       &expired,
	})
	harness.insertCredential(t, credentialSeed{
		AssetRefExternalID:    "github_app_installation:install-2",
		CredentialKind:        "github_pat_fine_grained",
		ExternalID:            "pat-high",
		DisplayName:           "pat-high",
		Status:                "revoked",
		CreatedByExternalID:   "creator@example.com",
		CreatedByDisplayName:  "Creator One",
		ApprovedByExternalID:  "approver@example.com",
		ApprovedByDisplayName: "Approver One",
		ExpiresAtSource:       &expired,
		LastUsedAtSource:      &recentUsage,
	})
	harness.insertCredential(t, credentialSeed{
		AssetRefExternalID:    "github_app_installation:install-3",
		CredentialKind:        "github_pat_fine_grained",
		ExternalID:            "pat-low",
		DisplayName:           "pat-low",
		Status:                "active",
		CreatedByExternalID:   "creator@example.com",
		CreatedByDisplayName:  "Creator One",
		ApprovedByExternalID:  "approver@example.com",
		ApprovedByDisplayName: "Approver One",
		ExpiresAtSource:       &healthyExpiry,
		LastUsedAtSource:      &recentUsage,
	})

	target := "/credentials?source_kind=github&source_name=" + url.QueryEscape(harness.sourceName) + "&risk_level=high"
	c, rec := newTestContext(http.MethodGet, target)
	if err := harness.handlers.HandleCredentials(c); err != nil {
		t.Fatalf("HandleCredentials() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "pat-high") {
		t.Fatalf("response missing expired non-active high-risk credential row: %q", body)
	}
	if strings.Contains(body, "pat-critical") {
		t.Fatalf("response included critical-risk credential in high filter: %q", body)
	}
	if strings.Contains(body, "pat-low") {
		t.Fatalf("response included filtered-out low-risk credential: %q", body)
	}
}

func TestHandleCredentials_DBBackedExpiresInDaysExcludesAlreadyExpired(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	now := time.Now().UTC()
	expired := now.Add(-2 * time.Hour)
	expiresSoon := now.Add(2 * 24 * time.Hour)

	harness.insertCredential(t, credentialSeed{
		AssetRefExternalID:   "github_app_installation:install-1",
		CredentialKind:       "github_deploy_key",
		ExternalID:           "key-expired",
		DisplayName:          "key-expired",
		Status:               "active",
		CreatedByExternalID:  "owner@example.com",
		CreatedByDisplayName: "Owner One",
		ExpiresAtSource:      &expired,
	})
	harness.insertCredential(t, credentialSeed{
		AssetRefExternalID:   "github_app_installation:install-2",
		CredentialKind:       "github_deploy_key",
		ExternalID:           "key-expiring-soon",
		DisplayName:          "key-expiring-soon",
		Status:               "active",
		CreatedByExternalID:  "owner@example.com",
		CreatedByDisplayName: "Owner One",
		ExpiresAtSource:      &expiresSoon,
	})

	target := "/credentials?source_kind=github&source_name=" + url.QueryEscape(harness.sourceName) + "&expires_in_days=7"
	c, rec := newTestContext(http.MethodGet, target)
	if err := harness.handlers.HandleCredentials(c); err != nil {
		t.Fatalf("HandleCredentials() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "key-expiring-soon") {
		t.Fatalf("response missing credential expiring within 7 days: %q", body)
	}
	if strings.Contains(body, "key-expired") {
		t.Fatalf("response included already expired credential: %q", body)
	}
}

func TestHandleCredentialShow_DBBackedNotFound(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	c, rec := newTestContext(http.MethodGet, "/credentials/999999")
	c.SetPathValues(echo.PathValues{{Name: "id", Value: "999999"}})

	if err := harness.handlers.HandleCredentialShow(c); err != nil {
		t.Fatalf("HandleCredentialShow() error = %v", err)
	}

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "404 page not found" {
		t.Fatalf("body = %q, want %q", got, "404 page not found")
	}
}

func TestHandleIdentities_DBBackedDirectoryTable(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	identitiesExists, err := relationExists(harness.ctx, harness.tx, "identities")
	if err != nil {
		t.Fatalf("check identities relation: %v", err)
	}
	identityAccountsExists, err := relationExists(harness.ctx, harness.tx, "identity_accounts")
	if err != nil {
		t.Fatalf("check identity_accounts relation: %v", err)
	}
	if !identitiesExists || !identityAccountsExists {
		t.Skip("skipping DB-backed identities route test: identity graph schema not available")
	}

	identityCreatedAt := time.Date(2025, time.December, 20, 0, 0, 0, 0, time.UTC)
	var identityID int64
	if err := harness.tx.QueryRow(harness.ctx, `
		INSERT INTO identities (kind, display_name, primary_email, created_at, updated_at)
		VALUES ('human', 'Alice Admin', 'alice@example.com', $1, now())
		RETURNING id
	`, identityCreatedAt).Scan(&identityID); err != nil {
		t.Fatalf("insert identity: %v", err)
	}

	insertAccount := func(sourceKind, sourceName, externalID string, createdAt, lastObservedAt time.Time) int64 {
		t.Helper()
		var accountID int64
		err := harness.tx.QueryRow(harness.ctx, `
			INSERT INTO accounts (
				source_kind,
				source_name,
				external_id,
				email,
				display_name,
				status,
				raw_json,
				seen_in_run_id,
				seen_at,
				last_observed_run_id,
				last_observed_at,
				created_at,
				updated_at
			)
			VALUES (
				$1, $2, $3, $4, $5, 'active', '{}'::jsonb, $6, now(), $6, $7, $8, now()
			)
			RETURNING id
		`, sourceKind, sourceName, externalID, "alice@example.com", "Alice Admin", harness.runID, lastObservedAt.UTC(), createdAt.UTC()).Scan(&accountID)
		if err != nil {
			t.Fatalf("insert account (%s:%s:%s): %v", sourceKind, sourceName, externalID, err)
		}
		return accountID
	}

	githubAccountID := insertAccount(
		"github",
		harness.sourceName,
		"alice-gh",
		time.Date(2026, time.January, 1, 8, 0, 0, 0, time.UTC),
		time.Date(2026, time.February, 10, 9, 0, 0, 0, time.UTC),
	)
	datadogAccountID := insertAccount(
		"datadog",
		"datadoghq.com",
		"alice-dd",
		time.Date(2026, time.January, 3, 8, 0, 0, 0, time.UTC),
		time.Date(2026, time.February, 12, 14, 0, 0, 0, time.UTC),
	)
	awsAccountID := insertAccount(
		"aws",
		"aws-prod",
		"alice-aws",
		time.Date(2026, time.January, 5, 8, 0, 0, 0, time.UTC),
		time.Date(2026, time.February, 11, 13, 0, 0, 0, time.UTC),
	)

	for _, accountID := range []int64{githubAccountID, datadogAccountID, awsAccountID} {
		if _, err := harness.tx.Exec(harness.ctx, `
			INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
			VALUES ($1, $2, 'manual', 1.0, now())
		`, identityID, accountID); err != nil {
			t.Fatalf("insert identity account link for account %d: %v", accountID, err)
		}
	}

	if _, err := harness.tx.Exec(harness.ctx, `
		INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative, updated_at)
		VALUES ('github', $1, true, now())
		ON CONFLICT (source_kind, source_name) DO UPDATE
		SET is_authoritative = EXCLUDED.is_authoritative, updated_at = EXCLUDED.updated_at
	`, harness.sourceName); err != nil {
		t.Fatalf("upsert identity_source_settings: %v", err)
	}

	insertEntitlement := func(accountID int64, kind, resource, permission string, rawJSON []byte) {
		t.Helper()
		if _, err := harness.tx.Exec(harness.ctx, `
			INSERT INTO entitlements (
				app_user_id,
				kind,
				resource,
				permission,
				raw_json,
				created_at,
				seen_in_run_id,
				seen_at,
				last_observed_run_id,
				last_observed_at,
				updated_at
			)
			VALUES ($1, $2, $3, $4, $5::jsonb, now(), $6, now(), $6, now(), now())
		`, accountID, kind, resource, permission, rawJSON, harness.runID); err != nil {
			t.Fatalf("insert entitlement (%s): %v", kind, err)
		}
	}

	insertEntitlement(
		githubAccountID,
		"github_team_repo_permission",
		"github_repo:acme/repo-one",
		"admin",
		[]byte(`{"team":"platform"}`),
	)
	insertEntitlement(
		datadogAccountID,
		"datadog_role",
		"datadog_role:billing_admin",
		"member",
		[]byte(`{"role_name":"Billing Administrator"}`),
	)
	insertEntitlement(
		awsAccountID,
		"aws_permission_set",
		"aws_account:123456789012",
		"PowerUserAccess",
		[]byte(`{"permission_set_arn":"arn:aws:sso:::permissionSet/test"}`),
	)

	c, rec := newTestContext(http.MethodGet, "/identities?q=alice")
	if err := harness.handlers.HandleIdentities(c); err != nil {
		t.Fatalf("HandleIdentities() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Alice Admin") {
		t.Fatalf("response missing identity display name: %q", body)
	}
	if !strings.Contains(body, "alice@example.com") {
		t.Fatalf("response missing identity secondary email: %q", body)
	}
	if strings.Count(body, `>3</td>`) < 2 {
		t.Fatalf("response missing expected integrations/privileged role counts: %q", body)
	}
	if !strings.Contains(body, "Feb 12, 2026") {
		t.Fatalf("response missing expected last-seen date: %q", body)
	}
	if !strings.Contains(body, "Jan 1, 2026") {
		t.Fatalf("response missing expected first-created date: %q", body)
	}
}

type programmaticAccessRouteHarness struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	tx         pgx.Tx
	q          *gen.Queries
	handlers   *Handlers
	sourceName string
	runID      int64
}

func newProgrammaticAccessRouteHarness(t *testing.T) *programmaticAccessRouteHarness {
	t.Helper()

	dsn := testDatabaseURLFromEnv()
	if dsn == "" {
		t.Skip("skipping DB-backed route test: set OPEN_SSPM_TEST_DATABASE_URL or DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		cancel()
		t.Skipf("skipping DB-backed route test: open database pool failed: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		cancel()
		t.Skipf("skipping DB-backed route test: database ping failed: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		pool.Close()
		cancel()
		t.Fatalf("begin tx: %v", err)
	}

	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
		pool.Close()
		cancel()
	})

	q := gen.New(tx)
	ensureProgrammaticAccessSchema(t, ctx, tx)

	reg := registry.NewRegistry()
	if err := reg.Register(githubconnector.NewDefinition(1)); err != nil {
		t.Fatalf("register github connector: %v", err)
	}

	sourceName := fmt.Sprintf("route-db-test-%d", time.Now().UnixNano())
	configJSON, err := json.Marshal(map[string]any{
		"org":   sourceName,
		"token": "test-token",
	})
	if err != nil {
		t.Fatalf("marshal connector config: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO connector_configs (kind, enabled, config)
		VALUES ('github', true, $1::jsonb)
		ON CONFLICT (kind) DO UPDATE
		SET enabled = EXCLUDED.enabled, config = EXCLUDED.config, updated_at = now()
	`, configJSON); err != nil {
		t.Fatalf("configure github connector: %v", err)
	}

	runID, err := q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: "github",
		SourceName: sourceName,
	})
	if err != nil {
		t.Fatalf("create sync run: %v", err)
	}
	if err := q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{
		ID:    runID,
		Stats: []byte(`{}`),
	}); err != nil {
		t.Fatalf("mark sync run success: %v", err)
	}

	h := &Handlers{
		Q:        q,
		Registry: reg,
	}

	return &programmaticAccessRouteHarness{
		ctx:        ctx,
		pool:       pool,
		tx:         tx,
		q:          q,
		handlers:   h,
		sourceName: sourceName,
		runID:      runID,
	}
}

func ensureProgrammaticAccessSchema(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()

	exists, err := relationExists(ctx, tx, "credential_artifacts")
	if err != nil {
		t.Fatalf("check credential_artifacts relation: %v", err)
	}
	if exists {
		return
	}

	migrationSQL, err := readFirstFile(
		filepath.Join("db", "migrations", "000021_app_access_governance.up.sql"),
		filepath.Join("..", "..", "..", "db", "migrations", "000021_app_access_governance.up.sql"),
	)
	if err != nil {
		t.Skipf("skipping DB-backed route test: app-access migration not found: %v", err)
	}

	if _, err := tx.Exec(ctx, migrationSQL); err != nil {
		t.Skipf("skipping DB-backed route test: applying app-access migration failed: %v", err)
	}
}

func relationExists(ctx context.Context, tx pgx.Tx, relationName string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `SELECT to_regclass('public.' || $1) IS NOT NULL`, relationName).Scan(&exists)
	return exists, err
}

func readFirstFile(paths ...string) (string, error) {
	for _, candidate := range paths {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		return string(data), nil
	}
	return "", fmt.Errorf("none of the candidate paths exist")
}

func testDatabaseURLFromEnv() string {
	for _, key := range []string{"OPEN_SSPM_TEST_DATABASE_URL", "DATABASE_URL"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}

	for _, candidate := range []string{".env", filepath.Join("..", "..", "..", ".env")} {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		if value := parseDotenvValue(string(data), "DATABASE_URL"); value != "" {
			return value
		}
	}

	return ""
}

func parseDotenvValue(contents, key string) string {
	prefix := key + "="
	for line := range strings.SplitSeq(contents, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, prefix) {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, prefix))
		value = strings.Trim(value, `"'`)
		if value != "" {
			return value
		}
	}
	return ""
}

type credentialSeed struct {
	AssetRefExternalID    string
	CredentialKind        string
	ExternalID            string
	DisplayName           string
	Status                string
	CreatedByExternalID   string
	CreatedByDisplayName  string
	ApprovedByExternalID  string
	ApprovedByDisplayName string
	ExpiresAtSource       *time.Time
	LastUsedAtSource      *time.Time
}

func (h *programmaticAccessRouteHarness) insertCredential(t *testing.T, seed credentialSeed) int64 {
	t.Helper()

	const insertSQL = `
		INSERT INTO credential_artifacts (
			source_kind,
			source_name,
			asset_ref_kind,
			asset_ref_external_id,
			credential_kind,
			external_id,
			display_name,
			status,
			scope_json,
			created_by_external_id,
			created_by_display_name,
			approved_by_external_id,
			approved_by_display_name,
			expires_at_source,
			last_used_at_source,
			raw_json,
			seen_in_run_id,
			last_observed_run_id,
			seen_at,
			last_observed_at
		)
		VALUES (
			$1, $2, 'app_asset', $3, $4, $5, $6, $7, '{}'::jsonb,
			$8, $9, $10, $11, $12, $13, '{}'::jsonb, $14, $14, now(), now()
		)
		RETURNING id
	`

	var id int64
	err := h.tx.QueryRow(h.ctx, insertSQL,
		"github",
		h.sourceName,
		seed.AssetRefExternalID,
		seed.CredentialKind,
		seed.ExternalID,
		seed.DisplayName,
		seed.Status,
		seed.CreatedByExternalID,
		seed.CreatedByDisplayName,
		seed.ApprovedByExternalID,
		seed.ApprovedByDisplayName,
		seed.ExpiresAtSource,
		seed.LastUsedAtSource,
		h.runID,
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert credential %q: %v", seed.ExternalID, err)
	}
	return id
}
