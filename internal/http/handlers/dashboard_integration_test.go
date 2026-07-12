package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/findings"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestHandleDashboardUsesGenericInventoryMetrics(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{Domain: "acme.okta.com"})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{TenantID: "tenant-1"})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{Org: "acme"})

		oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

		oktaAccountID := insertCommandSearchAccount(t, ctx, pool, oktaRunID, commandSearchAccountSeed{
			SourceKind:     configstore.KindOkta,
			SourceName:     "acme.okta.com",
			ExternalID:     "okta-user-1",
			Email:          "alice@example.com",
			DisplayName:    "Alice Okta",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		oktaIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "alice@example.com", "Alice Okta")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, oktaIdentityID, oktaAccountID)

		githubAccountID := insertCommandSearchAccount(t, ctx, pool, githubRunID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "github-user-1",
			Email:          "bob@example.com",
			DisplayName:    "Bob GitHub",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		githubIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "bob@example.com", "Bob GitHub")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, githubIdentityID, githubAccountID)

		insertCommandSearchDiscoveryApp(t, ctx, pool, q, entraRunID, configstore.KindEntra, "tenant-1", "azure-cloud", "Azure Cloud", "azure.com", "Microsoft", "azure-cloud")

		insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_application", "azure-enterprise-app", "", "Azure Enterprise App", "active")
		insertCommandSearchAppAsset(t, ctx, q, githubRunID, configstore.KindGitHub, "acme", "github_app", "github-actions", "", "GitHub Actions", "active")

		insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "reference-app-1", "Reference App 1", "reference-app-1", "active")
		insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "reference-app-2", "Reference App 2", "reference-app-2", "active")
		insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "reference-app-3", "Reference App 3", "reference-app-3", "active")
		if err := q.UpsertIntegrationOktaAppMap(ctx, gen.UpsertIntegrationOktaAppMapParams{
			IntegrationKind:   configstore.KindGitHub,
			OktaSourceKind:    configstore.KindOkta,
			OktaSourceName:    "example.okta.com",
			OktaAppExternalID: "reference-app-1",
		}); err != nil {
			t.Fatalf("UpsertIntegrationOktaAppMap: %v", err)
		}

		body := renderDashboard(t, h, "http://example.com/")
		assertContains(t, body, `id="dashboard-content"`)
		assertContains(t, body, `osspm:data-sync-changed from:body`)
		assertDashboardMetric(t, body, "Identities", 2)
		assertDashboardMetric(t, body, "Discovered SaaS apps", 1)
		assertDashboardMetric(t, body, "App assets", 2)
		assertContains(t, body, "Needs attention")
		assertContains(t, body, "Credentials to rotate")
		assertContains(t, body, "/discovery/apps?review_state=unreviewed")
		assertContains(t, body, "/identities?identity_type=human&amp;status=suspended")
		assertContains(t, body, "/settings/connector-health")

		fragment := renderDashboardFragment(t, h, "http://example.com/")
		assertContains(t, fragment, `id="dashboard-content"`)
		assertDashboardMetric(t, fragment, "Identities", 2)
		assertNotContains(t, fragment, "<!doctype html>")
	})
}

func TestHandleDashboardUsesCanonicalFindingsForFrameworkPosture(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "acme.okta.com",
			Token:  "okta-token",
		})

		ruleset := seedRulesetForFindingsHandler(t, ctx, q)
		passRule := seedRuleForFindingsHandler(t, ctx, q, ruleset.ID, "001-pass")
		failRule := seedRuleForFindingsHandler(t, ctx, q, ruleset.ID, "002-fail")
		evaluatedAt := time.Date(2026, time.May, 22, 9, 0, 0, 0, time.UTC)
		writer := findings.NewWriter(q)
		if err := writer.Write(ctx, canonicalRuleFindingForHandlerSource(ruleset.Key, passRule, "acme.okta.com", "pass", findings.StatusResolved, evaluatedAt)); err != nil {
			t.Fatalf("Write(pass) err = %v", err)
		}
		if err := writer.Write(ctx, canonicalRuleFindingForHandlerSource(ruleset.Key, failRule, "acme.okta.com", "fail", findings.StatusOpen, evaluatedAt.Add(time.Minute))); err != nil {
			t.Fatalf("Write(fail) err = %v", err)
		}

		body := renderDashboard(t, h, "http://example.com/")
		assertContains(t, body, "CIS Okta")
		assertContains(t, body, "1/2")
		assertContains(t, body, "50%")
		assertContains(t, body, "Open findings by severity")
	})
}

func TestDashboardAccumulateFindingSeverityCountsOnlyEvaluatedFailures(t *testing.T) {
	evaluatedAt := pgtype.Timestamptz{Time: time.Now(), Valid: true}
	rows := []gen.ListFindingRulesetCurrentByRulesetKeyRow{
		{Severity: "critical", CurrentStatus: "fail", CurrentEvaluatedAt: evaluatedAt},
		{Severity: "CAT II", CurrentStatus: "fail", CurrentEvaluatedAt: evaluatedAt},
		{Severity: "medium", CurrentStatus: "pass", CurrentEvaluatedAt: evaluatedAt},
		{Severity: "low", CurrentStatus: "fail"},
	}

	var got viewmodels.DashboardFindingSeverity
	dashboardAccumulateFindingSeverity(&got, rows)
	if got.Evaluated != 3 || got.Open != 2 || got.Critical != 1 || got.High != 1 || got.Medium != 0 {
		t.Fatalf("severity summary = %+v", got)
	}
}

func TestDashboardCredentialsAttentionMatchesEnabledSources(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{TenantID: "tenant-1"})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, false, configstore.GitHubConfig{Org: "acme"})
		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

		criticalID := insertNonHumanCredentialArtifact(t, ctx, pool, entraRunID, nonHumanCredentialArtifactSeed{
			SourceKind: configstore.KindEntra, SourceName: "tenant-1", AssetRefKind: "app_asset", AssetRefExternalID: "app-1", CredentialKind: "entra_client_secret", ExternalID: "critical-1", DisplayName: "Critical secret", Status: "active",
		})
		highID := insertNonHumanCredentialArtifact(t, ctx, pool, entraRunID, nonHumanCredentialArtifactSeed{
			SourceKind: configstore.KindEntra, SourceName: "tenant-1", AssetRefKind: "app_asset", AssetRefExternalID: "app-2", CredentialKind: "entra_certificate", ExternalID: "high-1", DisplayName: "High certificate", Status: "active",
		})
		disabledID := insertNonHumanCredentialArtifact(t, ctx, pool, githubRunID, nonHumanCredentialArtifactSeed{
			SourceKind: configstore.KindGitHub, SourceName: "acme", AssetRefKind: "repository", AssetRefExternalID: "repo-1", CredentialKind: "github_pat", ExternalID: "disabled-critical", DisplayName: "Disabled source token", Status: "active",
		})

		for _, row := range []struct {
			id    int64
			level string
			rank  int
		}{{criticalID, "critical", 4}, {highID, "high", 3}, {disabledID, "critical", 4}} {
			if _, err := pool.Exec(ctx, `
				INSERT INTO credential_artifact_risk_read_models (
					credential_artifact_id, risk_level, risk_rank, risk_signals_json, policy_packs_json, projection_refreshed_at
				) VALUES ($1, $2, $3, '[]'::jsonb, '[]'::jsonb, now())
			`, row.id, row.level, row.rank); err != nil {
				t.Fatalf("insert credential risk read model: %v", err)
			}
		}

		got, err := h.dashboardCredentialsAttention(ctx)
		if err != nil {
			t.Fatalf("dashboardCredentialsAttention(): %v", err)
		}
		if got.Total != 2 || got.Critical != 1 || got.High != 1 {
			t.Fatalf("credential attention = %+v", got)
		}
	})
}

func TestHandleDashboardIgnoresUnconfiguredGoogleConnectedAppsWithoutConfiguredConnector(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		googleRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGoogleWorkspace, "C0123")
		insertCommandSearchAppAsset(t, ctx, q, googleRunID, configstore.KindGoogleWorkspace, "C0123", "google_oauth_client", "client-123.apps.googleusercontent.com", "", "OAuth Approval Client", "active")

		dashboardBody := renderDashboard(t, h, "http://example.com/")
		assertDashboardMetric(t, dashboardBody, "Identities", 0)
		assertDashboardMetric(t, dashboardBody, "Discovered SaaS apps", 0)
		assertDashboardMetric(t, dashboardBody, "App assets", 0)

		appAssetsBody := renderGoogleOAuthAppAssets(t, h, "http://example.com/app-assets?source_kind=google_workspace&asset_kind=google_oauth_client")
		if !strings.Contains(appAssetsBody, "Google Workspace is not configured yet. Add credentials in Connectors.") {
			t.Fatalf("app-assets body missing unconfigured state: %s", appAssetsBody)
		}
	})
}

func TestHandleDashboardCountsAppAssetsFromDisabledConfiguredConnector(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, false, configstore.GitHubConfig{Org: "acme"})

		githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		insertCommandSearchAppAsset(t, ctx, q, githubRunID, configstore.KindGitHub, "acme", "github_app", "github-actions", "", "GitHub Actions", "active")

		body := renderDashboard(t, h, "http://example.com/")
		assertDashboardMetric(t, body, "App assets", 1)
	})
}

func TestHandleDashboardExcludesDisabledIdentitySourcesFromIdentityMetric(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{Domain: "acme.okta.com"})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, false, configstore.GitHubConfig{Org: "disabled-github-org"})

		oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "disabled-github-org")

		oktaAccountID := insertCommandSearchAccount(t, ctx, pool, oktaRunID, commandSearchAccountSeed{
			SourceKind:     configstore.KindOkta,
			SourceName:     "acme.okta.com",
			ExternalID:     "okta-user-1",
			Email:          "alice@example.com",
			DisplayName:    "Alice Okta",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		oktaIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "alice@example.com", "Alice Okta")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, oktaIdentityID, oktaAccountID)

		githubAccountID := insertCommandSearchAccount(t, ctx, pool, githubRunID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "disabled-github-org",
			ExternalID:     "github-user-1",
			Email:          "bob@example.com",
			DisplayName:    "Bob GitHub",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		githubIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "bob@example.com", "Bob GitHub")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, githubIdentityID, githubAccountID)

		body := renderDashboard(t, h, "http://example.com/")
		assertDashboardMetric(t, body, "Identities", 1)
	})
}

func renderDashboard(t *testing.T, h *Handlers, target string) string {
	t.Helper()
	refreshHandlerReadModels(t, h)

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleDashboard(c); err != nil {
		t.Fatalf("HandleDashboard(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderDashboardFragment(t *testing.T, h *Handlers, target string) string {
	t.Helper()
	refreshHandlerReadModels(t, h)

	c, rec := newTestContext(http.MethodGet, target)
	(*c).Request().Header.Set("HX-Request", "true")
	(*c).Request().Header.Set("HX-Target", "dashboard-content")
	if err := h.HandleDashboard(c); err != nil {
		t.Fatalf("HandleDashboard(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderGoogleOAuthAppAssets(t *testing.T, h *Handlers, target string) string {
	t.Helper()
	refreshHandlerReadModels(t, h)

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleAppAssets(c); err != nil {
		t.Fatalf("HandleAppAssets(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func assertDashboardMetric(t *testing.T, body, label string, count int64) {
	t.Helper()

	countStr := strconv.FormatInt(count, 10)
	if !strings.Contains(body, label) {
		t.Fatalf("dashboard missing metric label %q: %s", label, body)
	}
	if !strings.Contains(body, countStr) {
		t.Fatalf("dashboard missing metric count %q for label %q: %s", countStr, label, body)
	}
}

func insertDashboardEntitlement(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, accountID int64, kind, resource, permission, rawJSON string) {
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
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, now(), $6, now(), now())
	`, accountID, kind, resource, permission, rawJSON, runID); err != nil {
		t.Fatalf("insert dashboard entitlement %s/%s: %v", kind, permission, err)
	}
}
