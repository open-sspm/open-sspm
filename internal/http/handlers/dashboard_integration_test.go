package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
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

		insertCommandSearchDiscoveryApp(t, ctx, pool, q, entraRunID, configstore.KindEntra, "tenant-1", "azure-cloud", "Azure Cloud", "azure.com", "Microsoft", "managed", "high", 80, "azure-cloud")

		insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_application", "azure-enterprise-app", "", "Azure Enterprise App", "active")
		insertCommandSearchAppAsset(t, ctx, q, githubRunID, configstore.KindGitHub, "acme", "github_app", "github-actions", "", "GitHub Actions", "active")

		insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "legacy-app-1", "Legacy App 1", "legacy-app-1", "active")
		insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "legacy-app-2", "Legacy App 2", "legacy-app-2", "active")
		insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "legacy-app-3", "Legacy App 3", "legacy-app-3", "active")
		if err := q.UpsertIntegrationOktaAppMap(ctx, gen.UpsertIntegrationOktaAppMapParams{
			IntegrationKind:   configstore.KindGitHub,
			OktaAppExternalID: "legacy-app-1",
		}); err != nil {
			t.Fatalf("UpsertIntegrationOktaAppMap: %v", err)
		}

		body := renderDashboard(t, h, "http://example.com/")
		assertDashboardMetric(t, body, "Identities", 2)
		assertDashboardMetric(t, body, "Discovered SaaS apps", 1)
		assertDashboardMetric(t, body, "App assets", 2)

		if strings.Contains(body, "Active users") {
			t.Fatalf("dashboard unexpectedly rendered old active users label: %s", body)
		}
		if strings.Contains(body, ">Apps</span>") {
			t.Fatalf("dashboard unexpectedly rendered old apps label: %s", body)
		}
		if strings.Contains(body, "Connected apps") {
			t.Fatalf("dashboard unexpectedly rendered old connected apps label: %s", body)
		}
	})
}

func TestHandleDashboardIgnoresLegacyGoogleConnectedAppsWithoutConfiguredConnector(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		googleRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGoogleWorkspace, "C0123")
		insertCommandSearchAppAsset(t, ctx, q, googleRunID, configstore.KindGoogleWorkspace, "C0123", "google_oauth_client", "client-123.apps.googleusercontent.com", "", "OAuth Approval Client", "active")

		dashboardBody := renderDashboard(t, h, "http://example.com/")
		assertDashboardMetric(t, dashboardBody, "Identities", 0)
		assertDashboardMetric(t, dashboardBody, "Discovered SaaS apps", 0)
		assertDashboardMetric(t, dashboardBody, "App assets", 0)
		if strings.Contains(dashboardBody, "Connected apps") {
			t.Fatalf("dashboard unexpectedly rendered old connected apps label: %s", dashboardBody)
		}

		connectedAppsBody := renderConnectedApps(t, h, "http://example.com/connected-apps")
		if !strings.Contains(connectedAppsBody, "Google Workspace is not configured yet. Add credentials in Connectors.") {
			t.Fatalf("connected-apps body missing unconfigured state: %s", connectedAppsBody)
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

func renderDashboard(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleDashboard(c); err != nil {
		t.Fatalf("HandleDashboard(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderConnectedApps(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleConnectedApps(c); err != nil {
		t.Fatalf("HandleConnectedApps(%s): %v", target, err)
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
