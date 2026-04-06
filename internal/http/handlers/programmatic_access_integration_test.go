package handlers

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleAppAssetsPaginatesAcrossConfiguredSources(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

		for i := 1; i <= 11; i++ {
			insertCommandSearchAppAsset(
				t,
				ctx,
				q,
				entraRunID,
				configstore.KindEntra,
				"tenant-1",
				"entra_application",
				fmt.Sprintf("entra-asset-%02d", i),
				"",
				fmt.Sprintf("Asset %02d", i),
				"active",
			)
		}
		for i := 12; i <= 21; i++ {
			insertCommandSearchAppAsset(
				t,
				ctx,
				q,
				githubRunID,
				configstore.KindGitHub,
				"acme",
				"github_app",
				fmt.Sprintf("github-asset-%02d", i),
				"",
				fmt.Sprintf("Asset %02d", i),
				"active",
			)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/app-assets?page=2")

		if err := h.HandleAppAssets(c); err != nil {
			t.Fatalf("HandleAppAssets(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Asset 21")
		assertContains(t, body, "Page 2 of 2")
		assertContains(t, body, "Showing 21-21 of 21")
		assertContains(t, body, `data-busy-inline-indicator`)
		assertContains(t, body, `data-enter-only-query="q"`)
		assertContains(t, body, `hx-get="/app-assets?page=1"`)
		assertContains(t, body, `hx-trigger="change delay:150ms from:select, submit"`)
		assertNotContains(t, body, "Asset 01")
		assertNotContains(t, body, `input changed delay:300ms from:input[name='q']`)
	})
}

func TestHandleAppAssetsRendersGoogleOAuthSlice(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGoogleWorkspace, true, configstore.GoogleWorkspaceConfig{
			CustomerID:          "C0123",
			DelegatedAdminEmail: "admin@example.com",
			AuthType:            configstore.GoogleWorkspaceAuthTypeADC,
			ServiceAccountEmail: "svc@example.com",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGoogleWorkspace, "C0123")
		needsReviewID := insertCommandSearchAppAsset(t, ctx, q, runID, configstore.KindGoogleWorkspace, "C0123", connectedAppAssetKindGoogle, "client-123.apps.googleusercontent.com", "", "OAuth Approval Client", "active")
		unreviewedID := insertCommandSearchAppAsset(t, ctx, q, runID, configstore.KindGoogleWorkspace, "C0123", connectedAppAssetKindGoogle, "client-456.apps.googleusercontent.com", "", "Shadow OAuth Client", "active")

		if _, err := q.UpsertAppAssetGovernance(ctx, gen.UpsertAppAssetGovernanceParams{
			AppAssetID:      needsReviewID,
			GovernanceState: "action_required",
		}); err != nil {
			t.Fatalf("UpsertAppAssetGovernance: %v", err)
		}

		target := "http://example.com/app-assets?source_kind=google_workspace&asset_kind=google_oauth_client&governance_state=action_required"
		c, rec := newTestContext(http.MethodGet, target)

		if err := h.HandleAppAssets(c); err != nil {
			t.Fatalf("HandleAppAssets(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "OAuth Apps")
		assertContains(t, body, "/app-assets/"+fmt.Sprint(needsReviewID))
		assertNotContains(t, body, "/app-assets/"+fmt.Sprint(unreviewedID))
		assertContains(t, body, "Action Required")
		assertContains(t, body, `data-busy-inline-indicator`)
		assertContains(t, body, `data-enter-only-query="q"`)
		assertContains(t, body, `hx-get="/app-assets?asset_kind=google_oauth_client&amp;governance_state=action_required&amp;source_kind=google_workspace&amp;page=1"`)
		assertContains(t, body, `name="source_kind" value="google_workspace"`)
		assertContains(t, body, `name="asset_kind" value="google_oauth_client"`)
	})
}

func TestHandleAppAssetsRendersGoogleOAuthSliceForLegacyReviewStateFilter(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGoogleWorkspace, true, configstore.GoogleWorkspaceConfig{
			CustomerID:          "C0123",
			DelegatedAdminEmail: "admin@example.com",
			AuthType:            configstore.GoogleWorkspaceAuthTypeADC,
			ServiceAccountEmail: "svc@example.com",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGoogleWorkspace, "C0123")
		actionRequiredID := insertCommandSearchAppAsset(t, ctx, q, runID, configstore.KindGoogleWorkspace, "C0123", connectedAppAssetKindGoogle, "client-123.apps.googleusercontent.com", "", "OAuth Approval Client", "active")
		unreviewedID := insertCommandSearchAppAsset(t, ctx, q, runID, configstore.KindGoogleWorkspace, "C0123", connectedAppAssetKindGoogle, "client-456.apps.googleusercontent.com", "", "Shadow OAuth Client", "active")

		if _, err := q.UpsertAppAssetGovernance(ctx, gen.UpsertAppAssetGovernanceParams{
			AppAssetID:      actionRequiredID,
			GovernanceState: "action_required",
		}); err != nil {
			t.Fatalf("UpsertAppAssetGovernance: %v", err)
		}

		target := "http://example.com/app-assets?source_kind=google_workspace&asset_kind=google_oauth_client&review_state=needs_revocation"
		c, rec := newTestContext(http.MethodGet, target)

		if err := h.HandleAppAssets(c); err != nil {
			t.Fatalf("HandleAppAssets(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "OAuth Apps")
		assertContains(t, body, "/app-assets/"+fmt.Sprint(actionRequiredID))
		assertNotContains(t, body, "/app-assets/"+fmt.Sprint(unreviewedID))
		assertContains(t, body, "Action Required")
		assertContains(t, body, `hx-get="/app-assets?asset_kind=google_oauth_client&amp;governance_state=action_required&amp;source_kind=google_workspace&amp;page=1"`)
		assertNotContains(t, body, "review_state=needs_revocation")
	})
}

func TestHandleAppAssetsRendersGoogleOAuthSliceWhenGoogleWorkspaceUnavailable(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, rec := newTestContext(http.MethodGet, "http://example.com/app-assets?source_kind=google_workspace&asset_kind=google_oauth_client")

		if err := h.HandleAppAssets(c); err != nil {
			t.Fatalf("HandleAppAssets(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "OAuth Apps")
		assertContains(t, body, connectorUnavailableMessage("Google Workspace", false, false))
		assertNotContains(t, body, "Configure and enable GitHub, Google Workspace, Microsoft Entra, or Vault connectors to populate app assets.")
	})
}

func TestHandleConnectedAppsRedirectPreservesUnavailableOAuthSlice(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, rec := newTestContext(http.MethodGet, "http://example.com/oauth-apps")

		if err := h.HandleConnectedApps(c); err != nil {
			t.Fatalf("HandleConnectedApps(): %v", err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
		}

		location := rec.Header().Get(echo.HeaderLocation)
		if location != "/app-assets?asset_kind=google_oauth_client&source_kind=google_workspace" {
			t.Fatalf("location = %q", location)
		}

		follow, followRec := newTestContext(http.MethodGet, "http://example.com"+location)
		if err := h.HandleAppAssets(follow); err != nil {
			t.Fatalf("HandleAppAssets(): %v", err)
		}
		if followRec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", followRec.Code, http.StatusOK, followRec.Body.String())
		}

		body := followRec.Body.String()
		assertContains(t, body, "OAuth Apps")
		assertContains(t, body, connectorUnavailableMessage("Google Workspace", false, false))
		assertNotContains(t, body, "Configure and enable GitHub, Google Workspace, Microsoft Entra, or Vault connectors to populate app assets.")
	})
}

func TestHandleConnectedAppsRedirectCanonicalizesLegacyReviewState(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, rec := newTestContext(http.MethodGet, "http://example.com/oauth-apps?review_state=under_review")

		if err := h.HandleConnectedApps(c); err != nil {
			t.Fatalf("HandleConnectedApps(): %v", err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
		}

		location := rec.Header().Get(echo.HeaderLocation)
		if location != "/app-assets?asset_kind=google_oauth_client&governance_state=in_review&source_kind=google_workspace" {
			t.Fatalf("location = %q", location)
		}
	})
}

func TestHandleCredentialsUsesSubmitOnlySearchTrigger(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		c, rec := newTestContext(http.MethodGet, "http://example.com/credentials")

		if err := h.HandleCredentials(c); err != nil {
			t.Fatalf("HandleCredentials(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, `data-busy-inline-indicator`)
		assertContains(t, body, `data-enter-only-query="q"`)
		assertContains(t, body, `hx-trigger="change delay:150ms from:select, submit"`)
		assertNotContains(t, body, `input changed delay:300ms from:input[name='q']`)
	})
}

func TestHandleCredentialsHTMXReturnsResultsShellOnly(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		c, rec := newTestContext(http.MethodGet, "http://example.com/credentials")
		(*c).Request().Header.Set("HX-Request", "true")
		(*c).Request().Header.Set("HX-Target", "credentials-results")

		if err := h.HandleCredentials(c); err != nil {
			t.Fatalf("HandleCredentials(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, `id="credentials-results"`)
		assertNotContains(t, body, "<!doctype html>")
	})
}
