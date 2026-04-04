package handlers

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
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
		assertNotContains(t, body, "Asset 01")
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

		if _, err := q.UpsertConnectedAppGovernance(ctx, gen.UpsertConnectedAppGovernanceParams{
			AppAssetID:  needsReviewID,
			ReviewState: "needs_revocation",
		}); err != nil {
			t.Fatalf("UpsertConnectedAppGovernance: %v", err)
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
		assertContains(t, body, "/app-assets/"+fmt.Sprint(needsReviewID))
		assertNotContains(t, body, "/app-assets/"+fmt.Sprint(unreviewedID))
		assertContains(t, body, "Needs revocation")
	})
}
