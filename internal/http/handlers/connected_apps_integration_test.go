package handlers

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleConnectedAppShowUsesLiveDiscoveryPosture(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGoogleWorkspace, true, configstore.GoogleWorkspaceConfig{
			CustomerID:          "C0123",
			DelegatedAdminEmail: "admin@example.com",
			AuthType:            configstore.GoogleWorkspaceAuthTypeADC,
			ServiceAccountEmail: "svc@example.com",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGoogleWorkspace, "C0123")
		appID := insertCommandSearchAppAsset(t, ctx, q, runID, configstore.KindGoogleWorkspace, "C0123", connectedAppAssetKindGoogle, "client-123.apps.googleusercontent.com", "", "OAuth Approval Client", "active")
		insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			runID,
			configstore.KindGoogleWorkspace,
			"C0123",
			"orphaned-client",
			"Orphaned Client",
			"example.com",
			"Example",
			"client-123.apps.googleusercontent.com",
		)

		body := renderGoogleOAuthAppAssetShow(t, h, appID)
		assertContains(t, body, "Orphaned Client")
		assertContains(t, body, "Unmanaged")
		assertContains(t, body, "High")
	})
}

func TestHandleConnectedAppsRedirectNormalizesLegacyReviewState(t *testing.T) {
	h := &Handlers{}

	c, rec := newTestContext(http.MethodGet, "http://example.com/oauth-apps?review_state=needs_revocation&page=2&q=drive")

	if err := h.HandleConnectedApps(c); err != nil {
		t.Fatalf("HandleConnectedApps(): %v", err)
	}
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
	}
	if got := rec.Header().Get(echo.HeaderLocation); got != "/app-assets?asset_kind=google_oauth_client&governance_state=action_required&page=2&q=drive&source_kind=google_workspace" {
		t.Fatalf("location = %q", got)
	}
}

func TestHandleConnectedAppShowRedirectsToCanonicalAppAsset(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGoogleWorkspace, "C0123")
		appID := insertCommandSearchAppAsset(t, ctx, q, runID, configstore.KindGoogleWorkspace, "C0123", connectedAppAssetKindGoogle, "client-123.apps.googleusercontent.com", "", "OAuth Approval Client", "active")

		target := "http://example.com/oauth-apps/" + strconv.FormatInt(appID, 10)
		c, rec := newTestContext(http.MethodGet, target)
		(*c).SetPath("/oauth-apps/:id")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(appID, 10)}})

		if err := h.HandleConnectedAppShow(c); err != nil {
			t.Fatalf("HandleConnectedAppShow(%d): %v", appID, err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
		}
		if got := rec.Header().Get(echo.HeaderLocation); got != "/app-assets/"+strconv.FormatInt(appID, 10) {
			t.Fatalf("location = %q, want %q", got, "/app-assets/"+strconv.FormatInt(appID, 10))
		}
	})
}

func renderGoogleOAuthAppAssetShow(t *testing.T, h *Handlers, appID int64) string {
	t.Helper()
	refreshHandlerReadModels(t, h)

	target := "http://example.com/app-assets/" + strconv.FormatInt(appID, 10)
	c, rec := newTestContext(http.MethodGet, target)
	(*c).SetPath("/app-assets/:id")
	(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(appID, 10)}})

	if err := h.HandleAppAssetShow(c); err != nil {
		t.Fatalf("HandleAppAssetShow(%d): %v", appID, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}
