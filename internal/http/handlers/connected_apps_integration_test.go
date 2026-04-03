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

		body := renderConnectedAppShow(t, h, appID)
		assertContains(t, body, "Orphaned Client")
		assertContains(t, body, "Unmanaged")
		assertContains(t, body, "High")
	})
}

func renderConnectedAppShow(t *testing.T, h *Handlers, appID int64) string {
	t.Helper()

	target := "http://example.com/oauth-apps/" + strconv.FormatInt(appID, 10)
	c, rec := newTestContext(http.MethodGet, target)
	(*c).SetPath("/oauth-apps/:id")
	(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(appID, 10)}})

	if err := h.HandleConnectedAppShow(c); err != nil {
		t.Fatalf("HandleConnectedAppShow(%d): %v", appID, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}
