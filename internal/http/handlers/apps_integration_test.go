package handlers

import (
	"context"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleOktaAppShowRedirectsIntegratedApps(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		insertCommandSearchOktaApp(t, ctx, q, runID, "github-sso", "GitHub SSO", "GitHub SSO", "active")

		if err := q.UpsertIntegrationOktaAppMap(ctx, gen.UpsertIntegrationOktaAppMapParams{
			IntegrationKind:   configstore.KindGitHub,
			OktaAppExternalID: "github-sso",
		}); err != nil {
			t.Fatalf("UpsertIntegrationOktaAppMap(): %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/assigned-apps/github-sso")
		(*c).SetPath("/assigned-apps/:externalID")
		(*c).SetPathValues(echo.PathValues{{Name: "externalID", Value: "github-sso"}})

		if err := h.HandleOktaAppShow(c); err != nil {
			t.Fatalf("HandleOktaAppShow(): %v", err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusSeeOther)
		}
		if location := rec.Header().Get(echo.HeaderLocation); location != "/accounts/github" {
			t.Fatalf("location = %q, want %q", location, "/accounts/github")
		}
	})
}

func TestHandleOktaAppShowRejectsNestedExternalIDs(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/assigned-apps/nested/app")
	(*c).SetPath("/assigned-apps/:externalID")
	(*c).SetPathValues(echo.PathValues{{Name: "externalID", Value: "nested/app"}})

	h := &Handlers{}
	if err := h.HandleOktaAppShow(c); err != nil {
		t.Fatalf("HandleOktaAppShow(): %v", err)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
