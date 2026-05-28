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

func TestHandleAppsTrimsStatusFilterMatches(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		insertCommandSearchOktaApp(t, ctx, q, runID, "active-app", "Whitespace Active App", "whitespace-active", " active ")
		insertCommandSearchOktaApp(t, ctx, q, runID, "inactive-app", "Inactive App", "inactive-app", "inactive")

		c, rec := newTestContext(http.MethodGet, "http://example.com/assigned-apps?status=ACTIVE")

		if err := h.HandleApps(c); err != nil {
			t.Fatalf("HandleApps(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Whitespace Active App")
		assertNotContains(t, body, "Inactive App")
		assertContains(t, body, `href="/assigned-apps?status=ACTIVE"`)
	})
}

func TestHandleOktaAppShowRedirectsIntegratedApps(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		insertCommandSearchOktaApp(t, ctx, q, runID, "github-sso", "GitHub SSO", "GitHub SSO", "active")

		if err := q.UpsertIntegrationOktaAppMap(ctx, gen.UpsertIntegrationOktaAppMapParams{
			IntegrationKind:   configstore.KindGitHub,
			OktaSourceKind:    configstore.KindOkta,
			OktaSourceName:    "example.okta.com",
			OktaAppExternalID: "github-sso",
		}); err != nil {
			t.Fatalf("UpsertIntegrationOktaAppMap(): %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/assigned-apps/example.okta.com/github-sso")
		(*c).SetPath("/assigned-apps/:sourceName/:externalID")
		(*c).SetPathValues(echo.PathValues{
			{Name: "sourceName", Value: "example.okta.com"},
			{Name: "externalID", Value: "github-sso"},
		})

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

func TestHandleOktaAppShowReadsAssignmentsFromGenericEntitlements(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "example.okta.com")
		insertCommandSearchOktaApp(t, ctx, q, runID, "payroll-app", "Payroll", "payroll", "active")
		userID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     "okta",
			SourceName:     "example.okta.com",
			ExternalID:     "00u-alice",
			Email:          "alice@example.com",
			DisplayName:    "Alice Example",
			Status:         "ACTIVE",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"ACTIVE"}`,
		})
		groupID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     "okta",
			SourceName:     "example.okta.com",
			ExternalID:     "group:00g-eng",
			DisplayName:    "Engineering",
			Status:         "ACTIVE",
			AccountKind:    "service",
			EntityCategory: "group",
			RawJSON:        `{"entity_category":"group"}`,
		})
		insertDashboardEntitlement(t, ctx, pool, runID, userID, "application_assignment", "payroll-app", "GROUP", `{"attributes":{"profile":{"appUserName":"alice.payroll"}}}`)
		insertDashboardEntitlement(t, ctx, pool, runID, userID, "group_membership", "group:00g-eng", "member", `{"attributes":{"target":{"external_id":"00g-eng","display_name":"Engineering"}}}`)
		insertDashboardEntitlement(t, ctx, pool, runID, groupID, "application_assignment", "payroll-app", "1", `{"attributes":{"target":{"external_id":"payroll-app","display_name":"Payroll"}}}`)

		c, rec := newTestContext(http.MethodGet, "http://example.com/assigned-apps/example.okta.com/payroll-app")
		(*c).SetPath("/assigned-apps/:sourceName/:externalID")
		(*c).SetPathValues(echo.PathValues{
			{Name: "sourceName", Value: "example.okta.com"},
			{Name: "externalID", Value: "payroll-app"},
		})

		if err := h.HandleOktaAppShow(c); err != nil {
			t.Fatalf("HandleOktaAppShow(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Payroll")
		assertContains(t, body, "Alice Example")
		assertContains(t, body, "alice@example.com")
		assertContains(t, body, "Engineering")
		assertContains(t, body, "appUserName: alice.payroll")
		assertNotContains(t, body, "No Okta users are assigned to this app.")
	})
}

func TestHandleOktaAppShowRejectsNestedExternalIDs(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/assigned-apps/example.okta.com/nested/app")
	(*c).SetPath("/assigned-apps/:sourceName/:externalID")
	(*c).SetPathValues(echo.PathValues{
		{Name: "sourceName", Value: "example.okta.com"},
		{Name: "externalID", Value: "nested/app"},
	})

	h := &Handlers{}
	if err := h.HandleOktaAppShow(c); err != nil {
		t.Fatalf("HandleOktaAppShow(): %v", err)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
