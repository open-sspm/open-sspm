package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleConnectorSaveRefreshesSourceStateAndDiscoveryVisibility(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			runID,
			configstore.KindOkta,
			"acme.okta.com",
			"visible-after-save",
			"Visible After Save",
			"visible.example.com",
			"Example",
			"visible-after-save",
		)

		body := renderDiscoveryApps(t, h, "http://example.com/discovery/apps")
		assertNotContains(t, body, "Visible After Save")

		c, rec := newConnectorActionFormContext(http.MethodPost, "http://example.com/settings/connectors/okta", "okta", url.Values{
			"domain":            {"acme.okta.com"},
			"token":             {"token-1"},
			"discovery_enabled": {"true"},
		})

		if err := h.HandleConnectorAction(c); err != nil {
			t.Fatalf("HandleConnectorAction(save okta): %v", err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
		}

		var enabled, configured, discoveryEnabled bool
		if err := pool.QueryRow(ctx, `
			SELECT enabled, configured, discovery_enabled
			FROM connector_source_state
			WHERE source_kind = 'okta' AND source_name = 'acme.okta.com'
		`).Scan(&enabled, &configured, &discoveryEnabled); err != nil {
			t.Fatalf("select connector_source_state: %v", err)
		}
		if !enabled || !configured || !discoveryEnabled {
			t.Fatalf("connector_source_state = enabled=%v configured=%v discovery_enabled=%v", enabled, configured, discoveryEnabled)
		}

		body = renderDiscoveryApps(t, h, "http://example.com/discovery/apps")
		assertContains(t, body, "Visible After Save")
	})
}

func TestHandleConnectorToggleRefreshesManagedStateThroughSourceState(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain:           "acme.okta.com",
			Token:            "token-1",
			DiscoveryEnabled: true,
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "github-token",
		})

		oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		_ = insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

		appID := insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			oktaRunID,
			configstore.KindOkta,
			"acme.okta.com",
			"github-bound-app",
			"GitHub Bound App",
			"github.com",
			"GitHub",
			"github-bound-app",
		)
		upsertDiscoveryPrimaryBinding(t, ctx, q, appID, configstore.KindGitHub, "acme")

		before := getDiscoveryAppByIDForTest(t, ctx, q, h, appID)
		if before.ManagedState != "managed" || before.ManagedReason != "active_binding_fresh_sync" {
			t.Fatalf("before toggle posture = (%q, %q)", before.ManagedState, before.ManagedReason)
		}

		c, rec := newConnectorActionFormContext(http.MethodPost, "http://example.com/settings/connectors/github/toggle", "github/toggle", url.Values{
			"enabled": {"false"},
		})

		if err := h.HandleConnectorAction(c); err != nil {
			t.Fatalf("HandleConnectorAction(toggle github): %v", err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
		}

		var enabled bool
		if err := pool.QueryRow(ctx, `
			SELECT enabled
			FROM connector_source_state
			WHERE source_kind = 'github' AND source_name = 'acme'
		`).Scan(&enabled); err != nil {
			t.Fatalf("select connector_source_state github: %v", err)
		}
		if enabled {
			t.Fatalf("connector_source_state enabled = true, want false")
		}

		after := getDiscoveryAppByIDForTest(t, ctx, q, h, appID)
		if after.ManagedState != "unmanaged" || after.ManagedReason != "connector_disabled" {
			t.Fatalf("after toggle posture = (%q, %q)", after.ManagedState, after.ManagedReason)
		}
	})
}

func newConnectorActionFormContext(method, target, wildcard string, values url.Values) (*echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(method, target, strings.NewReader(values.Encode()))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	c.SetPath("/settings/connectors/*")
	c.SetPathValues(echo.PathValues{{Name: "*", Value: wildcard}})
	return c, rec
}
