package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
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

func TestHandleConnectorDialogRendersLazyHTMXDialog(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/settings/connectors/okta/dialog", nil)
		req.Header.Set("HX-Request", "true")
		rec := httptest.NewRecorder()
		e := echo.New()
		c := e.NewContext(req, rec)
		c.SetPath("/settings/connectors/:kind/dialog")
		c.SetPathValues(echo.PathValues{{Name: "kind", Value: "okta"}})

		if err := h.HandleConnectorDialog(c); err != nil {
			t.Fatalf("HandleConnectorDialog(): %v", err)
		}
		body := rec.Body.String()
		assertContains(t, body, `id="connector-okta-modal"`)
		assertContains(t, body, `data-remove-on-close`)
		assertContains(t, body, `hx-post="/settings/connectors/okta"`)
		assertNotContains(t, body, `<body`)
	})
}

func TestHandleConnectorSaveHTMXReturnsToastAndOOBPanel(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, rec := newConnectorActionFormContext(http.MethodPost, "http://example.com/settings/connectors/okta", "okta", url.Values{
			"domain":            {"acme.okta.com"},
			"token":             {"token-1"},
			"discovery_enabled": {"true"},
		})
		c.Request().Header.Set("HX-Request", "true")

		if err := h.HandleConnectorAction(c); err != nil {
			t.Fatalf("HandleConnectorAction(save okta htmx): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		trigger := rec.Header().Get("HX-Trigger")
		if !strings.Contains(trigger, `"osspm:toast"`) || !strings.Contains(trigger, `"osspm:connectors-changed"`) {
			t.Fatalf("HX-Trigger = %q, want toast and connectors-changed", trigger)
		}
		body := rec.Body.String()
		assertContains(t, body, `id="connectors-panel"`)
		assertContains(t, body, `hx-swap-oob="outerHTML"`)
		assertNotContains(t, body, `<body`)
	})
}

func TestRenderConnectorsPanelTargetIncludesAlerts(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, rec := newTestContext(http.MethodGet, "http://example.com/settings/connectors")
		c.Request().Header.Set("HX-Request", "true")
		c.Request().Header.Set("HX-Target", "connectors-panel")

		err := h.renderConnectorsPage(c, "", "", &viewmodels.ConnectorAlert{
			Class:   "alert-error",
			Title:   "Connector problem",
			Message: "Credentials need attention.",
		})
		if err != nil {
			t.Fatalf("renderConnectorsPage(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := rec.Body.String()
		assertContains(t, body, `id="connectors-panel"`)
		assertContains(t, body, "Connector problem")
		assertContains(t, body, "Credentials need attention.")
		assertNotContains(t, body, "<!doctype html>")
	})
}

func TestHandleConnectorToggleHTMXInvalidConfigRerendersRow(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, rec := newConnectorActionFormContext(http.MethodPost, "http://example.com/settings/connectors/okta/toggle", "okta/toggle", url.Values{
			"enabled": {"true"},
		})
		c.Request().Header.Set("HX-Request", "true")
		c.Request().Header.Set("HX-Target", "connector-row-okta")

		if err := h.HandleConnectorAction(c); err != nil {
			t.Fatalf("HandleConnectorAction(toggle invalid okta htmx): %v", err)
		}
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
		}
		if got := rec.Header().Get("HX-Retarget"); got != "" {
			t.Fatalf("HX-Retarget = %q, want empty", got)
		}
		if got := rec.Header().Get("HX-Reswap"); got != "" {
			t.Fatalf("HX-Reswap = %q, want empty", got)
		}
		trigger := rec.Header().Get("HX-Trigger")
		if !strings.Contains(trigger, `"osspm:toast"`) {
			t.Fatalf("HX-Trigger = %q, want toast", trigger)
		}
		body := rec.Body.String()
		assertContains(t, body, `id="connector-row-okta"`)
		assertNotContains(t, body, `id="connector-okta-modal"`)
		assertNotContains(t, body, `HX-Redirect`)
	})
}

func TestHandleConnectorAuthoritativeToggleHTMXErrorRerendersRow(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, rec := newConnectorActionFormContext(http.MethodPost, "http://example.com/settings/connectors/okta/authoritative", "okta/authoritative", url.Values{
			"authoritative": {"true"},
		})
		c.Request().Header.Set("HX-Request", "true")
		c.Request().Header.Set("HX-Target", "connector-row-okta")

		if err := h.HandleConnectorAction(c); err != nil {
			t.Fatalf("HandleConnectorAction(authoritative invalid okta htmx): %v", err)
		}
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
		}
		trigger := rec.Header().Get("HX-Trigger")
		if !strings.Contains(trigger, `"osspm:toast"`) {
			t.Fatalf("HX-Trigger = %q, want toast", trigger)
		}
		body := rec.Body.String()
		assertContains(t, body, `id="connector-row-okta"`)
		assertNotContains(t, body, `id="connector-okta-modal"`)
		assertNotContains(t, body, `hx-post="/settings/connectors/okta"`)
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

func TestHandleSettingsUserDeleteDialogDoesNotOpenWhenForbidden(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, _ *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		user, err := q.CreateAuthUser(ctx, gen.CreateAuthUserParams{
			Email:        "admin@example.com",
			PasswordHash: "test-password-hash",
			Role:         auth.RoleAdmin,
			IsActive:     true,
		})
		if err != nil {
			t.Fatalf("CreateAuthUser(): %v", err)
		}

		target := "http://example.com/settings/users/" + strconv.FormatInt(user.ID, 10) + "/delete"
		c, rec := newTestContext(http.MethodGet, target)
		c.SetPath("/settings/users/:id/delete")
		c.SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(user.ID, 10)}})
		c.Set(authn.ContextKeyPrincipal, auth.Principal{UserID: user.ID, Email: user.Email, Role: user.Role})
		c.Request().Header.Set("HX-Request", "true")

		if err := h.HandleSettingsUserDeleteDialog(c); err != nil {
			t.Fatalf("HandleSettingsUserDeleteDialog(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := rec.Body.String()
		assertContains(t, body, "Delete not allowed")
		assertContains(t, body, "You cannot delete your own user.")
		assertNotContains(t, body, `id="settings-users-delete-modal"`)
		assertNotContains(t, body, "Delete user</button>")
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
