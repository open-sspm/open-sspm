package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/alexedwards/scs/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func newAuthHandlerWithSessionContext(t *testing.T, c *echo.Context) *Handlers {
	t.Helper()

	sessions := scs.New()
	sessionCtx, err := sessions.Load(c.Request().Context(), "")
	if err != nil {
		t.Fatalf("sessions.Load() error = %v", err)
	}
	c.SetRequest(c.Request().WithContext(sessionCtx))

	return &Handlers{Sessions: sessions}
}

func TestHandleLogoutPostRedirectsNormallyForNonHTMX(t *testing.T) {
	c, rec := newTestContext(http.MethodPost, "http://example.com/logout")
	h := newAuthHandlerWithSessionContext(t, c)

	if err := h.HandleLogoutPost(c); err != nil {
		t.Fatalf("HandleLogoutPost() error = %v", err)
	}

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusSeeOther)
	}
	if got := rec.Header().Get("Location"); got != "/login" {
		t.Fatalf("Location = %q, want %q", got, "/login")
	}

	vary := parseVaryHeader(rec.Header().Get("Vary"))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
}

func TestHandleLogoutPostUsesHXRedirectForHTMX(t *testing.T) {
	c, rec := newTestContext(http.MethodPost, "http://example.com/logout")
	c.Request().Header.Set("HX-Request", "true")
	h := newAuthHandlerWithSessionContext(t, c)

	if err := h.HandleLogoutPost(c); err != nil {
		t.Fatalf("HandleLogoutPost() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("HX-Redirect"); got != "/login" {
		t.Fatalf("HX-Redirect = %q, want %q", got, "/login")
	}
	if got := rec.Header().Get("HX-Trigger"); got != "" {
		t.Fatalf("HX-Trigger = %q, want empty so flash survives redirect", got)
	}
	if !hasSetCookie(rec, flashToastCookieName) {
		t.Fatalf("Set-Cookie missing %s: %v", flashToastCookieName, rec.Header().Values(echo.HeaderSetCookie))
	}

	vary := parseVaryHeader(rec.Header().Get("Vary"))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
}

func TestHandleLogoutPostUsesHXLocationForBoostedHTMX(t *testing.T) {
	c, rec := newTestContext(http.MethodPost, "http://example.com/logout")
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Boosted", "true")
	h := newAuthHandlerWithSessionContext(t, c)

	if err := h.HandleLogoutPost(c); err != nil {
		t.Fatalf("HandleLogoutPost() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("HX-Location"); got != "/login" {
		t.Fatalf("HX-Location = %q, want %q", got, "/login")
	}
	if got := rec.Header().Get("HX-Redirect"); got != "" {
		t.Fatalf("HX-Redirect = %q, want empty for boosted request", got)
	}
	if got := rec.Header().Get("HX-Trigger"); got != "" {
		t.Fatalf("HX-Trigger = %q, want empty so flash survives navigation", got)
	}
	if !hasSetCookie(rec, flashToastCookieName) {
		t.Fatalf("Set-Cookie missing %s: %v", flashToastCookieName, rec.Header().Values(echo.HeaderSetCookie))
	}
}

func TestHandleLoginGetPublicLayoutIncludesCSRFMeta(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, rec := newTestContext(http.MethodGet, "http://example.com/login")
		c.Set(middleware.DefaultCSRFConfig.ContextKey, "csrf-public")
		h.Sessions = scs.New()
		sessionCtx, err := h.Sessions.Load(c.Request().Context(), "")
		if err != nil {
			t.Fatalf("sessions.Load() error = %v", err)
		}
		c.SetRequest(c.Request().WithContext(sessionCtx))

		if err := h.HandleLoginGet(c); err != nil {
			t.Fatalf("HandleLoginGet() error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		assertContains(t, rec.Body.String(), `<meta name="csrf-token" content="csrf-public">`)
	})
}

func TestHandleLoginPostHTMXInvalidCredentialsSwapsForm(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, _ *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		if _, err := q.CreateAuthUser(ctx, gen.CreateAuthUserParams{
			Email:        "admin@example.com",
			PasswordHash: "unused-for-empty-password",
			Role:         "admin",
			IsActive:     true,
		}); err != nil {
			t.Fatalf("CreateAuthUser(): %v", err)
		}

		values := url.Values{"email": {"admin@example.com"}, "password": {""}}
		req := httptest.NewRequest(http.MethodPost, "http://example.com/login", strings.NewReader(values.Encode()))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
		req.Header.Set("HX-Request", "true")
		rec := httptest.NewRecorder()
		e := echo.New()
		c := e.NewContext(req, rec)
		c.Set(middleware.DefaultCSRFConfig.ContextKey, "csrf-invalid")
		h.Sessions = scs.New()
		sessionCtx, err := h.Sessions.Load(c.Request().Context(), "")
		if err != nil {
			t.Fatalf("sessions.Load() error = %v", err)
		}
		c.SetRequest(c.Request().WithContext(sessionCtx))

		if err := h.HandleLoginPost(c); err != nil {
			t.Fatalf("HandleLoginPost() error = %v", err)
		}
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusUnauthorized, rec.Body.String())
		}
		assertContains(t, rec.Header().Get(echo.HeaderContentType), "text/html")
		body := rec.Body.String()
		assertContains(t, body, `id="login-form-shell"`)
		assertContains(t, body, "Invalid email or password.")
		assertNotContains(t, body, "<!doctype html>")
	})
}

func hasSetCookie(rec *httptest.ResponseRecorder, name string) bool {
	prefix := name + "="
	for _, cookie := range rec.Header().Values(echo.HeaderSetCookie) {
		if strings.HasPrefix(cookie, prefix) {
			return true
		}
	}
	return false
}
