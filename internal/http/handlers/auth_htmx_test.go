package handlers

import (
	"net/http"
	"testing"

	"github.com/alexedwards/scs/v2"
	"github.com/labstack/echo/v5"
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

	vary := parseVaryHeader(rec.Header().Get("Vary"))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
}
