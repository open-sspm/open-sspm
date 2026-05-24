package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a-h/templ"
	"github.com/labstack/echo/v5"
)

func newTestContext(method, target string) (*echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	return c, rec
}

func parseVaryHeader(value string) map[string]int {
	parts := strings.Split(value, ",")
	out := make(map[string]int, len(parts))
	for _, part := range parts {
		token := strings.ToLower(strings.TrimSpace(part))
		if token == "" {
			continue
		}
		out[token]++
	}
	return out
}

func staticTestComponent(body string) templ.Component {
	return templ.ComponentFunc(func(_ context.Context, w io.Writer) error {
		_, err := io.WriteString(w, body)
		return err
	})
}

func TestAddVary(t *testing.T) {
	c, _ := newTestContext(http.MethodGet, "http://example.com/")
	c.Response().Header().Set(echo.HeaderVary, "Accept-Encoding")

	addVary(c, "HX-Request", "hx-target", "Accept-Encoding")

	got := parseVaryHeader(c.Response().Header().Get(echo.HeaderVary))
	if got["accept-encoding"] != 1 {
		t.Fatalf("Vary missing accept-encoding: %v", got)
	}
	if got["hx-request"] != 1 {
		t.Fatalf("Vary missing hx-request: %v", got)
	}
	if got["hx-target"] != 1 {
		t.Fatalf("Vary missing hx-target: %v", got)
	}
}

func TestAddVaryPreservesWildcard(t *testing.T) {
	c, _ := newTestContext(http.MethodGet, "http://example.com/")
	c.Response().Header().Set(echo.HeaderVary, "*")

	addVary(c, "HX-Request")

	if got := c.Response().Header().Get(echo.HeaderVary); got != "*" {
		t.Fatalf("Vary = %q, want *", got)
	}
}

func TestRenderListWithHXUsesFullPageForNonHTMX(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/accounts/okta")
	h := &Handlers{}

	err := h.renderListWithHX(c, "okta-accounts-results", staticTestComponent("fragment"), staticTestComponent("full"))
	if err != nil {
		t.Fatalf("renderListWithHX() error = %v", err)
	}

	if got := rec.Body.String(); got != "full" {
		t.Fatalf("body = %q, want full", got)
	}
}

func TestRenderListWithHXRendersFragmentForMatchingTarget(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/accounts/okta")
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Target", "okta-accounts-results")
	h := &Handlers{}

	err := h.renderListWithHX(c, "okta-accounts-results", staticTestComponent("fragment"), staticTestComponent("full"))
	if err != nil {
		t.Fatalf("renderListWithHX() error = %v", err)
	}

	if got := rec.Body.String(); got != "fragment" {
		t.Fatalf("body = %q, want fragment", got)
	}
}

func TestRenderListWithHXRejectsMismatchedTarget(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/accounts/okta")
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Target", "other-results")
	h := &Handlers{}

	err := h.renderListWithHX(c, "okta-accounts-results", staticTestComponent("fragment"), staticTestComponent("full"))
	if err != nil {
		t.Fatalf("renderListWithHX() error = %v", err)
	}

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if body := rec.Body.String(); strings.Contains(body, "full") || strings.Contains(body, "fragment") {
		t.Fatalf("body rendered page content for mismatched target: %q", body)
	}
}

func TestRenderListWithHXReturnsFullPageForBoostedRequest(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/identity-resolution")
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Boosted", "true")
	h := &Handlers{}

	err := h.renderListWithHX(c, "identity-resolution-results", staticTestComponent("fragment"), staticTestComponent("full"))
	if err != nil {
		t.Fatalf("renderListWithHX() error = %v", err)
	}

	if got := rec.Body.String(); got != "full" {
		t.Fatalf("body = %q, want full", got)
	}
}

func TestHandleFindingsRulesetAddsVaryForHTMXVariants(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/findings/rulesets/")
	c.SetPathValues(echo.PathValues{{Name: "rulesetKey", Value: ""}})

	h := &Handlers{}
	if err := h.HandleFindingsRuleset(c); err != nil {
		t.Fatalf("HandleFindingsRuleset() error = %v", err)
	}

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	vary := parseVaryHeader(rec.Header().Get(echo.HeaderVary))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
	if vary["hx-target"] != 1 {
		t.Fatalf("Vary header missing hx-target: %v", vary)
	}
}
