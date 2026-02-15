package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func TestHandleIdpUserAccessTreeInvalidID(t *testing.T) {
	t.Run("non htmx request returns bad request text", func(t *testing.T) {
		c, rec := newTestContext(http.MethodGet, "http://example.com/api/idp-users/not-a-number/access-tree")
		c.SetPathValues(echo.PathValues{{Name: "id", Value: "not-a-number"}})

		h := &Handlers{}
		if err := h.HandleIdpUserAccessTree(c); err != nil {
			t.Fatalf("HandleIdpUserAccessTree() error = %v", err)
		}

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if !strings.Contains(rec.Body.String(), "invalid idp user id") {
			t.Fatalf("body = %q, want invalid id message", rec.Body.String())
		}
		if vary := parseVaryHeader(rec.Header().Get(echo.HeaderVary)); vary["hx-request"] != 1 {
			t.Fatalf("Vary header missing hx-request: %v", vary)
		}
	})

	t.Run("htmx request returns html fragment", func(t *testing.T) {
		c, rec := newTestContext(http.MethodGet, "http://example.com/api/idp-users/not-a-number/access-tree")
		c.SetPathValues(echo.PathValues{{Name: "id", Value: "not-a-number"}})
		c.Request().Header.Set("HX-Request", "true")

		h := &Handlers{}
		if err := h.HandleIdpUserAccessTree(c); err != nil {
			t.Fatalf("HandleIdpUserAccessTree() error = %v", err)
		}

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if !strings.Contains(rec.Body.String(), "invalid idp user id") {
			t.Fatalf("body = %q, want invalid id message", rec.Body.String())
		}
		if got := rec.Header().Get(echo.HeaderContentType); !strings.Contains(got, "text/html") {
			t.Fatalf("content-type = %q, want html", got)
		}
		if vary := parseVaryHeader(rec.Header().Get(echo.HeaderVary)); vary["hx-request"] != 1 {
			t.Fatalf("Vary header missing hx-request: %v", vary)
		}
	})
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
