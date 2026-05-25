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
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
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

func TestSetResponseToastUsesHXTriggerForHTMXRequests(t *testing.T) {
	c, _ := newTestContext(http.MethodPost, "http://example.com/settings/users")
	c.Request().Header.Set("HX-Request", "true")

	setResponseToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Saved",
		Description: "Done",
	})

	trigger := c.Response().Header().Get("HX-Trigger")
	if !strings.Contains(trigger, `"osspm:toast"`) {
		t.Fatalf("HX-Trigger missing toast event: %q", trigger)
	}
	if !strings.Contains(trigger, `"category":"success"`) || !strings.Contains(trigger, `"title":"Saved"`) {
		t.Fatalf("HX-Trigger toast payload not normalized for JS: %q", trigger)
	}
	if cookie := c.Response().Header().Get(echo.HeaderSetCookie); cookie != "" {
		t.Fatalf("Set-Cookie = %q, want empty for HTMX toast", cookie)
	}
}

func TestAddHXTriggerMergesPlainExistingTriggerHeader(t *testing.T) {
	c, _ := newTestContext(http.MethodPost, "http://example.com/settings")
	c.Response().Header().Set("HX-Trigger", "refresh, other-event")

	addHXTrigger(c, "osspm:toast", ToastPayload{Category: "success", Title: "Saved"})

	trigger := c.Response().Header().Get("HX-Trigger")
	for _, want := range []string{`"refresh"`, `"other-event"`, `"osspm:toast"`, `"title":"Saved"`} {
		if !strings.Contains(trigger, want) {
			t.Fatalf("HX-Trigger = %q, missing %s", trigger, want)
		}
	}
}

func TestRedirectWithFlashUsesHXLocationForBoostedHTMX(t *testing.T) {
	c, rec := newTestContext(http.MethodPost, "http://example.com/findings/rulesets/demo/override")
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Boosted", "true")

	err := redirectWithFlash(c, "/findings/rulesets/demo", viewmodels.ToastViewData{
		Category: "success",
		Title:    "Saved",
	})
	if err != nil {
		t.Fatalf("redirectWithFlash() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("HX-Location"); got != "/findings/rulesets/demo" {
		t.Fatalf("HX-Location = %q, want %q", got, "/findings/rulesets/demo")
	}
	if got := rec.Header().Get("HX-Redirect"); got != "" {
		t.Fatalf("HX-Redirect = %q, want empty for boosted request", got)
	}
	if trigger := rec.Header().Get("HX-Trigger"); !strings.Contains(trigger, `"osspm:toast"`) {
		t.Fatalf("HX-Trigger missing toast: %q", trigger)
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
