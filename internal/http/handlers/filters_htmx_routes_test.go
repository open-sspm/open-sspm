package handlers

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestHandleIdentities_HTMXTargetReturnsFragmentAndVary(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	c, rec := newTestContext(http.MethodGet, "/identities")
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Target", "identities-results")

	if err := harness.handlers.HandleIdentities(c); err != nil {
		t.Fatalf("HandleIdentities() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `id="identities-results"`) {
		t.Fatalf("response missing HTMX identities fragment root: %q", body)
	}
	if strings.Contains(strings.ToLower(body), "<!doctype html>") {
		t.Fatalf("response unexpectedly contains full document shell")
	}

	vary := parseVaryHeader(rec.Header().Get("Vary"))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
	if vary["hx-target"] != 1 {
		t.Fatalf("Vary header missing hx-target: %v", vary)
	}
}

func TestHandleApps_HTMXTargetReturnsFragmentAndVary(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	c, rec := newTestContext(http.MethodGet, "/apps")
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Target", "apps-results")

	if err := harness.handlers.HandleApps(c); err != nil {
		t.Fatalf("HandleApps() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `id="apps-results"`) {
		t.Fatalf("response missing HTMX apps fragment root: %q", body)
	}
	if strings.Contains(strings.ToLower(body), "<!doctype html>") {
		t.Fatalf("response unexpectedly contains full document shell")
	}

	vary := parseVaryHeader(rec.Header().Get("Vary"))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
	if vary["hx-target"] != 1 {
		t.Fatalf("Vary header missing hx-target: %v", vary)
	}
}

func TestHandleAppAssets_HTMXTargetReturnsFragmentAndVary(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	target := "/app-assets?source_kind=github&source_name=" + url.QueryEscape(harness.sourceName)
	c, rec := newTestContext(http.MethodGet, target)
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Target", "app-assets-results")

	if err := harness.handlers.HandleAppAssets(c); err != nil {
		t.Fatalf("HandleAppAssets() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `id="app-assets-results"`) {
		t.Fatalf("response missing HTMX app-assets fragment root: %q", body)
	}
	if strings.Contains(strings.ToLower(body), "<!doctype html>") {
		t.Fatalf("response unexpectedly contains full document shell")
	}

	vary := parseVaryHeader(rec.Header().Get("Vary"))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
	if vary["hx-target"] != 1 {
		t.Fatalf("Vary header missing hx-target: %v", vary)
	}
}

func TestHandleCredentials_HTMXTargetReturnsFragmentAndVary(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	target := "/credentials?source_kind=github&source_name=" + url.QueryEscape(harness.sourceName)
	c, rec := newTestContext(http.MethodGet, target)
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Target", "credentials-results")

	if err := harness.handlers.HandleCredentials(c); err != nil {
		t.Fatalf("HandleCredentials() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `id="credentials-results"`) {
		t.Fatalf("response missing HTMX credentials fragment root: %q", body)
	}
	if strings.Contains(strings.ToLower(body), "<!doctype html>") {
		t.Fatalf("response unexpectedly contains full document shell")
	}

	vary := parseVaryHeader(rec.Header().Get("Vary"))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
	if vary["hx-target"] != 1 {
		t.Fatalf("Vary header missing hx-target: %v", vary)
	}
}
