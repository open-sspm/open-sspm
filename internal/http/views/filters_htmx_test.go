package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/a-h/templ"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

const (
	selectFilterTrigger    = "change delay:150ms from:select, submit"
	debouncedFilterTrigger = "input changed delay:300ms from:input[name='q'], " + selectFilterTrigger
)

func renderViewComponent(t *testing.T, component templ.Component) string {
	t.Helper()

	var buf bytes.Buffer
	if err := component.Render(context.Background(), &buf); err != nil {
		t.Fatalf("render component: %v", err)
	}
	return buf.String()
}

func TestIdentitiesPageResultsUsesDebouncedHTMXFilters(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, IdentitiesPageResults(viewmodels.IdentitiesViewData{}))
	assertContains(t, html, `hx-get="/identities"`)
	assertContains(t, html, `hx-target="#identities-results"`)
	assertContains(t, html, `hx-swap="outerHTML"`)
	assertContains(t, html, `hx-push-url="true"`)
	assertContains(t, html, `hx-trigger="`+debouncedFilterTrigger+`"`)
}

func TestAppsPageResultsUsesDebouncedHTMXFilters(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, AppsPageResults(viewmodels.AppsViewData{}))
	assertContains(t, html, `hx-get="/apps"`)
	assertContains(t, html, `hx-target="#apps-results"`)
	assertContains(t, html, `hx-swap="outerHTML"`)
	assertContains(t, html, `hx-push-url="true"`)
	assertContains(t, html, `hx-trigger="`+debouncedFilterTrigger+`"`)
}

func TestAppAssetsPageResultsUsesDebouncedHTMXFiltersAndNoAutosubmit(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, AppAssetsPageResults(viewmodels.AppAssetsViewData{}))
	assertContains(t, html, `hx-get="/app-assets"`)
	assertContains(t, html, `hx-target="#app-assets-results"`)
	assertContains(t, html, `hx-swap="outerHTML"`)
	assertContains(t, html, `hx-push-url="true"`)
	assertContains(t, html, `hx-trigger="`+debouncedFilterTrigger+`"`)
	assertNotContains(t, html, `data-autosubmit="true"`)
}

func TestCredentialsPageResultsUsesDebouncedHTMXFiltersAndNoAutosubmit(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, CredentialsPageResults(viewmodels.CredentialsViewData{}))
	assertContains(t, html, `hx-get="/credentials"`)
	assertContains(t, html, `hx-target="#credentials-results"`)
	assertContains(t, html, `hx-swap="outerHTML"`)
	assertContains(t, html, `hx-push-url="true"`)
	assertContains(t, html, `hx-trigger="`+debouncedFilterTrigger+`"`)
	assertNotContains(t, html, `data-autosubmit="true"`)
}

func TestDiscoveryAppsPageResultsUsesSelectChangeHTMXFilters(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, DiscoveryAppsPageResults(viewmodels.DiscoveryAppsViewData{}))
	assertContains(t, html, `hx-get="/discovery/apps"`)
	assertContains(t, html, `hx-target="#discovery-apps-results"`)
	assertContains(t, html, `hx-swap="outerHTML"`)
	assertContains(t, html, `hx-push-url="true"`)
	assertContains(t, html, `hx-trigger="`+selectFilterTrigger+`"`)
}

func TestDiscoveryHotspotsPageResultsUsesSelectChangeHTMXFilters(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, DiscoveryHotspotsPageResults(viewmodels.DiscoveryHotspotsViewData{}))
	assertContains(t, html, `hx-get="/discovery/hotspots"`)
	assertContains(t, html, `hx-target="#discovery-hotspots-results"`)
	assertContains(t, html, `hx-swap="outerHTML"`)
	assertContains(t, html, `hx-push-url="true"`)
	assertContains(t, html, `hx-trigger="`+selectFilterTrigger+`"`)
}

func assertContains(t *testing.T, content, want string) {
	t.Helper()
	if !strings.Contains(content, want) {
		t.Fatalf("expected rendered HTML to contain %q", want)
	}
}

func assertNotContains(t *testing.T, content, disallowed string) {
	t.Helper()
	if strings.Contains(content, disallowed) {
		t.Fatalf("expected rendered HTML to not contain %q", disallowed)
	}
}
