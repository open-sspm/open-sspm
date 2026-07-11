package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestIdentityShowLazySectionIncludesTimeoutAndFallback(t *testing.T) {
	var body bytes.Buffer
	data := viewmodels.IdentityShowViewData{
		Entitlements: viewmodels.IdentityShowEntitlementsPanel{
			LoadHref: "/identities/42?admin=true",
		},
	}
	if err := IdentityShowEntitlementsSection(data).Render(context.Background(), &body); err != nil {
		t.Fatalf("render lazy entitlements section: %v", err)
	}

	html := body.String()
	for _, want := range []string{
		`hx-trigger="intersect once, oss-panel-visible"`,
		`hx-swap="outerHTML"`,
		`hx-sync="this:drop"`,
		`hx-request=`,
		`timeout`,
		`data-hx-lazy-error-template`,
		`data-hx-lazy-retry`,
		`href="/identities/42?admin=true"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("lazy section missing %q: %s", want, html)
		}
	}
}

func TestIdentityShowLazySectionErrorRendersRecoverableFragment(t *testing.T) {
	var body bytes.Buffer
	if err := IdentityShowLazySectionError(
		"identity-entitlements-section",
		"Access grants",
		"/identities/42?admin=true",
		"The server couldn't load this section.",
		"request-123",
	).Render(context.Background(), &body); err != nil {
		t.Fatalf("render lazy section error: %v", err)
	}

	html := body.String()
	for _, want := range []string{
		`id="identity-entitlements-section"`,
		`data-hx-lazy-error`,
		`role="alert"`,
		`href="/identities/42?admin=true"`,
		`hx-get="/identities/42?admin=true"`,
		`hx-target="closest section"`,
		`hx-swap="outerHTML"`,
		`data-hx-lazy-retry`,
		"request-123",
		"Retry",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("lazy error fragment missing %q: %s", want, html)
		}
	}
}
