package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestAskBarSavedQueryPillUsesAriaCurrentForActiveLink(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	pill := AskBarSavedQuery{Href: "/identities", Label: "All", Active: true}
	if err := askBarSavedQueryPill(pill, "#identities-results").Render(context.Background(), &body); err != nil {
		t.Fatalf("render askBarSavedQueryPill: %v", err)
	}

	html := body.String()
	if !strings.Contains(html, `aria-current="page"`) {
		t.Fatalf("saved query pill should render aria-current: %s", html)
	}
	if strings.Contains(html, `aria-pressed=`) {
		t.Fatalf("saved query pill should not render aria-pressed: %s", html)
	}
}

func TestProductIconForUsesVendoredSimpleIcons(t *testing.T) {
	t.Parallel()

	icon := ProductIconFor("GitHub")
	if !icon.HasIcon() {
		t.Fatal("expected GitHub to resolve to a vendored product icon")
	}
	if icon.Slug != "github" {
		t.Fatalf("slug = %q, want github", icon.Slug)
	}
	if icon.Path == "" {
		t.Fatal("expected GitHub icon path data from vendored SVG")
	}
}

func TestProductIconRendersVendoredMask(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	if err := ProductIcon("GitHub", "", "", "size-8").Render(context.Background(), &body); err != nil {
		t.Fatalf("render product icon: %v", err)
	}

	html := body.String()
	if !strings.Contains(html, "product-icon-svg") {
		t.Fatalf("product icon should render inline svg element: %s", html)
	}
	if !strings.Contains(html, "<path d=") {
		t.Fatalf("product icon should render vendored SVG path data: %s", html)
	}
}

func TestProductIconForUsesConnectorAliases(t *testing.T) {
	t.Parallel()

	icon := ProductIconFor("google_workspace")
	if !icon.HasIcon() {
		t.Fatal("expected google_workspace to resolve via product icon alias")
	}
	if icon.Slug != "google" {
		t.Fatalf("slug = %q, want google", icon.Slug)
	}
}

func TestProductIconForFallsBackToInitials(t *testing.T) {
	t.Parallel()

	icon := ProductIconFor("Internal Payroll Portal")
	if icon.HasIcon() {
		t.Fatalf("unexpected icon match: %+v", icon)
	}
	if got := icon.Initial(); got != "I" {
		t.Fatalf("initial = %q, want I", got)
	}
}

func TestNonHumanIdentitiesInventoryResultsOmitCurrentFreshnessLabel(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	err := NonHumanIdentitiesInventoryResults(viewmodels.NonHumanIdentitiesViewData{
		HasItems: true,
		Items: []viewmodels.NonHumanIdentitiesListItem{
			{
				PrincipalRef:           "svc-1",
				DisplayName:            "Service Principal",
				PrincipalType:          "service",
				SourceKind:             "github",
				SourceName:             "acme",
				LinkedAssetsCount:      1,
				LinkedCredentialsCount: 2,
				LastSeen:               viewmodels.TimeDisplay{Label: "Apr 1, 2026"},
				ActivityState:          "stale",
				FreshnessState:         "current",
				OwnerPresence:          "present",
				AccountableOwner:       "Owner Example",
				RiskLevel:              "medium",
			},
		},
	}).Render(context.Background(), &body)
	if err != nil {
		t.Fatalf("render non-human inventory: %v", err)
	}

	html := body.String()
	if !strings.Contains(html, "90d+") {
		t.Fatalf("inventory should render stale activity label: %s", html)
	}
	if strings.Contains(html, "Current evidence") {
		t.Fatalf("inventory should not render default current freshness label: %s", html)
	}
}

func TestNonHumanShouldShowActivityUsesHealthyBaseline(t *testing.T) {
	t.Parallel()

	if nonHumanShouldShowActivity("recent", "current") {
		t.Fatal("recent/current should be treated as the quiet baseline")
	}
	if !nonHumanShouldShowActivity("aging", "current") {
		t.Fatal("aging activity should still be surfaced")
	}
	if !nonHumanShouldShowActivity("recent", "stale") {
		t.Fatal("stale freshness should still be surfaced")
	}
}
