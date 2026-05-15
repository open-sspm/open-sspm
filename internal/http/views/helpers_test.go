package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestOktaAppMetaPartsUseCanonicalSignOnLabels(t *testing.T) {
	t.Parallel()

	parts := OktaAppMetaParts(viewmodels.OktaAppSummaryView{
		Name:       "Example App",
		SignOnMode: "OPENID_CONNECT",
		ExternalID: "app-123",
	})
	if len(parts) != 3 {
		t.Fatalf("parts len = %d, want 3", len(parts))
	}
	if parts[1].Text != "OpenID Connect" {
		t.Fatalf("sign-on mode = %q, want %q", parts[1].Text, "OpenID Connect")
	}
}

func TestSeverityTextHelpersStayAligned(t *testing.T) {
	t.Parallel()

	if got, want := CredentialRiskTextClass("high"), RuleSeverityTextClass("high"); got != want {
		t.Fatalf("severity text helpers differ: %q != %q", got, want)
	}
}

func TestOverviewMapSourceClassUsesExplicitUnknownFallback(t *testing.T) {
	t.Parallel()

	if got := OverviewMapSourceClass("github"); !strings.Contains(got, "overview-map-source-github") {
		t.Fatalf("github tone class = %q, want github class", got)
	}
	if got := OverviewMapSourceClass("gitub"); !strings.Contains(got, "overview-map-source-unknown") {
		t.Fatalf("typo tone class = %q, want explicit unknown class", got)
	}
	if got := OverviewMapSourceClass(viewmodels.OverviewMapToneDefault); strings.Contains(got, "overview-map-source-unknown") {
		t.Fatalf("default tone class = %q, should not be unknown", got)
	}
}

func TestOverviewMapSeverityClassIsExplicit(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"critical": "overview-risk-bucket-critical",
		"high":     "overview-risk-bucket-high",
		"medium":   "overview-risk-bucket-medium",
		"low":      "overview-risk-bucket-low",
		"info":     "overview-risk-bucket-info",
		"":         "overview-risk-bucket-unknown",
		"typo":     "overview-risk-bucket-unknown",
	}

	for severity, want := range tests {
		if got := OverviewMapSeverityClass(severity); !strings.Contains(got, want) {
			t.Fatalf("severity %q class = %q, want %q", severity, got, want)
		}
	}
}

func TestOverviewMapCoordinatesClampBeforeRendering(t *testing.T) {
	t.Parallel()

	if got, want := OverviewMapNodeStyle(-50, 150), "left: 0%; top: 100%;"; got != want {
		t.Fatalf("node style = %q, want %q", got, want)
	}
	if got, want := OverviewMapEdgePath(-1, 25, 120, 75), "M 0 25 C 50 25, 50 75, 100 75"; got != want {
		t.Fatalf("edge path = %q, want %q", got, want)
	}
}

func TestOverviewMapRendersStableHooks(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	err := OverviewMap(viewmodels.OverviewMapGraph{
		CenterX:       50,
		CenterY:       50,
		IdentityCount: 3,
		AccountCount:  5,
		Sources: []viewmodels.OverviewMapSourceNode{
			{
				Kind:            "github",
				Label:           "GitHub",
				Href:            "/accounts/github",
				X:               50,
				Y:               16,
				AccountCount:    5,
				CoveragePercent: 80,
				Tone:            viewmodels.OverviewMapToneGitHub,
				Buckets: []viewmodels.OverviewMapBucket{
					{Label: "Admins", Severity: viewmodels.OverviewMapSeverityHigh, AffectedCount: 2},
				},
			},
		},
	}).Render(context.Background(), &body)
	if err != nil {
		t.Fatalf("render overview map: %v", err)
	}

	html := body.String()
	for _, hook := range []string{
		`data-testid="overview-map"`,
		`data-testid="overview-map-center"`,
		`data-testid="overview-map-edge"`,
		`data-testid="overview-map-node"`,
		`data-testid="overview-map-list-item"`,
		`data-testid="overview-map-bucket"`,
		`data-overview-map-surface`,
		`data-overview-map-edge="0"`,
		`data-overview-map-node="0"`,
	} {
		if !strings.Contains(html, hook) {
			t.Fatalf("overview map should render %s: %s", hook, html)
		}
	}
	if strings.Contains(html, `overview-map-source-count`) {
		t.Fatalf("overview map source title should not render a separate account count: %s", html)
	}
}

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
