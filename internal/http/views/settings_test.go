package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestSettingsPageRendersRiskPolicyPacks(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	err := SettingsPage(viewmodels.SettingsViewData{
		Layout: viewmodels.LayoutData{
			Title: "Settings",
		},
		RiskPolicyPacks: []viewmodels.RiskPolicyPackSummary{
			{Domain: "credential", ID: "builtin.credential.risk", Version: "1.0.0"},
			{Domain: "saas", ID: "builtin.saas.risk", Version: "1.0.0"},
		},
		RiskPolicyExpressions: 12,
	}).Render(context.Background(), &body)
	if err != nil {
		t.Fatalf("render settings page: %v", err)
	}

	html := body.String()
	for _, want := range []string{
		"System operations",
		"Connector health",
		"Risk policies",
		"Built-in policy packs loaded by this server.",
		"2 packs",
		"12 CEL expressions",
		"builtin.credential.risk",
		"builtin.saas.risk",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("settings page should render %q: %s", want, html)
		}
	}
}
