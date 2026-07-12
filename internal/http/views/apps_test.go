package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestAppRowMobileExposesSuggestedMappingAction(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	app := viewmodels.AppListItem{
		Label:         "GitHub Enterprise",
		SourceName:    "okta-main",
		ExternalID:    "app-42",
		SuggestedKind: "github",
	}
	if err := AppRowMobile(app, true, "csrf-token", 0).Render(context.Background(), &body); err != nil {
		t.Fatalf("render mobile app row: %v", err)
	}

	html := body.String()
	for _, want := range []string{
		`action="/assigned-apps/map"`,
		`aria-label="Map GitHub for GitHub Enterprise"`,
		`name="okta_app_external_id" value="app-42"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("mobile app row missing mapping action %q: %s", want, html)
		}
	}
}

func TestAppRowMobileHidesSuggestedMappingActionFromNonAdmins(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	app := viewmodels.AppListItem{Label: "GitHub", SuggestedKind: "github"}
	if err := AppRowMobile(app, false, "csrf-token", 0).Render(context.Background(), &body); err != nil {
		t.Fatalf("render mobile app row: %v", err)
	}

	if strings.Contains(body.String(), `action="/assigned-apps/map"`) {
		t.Fatalf("non-admin mobile app row should not expose mapping action: %s", body.String())
	}
}
