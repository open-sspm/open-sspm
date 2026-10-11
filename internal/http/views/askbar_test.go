package views

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestAskBarRendersNativeSearchAndSeparateFilterDialog(t *testing.T) {
	t.Parallel()

	cfg := AskBarConfig{
		AriaLabel:   "Filter people",
		SearchValue: "alice",
		Hidden:      []AskBarHidden{{Name: "q", Value: "legacy"}},
		KeywordTokens: map[string]AskBarKeyword{
			"active": {Field: "state", Value: "active", Label: "active"},
		},
		SuggestionScope: "people",
	}
	var body bytes.Buffer
	if err := AskBar(cfg).Render(context.Background(), &body); err != nil {
		t.Fatalf("render AskBar: %v", err)
	}

	html := body.String()
	for _, expected := range []string{
		`<label class="sr-only" for="askbar-people-search">Filter people</label>`,
		`type="search" name="q" value="alice" autocomplete="off"`,
		`data-osspm-askbar-clear`,
		`aria-label="Clear search"`,
		`aria-haspopup="dialog"`,
		`role="dialog"`,
		`data-osspm-askbar-filter-input`,
		`data-osspm-askbar-chip-remove-template`,
	} {
		if !strings.Contains(html, expected) {
			t.Fatalf("AskBar missing %q: %s", expected, html)
		}
	}
	if strings.Count(html, `name="q"`) != 1 {
		t.Fatalf("AskBar should render exactly one q control: %s", html)
	}
}

func TestAskBarSuggestionsAreNativeButtons(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	err := AskBarSuggestions(AskBarSuggestionsViewData{
		Items: []AskBarSuggestionItem{{
			Field:      "state",
			Value:      "active",
			Label:      "active",
			FieldLabel: "State",
		}},
	}).Render(context.Background(), &body)
	if err != nil {
		t.Fatalf("render AskBarSuggestions: %v", err)
	}

	html := body.String()
	if !strings.Contains(html, `<button type="button"`) {
		t.Fatalf("suggestion should be a native button: %s", html)
	}
	if !strings.Contains(html, `data-field="state"`) {
		t.Fatalf("suggestion should expose its structured field: %s", html)
	}
}

func TestIdentitiesAskBarFormOffersFilterGroupsAndHints(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	if err := IdentitiesPageFilters(viewmodels.IdentitiesViewData{}).Render(context.Background(), &body); err != nil {
		t.Fatalf("render identity filters: %v", err)
	}

	html := body.String()
	for _, expected := range []string{
		`data-enter-only-query="q"`,
		`hx-target="#identities-results"`,
		`placeholder="Search identities by name, email, or ID"`,
		`data-query="state:"`,
		`>privileged</a>`,
		`seen:stale`,
		`status:suspended`,
	} {
		if !strings.Contains(html, expected) {
			t.Fatalf("identity filters missing %q: %s", expected, html)
		}
	}
}

func TestAppImportMapPinsEveryImportOfAVersionedModuleToOneURL(t *testing.T) {
	t.Parallel()

	var importMap struct {
		Imports map[string]string `json:"imports"`
	}
	if err := json.Unmarshal([]byte(appImportMap()), &importMap); err != nil {
		t.Fatalf("decode import map: %v", err)
	}
	for _, module := range versionedAppModules {
		want := StaticAssetURL("/static/app/" + module)
		for _, specifier := range []string{"open-sspm-app/" + module, "/static/app/" + module} {
			if got := importMap.Imports[specifier]; got != want {
				t.Fatalf("import map %q = %q, want %q", specifier, got, want)
			}
		}
	}
}
