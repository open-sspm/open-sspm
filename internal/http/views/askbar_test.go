package views

import (
	"bytes"
	"context"
	"strings"
	"testing"
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
		`type="search" name="q" value="alice"`,
		`aria-haspopup="dialog"`,
		`role="dialog"`,
		`data-osspm-askbar-filter-input`,
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
