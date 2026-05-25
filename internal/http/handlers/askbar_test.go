package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func TestBuildAskBarSuggestionsRendersGrammarError(t *testing.T) {
	cfg := views.IdentitiesAskBar(querystate.IdentitiesQuery{})
	data := buildAskBarSuggestions(cfg, "state:tomrow", false)

	if data.Error == "" {
		t.Fatalf("Error is empty, want grammar error")
	}
	if len(data.Items) != 0 {
		t.Fatalf("Items = %d, want 0 for unknown non-free-text value", len(data.Items))
	}
	if !data.ShowFreeText {
		t.Fatalf("ShowFreeText = false, want true")
	}
}

func TestBuildAskBarSuggestionsReturnsFieldValueToken(t *testing.T) {
	cfg := views.IdentitiesAskBar(querystate.IdentitiesQuery{})
	data := buildAskBarSuggestions(cfg, "state:review", false)

	if data.Error != "" {
		t.Fatalf("Error = %q, want empty", data.Error)
	}
	if len(data.Items) == 0 {
		t.Fatalf("Items is empty, want review suggestion")
	}
	if got := data.Items[0].Value; got != "review" {
		t.Fatalf("first value = %q, want review", got)
	}
}
