package handlers

import (
	"net/url"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func TestIdentityAskBarHintsMatchTheirFilterLinks(t *testing.T) {
	t.Parallel()

	cfg := views.IdentitiesAskBar(querystate.IdentitiesQuery{})
	for _, hint := range cfg.GrammarHints {
		t.Run(hint.Token, func(t *testing.T) {
			data := buildAskBarSuggestions(cfg, hint.Token, true)
			if data.Error != "" || len(data.Items) != 1 {
				t.Fatalf("hint %q should resolve to one filter, got %+v", hint.Token, data)
			}
			link, err := url.Parse(hint.Href)
			if err != nil {
				t.Fatalf("parse hint link: %v", err)
			}
			item := data.Items[0]
			if got := link.Query().Get(cfg.FieldParam[item.Field]); got != item.Value {
				t.Fatalf("hint filter %s=%s differs from link value %q", item.Field, item.Value, got)
			}
		})
	}
}

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
