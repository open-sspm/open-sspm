package views

import (
	"encoding/json"
	"net/url"
	"strings"
)

// AskBarChip is one server-rendered chip on an ask bar. Each chip mirrors one
// active filter dimension. The JS layer hydrates from the DOM attributes so
// chip state stays driven by the query state even when JS is slow to boot.
type AskBarChip struct {
	Field    string
	Value    string
	KeyLabel string
	Label    string
	Tone     string
}

// AskBarKeyword is one entry in the parser vocabulary. Typing the matching
// word (or `field:value` with the keyword as `value`) creates a chip from this
// definition.
type AskBarKeyword struct {
	Field string `json:"field"`
	Value string `json:"value"`
	Label string `json:"label"`
	Tone  string `json:"tone,omitempty"`
}

// AskBarHidden is one hidden form input written by the server. The JS bank
// replaces these on every chip change so htmx submits the canonical set.
type AskBarHidden struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type AskBarSuggestionItem struct {
	Field      string
	Value      string
	Label      string
	Tone       string
	FieldLabel string
	Active     bool
}

type AskBarSuggestionsViewData struct {
	Items        []AskBarSuggestionItem
	DisplayText  string
	Error        string
	ShowFreeText bool
}

// AskBarSavedQuery is one pill rendered below the bar. Pills are plain anchors
// with htmx attributes so they work with browser history and no JS.
type AskBarSavedQuery struct {
	Href   string
	Label  string
	Active bool
}

// AskBarGrammarHint is one code-token suggestion in the "Try" row below the
// bar. Token is rendered in monospace so the operator can read the grammar
// (e.g. `expires:<30d`). Clicking the hint navigates to the canonical
// querystate URL — the parser-friendly grammar surface is taught visually
// without binding the click target to a literal parse step.
type AskBarGrammarHint struct {
	Token  string
	Href   string
	Active bool
}

// AskBarConfig fully describes one page's ask bar — the active chips, the JS
// vocabulary, the form bank, and the saved-query row.
type AskBarConfig struct {
	Placeholder     string
	AriaLabel       string
	Chips           []AskBarChip
	Hidden          []AskBarHidden
	StaticHidden    []AskBarHidden
	FieldParam      map[string]string
	KeyLabel        map[string]string
	FieldLabel      map[string]string
	KeywordTokens   map[string]AskBarKeyword
	FieldAliases    map[string]string
	Stopwords       []string
	SingletonFields []string
	FreeTextFields  []string
	SavedQueries    []AskBarSavedQuery
	GrammarHints    []AskBarGrammarHint
	HasRightActions bool
	HxTarget        string
	SuggestionScope string
}

// AskBarChipClass returns the CSS class for a chip given its tone.
func AskBarChipClass(tone string) string {
	switch strings.TrimSpace(tone) {
	case "warn":
		return "osspm-askbar-chip osspm-askbar-chip-warn"
	case "danger":
		return "osspm-askbar-chip osspm-askbar-chip-danger"
	case "ok":
		return "osspm-askbar-chip osspm-askbar-chip-ok"
	default:
		return "osspm-askbar-chip"
	}
}

// AskBarConfigJSON marshals the JS-facing slice of the config for the data
// attribute. Empty maps/slices are emitted as `{}`/`[]` to keep the JS branch
// detection simple.
func AskBarConfigJSON(cfg AskBarConfig) string {
	payload := map[string]any{
		"fieldParam":      ensureMapString(cfg.FieldParam),
		"keyLabel":        ensureMapString(cfg.KeyLabel),
		"fieldLabel":      ensureMapString(cfg.FieldLabel),
		"keywordTokens":   ensureKeywords(cfg.KeywordTokens),
		"fieldAliases":    ensureMapString(cfg.FieldAliases),
		"stopwords":       ensureSlice(cfg.Stopwords),
		"singletonFields": ensureSlice(cfg.SingletonFields),
		"freeTextFields":  ensureSlice(cfg.FreeTextFields),
		"staticHidden":    ensureHiddenSlice(cfg.StaticHidden),
		"suggestEndpoint": askBarSuggestEndpoint(cfg.SuggestionScope),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// AskBarPlaceholder returns the placeholder text or a sensible default.
func AskBarPlaceholder(cfg AskBarConfig) string {
	if strings.TrimSpace(cfg.Placeholder) != "" {
		return cfg.Placeholder
	}
	return "Search…"
}

// AskBarAriaLabel returns the screen-reader label or a sensible default.
func AskBarAriaLabel(cfg AskBarConfig) string {
	if strings.TrimSpace(cfg.AriaLabel) != "" {
		return cfg.AriaLabel
	}
	return "Filter results"
}

func AskBarHiddenInputs(cfg AskBarConfig) []AskBarHidden {
	hidden := make([]AskBarHidden, 0, len(cfg.StaticHidden)+len(cfg.Hidden))
	hidden = append(hidden, cfg.StaticHidden...)
	hidden = append(hidden, cfg.Hidden...)
	return hidden
}

func askBarSuggestEndpoint(scope string) string {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return ""
	}
	return "/askbar/suggestions?scope=" + url.QueryEscape(scope)
}

func ensureMapString(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func ensureKeywords(m map[string]AskBarKeyword) map[string]AskBarKeyword {
	if m == nil {
		return map[string]AskBarKeyword{}
	}
	return m
}

func ensureSlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func ensureHiddenSlice(s []AskBarHidden) []AskBarHidden {
	if s == nil {
		return []AskBarHidden{}
	}
	return s
}
