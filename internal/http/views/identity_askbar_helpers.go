package views

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
)

// IdentitiesAskBar returns the full askbar config for the identities page —
// active chips, parser vocabulary, hidden form bank, and saved-query pills.
func IdentitiesAskBar(q querystate.IdentitiesQuery) AskBarConfig {
	return AskBarConfig{
		Placeholder:     "try: priv needs-anchor · source:github · stale privileged · or just type a name",
		AriaLabel:       "Filter identities",
		Chips:           identitiesAskBarChips(q),
		Hidden:          identitiesAskBarHidden(q),
		FieldParam:      identitiesFieldParam(),
		KeyLabel:        identitiesKeyLabel(),
		FieldLabel:      identitiesFieldLabel(),
		KeywordTokens:   identitiesKeywordTokens(),
		FieldAliases:    identitiesFieldAliases(),
		Stopwords:       identitiesStopwords(),
		SingletonFields: identitiesSingletonFields(),
		SavedQueries:    identitiesSavedQueries(q),
		HxTarget:        "#identities-results",
	}
}

func identitiesAskBarChips(q querystate.IdentitiesQuery) []AskBarChip {
	chips := make([]AskBarChip, 0, 8)

	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{
			Field: "search",
			Value: v,
			Label: `"` + v + `"`,
		})
	}
	if q.Source.Kind != "" {
		chips = append(chips, AskBarChip{
			Field:    "source_kind",
			Value:    q.Source.Kind,
			KeyLabel: "source",
			Label:    HumanizeProgrammaticKind(q.Source.Kind),
		})
	}
	if q.IdentityType != "" {
		chips = append(chips, AskBarChip{
			Field:    "identity_type",
			Value:    q.IdentityType,
			KeyLabel: "type",
			Label:    strings.ToLower(HumanizeIdentityType(q.IdentityType)),
		})
	}
	switch q.AnchorState {
	case "anchored":
		chips = append(chips, AskBarChip{
			Field: "anchor",
			Value: "anchored",
			Label: "anchored",
		})
	case "missing_anchor":
		chips = append(chips, AskBarChip{
			Field: "anchor",
			Value: "missing_anchor",
			Label: "needs anchor",
			Tone:  "warn",
		})
	}
	if q.PrivilegedOnly {
		chips = append(chips, AskBarChip{
			Field: "privileged_only",
			Value: "1",
			Label: "privileged",
			Tone:  "warn",
		})
	}
	if q.Status != "" {
		tone := ""
		switch q.Status {
		case "suspended", "orphaned":
			tone = "danger"
		}
		chips = append(chips, AskBarChip{
			Field:    "status",
			Value:    q.Status,
			KeyLabel: "status",
			Label:    strings.ToLower(HumanizeIdentityStatus(q.Status)),
			Tone:     tone,
		})
	}
	if q.ActivityState != "" {
		chips = append(chips, AskBarChip{
			Field:    "activity_state",
			Value:    q.ActivityState,
			KeyLabel: "seen",
			Label:    humanizeIdentityActivityState(q.ActivityState),
		})
	}
	if q.RowState != "" {
		tone := ""
		label := strings.ToLower(HumanizeIdentityRowState(q.RowState))
		switch q.RowState {
		case "action_required":
			tone = "danger"
		case "review":
			tone = "warn"
		case "healthy":
			tone = "ok"
		}
		chips = append(chips, AskBarChip{
			Field:    "row_state",
			Value:    q.RowState,
			KeyLabel: "state",
			Label:    label,
			Tone:     tone,
		})
	}

	return chips
}

func identitiesAskBarHidden(q querystate.IdentitiesQuery) []AskBarHidden {
	hidden := make([]AskBarHidden, 0, 8)
	if v := strings.TrimSpace(q.Q); v != "" {
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	if q.Source.Kind != "" {
		hidden = append(hidden, AskBarHidden{Name: "source_kind", Value: q.Source.Kind})
	}
	if q.Source.Name != "" {
		hidden = append(hidden, AskBarHidden{Name: "source_name", Value: q.Source.Name})
	}
	if q.IdentityType != "" {
		hidden = append(hidden, AskBarHidden{Name: "identity_type", Value: q.IdentityType})
	}
	if q.AnchorState != "" {
		hidden = append(hidden, AskBarHidden{Name: "anchor_state", Value: q.AnchorState})
	}
	if q.PrivilegedOnly {
		hidden = append(hidden, AskBarHidden{Name: "privileged", Value: "1"})
	}
	if q.Status != "" {
		hidden = append(hidden, AskBarHidden{Name: "status", Value: q.Status})
	}
	if q.ActivityState != "" {
		hidden = append(hidden, AskBarHidden{Name: "activity_state", Value: q.ActivityState})
	}
	if q.RowState != "" {
		hidden = append(hidden, AskBarHidden{Name: "row_state", Value: q.RowState})
	}
	if q.SortBy != "" {
		hidden = append(hidden, AskBarHidden{Name: "sort_by", Value: q.SortBy})
		if q.SortDir != "" {
			hidden = append(hidden, AskBarHidden{Name: "sort_dir", Value: q.SortDir})
		}
	}
	return hidden
}

func identitiesFieldParam() map[string]string {
	return map[string]string{
		"search":          "q",
		"source_kind":     "source_kind",
		"identity_type":   "identity_type",
		"anchor":          "anchor_state",
		"privileged_only": "privileged",
		"status":          "status",
		"activity_state":  "activity_state",
		"row_state":       "row_state",
	}
}

func identitiesKeyLabel() map[string]string {
	return map[string]string{
		"search":          "",
		"source_kind":     "source",
		"identity_type":   "type",
		"status":          "status",
		"activity_state":  "seen",
		"row_state":       "state",
		"anchor":          "",
		"privileged_only": "",
	}
}

func identitiesFieldLabel() map[string]string {
	return map[string]string{
		"search":          "Search",
		"source_kind":     "Source",
		"identity_type":   "Type",
		"status":          "Status",
		"activity_state":  "Activity",
		"row_state":       "State",
		"anchor":          "Anchor",
		"privileged_only": "Access",
	}
}

func identitiesKeywordTokens() map[string]AskBarKeyword {
	return map[string]AskBarKeyword{
		"okta":      {Field: "source_kind", Value: "okta", Label: "Okta"},
		"entra":     {Field: "source_kind", Value: "entra", Label: "Microsoft Entra"},
		"azure":     {Field: "source_kind", Value: "entra", Label: "Microsoft Entra"},
		"github":    {Field: "source_kind", Value: "github", Label: "GitHub"},
		"aws":       {Field: "source_kind", Value: "aws", Label: "AWS Identity Center"},
		"gcp":       {Field: "source_kind", Value: "gcp", Label: "GCP"},
		"workspace": {Field: "source_kind", Value: "google_workspace", Label: "Google Workspace"},
		"google":    {Field: "source_kind", Value: "google_workspace", Label: "Google Workspace"},
		"datadog":   {Field: "source_kind", Value: "datadog", Label: "Datadog"},

		"active":    {Field: "status", Value: "active", Label: "active"},
		"suspended": {Field: "status", Value: "suspended", Label: "suspended", Tone: "danger"},
		"deleted":   {Field: "status", Value: "deleted", Label: "deleted"},
		"orphaned":  {Field: "status", Value: "orphaned", Label: "orphaned", Tone: "danger"},
		"unknown":   {Field: "status", Value: "unknown", Label: "unknown"},

		"review":       {Field: "row_state", Value: "review", Label: "review", Tone: "warn"},
		"healthy":      {Field: "row_state", Value: "healthy", Label: "healthy", Tone: "ok"},
		"needs-action": {Field: "row_state", Value: "action_required", Label: "needs action", Tone: "danger"},

		"human":  {Field: "identity_type", Value: "human", Label: "humans"},
		"humans": {Field: "identity_type", Value: "human", Label: "humans"},

		"anchored":     {Field: "anchor", Value: "anchored", Label: "anchored"},
		"needs-anchor": {Field: "anchor", Value: "missing_anchor", Label: "needs anchor", Tone: "warn"},

		"stale":      {Field: "activity_state", Value: "stale", Label: "stale (90d+)"},
		"aging":      {Field: "activity_state", Value: "aging", Label: "30-89d"},
		"recent":     {Field: "activity_state", Value: "recent", Label: "<30d"},
		"never-seen": {Field: "activity_state", Value: "never_seen", Label: "never seen"},
		"30d":        {Field: "activity_state", Value: "aging", Label: "30-89d"},
		"90d":        {Field: "activity_state", Value: "stale", Label: "90d+"},

		"priv":       {Field: "privileged_only", Value: "1", Label: "privileged", Tone: "warn"},
		"privileged": {Field: "privileged_only", Value: "1", Label: "privileged", Tone: "warn"},
	}
}

func identitiesFieldAliases() map[string]string {
	return map[string]string{
		"source":          "source_kind",
		"src":             "source_kind",
		"source_kind":     "source_kind",
		"status":          "status",
		"state":           "row_state",
		"row":             "row_state",
		"row_state":       "row_state",
		"activity":        "activity_state",
		"seen":            "activity_state",
		"activity_state":  "activity_state",
		"type":            "identity_type",
		"kind":            "identity_type",
		"identity_type":   "identity_type",
		"anchor":          "anchor",
		"anchor_state":    "anchor",
		"priv":            "privileged_only",
		"privileged":      "privileged_only",
		"privileged_only": "privileged_only",
	}
}

func identitiesStopwords() []string {
	return []string{"accounts", "account", "identities", "identity", "in", "a", "an", "the", "and", "with", "are"}
}

func identitiesSingletonFields() []string {
	return []string{"source_kind", "identity_type", "anchor", "privileged_only", "status", "activity_state", "row_state"}
}

func identitiesSavedQueries(q querystate.IdentitiesQuery) []AskBarSavedQuery {
	return []AskBarSavedQuery{
		{
			Href:   q.SegmentPrivilegedMissingAnchor().Href(),
			Label:  "Privileged needs anchor",
			Active: q.PrivilegedOnly && q.AnchorState == "missing_anchor" && q.ActivityState == "" && q.Status == "" && q.RowState == "",
		},
		{
			Href:   q.SegmentStalePrivileged().Href(),
			Label:  "Stale privileged",
			Active: q.PrivilegedOnly && q.ActivityState == "stale" && q.AnchorState == "" && q.Status == "" && q.RowState == "",
		},
		{
			Href:   q.SegmentNeedsAction().Href(),
			Label:  "Needs action",
			Active: q.RowState == "action_required",
		},
		{
			Href:   q.ClearSegments().WithStatus("suspended").Href(),
			Label:  "Suspended",
			Active: q.Status == "suspended",
		},
		{
			Href:   q.SegmentReview().Href(),
			Label:  "Review",
			Active: q.RowState == "review",
		},
	}
}

func humanizeIdentityActivityState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "recent":
		return "<30d"
	case "aging":
		return "30-89d"
	case "stale":
		return "stale (90d+)"
	case "never_seen":
		return "never seen"
	default:
		return fallbackHumanized(state)
	}
}
