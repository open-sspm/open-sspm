package views

import (
	"strconv"
	"strings"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

// SearchOnlyAskBar returns a minimal askbar with one free-text search chip and
// no field vocabulary. Suitable for pages whose only filter is `q`.
func SearchOnlyAskBar(q querystate.BasicListQuery, placeholder, hxTarget string) AskBarConfig {
	chips := []AskBarChip{}
	hidden := []AskBarHidden{}
	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{Field: "search", Value: v, Label: `"` + v + `"`})
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	return AskBarConfig{
		Placeholder: placeholder,
		AriaLabel:   "Filter results",
		Chips:       chips,
		Hidden:      hidden,
		HxTarget:    hxTarget,
	}
}

// StateAskBar adds an `active`/`inactive` keyword vocabulary on top of search.
// Used by pages backed by querystate.BasicListQuery that read a `state` param.
func StateAskBar(q querystate.BasicListQuery, placeholder, hxTarget string) AskBarConfig {
	chips := []AskBarChip{}
	hidden := []AskBarHidden{}
	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{Field: "search", Value: v, Label: `"` + v + `"`})
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	if v := strings.TrimSpace(q.State); v != "" {
		chips = append(chips, AskBarChip{
			Field:    "state",
			Value:    v,
			KeyLabel: "state",
			Label:    strings.ToLower(v),
		})
		hidden = append(hidden, AskBarHidden{Name: "state", Value: v})
	}
	return AskBarConfig{
		Placeholder: placeholder,
		AriaLabel:   "Filter results",
		Chips:       chips,
		Hidden:      hidden,
		FieldParam: map[string]string{
			"search": "q",
			"state":  "state",
		},
		KeyLabel: map[string]string{
			"search": "",
			"state":  "state",
		},
		FieldLabel: map[string]string{
			"search": "Search",
			"state":  "State",
		},
		KeywordTokens: map[string]AskBarKeyword{
			"active":   {Field: "state", Value: "active", Label: "active"},
			"inactive": {Field: "state", Value: "inactive", Label: "inactive"},
		},
		FieldAliases: map[string]string{
			"state":  "state",
			"status": "state",
		},
		SingletonFields: []string{"state"},
		HxTarget:        hxTarget,
	}
}

// AppsAskBar is the askbar config for the assigned-apps page.
func AppsAskBar(data viewmodels.AppsViewData) AskBarConfig {
	q := data.Query

	chips := []AskBarChip{}
	hidden := []AskBarHidden{}
	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{Field: "search", Value: v, Label: `"` + v + `"`})
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	if q.Integration != "" {
		chips = append(chips, AskBarChip{
			Field:    "integration",
			Value:    q.Integration,
			KeyLabel: "integration",
			Label:    strings.ReplaceAll(q.Integration, "_", " "),
		})
		hidden = append(hidden, AskBarHidden{Name: "integration", Value: q.Integration})
	}
	if q.Status != "" {
		chips = append(chips, AskBarChip{
			Field:    "status",
			Value:    q.Status,
			KeyLabel: "status",
			Label:    strings.ToLower(fallbackHumanized(q.Status)),
		})
		hidden = append(hidden, AskBarHidden{Name: "status", Value: q.Status})
	}

	keywords := map[string]AskBarKeyword{
		"connected":     {Field: "integration", Value: "connected", Label: "connected", Tone: "ok"},
		"not-connected": {Field: "integration", Value: "not_connected", Label: "not connected"},
		"unconnected":   {Field: "integration", Value: "not_connected", Label: "not connected"},
	}
	for _, status := range data.StatusOptions {
		key := strings.ToLower(strings.ReplaceAll(status, "_", "-"))
		if key == "" {
			continue
		}
		keywords[key] = AskBarKeyword{
			Field: "status",
			Value: status,
			Label: strings.ToLower(fallbackHumanized(status)),
		}
	}

	return AskBarConfig{
		Placeholder: "Search apps · connected · status:active",
		AriaLabel:   "Filter apps",
		Chips:       chips,
		Hidden:      hidden,
		FieldParam: map[string]string{
			"search":      "q",
			"integration": "integration",
			"status":      "status",
		},
		KeyLabel: map[string]string{
			"search":      "",
			"integration": "integration",
			"status":      "status",
		},
		FieldLabel: map[string]string{
			"search":      "Search",
			"integration": "Integration",
			"status":      "Status",
		},
		KeywordTokens: keywords,
		FieldAliases: map[string]string{
			"integration": "integration",
			"status":      "status",
		},
		SingletonFields: []string{"integration", "status"},
		HxTarget:        "#apps-results",
	}
}

// CredentialsAskBar is the askbar config for the credentials page.
func CredentialsAskBar(data viewmodels.CredentialsViewData) AskBarConfig {
	q := data.Query

	chips := []AskBarChip{}
	hidden := []AskBarHidden{}
	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{Field: "search", Value: v, Label: `"` + v + `"`})
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	if q.Source.Kind != "" {
		chips = append(chips, AskBarChip{
			Field:    "source_kind",
			Value:    q.Source.Kind,
			KeyLabel: "source",
			Label:    HumanizeProgrammaticKind(q.Source.Kind),
		})
		hidden = append(hidden, AskBarHidden{Name: "source_kind", Value: q.Source.Kind})
	}
	if q.CredentialKind != "" {
		chips = append(chips, AskBarChip{
			Field:    "credential_kind",
			Value:    q.CredentialKind,
			KeyLabel: "kind",
			Label:    credentialKindFilterLabel(q.CredentialKind),
		})
		hidden = append(hidden, AskBarHidden{Name: "credential_kind", Value: q.CredentialKind})
	}
	if q.Status != "" {
		tone := ""
		switch q.Status {
		case "expired", "revoked":
			tone = "danger"
		case "pending_approval":
			tone = "warn"
		}
		chips = append(chips, AskBarChip{
			Field:    "status",
			Value:    q.Status,
			KeyLabel: "status",
			Label:    strings.ToLower(HumanizeCredentialStatus(q.Status)),
			Tone:     tone,
		})
		hidden = append(hidden, AskBarHidden{Name: "status", Value: q.Status})
	}
	if q.RiskLevel != "" {
		chips = append(chips, AskBarChip{
			Field:    "risk_level",
			Value:    q.RiskLevel,
			KeyLabel: "risk",
			Label:    credentialRiskFilterLabel(q.RiskLevel),
			Tone:     credentialRiskFilterTone(q.RiskLevel),
		})
		hidden = append(hidden, AskBarHidden{Name: "risk_level", Value: q.RiskLevel})
	}
	// Suppress the implicit expiry_state=active chip when expires_in_days is
	// already driving the scope: the days chip carries the meaningful signal
	// and the two would otherwise render as near-duplicate "active" chips.
	// Hidden input still ships so the filter round-trips on submit.
	if q.ExpiryState != "" && !(q.ExpiryState == "active" && q.ExpiresInDays > 0) {
		tone := ""
		if q.ExpiryState == "expired" {
			tone = "danger"
		}
		chips = append(chips, AskBarChip{
			Field:    "expiry_state",
			Value:    q.ExpiryState,
			KeyLabel: "expiry",
			Label:    q.ExpiryState,
			Tone:     tone,
		})
	}
	if q.ExpiryState != "" {
		hidden = append(hidden, AskBarHidden{Name: "expiry_state", Value: q.ExpiryState})
	}
	if q.ExpiresInDays > 0 {
		days := strconv.Itoa(q.ExpiresInDays)
		chips = append(chips, AskBarChip{
			Field:    "expires_in_days",
			Value:    days,
			KeyLabel: "expires-in",
			Label:    "≤ " + days + "d",
			Tone:     "warn",
		})
		hidden = append(hidden, AskBarHidden{Name: "expires_in_days", Value: days})
	}
	if q.Owner != "" {
		chips = append(chips, AskBarChip{
			Field:    "owner",
			Value:    q.Owner,
			KeyLabel: "owner",
			Label:    q.Owner,
		})
		hidden = append(hidden, AskBarHidden{Name: "owner", Value: q.Owner})
	}
	if q.Asset != "" {
		chips = append(chips, AskBarChip{
			Field:    "asset",
			Value:    q.Asset,
			KeyLabel: "asset",
			Label:    q.Asset,
		})
		hidden = append(hidden, AskBarHidden{Name: "asset", Value: q.Asset})
	}
	if q.NewerThanDays > 0 {
		days := strconv.Itoa(q.NewerThanDays)
		chips = append(chips, AskBarChip{
			Field:    "newer_days",
			Value:    days,
			KeyLabel: "newer",
			Label:    days + "d",
		})
		hidden = append(hidden, AskBarHidden{Name: "newer_days", Value: days})
	}

	keywords := map[string]AskBarKeyword{
		"critical":         {Field: "risk_level", Value: "critical", Label: "critical", Tone: "danger"},
		"high":             {Field: "risk_level", Value: "high", Label: "high", Tone: "danger"},
		"critical-or-high": {Field: "risk_level", Value: "critical,high", Label: "critical or high", Tone: "danger"},
		"needs-action":     {Field: "risk_level", Value: "critical,high", Label: "critical or high", Tone: "danger"},
		"medium":           {Field: "risk_level", Value: "medium", Label: "medium", Tone: "warn"},
		"low":              {Field: "risk_level", Value: "low", Label: "low"},
		"expired":          {Field: "expiry_state", Value: "expired", Label: "expired", Tone: "danger"},
		"active":           {Field: "status", Value: "active", Label: "active"},
		"approved":         {Field: "status", Value: "approved", Label: "approved"},
		"pending":          {Field: "status", Value: "pending_approval", Label: "pending approval", Tone: "warn"},
		"pending-approval": {Field: "status", Value: "pending_approval", Label: "pending approval", Tone: "warn"},
		"revoked":          {Field: "status", Value: "revoked", Label: "revoked", Tone: "danger"},
		"pat":              {Field: "credential_kind", Value: "github_pat_request,github_pat_fine_grained", Label: "PAT"},
		"certificate":      {Field: "credential_kind", Value: "entra_certificate", Label: "certificate"},
		"cert":             {Field: "credential_kind", Value: "entra_certificate", Label: "certificate"},
		"secret":           {Field: "credential_kind", Value: "entra_client_secret", Label: "secret"},
		"owner-me":         {Field: "owner", Value: "me", Label: "me"},
		"7d":               {Field: "expires_in_days", Value: "7", Label: "< 7d", Tone: "warn"},
		"30d":              {Field: "expires_in_days", Value: "30", Label: "< 30d", Tone: "warn"},
		"90d":              {Field: "expires_in_days", Value: "90", Label: "< 90d", Tone: "warn"},
		"newer-7d":         {Field: "newer_days", Value: "7", Label: "7d"},
	}
	for key, sourceKeyword := range programmaticSourceKeywords(data.Sources) {
		keywords[key] = sourceKeyword
	}

	hints := []AskBarGrammarHint{
		{
			Token:  "expires:<30d",
			Href:   q.SegmentExpiringSoon().Href(),
			Active: q.ExpiryState == "active" && q.ExpiresInDays > 0,
		},
		{
			Token:  "owner:me",
			Href:   q.WithOwner("me").Href(),
			Active: q.Owner == "me",
		},
		{
			Token:  "kind:PAT",
			Href:   q.WithCredentialKind("pat").Href(),
			Active: q.CredentialKind == querystate.NormalizeCredentialKindFilter("pat"),
		},
		{
			Token:  "asset:GitHub",
			Href:   q.WithAsset("GitHub").Href(),
			Active: strings.EqualFold(q.Asset, "GitHub"),
		},
		{
			Token:  "newer:7d",
			Href:   q.WithNewerThanDays(7).Href(),
			Active: q.NewerThanDays == 7,
		},
	}

	return AskBarConfig{
		Placeholder: "Search",
		AriaLabel:   "Filter credentials",
		Chips:       chips,
		Hidden:      hidden,
		FieldParam: map[string]string{
			"search":          "q",
			"source_kind":     "source_kind",
			"credential_kind": "credential_kind",
			"status":          "status",
			"risk_level":      "risk_level",
			"expiry_state":    "expiry_state",
			"expires_in_days": "expires_in_days",
			"owner":           "owner",
			"asset":           "asset",
			"newer_days":      "newer_days",
		},
		KeyLabel: map[string]string{
			"search":          "",
			"source_kind":     "source",
			"credential_kind": "kind",
			"status":          "status",
			"risk_level":      "risk",
			"expiry_state":    "expiry",
			"expires_in_days": "expires",
			"owner":           "owner",
			"asset":           "asset",
			"newer_days":      "newer",
		},
		FieldLabel: map[string]string{
			"search":          "Search",
			"source_kind":     "Source",
			"credential_kind": "Credential kind",
			"status":          "Status",
			"risk_level":      "Risk",
			"expiry_state":    "Expiry",
			"expires_in_days": "Expires in",
			"owner":           "Owner",
			"asset":           "Asset",
			"newer_days":      "Newer than",
		},
		KeywordTokens: keywords,
		FieldAliases: map[string]string{
			"source":          "source_kind",
			"src":             "source_kind",
			"source_kind":     "source_kind",
			"kind":            "credential_kind",
			"credential_kind": "credential_kind",
			"status":          "status",
			"risk":            "risk_level",
			"risk_level":      "risk_level",
			"expiry":          "expiry_state",
			"expiry_state":    "expiry_state",
			"expires-in":      "expires_in_days",
			"expires":         "expires_in_days",
			"expires_in_days": "expires_in_days",
			"owner":           "owner",
			"asset":           "asset",
			"newer":           "newer_days",
			"newer_days":      "newer_days",
		},
		SingletonFields: []string{"source_kind", "credential_kind", "status", "risk_level", "expiry_state", "expires_in_days", "owner", "asset", "newer_days"},
		FreeTextFields:  []string{"owner", "asset"},
		GrammarHints:    hints,
		HasRightActions: true,
		HxTarget:        "#credentials-results",
	}
}

func credentialKindFilterLabel(value string) string {
	value = querystate.NormalizeCredentialKindFilter(value)
	switch value {
	case "github_pat_request,github_pat_fine_grained":
		return "PAT"
	case "entra_certificate":
		return "certificate"
	case "entra_client_secret":
		return "secret"
	}
	labels := []string{}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		labels = append(labels, strings.ToLower(HumanizeCredentialKind(part)))
	}
	if len(labels) == 0 {
		return strings.TrimSpace(value)
	}
	return strings.Join(labels, " or ")
}

func credentialRiskFilterLabel(value string) string {
	value = querystate.NormalizeCredentialRiskLevel(value)
	labels := []string{}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		labels = append(labels, strings.ToLower(HumanizeCredentialRisk(part)))
	}
	if len(labels) == 0 {
		return strings.TrimSpace(value)
	}
	return strings.Join(labels, " or ")
}

func credentialRiskFilterTone(value string) string {
	value = querystate.NormalizeCredentialRiskLevel(value)
	for _, part := range strings.Split(value, ",") {
		switch strings.TrimSpace(part) {
		case "critical", "high":
			return "danger"
		}
	}
	for _, part := range strings.Split(value, ",") {
		if strings.TrimSpace(part) == "medium" {
			return "warn"
		}
	}
	return ""
}

// ConnectedAppsAskBar is the askbar config for the OAuth-apps page.
func ConnectedAppsAskBar(data viewmodels.ConnectedAppsViewData) AskBarConfig {
	q := data.Query

	chips := []AskBarChip{}
	hidden := []AskBarHidden{}
	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{Field: "search", Value: v, Label: `"` + v + `"`})
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	if q.GovernanceState != "" {
		tone := ""
		switch q.GovernanceState {
		case "action_required":
			tone = "danger"
		case "in_review", "unreviewed":
			tone = "warn"
		case "approved":
			tone = "ok"
		}
		chips = append(chips, AskBarChip{
			Field:    "governance_state",
			Value:    q.GovernanceState,
			KeyLabel: "governance",
			Label:    strings.ToLower(HumanizeAppAssetGovernanceState(q.GovernanceState)),
			Tone:     tone,
		})
		hidden = append(hidden, AskBarHidden{Name: "governance_state", Value: q.GovernanceState})
	}

	return AskBarConfig{
		Placeholder: "try: unreviewed · approved · action-required · or type an app name",
		AriaLabel:   "Filter OAuth apps",
		Chips:       chips,
		Hidden:      hidden,
		StaticHidden: []AskBarHidden{
			{Name: "source_kind", Value: "google_workspace"},
			{Name: "asset_kind", Value: "google_oauth_client"},
		},
		FieldParam: map[string]string{
			"search":           "q",
			"governance_state": "governance_state",
		},
		KeyLabel: map[string]string{
			"search":           "",
			"governance_state": "governance",
		},
		FieldLabel: map[string]string{
			"search":           "Search",
			"governance_state": "Governance",
		},
		KeywordTokens: map[string]AskBarKeyword{
			"unreviewed":      {Field: "governance_state", Value: "unreviewed", Label: "unreviewed", Tone: "warn"},
			"in-review":       {Field: "governance_state", Value: "in_review", Label: "in review", Tone: "warn"},
			"approved":        {Field: "governance_state", Value: "approved", Label: "approved", Tone: "ok"},
			"action-required": {Field: "governance_state", Value: "action_required", Label: "action required", Tone: "danger"},
			"ticketed":        {Field: "governance_state", Value: "ticketed", Label: "ticketed"},
		},
		FieldAliases: map[string]string{
			"governance":       "governance_state",
			"governance_state": "governance_state",
		},
		SingletonFields: []string{"governance_state"},
		HxTarget:        "#app-assets-results",
	}
}

// AppAssetsAskBar is the askbar config for the app-assets inventory page.
func AppAssetsAskBar(data viewmodels.AppAssetsViewData) AskBarConfig {
	q := data.Query

	chips := []AskBarChip{}
	hidden := []AskBarHidden{}
	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{Field: "search", Value: v, Label: `"` + v + `"`})
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	if q.Source.Kind != "" {
		chips = append(chips, AskBarChip{
			Field:    "source_kind",
			Value:    q.Source.Kind,
			KeyLabel: "source",
			Label:    HumanizeProgrammaticKind(q.Source.Kind),
		})
		hidden = append(hidden, AskBarHidden{Name: "source_kind", Value: q.Source.Kind})
	}
	if q.AssetKind != "" {
		chips = append(chips, AskBarChip{
			Field:    "asset_kind",
			Value:    q.AssetKind,
			KeyLabel: "kind",
			Label:    strings.ToLower(fallbackHumanized(q.AssetKind)),
		})
		hidden = append(hidden, AskBarHidden{Name: "asset_kind", Value: q.AssetKind})
	}

	keywords := programmaticSourceKeywords(data.Sources)
	for _, ak := range []struct{ value, label string }{
		{"entra_application", "Entra application"},
		{"entra_service_principal", "Entra service principal"},
		{"github_app_installation", "GitHub app installation"},
		{"google_oauth_client", "Google OAuth client"},
		{"vault_auth_mount", "Vault auth mount"},
		{"vault_secrets_mount", "Vault secrets mount"},
		{"vault_auth_role", "Vault auth role"},
	} {
		keywords[strings.ReplaceAll(ak.value, "_", "-")] = AskBarKeyword{
			Field: "asset_kind",
			Value: ak.value,
			Label: strings.ToLower(ak.label),
		}
	}

	return AskBarConfig{
		Placeholder: "try: github · entra · oauth · or type a name",
		AriaLabel:   "Filter app assets",
		Chips:       chips,
		Hidden:      hidden,
		FieldParam: map[string]string{
			"search":      "q",
			"source_kind": "source_kind",
			"asset_kind":  "asset_kind",
		},
		KeyLabel: map[string]string{
			"search":      "",
			"source_kind": "source",
			"asset_kind":  "kind",
		},
		FieldLabel: map[string]string{
			"search":      "Search",
			"source_kind": "Source",
			"asset_kind":  "Asset kind",
		},
		KeywordTokens: keywords,
		FieldAliases: map[string]string{
			"source":      "source_kind",
			"src":         "source_kind",
			"source_kind": "source_kind",
			"kind":        "asset_kind",
			"asset":       "asset_kind",
			"asset_kind":  "asset_kind",
		},
		SingletonFields: []string{"source_kind", "asset_kind"},
		HxTarget:        "#app-assets-results",
	}
}

func programmaticSourceKeywords(sources []viewmodels.ProgrammaticSourceOption) map[string]AskBarKeyword {
	keywords := map[string]AskBarKeyword{}
	for _, src := range sources {
		kind := strings.TrimSpace(src.SourceKind)
		if kind == "" {
			continue
		}
		label := strings.TrimSpace(src.Label)
		if label == "" {
			label = HumanizeProgrammaticKind(kind)
		}
		for _, key := range programmaticSourceTokenKeys(kind) {
			addProgrammaticSourceKeyword(keywords, key, kind, label)
		}
	}
	return keywords
}

func addProgrammaticSourceKeyword(keywords map[string]AskBarKeyword, key, value, label string) {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return
	}
	if _, exists := keywords[key]; exists {
		return
	}
	keywords[key] = AskBarKeyword{
		Field: "source_kind",
		Value: value,
		Label: label,
	}
}

func programmaticSourceTokenKeys(kind string) []string {
	normalized := strings.ToLower(strings.TrimSpace(kind))
	keys := []string{normalized}
	hyphenated := strings.ReplaceAll(normalized, "_", "-")
	if hyphenated != normalized {
		keys = append(keys, hyphenated)
	}
	switch normalized {
	case "github":
		keys = append(keys, "github")
	case "entra":
		keys = append(keys, "entra", "azure")
	case "google_workspace":
		keys = append(keys, "google", "workspace")
	case "hashicorp_vault":
		keys = append(keys, "vault")
	case "aws":
		keys = append(keys, "aws")
	}
	return keys
}

// DiscoveryAppsAskBar is the askbar config for the discovery-apps page.
func DiscoveryAppsAskBar(data viewmodels.DiscoveryAppsViewData) AskBarConfig {
	q := data.Query

	chips := []AskBarChip{}
	hidden := []AskBarHidden{}
	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{Field: "search", Value: v, Label: `"` + v + `"`})
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	if q.Source.Kind != "" {
		chips = append(chips, AskBarChip{
			Field:    "source_kind",
			Value:    q.Source.Kind,
			KeyLabel: "source",
			Label:    HumanizeProgrammaticKind(q.Source.Kind),
		})
		hidden = append(hidden, AskBarHidden{Name: "source_kind", Value: q.Source.Kind})
	}
	if q.ManagedState != "" {
		tone := ""
		if q.ManagedState == "unmanaged" {
			tone = "warn"
		}
		chips = append(chips, AskBarChip{
			Field:    "managed_state",
			Value:    q.ManagedState,
			KeyLabel: "managed",
			Label:    q.ManagedState,
			Tone:     tone,
		})
		hidden = append(hidden, AskBarHidden{Name: "managed_state", Value: q.ManagedState})
	}
	if q.RiskLevel != "" {
		tone := ""
		switch q.RiskLevel {
		case "critical", "high":
			tone = "danger"
		case "medium":
			tone = "warn"
		}
		chips = append(chips, AskBarChip{
			Field:    "risk_level",
			Value:    q.RiskLevel,
			KeyLabel: "risk",
			Label:    q.RiskLevel,
			Tone:     tone,
		})
		hidden = append(hidden, AskBarHidden{Name: "risk_level", Value: q.RiskLevel})
	}

	keywords := map[string]AskBarKeyword{
		"managed":   {Field: "managed_state", Value: "managed", Label: "managed"},
		"unmanaged": {Field: "managed_state", Value: "unmanaged", Label: "unmanaged", Tone: "warn"},
		"critical":  {Field: "risk_level", Value: "critical", Label: "critical", Tone: "danger"},
		"high":      {Field: "risk_level", Value: "high", Label: "high", Tone: "danger"},
		"medium":    {Field: "risk_level", Value: "medium", Label: "medium", Tone: "warn"},
		"low":       {Field: "risk_level", Value: "low", Label: "low"},
	}
	for _, src := range data.SourceOptions {
		key := strings.ToLower(src.SourceKind)
		if key == "" {
			continue
		}
		if _, exists := keywords[key]; exists {
			continue
		}
		keywords[key] = AskBarKeyword{
			Field: "source_kind",
			Value: src.SourceKind,
			Label: src.Label,
		}
	}

	return AskBarConfig{
		Placeholder: "try: unmanaged · critical · github · or type a name",
		AriaLabel:   "Filter discovered apps",
		Chips:       chips,
		Hidden:      hidden,
		FieldParam: map[string]string{
			"search":        "q",
			"source_kind":   "source_kind",
			"managed_state": "managed_state",
			"risk_level":    "risk_level",
		},
		KeyLabel: map[string]string{
			"search":        "",
			"source_kind":   "source",
			"managed_state": "managed",
			"risk_level":    "risk",
		},
		FieldLabel: map[string]string{
			"search":        "Search",
			"source_kind":   "Source",
			"managed_state": "Managed",
			"risk_level":    "Risk",
		},
		KeywordTokens: keywords,
		FieldAliases: map[string]string{
			"source":        "source_kind",
			"src":           "source_kind",
			"source_kind":   "source_kind",
			"managed":       "managed_state",
			"managed_state": "managed_state",
			"risk":          "risk_level",
			"risk_level":    "risk_level",
		},
		SingletonFields: []string{"source_kind", "managed_state", "risk_level"},
		HxTarget:        "#discovery-apps-results",
	}
}

// NonHumanIdentitiesAskBar is the askbar config for the non-human identities
// page.
func NonHumanIdentitiesAskBar(data viewmodels.NonHumanIdentitiesViewData) AskBarConfig {
	q := data.Query

	chips := []AskBarChip{}
	hidden := []AskBarHidden{}
	if v := strings.TrimSpace(q.Q); v != "" {
		chips = append(chips, AskBarChip{Field: "search", Value: v, Label: `"` + v + `"`})
		hidden = append(hidden, AskBarHidden{Name: "q", Value: v})
	}
	if q.Source.Kind != "" {
		chips = append(chips, AskBarChip{
			Field:    "source_kind",
			Value:    q.Source.Kind,
			KeyLabel: "source",
			Label:    HumanizeProgrammaticKind(q.Source.Kind),
		})
		hidden = append(hidden, AskBarHidden{Name: "source_kind", Value: q.Source.Kind})
	}
	if q.PrincipalType != "" {
		chips = append(chips, AskBarChip{
			Field:    "principal_type",
			Value:    q.PrincipalType,
			KeyLabel: "type",
			Label:    strings.ToLower(HumanizeNonHumanPrincipalType(q.PrincipalType)),
		})
		hidden = append(hidden, AskBarHidden{Name: "principal_type", Value: q.PrincipalType})
	}
	if q.OwnerPresence != "" {
		tone := ""
		if q.OwnerPresence == "unknown" {
			tone = "warn"
		}
		chips = append(chips, AskBarChip{
			Field:    "owner_presence",
			Value:    q.OwnerPresence,
			KeyLabel: "owner",
			Label:    strings.ToLower(HumanizeNonHumanOwnerPresence(q.OwnerPresence)),
			Tone:     tone,
		})
		hidden = append(hidden, AskBarHidden{Name: "owner_presence", Value: q.OwnerPresence})
	}
	if q.GovernanceState != "" {
		tone := ""
		switch q.GovernanceState {
		case "action_required":
			tone = "danger"
		case "in_review", "unreviewed":
			tone = "warn"
		case "approved":
			tone = "ok"
		}
		chips = append(chips, AskBarChip{
			Field:    "governance_state",
			Value:    q.GovernanceState,
			KeyLabel: "governance",
			Label:    strings.ToLower(HumanizeAppAssetGovernanceState(q.GovernanceState)),
			Tone:     tone,
		})
		hidden = append(hidden, AskBarHidden{Name: "governance_state", Value: q.GovernanceState})
	}
	if q.RiskLevel != "" {
		tone := ""
		switch q.RiskLevel {
		case "critical", "high":
			tone = "danger"
		case "medium":
			tone = "warn"
		}
		chips = append(chips, AskBarChip{
			Field:    "risk_level",
			Value:    q.RiskLevel,
			KeyLabel: "risk",
			Label:    q.RiskLevel,
			Tone:     tone,
		})
		hidden = append(hidden, AskBarHidden{Name: "risk_level", Value: q.RiskLevel})
	}
	if q.ActivityState != "" {
		chips = append(chips, AskBarChip{
			Field:    "activity_state",
			Value:    q.ActivityState,
			KeyLabel: "seen",
			Label:    humanizeIdentityActivityState(q.ActivityState),
		})
		hidden = append(hidden, AskBarHidden{Name: "activity_state", Value: q.ActivityState})
	}
	if q.FreshnessState != "" {
		tone := ""
		if q.FreshnessState == "stale" {
			tone = "warn"
		}
		chips = append(chips, AskBarChip{
			Field:    "freshness_state",
			Value:    q.FreshnessState,
			KeyLabel: "freshness",
			Label:    q.FreshnessState,
			Tone:     tone,
		})
		hidden = append(hidden, AskBarHidden{Name: "freshness_state", Value: q.FreshnessState})
	}

	keywords := map[string]AskBarKeyword{
		"service":         {Field: "principal_type", Value: "service", Label: "service"},
		"services":        {Field: "principal_type", Value: "service", Label: "service"},
		"bot":             {Field: "principal_type", Value: "bot", Label: "bot"},
		"bots":            {Field: "principal_type", Value: "bot", Label: "bot"},
		"app":             {Field: "principal_type", Value: "app", Label: "app only"},
		"apps":            {Field: "principal_type", Value: "app", Label: "app only"},
		"owned":           {Field: "owner_presence", Value: "owned", Label: "owned", Tone: "ok"},
		"unowned":         {Field: "owner_presence", Value: "unknown", Label: "unknown owner", Tone: "warn"},
		"no-owner":        {Field: "owner_presence", Value: "unknown", Label: "unknown owner", Tone: "warn"},
		"unreviewed":      {Field: "governance_state", Value: "unreviewed", Label: "unreviewed", Tone: "warn"},
		"in-review":       {Field: "governance_state", Value: "in_review", Label: "in review", Tone: "warn"},
		"approved":        {Field: "governance_state", Value: "approved", Label: "approved", Tone: "ok"},
		"action-required": {Field: "governance_state", Value: "action_required", Label: "action required", Tone: "danger"},
		"ticketed":        {Field: "governance_state", Value: "ticketed", Label: "ticketed"},
		"critical":        {Field: "risk_level", Value: "critical", Label: "critical", Tone: "danger"},
		"high":            {Field: "risk_level", Value: "high", Label: "high", Tone: "danger"},
		"medium":          {Field: "risk_level", Value: "medium", Label: "medium", Tone: "warn"},
		"low":             {Field: "risk_level", Value: "low", Label: "low"},
		"recent":          {Field: "activity_state", Value: "recent", Label: "<30d"},
		"aging":           {Field: "activity_state", Value: "aging", Label: "30-89d"},
		"stale":           {Field: "activity_state", Value: "stale", Label: "stale (90d+)"},
		"never-seen":      {Field: "activity_state", Value: "never_seen", Label: "never seen"},
		"fresh":           {Field: "freshness_state", Value: "current", Label: "fresh"},
		"current":         {Field: "freshness_state", Value: "current", Label: "fresh"},
		"stale-evidence":  {Field: "freshness_state", Value: "stale", Label: "stale evidence", Tone: "warn"},
	}
	for _, src := range data.Sources {
		key := strings.ToLower(src.SourceKind)
		if key == "" {
			continue
		}
		if _, exists := keywords[key]; exists {
			continue
		}
		keywords[key] = AskBarKeyword{
			Field: "source_kind",
			Value: src.SourceKind,
			Label: src.Label,
		}
	}

	return AskBarConfig{
		Placeholder: "try: bots · unowned · critical · github · stale · or type a principal",
		AriaLabel:   "Filter non-human identities",
		Chips:       chips,
		Hidden:      hidden,
		FieldParam: map[string]string{
			"search":           "q",
			"source_kind":      "source_kind",
			"principal_type":   "principal_type",
			"owner_presence":   "owner_presence",
			"governance_state": "governance_state",
			"risk_level":       "risk_level",
			"activity_state":   "activity_state",
			"freshness_state":  "freshness_state",
		},
		KeyLabel: map[string]string{
			"search":           "",
			"source_kind":      "source",
			"principal_type":   "type",
			"owner_presence":   "owner",
			"governance_state": "governance",
			"risk_level":       "risk",
			"activity_state":   "seen",
			"freshness_state":  "freshness",
		},
		FieldLabel: map[string]string{
			"search":           "Search",
			"source_kind":      "Source",
			"principal_type":   "Type",
			"owner_presence":   "Owner",
			"governance_state": "Governance",
			"risk_level":       "Risk",
			"activity_state":   "Activity",
			"freshness_state":  "Freshness",
		},
		KeywordTokens: keywords,
		FieldAliases: map[string]string{
			"source":           "source_kind",
			"src":              "source_kind",
			"source_kind":      "source_kind",
			"type":             "principal_type",
			"kind":             "principal_type",
			"principal_type":   "principal_type",
			"owner":            "owner_presence",
			"owner_presence":   "owner_presence",
			"governance":       "governance_state",
			"governance_state": "governance_state",
			"risk":             "risk_level",
			"risk_level":       "risk_level",
			"activity":         "activity_state",
			"seen":             "activity_state",
			"activity_state":   "activity_state",
			"freshness":        "freshness_state",
			"freshness_state":  "freshness_state",
		},
		SingletonFields: []string{"source_kind", "principal_type", "owner_presence", "governance_state", "risk_level", "activity_state", "freshness_state"},
		HxTarget:        "#non-human-identities-inventory",
	}
}
