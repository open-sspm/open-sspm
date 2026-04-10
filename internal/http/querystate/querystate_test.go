package querystate

import (
	"net/url"
	"testing"
)

func TestParseIdentitiesQuery(t *testing.T) {
	sources := []SourceSelection{
		{Kind: "github", Name: "acme"},
		{Kind: "okta", Name: "acme"},
		{Kind: "entra", Name: "tenant-1"},
	}

	t.Run("infers unique kind from source name", func(t *testing.T) {
		query := ParseIdentitiesQuery(url.Values{
			"source_name": []string{"tenant-1"},
		}, sources)
		if query.Source.Kind != "entra" || query.Source.Name != "tenant-1" {
			t.Fatalf("source = %#v", query.Source)
		}
	})

	t.Run("keeps ambiguous source name without kind", func(t *testing.T) {
		query := ParseIdentitiesQuery(url.Values{
			"source_name": []string{"acme"},
		}, sources)
		if query.Source.Kind != "" || query.Source.Name != "acme" {
			t.Fatalf("source = %#v", query.Source)
		}
	})

	t.Run("drops unknown selection", func(t *testing.T) {
		query := ParseIdentitiesQuery(url.Values{
			"source_kind": []string{"missing"},
			"source_name": []string{"ghost"},
		}, sources)
		if query.Source.Kind != "" || query.Source.Name != "" {
			t.Fatalf("source = %#v", query.Source)
		}
	})

	t.Run("normalizes booleans and sort defaults", func(t *testing.T) {
		query := ParseIdentitiesQuery(url.Values{
			"q":                 []string{" alice "},
			"privileged":        []string{"true"},
			"show_first_seen":   []string{"1"},
			"show_link_quality": []string{"yes"},
			"show_link_reason":  []string{"on"},
			"sort_by":           []string{"identity"},
			"page":              []string{"0"},
		}, sources)
		if query.Q != "alice" || !query.PrivilegedOnly || !query.ShowFirstSeen || !query.ShowLinkQuality || !query.ShowLinkReason {
			t.Fatalf("query = %#v", query)
		}
		if query.SortDir != "desc" || query.Page != 1 {
			t.Fatalf("sort/page = (%q, %d)", query.SortDir, query.Page)
		}
	})

	t.Run("encodes stable href and omits page one", func(t *testing.T) {
		href := ParseIdentitiesQuery(url.Values{
			"source_name":    []string{"tenant-1"},
			"q":              []string{"alice"},
			"activity_state": []string{"recent"},
			"page":           []string{"1"},
		}, sources).Href()
		want := "/identities?activity_state=recent&q=alice&source_kind=entra&source_name=tenant-1"
		if href != want {
			t.Fatalf("href = %q, want %q", href, want)
		}
	})
}

func TestIdentitiesQueryMutations(t *testing.T) {
	query := IdentitiesQuery{
		Q:              "alice",
		ActivityState:  "stale",
		PrivilegedOnly: true,
		Page:           4,
	}

	if got := query.ClearQuery(); got.Q != "" || got.Page != 1 {
		t.Fatalf("ClearQuery() = %#v", got)
	}
	if got := query.WithActivityState("recent"); got.ActivityState != "recent" || got.Page != 1 {
		t.Fatalf("WithActivityState() = %#v", got)
	}
	if got := query.TogglePrivilegedOnly(); got.PrivilegedOnly || got.Page != 1 {
		t.Fatalf("TogglePrivilegedOnly() = %#v", got)
	}
	if got := query.WithSourceName("tenant-1").WithSourceKind(""); got.Source.Kind != "" || got.Source.Name != "" || got.Page != 1 {
		t.Fatalf("WithSourceKind(\"\") = %#v", got)
	}
	if !query.HasFilters() {
		t.Fatalf("expected active filters")
	}
}

func TestParseCredentialsQuery(t *testing.T) {
	sources := []SourceSelection{
		{Kind: "github", Name: "acme"},
		{Kind: "entra", Name: "tenant-1"},
	}

	query := ParseCredentialsQuery(url.Values{
		"source_name":     []string{"acme"},
		"risk_level":      []string{"High"},
		"expiry_state":    []string{"expired"},
		"expires_in_days": []string{"99999"},
		"page":            []string{"3"},
	}, sources)
	if query.Source.Kind != "github" || query.Source.Name != "" {
		t.Fatalf("source = %#v", query.Source)
	}
	if query.RiskLevel != "high" || query.ExpiryState != "expired" || query.ExpiresInDays != 3650 || query.Page != 3 {
		t.Fatalf("query = %#v", query)
	}
	if href := query.WithRiskLevel("critical").WithPage(1).Href(); href != "/credentials?expires_in_days=3650&expiry_state=expired&risk_level=critical&source_kind=github" {
		t.Fatalf("href = %q", href)
	}
	if href := query.WithRiskLevel("").WithExpiryState("").WithExpiresInDays(0).WithStatus("pending_approval").WithPage(1).Href(); href != "/credentials?source_kind=github&status=pending_approval" {
		t.Fatalf("href = %q", href)
	}
}

func TestAppAssetsQuery(t *testing.T) {
	sources := []SourceSelection{{Kind: "github", Name: "acme"}}

	t.Run("infers unique kind from source name", func(t *testing.T) {
		query := ParseAppAssetsQuery(url.Values{
			"source_name": []string{"acme"},
			"asset_kind":  []string{"repository"},
			"q":           []string{"api"},
			"page":        []string{"2"},
		}, sources)
		if query.Source.Kind != "github" || query.Source.Name != "" {
			t.Fatalf("source = %#v", query.Source)
		}
		want := "/app-assets?asset_kind=repository&page=2&q=api&source_kind=github"
		if query.Href() != want {
			t.Fatalf("href = %q, want %q", query.Href(), want)
		}
	})

	t.Run("drops unknown source selection back to all configured", func(t *testing.T) {
		query := ParseAppAssetsQuery(url.Values{
			"source_kind": []string{"missing"},
			"source_name": []string{"ghost"},
			"asset_kind":  []string{"repository"},
			"q":           []string{"api"},
		}, sources)
		if query.Source.Kind != "" || query.Source.Name != "" {
			t.Fatalf("source = %#v", query.Source)
		}
		if query.Href() != "/app-assets?asset_kind=repository&q=api" {
			t.Fatalf("href = %q", query.Href())
		}
	})

	t.Run("identifies the connected apps slice from typed filters", func(t *testing.T) {
		query := ParseAppAssetsQuery(url.Values{
			"source_kind": []string{"google_workspace"},
			"asset_kind":  []string{"google_oauth_client"},
		}, nil)
		if !query.IsConnectedAppsSlice() {
			t.Fatalf("expected connected apps slice: %#v", query)
		}
		if query.WithSourceKind("github").IsConnectedAppsSlice() {
			t.Fatalf("unexpected connected apps slice after source change: %#v", query)
		}
	})
}

func TestParseDiscoveryQueries(t *testing.T) {
	sources := []SourceSelection{
		{Kind: "okta", Name: "acme.okta.com"},
		{Kind: "entra", Name: "tenant-1"},
	}

	apps := ParseDiscoveryAppsQuery(url.Values{
		"source_name":   []string{"tenant-1"},
		"managed_state": []string{"managed"},
		"risk_level":    []string{"critical"},
		"page":          []string{"2"},
	}, sources)
	if apps.Source.Kind != "entra" || apps.Source.Name != "" {
		t.Fatalf("source = %#v", apps.Source)
	}
	wantApps := "/discovery/apps?managed_state=managed&page=2&risk_level=critical&source_kind=entra"
	if apps.Href() != wantApps {
		t.Fatalf("href = %q, want %q", apps.Href(), wantApps)
	}

	hotspots := ParseDiscoveryHotspotsQuery(url.Values{
		"source_name": []string{"acme.okta.com"},
	}, sources)
	if hotspots.Source.Kind != "okta" || hotspots.Source.Name != "" {
		t.Fatalf("source = %#v", hotspots.Source)
	}
	if hotspots.Href() != "/discovery/hotspots?source_kind=okta" {
		t.Fatalf("href = %q", hotspots.Href())
	}

	t.Run("drops unknown selection back to all configured", func(t *testing.T) {
		query := ParseDiscoveryAppsQuery(url.Values{
			"source_kind": []string{"missing"},
			"source_name": []string{"ghost"},
			"risk_level":  []string{"critical"},
		}, sources)
		if query.Source.Kind != "" || query.Source.Name != "" {
			t.Fatalf("source = %#v", query.Source)
		}
		if query.Href() != "/discovery/apps?risk_level=critical" {
			t.Fatalf("href = %q", query.Href())
		}
	})
}

func TestConnectedAppsQuery(t *testing.T) {
	t.Run("keeps canonical governance state hrefs", func(t *testing.T) {
		query := ParseConnectedAppsQuery(url.Values{
			"q":                []string{"drive"},
			"governance_state": []string{"in_review"},
			"page":             []string{"2"},
		})
		want := "/app-assets?asset_kind=google_oauth_client&governance_state=in_review&page=2&q=drive&source_kind=google_workspace"
		if query.Href() != want {
			t.Fatalf("href = %q, want %q", query.Href(), want)
		}
		if got := query.WithGovernanceState("").ClearQuery().Href(); got != "/app-assets?asset_kind=google_oauth_client&source_kind=google_workspace" {
			t.Fatalf("href = %q", got)
		}
	})

	t.Run("maps legacy review state aliases to canonical governance hrefs", func(t *testing.T) {
		query := ParseConnectedAppsQuery(url.Values{
			"q":            []string{"drive"},
			"review_state": []string{"needs_revocation"},
			"page":         []string{"2"},
		})
		want := "/app-assets?asset_kind=google_oauth_client&governance_state=action_required&page=2&q=drive&source_kind=google_workspace"
		if query.Href() != want {
			t.Fatalf("href = %q, want %q", query.Href(), want)
		}

		query = ParseConnectedAppsQuery(url.Values{
			"governance_state": []string{"under_review"},
		})
		if query.GovernanceState != "in_review" {
			t.Fatalf("governance state = %q, want %q", query.GovernanceState, "in_review")
		}
	})
}

func TestParseNonHumanAccessQuery(t *testing.T) {
	sources := []SourceSelection{
		{Kind: "github", Name: "acme"},
		{Kind: "entra", Name: "tenant-1"},
	}

	t.Run("normalizes canonical filters and sort defaults", func(t *testing.T) {
		query := ParseNonHumanAccessQuery(url.Values{
			"source_name":      []string{"tenant-1"},
			"q":                []string{" svc "},
			"principal_type":   []string{"SERVICE"},
			"owner_presence":   []string{"unknown"},
			"governance_state": []string{"action_required"},
			"risk_level":       []string{"HIGH"},
			"activity_state":   []string{"stale"},
			"freshness_state":  []string{"STALE"},
			"sort_by":          []string{"risk"},
			"page":             []string{"0"},
		}, sources)

		if query.Source.Kind != "entra" || query.Source.Name != "tenant-1" {
			t.Fatalf("source = %#v", query.Source)
		}
		if query.Q != "svc" || query.PrincipalType != "service" || query.OwnerPresence != "unknown" {
			t.Fatalf("query = %#v", query)
		}
		if query.GovernanceState != "action_required" || query.RiskLevel != "high" || query.ActivityState != "stale" || query.FreshnessState != "stale" {
			t.Fatalf("query = %#v", query)
		}
		if query.SortDir != "desc" || query.Page != 1 {
			t.Fatalf("sort/page = (%q, %d)", query.SortDir, query.Page)
		}
		if got := query.TrackingSignature(); got != "activity_state=stale&freshness_state=stale&governance_state=action_required&owner_presence=unknown&principal_type=service&q=svc&risk_level=high&sort_by=risk&sort_dir=desc&source_kind=entra&source_name=tenant-1" {
			t.Fatalf("signature = %q", got)
		}
	})

	t.Run("drops unsupported legacy aliases from hrefs", func(t *testing.T) {
		query := ParseNonHumanAccessQuery(url.Values{
			"source_kind":      []string{"entra"},
			"source_name":      []string{"tenant-1"},
			"principal_type":   []string{"legacy"},
			"owner_presence":   []string{"missing"},
			"governance_state": []string{"under_review"},
			"sort_by":          []string{"owner"},
			"sort_dir":         []string{"sideways"},
			"page":             []string{"2"},
		}, sources)

		want := "/non-human-access?page=2&sort_by=owner&sort_dir=asc&source_kind=entra&source_name=tenant-1"
		if query.Href() != want {
			t.Fatalf("href = %q, want %q", query.Href(), want)
		}
		if query.PrincipalType != "" || query.OwnerPresence != "" || query.GovernanceState != "" {
			t.Fatalf("expected unsupported aliases to be dropped: %#v", query)
		}
	})
}

func TestBasicListQuery(t *testing.T) {
	normalizeState := func(raw string) string {
		switch raw {
		case "active", "inactive":
			return raw
		default:
			return ""
		}
	}

	t.Run("normalizes aliases and encodes stable hrefs", func(t *testing.T) {
		query := ParseBasicListQuery("/accounts/okta", url.Values{
			"q":      []string{"alice"},
			"status": []string{"active"},
			"page":   []string{"2"},
		}, BasicListOptions{
			StateAliases:   []string{"status"},
			NormalizeState: normalizeState,
		})
		if query.State != "active" || query.Page != 2 {
			t.Fatalf("query = %#v", query)
		}
		if query.Href() != "/accounts/okta?page=2&q=alice&state=active" {
			t.Fatalf("href = %q", query.Href())
		}
		if got := query.ClearQuery().WithPage(1).Href(); got != "/accounts/okta?state=active" {
			t.Fatalf("href = %q", got)
		}
	})

	t.Run("prefers explicit state and drops unknown aliases", func(t *testing.T) {
		query := ParseBasicListQuery("/accounts/okta", url.Values{
			"state":  []string{"inactive"},
			"status": []string{"invalid"},
		}, BasicListOptions{
			StateAliases:   []string{"status"},
			NormalizeState: normalizeState,
		})
		if query.State != "inactive" {
			t.Fatalf("state = %q", query.State)
		}

		query = ParseBasicListQuery("/accounts/okta", url.Values{
			"status": []string{"invalid"},
		}, BasicListOptions{
			StateAliases:   []string{"status"},
			NormalizeState: normalizeState,
		})
		if query.State != "" || query.HasFilters() {
			t.Fatalf("query = %#v", query)
		}
	})
}
