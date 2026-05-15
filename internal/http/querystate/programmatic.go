package querystate

import (
	"net/url"
	"strings"
)

type AppAssetsQuery struct {
	Source    SourceSelection
	Q         string
	AssetKind string
	Page      int
}

func ParseAppAssetsQuery(values url.Values, sources []SourceSelection) AppAssetsQuery {
	assetKind := strings.TrimSpace(values.Get("asset_kind"))
	source := canonicalSourceSelection(values, sources)
	if normalizeSourceKind(values.Get("source_kind")) == connectedAppsSourceKey && assetKind == connectedAppsAssetKind {
		source = SourceSelection{Kind: connectedAppsSourceKey}
	}

	return AppAssetsQuery{
		Source:    source,
		Q:         strings.TrimSpace(values.Get("q")),
		AssetKind: assetKind,
		Page:      parsePage(values.Get("page")),
	}
}

func (q AppAssetsQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "source_kind", normalizeSourceKind(q.Source.Kind))
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "asset_kind", q.AssetKind)
	setIfPage(values, q.Page)
	return values
}

func (q AppAssetsQuery) Href() string {
	return encodeURL("/app-assets", q.Values())
}

func (q AppAssetsQuery) WithPage(page int) AppAssetsQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q AppAssetsQuery) ClearQuery() AppAssetsQuery {
	q.Q = ""
	q.Page = 1
	return q
}

func (q AppAssetsQuery) ClearFilters() AppAssetsQuery {
	q.Source = SourceSelection{}
	q.AssetKind = ""
	q.Page = 1
	return q
}

func (q AppAssetsQuery) IsConnectedAppsSlice() bool {
	return normalizeSourceKind(q.Source.Kind) == connectedAppsSourceKey &&
		strings.TrimSpace(q.AssetKind) == connectedAppsAssetKind
}

func (q AppAssetsQuery) HasFilters() bool {
	return q.Source.Kind != "" || strings.TrimSpace(q.Q) != "" || strings.TrimSpace(q.AssetKind) != ""
}

func (q AppAssetsQuery) FilterCount() int {
	count := 0
	if q.Source.Kind != "" {
		count++
	}
	if q.AssetKind != "" {
		count++
	}
	return count
}

type CredentialsQuery struct {
	Source         SourceSelection
	Q              string
	CredentialKind string
	Status         string
	RiskLevel      string
	ExpiryState    string
	ExpiresInDays  int
	Owner          string
	Asset          string
	NewerThanDays  int
	SortBy         string
	Page           int
}

func ParseCredentialsQuery(values url.Values, sources []SourceSelection) CredentialsQuery {
	q := CredentialsQuery{
		Source:         canonicalSourceSelection(values, sources),
		Q:              strings.TrimSpace(values.Get("q")),
		CredentialKind: NormalizeCredentialKindFilter(values.Get("credential_kind")),
		Status:         strings.TrimSpace(values.Get("status")),
		RiskLevel:      NormalizeCredentialRiskLevel(values.Get("risk_level")),
		ExpiryState:    normalizeExpiryState(values.Get("expiry_state")),
		ExpiresInDays:  parseNonNegativeInt(values.Get("expires_in_days"), 3650),
		Owner:          normalizeCredentialFreeText(values.Get("owner")),
		Asset:          normalizeCredentialFreeText(values.Get("asset")),
		NewerThanDays:  parseNonNegativeInt(strings.TrimSuffix(strings.TrimSpace(values.Get("newer_days")), "d"), 3650),
		SortBy:         normalizeCredentialsSortBy(values.Get("sort_by")),
		Page:           parsePage(values.Get("page")),
	}
	// When the operator lands on /credentials with no scoping at all, default to
	// non-expired credentials so the page reads as triage of what's still alive.
	// Expired credentials remain reachable via the "Expired" segment chip.
	// Important: any explicit filter (including expiry_state itself, even when
	// set to the empty "All" segment value) must opt out of this default to
	// avoid silently re-scoping user-driven searches and chip clicks.
	if !valuesHasAnyKey(values, credentialsQueryParamKeys...) {
		q.ExpiryState = "active"
	}
	return q
}

// credentialsQueryParamKeys lists every URL param CredentialsQuery reads. Keep
// this in sync with the struct fields and Values() — drift here silently
// re-scopes the default expiry_state.
var credentialsQueryParamKeys = []string{
	"q", "credential_kind", "status", "risk_level",
	"expiry_state", "expires_in_days", "owner", "asset",
	"newer_days", "sort_by", "source_kind", "page",
}

func valuesHasAnyKey(values url.Values, keys ...string) bool {
	for _, k := range keys {
		if _, ok := values[k]; ok {
			return true
		}
	}
	return false
}

func NormalizeCredentialRiskLevel(raw string) string {
	parts := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(raw)), func(r rune) bool {
		return r == ',' || r == '+' || r == '|'
	})
	seen := map[string]bool{}
	out := []string{}
	for _, part := range parts {
		part = strings.TrimSpace(strings.ReplaceAll(part, "-", "_"))
		switch part {
		case "critical", "high", "medium", "low":
			if !seen[part] {
				seen[part] = true
				out = append(out, part)
			}
		}
	}
	return strings.Join(out, ",")
}

func NormalizeCredentialKindFilter(raw string) string {
	parts := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(raw)), func(r rune) bool {
		return r == ',' || r == '+' || r == '|'
	})
	seen := map[string]bool{}
	out := []string{}
	appendKind := func(kind string) {
		if kind == "" || seen[kind] {
			return
		}
		seen[kind] = true
		out = append(out, kind)
	}
	for _, part := range parts {
		part = strings.TrimSpace(strings.ReplaceAll(part, "-", "_"))
		switch part {
		case "pat", "github_pat":
			appendKind("github_pat_request")
			appendKind("github_pat_fine_grained")
		case "certificate", "cert", "entra_certificate":
			appendKind("entra_certificate")
		case "secret", "client_secret", "entra_client_secret":
			appendKind("entra_client_secret")
		case "github_deploy_key", "github_pat_request", "github_pat_fine_grained":
			appendKind(part)
		}
	}
	return strings.Join(out, ",")
}

func normalizeCredentialFreeText(raw string) string {
	return strings.TrimSpace(raw)
}

func normalizeCredentialsSortBy(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "expires_soonest":
		return "expires_soonest"
	case "risk", "asset", "credential":
		return strings.ToLower(strings.TrimSpace(raw))
	}
	return "expires_soonest"
}

func normalizeExpiryState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "active":
		return "active"
	case "expired":
		return "expired"
	default:
		return ""
	}
}

func (q CredentialsQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "source_kind", normalizeSourceKind(q.Source.Kind))
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "credential_kind", q.CredentialKind)
	setIfNotEmpty(values, "status", q.Status)
	setIfNotEmpty(values, "risk_level", q.RiskLevel)
	setIfNotEmpty(values, "expiry_state", q.ExpiryState)
	setIfPositive(values, "expires_in_days", q.ExpiresInDays)
	setIfNotEmpty(values, "owner", q.Owner)
	setIfNotEmpty(values, "asset", q.Asset)
	setIfPositive(values, "newer_days", q.NewerThanDays)
	if normalizeCredentialsSortBy(q.SortBy) != "expires_soonest" {
		setIfNotEmpty(values, "sort_by", q.SortBy)
	}
	setIfPage(values, q.Page)
	return values
}

func (q CredentialsQuery) Href() string {
	return encodeURL("/credentials", q.Values())
}

func (q CredentialsQuery) ExportHref() string {
	return encodeURL("/credentials/export", q.Values())
}

func (q CredentialsQuery) WithPage(page int) CredentialsQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q CredentialsQuery) ClearQuery() CredentialsQuery {
	q.Q = ""
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithStatus(status string) CredentialsQuery {
	q.Status = strings.TrimSpace(status)
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithCredentialKind(kind string) CredentialsQuery {
	q.CredentialKind = NormalizeCredentialKindFilter(kind)
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithRiskLevel(riskLevel string) CredentialsQuery {
	q.RiskLevel = NormalizeCredentialRiskLevel(riskLevel)
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithExpiryState(expiryState string) CredentialsQuery {
	q.ExpiryState = normalizeExpiryState(expiryState)
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithExpiresInDays(days int) CredentialsQuery {
	q.ExpiresInDays = days
	if q.ExpiresInDays < 0 {
		q.ExpiresInDays = 0
	}
	if q.ExpiresInDays > 3650 {
		q.ExpiresInDays = 3650
	}
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithOwner(owner string) CredentialsQuery {
	q.Owner = normalizeCredentialFreeText(owner)
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithAsset(asset string) CredentialsQuery {
	q.Asset = normalizeCredentialFreeText(asset)
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithNewerThanDays(days int) CredentialsQuery {
	q.NewerThanDays = days
	if q.NewerThanDays < 0 {
		q.NewerThanDays = 0
	}
	if q.NewerThanDays > 3650 {
		q.NewerThanDays = 3650
	}
	q.Page = 1
	return q
}

func (q CredentialsQuery) WithSortBy(sortBy string) CredentialsQuery {
	q.SortBy = normalizeCredentialsSortBy(sortBy)
	q.Page = 1
	return q
}

func (q CredentialsQuery) ClearFilters() CredentialsQuery {
	q.Source = SourceSelection{}
	q.CredentialKind = ""
	q.Status = ""
	q.RiskLevel = ""
	q.ExpiryState = ""
	q.ExpiresInDays = 0
	q.Owner = ""
	q.Asset = ""
	q.NewerThanDays = 0
	q.Page = 1
	return q
}

// ClearSegments resets the segment-style filters while preserving source,
// search, credential kind, and pagination intent. It is the building block for
// the segment chips — each chip clears the others before applying its own.
func (q CredentialsQuery) ClearSegments() CredentialsQuery {
	q.Status = ""
	q.RiskLevel = ""
	q.ExpiryState = ""
	q.ExpiresInDays = 0
	q.Page = 1
	return q
}

func (q CredentialsQuery) SegmentActive() CredentialsQuery {
	q = q.ClearSegments()
	q.ExpiryState = "active"
	return q
}

func (q CredentialsQuery) SegmentExpired() CredentialsQuery {
	q = q.ClearSegments()
	q.ExpiryState = "expired"
	return q
}

func (q CredentialsQuery) SegmentExpiringSoon() CredentialsQuery {
	q = q.ClearSegments()
	q.ExpiryState = "active"
	q.ExpiresInDays = 30
	return q
}

func (q CredentialsQuery) SegmentCritical() CredentialsQuery {
	q = q.ClearSegments()
	q.RiskLevel = "critical,high"
	return q
}

func (q CredentialsQuery) SegmentPendingApproval() CredentialsQuery {
	q = q.ClearSegments()
	q.Status = "pending_approval"
	return q
}

// HasSegment reports whether any segment-style filter is currently active.
// Used to highlight the "All" chip when no segment is selected.
func (q CredentialsQuery) HasSegment() bool {
	return q.Status != "" || q.RiskLevel != "" || q.ExpiryState != "" || q.ExpiresInDays > 0
}

func (q CredentialsQuery) HasFilters() bool {
	return q.Source.Kind != "" ||
		strings.TrimSpace(q.Q) != "" ||
		strings.TrimSpace(q.CredentialKind) != "" ||
		strings.TrimSpace(q.Status) != "" ||
		strings.TrimSpace(q.RiskLevel) != "" ||
		strings.TrimSpace(q.ExpiryState) != "" ||
		q.ExpiresInDays > 0 ||
		strings.TrimSpace(q.Owner) != "" ||
		strings.TrimSpace(q.Asset) != "" ||
		q.NewerThanDays > 0 ||
		normalizeCredentialsSortBy(q.SortBy) != "expires_soonest"
}

func (q CredentialsQuery) FilterCount() int {
	count := 0
	if q.Source.Kind != "" {
		count++
	}
	if q.CredentialKind != "" {
		count++
	}
	if q.Status != "" {
		count++
	}
	if q.RiskLevel != "" {
		count++
	}
	if q.ExpiryState != "" {
		count++
	}
	if q.ExpiresInDays > 0 {
		count++
	}
	if q.Owner != "" {
		count++
	}
	if q.Asset != "" {
		count++
	}
	if q.NewerThanDays > 0 {
		count++
	}
	return count
}
