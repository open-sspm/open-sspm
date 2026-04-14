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
	Page           int
}

func ParseCredentialsQuery(values url.Values, sources []SourceSelection) CredentialsQuery {
	return CredentialsQuery{
		Source:         canonicalSourceSelection(values, sources),
		Q:              strings.TrimSpace(values.Get("q")),
		CredentialKind: strings.TrimSpace(values.Get("credential_kind")),
		Status:         strings.TrimSpace(values.Get("status")),
		RiskLevel:      NormalizeCredentialRiskLevel(values.Get("risk_level")),
		ExpiryState:    normalizeExpiryState(values.Get("expiry_state")),
		ExpiresInDays:  parseNonNegativeInt(values.Get("expires_in_days"), 3650),
		Page:           parsePage(values.Get("page")),
	}
}

func NormalizeCredentialRiskLevel(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "critical":
		return "critical"
	case "high":
		return "high"
	case "medium":
		return "medium"
	case "low":
		return "low"
	default:
		return ""
	}
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
	setIfPage(values, q.Page)
	return values
}

func (q CredentialsQuery) Href() string {
	return encodeURL("/credentials", q.Values())
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
	q.CredentialKind = strings.TrimSpace(kind)
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

func (q CredentialsQuery) ClearFilters() CredentialsQuery {
	q.Source = SourceSelection{}
	q.CredentialKind = ""
	q.Status = ""
	q.RiskLevel = ""
	q.ExpiryState = ""
	q.ExpiresInDays = 0
	q.Page = 1
	return q
}

func (q CredentialsQuery) HasFilters() bool {
	return q.Source.Kind != "" ||
		strings.TrimSpace(q.Q) != "" ||
		strings.TrimSpace(q.CredentialKind) != "" ||
		strings.TrimSpace(q.Status) != "" ||
		strings.TrimSpace(q.RiskLevel) != "" ||
		strings.TrimSpace(q.ExpiryState) != "" ||
		q.ExpiresInDays > 0
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
	return count
}
