package querystate

import (
	"net/url"
	"strings"
)

const nonHumanIdentitiesBasePath = "/non-human-identities"

func NonHumanIdentitiesBasePath() string {
	return nonHumanIdentitiesBasePath
}

type NonHumanIdentitiesQuery struct {
	Source          SourceSelection
	Q               string
	PrincipalType   string
	OwnerPresence   string
	GovernanceState string
	RiskLevel       string
	ActivityState   string
	FreshnessState  string
	SortBy          string
	SortDir         string
	Page            int
}

func ParseNonHumanIdentitiesQuery(values url.Values, sources []SourceSelection) NonHumanIdentitiesQuery {
	sortBy := normalizeNonHumanIdentitiesSortBy(values.Get("sort_by"))
	return NonHumanIdentitiesQuery{
		Source:          parseIdentitySourceSelection(values, sources),
		Q:               strings.TrimSpace(values.Get("q")),
		PrincipalType:   normalizeNonHumanIdentitiesPrincipalType(values.Get("principal_type")),
		OwnerPresence:   normalizeNonHumanIdentitiesOwnerPresence(values.Get("owner_presence")),
		GovernanceState: normalizeNonHumanIdentitiesGovernanceState(values.Get("governance_state")),
		RiskLevel:       NormalizeCredentialRiskLevel(values.Get("risk_level")),
		ActivityState:   normalizeIdentityActivityState(values.Get("activity_state")),
		FreshnessState:  normalizeNonHumanIdentitiesFreshnessState(values.Get("freshness_state")),
		SortBy:          sortBy,
		SortDir:         normalizeNonHumanIdentitiesSortDir(values.Get("sort_dir"), sortBy),
		Page:            parsePage(values.Get("page")),
	}
}

func (q NonHumanIdentitiesQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "source_kind", normalizeSourceKind(q.Source.Kind))
	setIfNotEmpty(values, "source_name", q.Source.Name)
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "principal_type", q.PrincipalType)
	setIfNotEmpty(values, "owner_presence", q.OwnerPresence)
	setIfNotEmpty(values, "governance_state", q.GovernanceState)
	setIfNotEmpty(values, "risk_level", q.RiskLevel)
	setIfNotEmpty(values, "activity_state", q.ActivityState)
	setIfNotEmpty(values, "freshness_state", q.FreshnessState)
	setIfNotEmpty(values, "sort_by", q.SortBy)
	if q.SortBy != "" {
		setIfNotEmpty(values, "sort_dir", q.SortDir)
	}
	setIfPage(values, q.Page)
	return values
}

func (q NonHumanIdentitiesQuery) Href() string {
	return encodeURL(nonHumanIdentitiesBasePath, q.Values())
}

func (q NonHumanIdentitiesQuery) TrackingSignature() string {
	return NonHumanIdentitiesTrackingSignature(q.Values())
}

func NonHumanIdentitiesTrackingSignature(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	cloned := make(url.Values, len(values))
	for key, entries := range values {
		cloned[key] = append([]string(nil), entries...)
	}
	cloned.Del("page")
	return cloned.Encode()
}

func (q NonHumanIdentitiesQuery) WithPage(page int) NonHumanIdentitiesQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q NonHumanIdentitiesQuery) ClearQuery() NonHumanIdentitiesQuery {
	q.Q = ""
	q.Page = 1
	return q
}

func (q NonHumanIdentitiesQuery) WithActivityState(state string) NonHumanIdentitiesQuery {
	q.ActivityState = normalizeIdentityActivityState(state)
	q.Page = 1
	return q
}

func (q NonHumanIdentitiesQuery) WithFreshnessState(state string) NonHumanIdentitiesQuery {
	q.FreshnessState = normalizeNonHumanIdentitiesFreshnessState(state)
	q.Page = 1
	return q
}

func (q NonHumanIdentitiesQuery) WithOwnerPresence(state string) NonHumanIdentitiesQuery {
	q.OwnerPresence = normalizeNonHumanIdentitiesOwnerPresence(state)
	q.Page = 1
	return q
}

func (q NonHumanIdentitiesQuery) WithRiskLevel(level string) NonHumanIdentitiesQuery {
	q.RiskLevel = NormalizeCredentialRiskLevel(level)
	q.Page = 1
	return q
}

func (q NonHumanIdentitiesQuery) ClearFilters() NonHumanIdentitiesQuery {
	q.Source = SourceSelection{}
	q.PrincipalType = ""
	q.OwnerPresence = ""
	q.GovernanceState = ""
	q.RiskLevel = ""
	q.ActivityState = ""
	q.FreshnessState = ""
	q.SortBy = ""
	q.SortDir = ""
	q.Page = 1
	return q
}

func (q NonHumanIdentitiesQuery) HasAdvancedFilters() bool {
	return q.PrincipalType != "" ||
		q.GovernanceState != "" ||
		q.FreshnessState != "" ||
		q.SortBy != ""
}

func (q NonHumanIdentitiesQuery) HasFilters() bool {
	return strings.TrimSpace(q.Q) != "" ||
		q.Source.Kind != "" ||
		q.Source.Name != "" ||
		q.PrincipalType != "" ||
		q.OwnerPresence != "" ||
		q.GovernanceState != "" ||
		q.RiskLevel != "" ||
		q.ActivityState != "" ||
		q.FreshnessState != ""
}

func (q NonHumanIdentitiesQuery) FilterCount() int {
	count := 0
	if q.RiskLevel != "" {
		count++
	}
	if q.OwnerPresence != "" {
		count++
	}
	if q.Source.Kind != "" {
		count++
	}
	if q.Source.Name != "" {
		count++
	}
	if q.ActivityState != "" {
		count++
	}
	if q.PrincipalType != "" {
		count++
	}
	if q.GovernanceState != "" {
		count++
	}
	if q.FreshnessState != "" {
		count++
	}
	if q.SortBy != "" {
		count++
	}
	return count
}

func normalizeNonHumanIdentitiesPrincipalType(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "service":
		return "service"
	case "bot":
		return "bot"
	case "app":
		return "app"
	default:
		return ""
	}
}

func normalizeNonHumanIdentitiesOwnerPresence(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "owned":
		return "owned"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeNonHumanIdentitiesGovernanceState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "unreviewed":
		return "unreviewed"
	case "in_review":
		return "in_review"
	case "approved":
		return "approved"
	case "action_required":
		return "action_required"
	case "ticketed":
		return "ticketed"
	default:
		return ""
	}
}

func normalizeNonHumanIdentitiesFreshnessState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "current":
		return "current"
	case "stale":
		return "stale"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeNonHumanIdentitiesSortBy(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "principal":
		return "principal"
	case "principal_type":
		return "principal_type"
	case "source":
		return "source"
	case "owner":
		return "owner"
	case "governance":
		return "governance"
	case "risk":
		return "risk"
	case "last_seen":
		return "last_seen"
	case "freshness":
		return "freshness"
	default:
		return ""
	}
}

func normalizeNonHumanIdentitiesSortDir(raw, sortBy string) string {
	sortBy = normalizeNonHumanIdentitiesSortBy(sortBy)
	if sortBy == "" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "asc":
		return "asc"
	case "desc":
		return "desc"
	default:
		switch sortBy {
		case "risk", "last_seen", "freshness":
			return "desc"
		default:
			return "asc"
		}
	}
}
