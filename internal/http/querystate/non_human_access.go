package querystate

import (
	"net/url"
	"strings"
)

const nonHumanAccessBasePath = "/non-human-access"

func NonHumanAccessBasePath() string {
	return nonHumanAccessBasePath
}

type NonHumanAccessQuery struct {
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

func ParseNonHumanAccessQuery(values url.Values, sources []SourceSelection) NonHumanAccessQuery {
	sortBy := normalizeNonHumanAccessSortBy(values.Get("sort_by"))
	return NonHumanAccessQuery{
		Source:          parseIdentitySourceSelection(values, sources),
		Q:               strings.TrimSpace(values.Get("q")),
		PrincipalType:   normalizeNonHumanAccessPrincipalType(values.Get("principal_type")),
		OwnerPresence:   normalizeNonHumanAccessOwnerPresence(values.Get("owner_presence")),
		GovernanceState: normalizeNonHumanAccessGovernanceState(values.Get("governance_state")),
		RiskLevel:       NormalizeCredentialRiskLevel(values.Get("risk_level")),
		ActivityState:   normalizeIdentityActivityState(values.Get("activity_state")),
		FreshnessState:  normalizeNonHumanAccessFreshnessState(values.Get("freshness_state")),
		SortBy:          sortBy,
		SortDir:         normalizeNonHumanAccessSortDir(values.Get("sort_dir"), sortBy),
		Page:            parsePage(values.Get("page")),
	}
}

func (q NonHumanAccessQuery) Values() url.Values {
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

func (q NonHumanAccessQuery) Href() string {
	return encodeURL(nonHumanAccessBasePath, q.Values())
}

func (q NonHumanAccessQuery) TrackingSignature() string {
	return NonHumanAccessTrackingSignature(q.Values())
}

func NonHumanAccessTrackingSignature(values url.Values) string {
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

func (q NonHumanAccessQuery) WithPage(page int) NonHumanAccessQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q NonHumanAccessQuery) ClearQuery() NonHumanAccessQuery {
	q.Q = ""
	q.Page = 1
	return q
}

func (q NonHumanAccessQuery) WithActivityState(state string) NonHumanAccessQuery {
	q.ActivityState = normalizeIdentityActivityState(state)
	q.Page = 1
	return q
}

func (q NonHumanAccessQuery) WithFreshnessState(state string) NonHumanAccessQuery {
	q.FreshnessState = normalizeNonHumanAccessFreshnessState(state)
	q.Page = 1
	return q
}

func (q NonHumanAccessQuery) WithOwnerPresence(state string) NonHumanAccessQuery {
	q.OwnerPresence = normalizeNonHumanAccessOwnerPresence(state)
	q.Page = 1
	return q
}

func (q NonHumanAccessQuery) WithRiskLevel(level string) NonHumanAccessQuery {
	q.RiskLevel = NormalizeCredentialRiskLevel(level)
	q.Page = 1
	return q
}

func (q NonHumanAccessQuery) ToggleOwnerPresence(state string) NonHumanAccessQuery {
	if q.OwnerPresence == normalizeNonHumanAccessOwnerPresence(state) {
		return q.WithOwnerPresence("")
	}
	return q.WithOwnerPresence(state)
}

func (q NonHumanAccessQuery) ToggleRiskLevel(level string) NonHumanAccessQuery {
	if q.RiskLevel == NormalizeCredentialRiskLevel(level) {
		return q.WithRiskLevel("")
	}
	return q.WithRiskLevel(level)
}

func (q NonHumanAccessQuery) ToggleActivityState(state string) NonHumanAccessQuery {
	if q.ActivityState == normalizeIdentityActivityState(state) {
		return q.WithActivityState("")
	}
	return q.WithActivityState(state)
}

func (q NonHumanAccessQuery) ToggleFreshnessState(state string) NonHumanAccessQuery {
	if q.FreshnessState == normalizeNonHumanAccessFreshnessState(state) {
		return q.WithFreshnessState("")
	}
	return q.WithFreshnessState(state)
}

func (q NonHumanAccessQuery) HasAdvancedFilters() bool {
	return q.PrincipalType != "" ||
		q.GovernanceState != "" ||
		q.FreshnessState != "" ||
		q.SortBy != ""
}

func (q NonHumanAccessQuery) HasFilters() bool {
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

func normalizeNonHumanAccessPrincipalType(raw string) string {
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

func normalizeNonHumanAccessOwnerPresence(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "owned":
		return "owned"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeNonHumanAccessGovernanceState(raw string) string {
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

func normalizeNonHumanAccessFreshnessState(raw string) string {
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

func normalizeNonHumanAccessSortBy(raw string) string {
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

func normalizeNonHumanAccessSortDir(raw, sortBy string) string {
	sortBy = normalizeNonHumanAccessSortBy(sortBy)
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
