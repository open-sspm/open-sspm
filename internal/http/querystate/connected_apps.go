package querystate

import (
	"net/url"
	"strings"
)

const (
	connectedAppsBasePath  = "/app-assets"
	connectedAppsSourceKey = "google_workspace"
	connectedAppsAssetKind = "google_oauth_client"
)

type ConnectedAppsQuery struct {
	Q               string
	GovernanceState string
	Page            int
}

func ParseConnectedAppsQuery(values url.Values) ConnectedAppsQuery {
	governanceState := parseConnectedAppsGovernanceState(values)
	return ConnectedAppsQuery{
		Q:               strings.TrimSpace(values.Get("q")),
		GovernanceState: governanceState,
		Page:            parsePage(values.Get("page")),
	}
}

func NormalizeConnectedAppGovernanceState(raw string, allowBlank bool) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		if allowBlank {
			return ""
		}
		return ""
	case "unreviewed":
		return "unreviewed"
	case "in_review", "under_review", "under review":
		return "in_review"
	case "approved", "sanctioned":
		return "approved"
	case "action_required", "action required", "needs_revocation", "needs revocation":
		return "action_required"
	case "ticketed":
		return "ticketed"
	default:
		return ""
	}
}

func parseConnectedAppsGovernanceState(values url.Values) string {
	for _, key := range []string{"governance_state", "review_state"} {
		if normalized := NormalizeConnectedAppGovernanceState(values.Get(key), true); normalized != "" {
			return normalized
		}
	}
	return ""
}

func (q ConnectedAppsQuery) Values() url.Values {
	values := url.Values{}
	values.Set("source_kind", connectedAppsSourceKey)
	values.Set("asset_kind", connectedAppsAssetKind)
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "governance_state", q.GovernanceState)
	setIfPage(values, q.Page)
	return values
}

func (q ConnectedAppsQuery) Href() string {
	return encodeURL(connectedAppsBasePath, q.Values())
}

func (q ConnectedAppsQuery) WithPage(page int) ConnectedAppsQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q ConnectedAppsQuery) ClearQuery() ConnectedAppsQuery {
	q.Q = ""
	q.Page = 1
	return q
}

func (q ConnectedAppsQuery) WithGovernanceState(governanceState string) ConnectedAppsQuery {
	q.GovernanceState = NormalizeConnectedAppGovernanceState(governanceState, true)
	q.Page = 1
	return q
}

func (q ConnectedAppsQuery) HasFilters() bool {
	return strings.TrimSpace(q.Q) != "" || strings.TrimSpace(q.GovernanceState) != ""
}
