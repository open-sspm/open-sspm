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
	Q           string
	ReviewState string
	Page        int
}

func ParseConnectedAppsQuery(values url.Values) ConnectedAppsQuery {
	return ConnectedAppsQuery{
		Q:           strings.TrimSpace(values.Get("q")),
		ReviewState: NormalizeConnectedAppReviewState(values.Get("review_state"), true),
		Page:        parsePage(values.Get("page")),
	}
}

func NormalizeConnectedAppReviewState(raw string, allowBlank bool) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		if allowBlank {
			return ""
		}
		return ""
	case "unreviewed":
		return "unreviewed"
	case "under_review":
		return "under_review"
	case "sanctioned":
		return "sanctioned"
	case "needs_revocation":
		return "needs_revocation"
	case "ticketed":
		return "ticketed"
	default:
		return ""
	}
}

func (q ConnectedAppsQuery) Values() url.Values {
	values := url.Values{}
	values.Set("source_kind", connectedAppsSourceKey)
	values.Set("asset_kind", connectedAppsAssetKind)
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "review_state", q.ReviewState)
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

func (q ConnectedAppsQuery) WithReviewState(reviewState string) ConnectedAppsQuery {
	q.ReviewState = NormalizeConnectedAppReviewState(reviewState, true)
	q.Page = 1
	return q
}

func (q ConnectedAppsQuery) HasFilters() bool {
	return strings.TrimSpace(q.Q) != "" || strings.TrimSpace(q.ReviewState) != ""
}
