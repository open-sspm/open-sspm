package querystate

import (
	"net/url"
	"strings"
)

type AppsQuery struct {
	Q           string
	Integration string // "", "connected", "not_connected"
	Status      string // "", "ACTIVE", "INACTIVE", etc.
	Page        int
}

func ParseAppsQuery(values url.Values) AppsQuery {
	return AppsQuery{
		Q:           strings.TrimSpace(values.Get("q")),
		Integration: normalizeAppsIntegration(values.Get("integration")),
		Status:      normalizeAppsStatus(values.Get("status")),
		Page:        parsePage(values.Get("page")),
	}
}

func (q AppsQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "integration", q.Integration)
	setIfNotEmpty(values, "status", q.Status)
	setIfPage(values, q.Page)
	return values
}

func (q AppsQuery) Href() string {
	return encodeURL("/assigned-apps", q.Values())
}

func (q AppsQuery) WithPage(page int) AppsQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q AppsQuery) WithIntegration(integration string) AppsQuery {
	q.Integration = normalizeAppsIntegration(integration)
	q.Page = 1
	return q
}

func (q AppsQuery) WithStatus(status string) AppsQuery {
	q.Status = normalizeAppsStatus(status)
	q.Page = 1
	return q
}

func (q AppsQuery) ClearQuery() AppsQuery {
	q.Q = ""
	q.Page = 1
	return q
}

func (q AppsQuery) ClearPanelFilters() AppsQuery {
	q.Integration = ""
	q.Status = ""
	q.Page = 1
	return q
}

func (q AppsQuery) ClearFilters() AppsQuery {
	return q.ClearPanelFilters()
}

func (q AppsQuery) HasFilters() bool {
	return q.Q != "" || q.Integration != "" || q.Status != ""
}

func (q AppsQuery) FilterCount() int {
	count := 0
	if q.Integration != "" {
		count++
	}
	if q.Status != "" {
		count++
	}
	return count
}

func normalizeAppsIntegration(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "connected":
		return "connected"
	case "not_connected":
		return "not_connected"
	default:
		return ""
	}
}

func normalizeAppsStatus(raw string) string {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	return raw
}
