package querystate

import (
	"net/url"
	"strings"
)

type ResourceAccessQuery struct {
	BasePath     string
	Q            string
	Permission   string
	Kind         string
	Link         string
	IdentityKind string
	SortBy       string
	SortDir      string
	Page         int
}

func ParseResourceAccessQuery(basePath string, values url.Values) ResourceAccessQuery {
	return ResourceAccessQuery{
		BasePath:     strings.TrimSpace(basePath),
		Q:            strings.TrimSpace(lastNonEmptyValue(values, "q")),
		Permission:   strings.TrimSpace(lastValue(values, "permission")),
		Kind:         strings.TrimSpace(lastValue(values, "kind")),
		Link:         normalizeResourceAccessLink(lastValue(values, "link")),
		IdentityKind: strings.ToLower(strings.TrimSpace(lastValue(values, "identity_kind"))),
		SortBy:       normalizeResourceAccessSort(lastValue(values, "sort_by")),
		SortDir:      normalizeResourceAccessSortDirection(lastValue(values, "sort_dir")),
		Page:         parsePage(lastValue(values, "page")),
	}
}

func (q ResourceAccessQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "permission", q.Permission)
	setIfNotEmpty(values, "kind", q.Kind)
	setIfNotEmpty(values, "link", q.Link)
	setIfNotEmpty(values, "identity_kind", q.IdentityKind)
	if q.SortBy != "identity" {
		setIfNotEmpty(values, "sort_by", q.SortBy)
	}
	if q.SortDir == "desc" {
		values.Set("sort_dir", q.SortDir)
	}
	setIfPage(values, q.Page)
	return values
}

func (q ResourceAccessQuery) WithSort(key, direction string) ResourceAccessQuery {
	q.SortBy = normalizeResourceAccessSort(key)
	q.SortDir = normalizeResourceAccessSortDirection(direction)
	q.Page = 1
	return q
}

func (q ResourceAccessQuery) Href() string {
	return encodeURL(q.BasePath, q.Values())
}

func (q ResourceAccessQuery) WithPage(page int) ResourceAccessQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q ResourceAccessQuery) FilterCount() int {
	count := 0
	for _, value := range []string{q.Permission, q.Kind, q.Link, q.IdentityKind} {
		if strings.TrimSpace(value) != "" {
			count++
		}
	}
	return count
}

func normalizeResourceAccessSort(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "account", "permission", "kind":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return "identity"
	}
}

func normalizeResourceAccessLink(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "orphan", "unlinked", "needs_anchor":
		return "unlinked"
	case "linked", "anchored":
		return "anchored"
	default:
		return value
	}
}

func normalizeResourceAccessSortDirection(raw string) string {
	if strings.EqualFold(strings.TrimSpace(raw), "desc") {
		return "desc"
	}
	return "asc"
}
