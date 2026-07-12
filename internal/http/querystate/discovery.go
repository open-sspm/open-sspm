package querystate

import (
	"net/url"
	"strings"

	discoverypkg "github.com/open-sspm/open-sspm/internal/discovery"
)

type DiscoveryAppsQuery struct {
	Source            SourceSelection
	Q                 string
	ManagedState      string
	RiskLevel         string
	ReviewDisposition string
	Page              int
}

func ParseDiscoveryAppsQuery(values url.Values, sources []SourceSelection) DiscoveryAppsQuery {
	return DiscoveryAppsQuery{
		Source:            canonicalSourceSelection(values, sources),
		Q:                 strings.TrimSpace(values.Get("q")),
		ManagedState:      normalizeDiscoveryManagedState(values.Get("managed_state")),
		RiskLevel:         normalizeDiscoveryRiskLevel(values.Get("risk_level")),
		ReviewDisposition: normalizeDiscoveryReviewDisposition(values.Get("review_state")),
		Page:              parsePage(values.Get("page")),
	}
}

func (q DiscoveryAppsQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "source_kind", normalizeSourceKind(q.Source.Kind))
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "managed_state", q.ManagedState)
	setIfNotEmpty(values, "risk_level", q.RiskLevel)
	setIfNotEmpty(values, "review_state", q.ReviewDisposition)
	setIfPage(values, q.Page)
	return values
}

func (q DiscoveryAppsQuery) Href() string {
	return encodeURL("/discovery/apps", q.Values())
}

func (q DiscoveryAppsQuery) WithPage(page int) DiscoveryAppsQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

type DiscoveryHotspotsQuery struct {
	Source SourceSelection
	Page   int
}

func ParseDiscoveryHotspotsQuery(values url.Values, sources []SourceSelection) DiscoveryHotspotsQuery {
	return DiscoveryHotspotsQuery{
		Source: canonicalSourceSelection(values, sources),
		Page:   parsePage(values.Get("page")),
	}
}

func (q DiscoveryHotspotsQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "source_kind", normalizeSourceKind(q.Source.Kind))
	setIfPage(values, q.Page)
	return values
}

func (q DiscoveryHotspotsQuery) Href() string {
	return encodeURL("/discovery/hotspots", q.Values())
}

func (q DiscoveryHotspotsQuery) ClearFilters() DiscoveryHotspotsQuery {
	q.Source = SourceSelection{}
	q.Page = 1
	return q
}

func (q DiscoveryHotspotsQuery) FilterCount() int {
	count := 0
	if q.Source.Kind != "" {
		count++
	}
	return count
}

func normalizeDiscoveryManagedState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case discoverypkg.ManagedStateManaged:
		return discoverypkg.ManagedStateManaged
	case discoverypkg.ManagedStateUnmanaged:
		return discoverypkg.ManagedStateUnmanaged
	default:
		return ""
	}
}

func normalizeDiscoveryRiskLevel(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low":
		return "low"
	case "medium":
		return "medium"
	case "high":
		return "high"
	case "critical":
		return "critical"
	default:
		return ""
	}
}

func normalizeDiscoveryReviewDisposition(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "unreviewed":
		return "unreviewed"
	case "under_review":
		return "under_review"
	case "sanctioned":
		return "sanctioned"
	case "tolerated":
		return "tolerated"
	case "replace":
		return "replace"
	default:
		return ""
	}
}
