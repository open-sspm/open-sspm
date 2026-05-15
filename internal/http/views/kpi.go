package views

import "strings"

// KPICard is one card in a list-page KPI strip. Each card is either a clickable
// active-filter trigger (Href set) or a static stat (Href empty). Cards know
// whether they reflect the current filter state via Active so the strip can
// render the dark "this is the active filter" ring from the mockup.
type KPICard struct {
	Label      string
	Metric     string
	MetricTone string
	Caption    string
	Breakdown  []KPIBreakdownSegment
	Trend      string
	Href       string
	HxTarget   string
	Active     bool
}

// KPIBreakdownSegment is one slice of the optional segmented bar beneath a
// card's metric. The strip composes the bar by proportional segment widths;
// zero-count segments are omitted at render time.
type KPIBreakdownSegment struct {
	Label string
	Count int64
	Tone  string
}

// KPICardClass returns the wrapper class for a card. The active variant carries
// a darker border so the operator can tell at a glance which filter the page
// is currently scoped to.
func KPICardClass(card KPICard) string {
	base := "osspm-kpi-card"
	if card.Href != "" {
		base += " osspm-kpi-card-clickable"
	}
	if card.Active {
		base += " osspm-kpi-card-active"
	}
	return base
}

// KPIMetricClass returns the class for the big metric number. Tone is applied
// only when non-empty so a calm inventory reads calm.
func KPIMetricClass(tone string) string {
	switch strings.TrimSpace(tone) {
	case "danger":
		return "osspm-kpi-metric osspm-kpi-metric-danger"
	case "warn":
		return "osspm-kpi-metric osspm-kpi-metric-warn"
	default:
		return "osspm-kpi-metric"
	}
}

// KPIBreakdownSegmentClass maps a segment tone to the bar-fill color class.
func KPIBreakdownSegmentClass(tone string) string {
	switch strings.TrimSpace(tone) {
	case "critical":
		return "osspm-kpi-breakdown-seg osspm-kpi-breakdown-seg-critical"
	case "warn":
		return "osspm-kpi-breakdown-seg osspm-kpi-breakdown-seg-warn"
	case "ok":
		return "osspm-kpi-breakdown-seg osspm-kpi-breakdown-seg-ok"
	default:
		return "osspm-kpi-breakdown-seg osspm-kpi-breakdown-seg-neutral"
	}
}

// KPIBreakdownTotal sums the visible segment counts. Used to compute the
// relative widths of each segment in the bar at render time.
func KPIBreakdownTotal(segments []KPIBreakdownSegment) int64 {
	var total int64
	for _, s := range segments {
		if s.Count > 0 {
			total += s.Count
		}
	}
	return total
}

// KPIBreakdownSegmentStyle returns the inline flex-grow style for one segment
// so the bar fills proportionally to counts without a JS layout pass.
func KPIBreakdownSegmentStyle(seg KPIBreakdownSegment, total int64) string {
	if total <= 0 || seg.Count <= 0 {
		return "flex-grow: 0;"
	}
	return "flex-grow: " + FormatInt64(seg.Count) + ";"
}

func kpiLegendCountClass(tone string) string {
	switch strings.TrimSpace(tone) {
	case "critical":
		return "osspm-kpi-breakdown-legend-count osspm-kpi-breakdown-legend-count-critical"
	case "warn":
		return "osspm-kpi-breakdown-legend-count osspm-kpi-breakdown-legend-count-warn"
	case "ok":
		return "osspm-kpi-breakdown-legend-count osspm-kpi-breakdown-legend-count-ok"
	default:
		return "osspm-kpi-breakdown-legend-count"
	}
}
