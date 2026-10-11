package querystate

import (
	"net/url"
	"strings"
)

type FindingsRulesQuery struct {
	BasePath   string
	Q          string
	Status     string
	Severity   string
	Monitoring string
}

func ParseFindingsRulesQuery(basePath string, values url.Values) FindingsRulesQuery {
	return FindingsRulesQuery{
		BasePath:   strings.TrimSpace(basePath),
		Q:          strings.TrimSpace(lastNonEmptyValue(values, "q")),
		Status:     normalizeEnum(lastValue(values, "status"), "pass", "fail", "unknown", "error", "not_applicable"),
		Severity:   normalizeEnum(lastValue(values, "severity"), "critical", "high", "medium", "low", "info"),
		Monitoring: normalizeEnum(lastValue(values, "monitoring"), "automated", "partial", "manual", "unsupported"),
	}
}

func (q FindingsRulesQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "status", q.Status)
	setIfNotEmpty(values, "severity", q.Severity)
	setIfNotEmpty(values, "monitoring", q.Monitoring)
	return values
}

func (q FindingsRulesQuery) Href() string {
	return encodeURL(q.BasePath, q.Values())
}

func (q FindingsRulesQuery) FilterCount() int {
	count := 0
	for _, value := range []string{q.Status, q.Severity, q.Monitoring} {
		if value != "" {
			count++
		}
	}
	return count
}

func normalizeEnum(raw string, allowed ...string) string {
	value := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(raw, "-", "_")))
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return ""
}
