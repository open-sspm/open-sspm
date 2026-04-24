package riskpolicy

import "strings"

const (
	SeverityLow      = "low"
	SeverityMedium   = "medium"
	SeverityHigh     = "high"
	SeverityCritical = "critical"
)

func NormalizeSeverity(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func SeverityRank(value string) int {
	switch NormalizeSeverity(value) {
	case SeverityLow:
		return 1
	case SeverityMedium:
		return 2
	case SeverityHigh:
		return 3
	case SeverityCritical:
		return 4
	default:
		return 0
	}
}

func ValidSeverity(value string) bool {
	return SeverityRank(value) > 0
}

func MaxSeverity(values ...string) string {
	maxRank := 0
	maxSeverity := SeverityLow
	for _, value := range values {
		rank := SeverityRank(value)
		if rank > maxRank {
			maxRank = rank
			maxSeverity = NormalizeSeverity(value)
		}
	}
	return maxSeverity
}
