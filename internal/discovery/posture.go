package discovery

import (
	"strings"
	"time"
)

const minFreshnessWindow = 30 * time.Minute

func ManagedStateAndReason(input ManagedStateInput) (string, string) {
	if !input.HasPrimaryBinding {
		return ManagedStateUnmanaged, ManagedReasonNoBinding
	}
	if !input.ConnectorConfigured {
		return ManagedStateUnmanaged, ManagedReasonConnectorNotConfigured
	}
	if !input.ConnectorEnabled {
		return ManagedStateUnmanaged, ManagedReasonConnectorDisabled
	}

	now := input.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	window := input.FreshnessWindow
	if window < minFreshnessWindow {
		window = minFreshnessWindow
	}
	if !input.HasLastSuccessfulSync || input.LastSuccessfulSyncAt.IsZero() {
		return ManagedStateUnmanaged, ManagedReasonStaleSync
	}
	if now.Sub(input.LastSuccessfulSyncAt.UTC()) > window {
		return ManagedStateUnmanaged, ManagedReasonStaleSync
	}

	return ManagedStateManaged, ManagedReasonActiveBindingFreshSync
}

func RiskScoreAndLevel(input RiskInput) (int32, string) {
	score := 0
	managed := strings.EqualFold(strings.TrimSpace(input.ManagedState), ManagedStateManaged)

	if !managed {
		score += 45
	}
	if input.HasPrivilegedScopes {
		score += 20
	}
	if !input.HasOwner {
		score += 15
	}
	if input.Actors30d >= 50 {
		score += 10
	}
	bc := normalizeBusinessCriticality(input.BusinessCriticality)
	if !managed && (bc == "high" || bc == "critical") {
		score += 10
	}
	dc := normalizeDataClassification(input.DataClassification)
	if !managed && (dc == "confidential" || dc == "restricted") {
		score += 5
	}

	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	return int32(score), RiskLevelFromScore(int32(score))
}

func RiskLevelFromScore(score int32) string {
	switch {
	case score >= 80:
		return "critical"
	case score >= 60:
		return "high"
	case score >= 30:
		return "medium"
	default:
		return "low"
	}
}

func SuggestedBusinessCriticality(actors30d int64, hasPrivilegedScopes bool) string {
	switch {
	case actors30d >= 200:
		return "critical"
	case actors30d >= 50 || hasPrivilegedScopes:
		return "high"
	case actors30d >= 10:
		return "medium"
	default:
		return "low"
	}
}

func SuggestedDataClassification(hasPrivilegedScopes, hasConfidentialScopes bool) string {
	if hasPrivilegedScopes {
		return "restricted"
	}
	if hasConfidentialScopes {
		return "confidential"
	}
	return "internal"
}

func normalizeBusinessCriticality(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low", "medium", "high", "critical":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return "unknown"
	}
}

func normalizeDataClassification(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "public", "internal", "confidential", "restricted":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return "unknown"
	}
}
