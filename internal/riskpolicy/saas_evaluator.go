package riskpolicy

import (
	"errors"
	"strings"
	"time"
)

type SaaSInput struct {
	CanonicalKey                  string
	DisplayName                   string
	PrimaryDomain                 string
	VendorName                    string
	SourceKind                    string
	SourceName                    string
	Category                      string
	Actors30d                     int64
	HasPrivilegedScope            bool
	HasConfidentialScope          bool
	ManagedState                  string
	ManagedReason                 string
	OwnerIdentityID               int64
	GovernanceState               string
	ReviewDisposition             string
	FollowUpDueDate               *time.Time
	ConfiguredBusinessCriticality string
	ConfiguredDataClassification  string
	ConnectorBindingConfigured    bool
	ConnectorBindingEnabled       bool
	ConnectorBindingStale         bool
	ConnectorBindingHealthy       bool
}

type SaaSResult struct {
	RiskScore                    int
	RiskLevel                    string
	RiskRank                     int
	SuggestedBusinessCriticality string
	SuggestedDataClassification  string
	EffectiveBusinessCriticality string
	EffectiveDataClassification  string
	PolicyPacks                  []PolicyPackRef
	Signals                      []RiskSignal
}

func (r *Registry) EvaluateSaaS(input SaaSInput) (SaaSResult, error) {
	if r == nil {
		return SaaSResult{}, errors.New("risk policy registry is nil")
	}

	input = normalizeSaaSInput(input)
	packs := r.packsFor(DomainSaaS, "saas_app_risk_input.v1")
	if len(packs) == 0 {
		return SaaSResult{}, errors.New("saas risk policy pack not found")
	}

	result := SaaSResult{
		Signals: make([]RiskSignal, 0, 6),
	}
	result.SuggestedBusinessCriticality = suggestedBusinessCriticality(input)
	result.SuggestedDataClassification = suggestedDataClassification(input)
	result.EffectiveBusinessCriticality = effectiveBusinessCriticality(input.ConfiguredBusinessCriticality, result.SuggestedBusinessCriticality)
	result.EffectiveDataClassification = effectiveDataClassification(input.ConfiguredDataClassification, result.SuggestedDataClassification)

	entity := saasEntityInput(input)
	for _, pack := range packs {
		evaluated, err := evaluateEntityPolicyPack(pack, entity)
		if err != nil {
			return SaaSResult{}, err
		}
		result.PolicyPacks = appendPolicyPackRef(result.PolicyPacks, pack.Policy.Metadata)
		result.Signals = appendEntitySignals(result.Signals, DomainSaaS, pack.Policy.Metadata, evaluated.Signals)
		result.RiskScore += evaluated.RiskScore
	}

	result.RiskScore = clampScore(result.RiskScore, 100)
	result.RiskLevel = saasRiskLevelFromScore(result.RiskScore)
	result.RiskRank = SeverityRank(result.RiskLevel)
	return result, nil
}

func normalizeSaaSInput(input SaaSInput) SaaSInput {
	input.CanonicalKey = strings.ToLower(strings.TrimSpace(input.CanonicalKey))
	input.DisplayName = strings.TrimSpace(input.DisplayName)
	input.PrimaryDomain = strings.ToLower(strings.TrimSpace(input.PrimaryDomain))
	input.VendorName = strings.ToLower(strings.TrimSpace(input.VendorName))
	input.SourceKind = normalizeSaaSSourceKind(input.SourceKind)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.Category = strings.ToLower(strings.TrimSpace(input.Category))
	input.ManagedState = strings.ToLower(strings.TrimSpace(input.ManagedState))
	input.ManagedReason = strings.ToLower(strings.TrimSpace(input.ManagedReason))
	input.GovernanceState = strings.ToLower(strings.TrimSpace(input.GovernanceState))
	input.ReviewDisposition = strings.ToLower(strings.TrimSpace(input.ReviewDisposition))
	input.ConfiguredBusinessCriticality = strings.ToLower(strings.TrimSpace(input.ConfiguredBusinessCriticality))
	input.ConfiguredDataClassification = strings.ToLower(strings.TrimSpace(input.ConfiguredDataClassification))
	input.FollowUpDueDate = normalizeTimePtr(input.FollowUpDueDate)
	return input
}

func normalizeSaaSSourceKind(kind string) string {
	switch normalized := strings.ToLower(strings.TrimSpace(kind)); normalized {
	case "aws_identity_center":
		return "aws"
	default:
		return normalized
	}
}

func saasEntityInput(input SaaSInput) map[string]any {
	return map[string]any{
		"canonical_key":                   input.CanonicalKey,
		"display_name":                    input.DisplayName,
		"primary_domain":                  input.PrimaryDomain,
		"vendor_name":                     input.VendorName,
		"source_kind":                     input.SourceKind,
		"source_name":                     input.SourceName,
		"category":                        input.Category,
		"actors_30d":                      input.Actors30d,
		"has_privileged_scope":            input.HasPrivilegedScope,
		"has_confidential_scope":          input.HasConfidentialScope,
		"managed_state":                   input.ManagedState,
		"managed_reason":                  input.ManagedReason,
		"owner_identity_id":               input.OwnerIdentityID,
		"governance_state":                input.GovernanceState,
		"review_disposition":              input.ReviewDisposition,
		"follow_up_due_date":              nullableTimeString(input.FollowUpDueDate),
		"configured_business_criticality": input.ConfiguredBusinessCriticality,
		"configured_data_classification":  input.ConfiguredDataClassification,
		"connector_binding_configured":    input.ConnectorBindingConfigured,
		"connector_binding_enabled":       input.ConnectorBindingEnabled,
		"connector_binding_stale":         input.ConnectorBindingStale,
		"connector_binding_healthy":       input.ConnectorBindingHealthy,
	}
}

func suggestedBusinessCriticality(input SaaSInput) string {
	switch {
	case input.Actors30d >= 200:
		return "critical"
	case input.Actors30d >= 50 || input.HasPrivilegedScope:
		return "high"
	case input.Actors30d >= 10:
		return "medium"
	default:
		return "low"
	}
}

func suggestedDataClassification(input SaaSInput) string {
	switch {
	case input.HasPrivilegedScope:
		return "restricted"
	case input.HasConfidentialScope:
		return "confidential"
	default:
		return "internal"
	}
}

func effectiveBusinessCriticality(value, suggested string) string {
	if validBusinessCriticality(value) {
		return strings.ToLower(strings.TrimSpace(value))
	}
	return suggested
}

func effectiveDataClassification(value, suggested string) string {
	if validDataClassification(value) {
		return strings.ToLower(strings.TrimSpace(value))
	}
	return suggested
}

func saasRiskLevelFromScore(score int) string {
	switch {
	case score >= 80:
		return SeverityCritical
	case score >= 60:
		return SeverityHigh
	case score >= 30:
		return SeverityMedium
	default:
		return SeverityLow
	}
}

func clampScore(score, maxScore int) int {
	if score < 0 {
		return 0
	}
	if maxScore > 0 && score > maxScore {
		return maxScore
	}
	return score
}
