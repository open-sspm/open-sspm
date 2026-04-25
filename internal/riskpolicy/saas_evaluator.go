package riskpolicy

import (
	"errors"
	"strings"
	"time"
)

type SaaSInput struct {
	CanonicalKey                 string
	DisplayName                  string
	PrimaryDomain                string
	VendorName                   string
	SourceKind                   string
	SourceName                   string
	Category                     string
	Actors30d                    int64
	HasPrivilegedScope           bool
	HasConfidentialScope         bool
	ManagedState                 string
	ManagedReason                string
	OwnerIdentityID              int64
	GovernanceState              string
	ReviewDisposition            string
	FollowUpDueDate              *time.Time
	EffectiveBusinessCriticality string
	EffectiveDataClassification  string
	ConnectorBindingConfigured   bool
	ConnectorBindingEnabled      bool
	ConnectorBindingStale        bool
	ConnectorBindingHealthy      bool
}

type SaaSResult struct {
	RiskScore                    int
	RiskLevel                    string
	RiskRank                     int
	SuggestedBusinessCriticality string
	SuggestedDataClassification  string
	EffectiveBusinessCriticality string
	EffectiveDataClassification  string
	Signals                      []RiskSignal
}

func EvaluateSaaS(input SaaSInput) (SaaSResult, error) {
	registry, err := BuiltinRegistry()
	if err != nil {
		return SaaSResult{}, err
	}
	return registry.EvaluateSaaS(input)
}

func (r *Registry) EvaluateSaaS(input SaaSInput) (SaaSResult, error) {
	if r == nil {
		return SaaSResult{}, errors.New("risk policy registry is nil")
	}

	input = normalizeSaaSInput(input)
	result := SaaSResult{
		Signals: make([]RiskSignal, 0, 6),
	}
	foundPack := false

	for _, pack := range r.packs {
		if pack.Policy.Metadata.Domain != DomainSaaS || pack.Policy.Spec.Inputs.Schema != "saas_app_risk_input.v1" {
			continue
		}
		foundPack = true

		activation := saasActivation(input, pack.Policy.Spec.Constants, pack.Policy.Spec.Scoring.Base)
		businessCriticality, err := evaluateSuggestionRules(pack, pack.Policy.Spec.Suggestions.BusinessCriticality, activation)
		if err != nil {
			return SaaSResult{}, err
		}
		dataClassification, err := evaluateSuggestionRules(pack, pack.Policy.Spec.Suggestions.DataClassification, activation)
		if err != nil {
			return SaaSResult{}, err
		}

		if businessCriticality != "" {
			result.SuggestedBusinessCriticality = businessCriticality
		}
		if dataClassification != "" {
			result.SuggestedDataClassification = dataClassification
		}
		input.EffectiveBusinessCriticality = effectiveBusinessCriticality(input.EffectiveBusinessCriticality, result.SuggestedBusinessCriticality)
		input.EffectiveDataClassification = effectiveDataClassification(input.EffectiveDataClassification, result.SuggestedDataClassification)
		result.EffectiveBusinessCriticality = input.EffectiveBusinessCriticality
		result.EffectiveDataClassification = input.EffectiveDataClassification

		score := pack.Policy.Spec.Scoring.Base
		for _, rule := range pack.Policy.Spec.Scoring.Rules {
			activation = saasActivation(input, pack.Policy.Spec.Constants, score)
			matched, err := pack.evaluateBool(rule.ID, activation)
			if err != nil {
				return SaaSResult{}, err
			}
			if !matched {
				continue
			}

			score += rule.Points
			if rule.Signal.Severity != "" {
				result.Signals = append(result.Signals, RiskSignal{
					ID:                rule.ID,
					Domain:            DomainSaaS,
					Severity:          rule.Signal.Severity,
					ScoreDelta:        rule.Points,
					Title:             rule.Signal.Title,
					Evidence:          rule.Signal.Evidence,
					PolicyPackID:      pack.Policy.Metadata.ID,
					PolicyPackVersion: pack.Policy.Metadata.Version,
				})
			}
		}

		result.RiskScore = clampScore(score, pack.Policy.Spec.Scoring.Max)
		activation = saasActivation(input, pack.Policy.Spec.Constants, result.RiskScore)
		level, err := evaluateLevelRules(pack, pack.Policy.Spec.Levels, activation)
		if err != nil {
			return SaaSResult{}, err
		}
		if level != "" {
			result.RiskLevel = level
		}
	}

	if !foundPack {
		return SaaSResult{}, errors.New("saas risk policy pack not found")
	}
	if result.RiskLevel == "" {
		result.RiskLevel = SeverityLow
	}
	result.RiskRank = SeverityRank(result.RiskLevel)
	return result, nil
}

func evaluateSuggestionRules(pack CompiledPack, rules []SuggestionRule, activation map[string]any) (string, error) {
	for _, rule := range rules {
		matched, err := pack.evaluateBool(rule.ID, activation)
		if err != nil {
			return "", err
		}
		if matched {
			return rule.Level, nil
		}
	}
	return "", nil
}

func evaluateLevelRules(pack CompiledPack, rules []LevelRule, activation map[string]any) (string, error) {
	for _, rule := range rules {
		matched, err := pack.evaluateBool("level:"+rule.Level, activation)
		if err != nil {
			return "", err
		}
		if matched {
			return rule.Level, nil
		}
	}
	return "", nil
}

func normalizeSaaSInput(input SaaSInput) SaaSInput {
	input.CanonicalKey = strings.ToLower(strings.TrimSpace(input.CanonicalKey))
	input.DisplayName = strings.TrimSpace(input.DisplayName)
	input.PrimaryDomain = strings.ToLower(strings.TrimSpace(input.PrimaryDomain))
	input.VendorName = strings.TrimSpace(input.VendorName)
	input.SourceKind = strings.ToLower(strings.TrimSpace(input.SourceKind))
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.Category = strings.ToLower(strings.TrimSpace(input.Category))
	input.ManagedState = strings.ToLower(strings.TrimSpace(input.ManagedState))
	input.ManagedReason = strings.ToLower(strings.TrimSpace(input.ManagedReason))
	input.GovernanceState = strings.ToLower(strings.TrimSpace(input.GovernanceState))
	input.ReviewDisposition = strings.ToLower(strings.TrimSpace(input.ReviewDisposition))
	input.EffectiveBusinessCriticality = strings.ToLower(strings.TrimSpace(input.EffectiveBusinessCriticality))
	input.EffectiveDataClassification = strings.ToLower(strings.TrimSpace(input.EffectiveDataClassification))
	input.FollowUpDueDate = normalizeTimePtr(input.FollowUpDueDate)
	return input
}

func saasActivation(input SaaSInput, constants map[string][]string, score int) map[string]any {
	activation := map[string]any{
		"canonical_key":                  input.CanonicalKey,
		"display_name":                   input.DisplayName,
		"primary_domain":                 input.PrimaryDomain,
		"vendor_name":                    input.VendorName,
		"source_kind":                    input.SourceKind,
		"source_name":                    input.SourceName,
		"category":                       input.Category,
		"actors_30d":                     input.Actors30d,
		"has_privileged_scope":           input.HasPrivilegedScope,
		"has_confidential_scope":         input.HasConfidentialScope,
		"managed_state":                  input.ManagedState,
		"managed_reason":                 input.ManagedReason,
		"owner_identity_id":              input.OwnerIdentityID,
		"governance_state":               input.GovernanceState,
		"review_disposition":             input.ReviewDisposition,
		"follow_up_due_date":             nullableTime(input.FollowUpDueDate),
		"effective_business_criticality": input.EffectiveBusinessCriticality,
		"effective_data_classification":  input.EffectiveDataClassification,
		"connector_binding_configured":   input.ConnectorBindingConfigured,
		"connector_binding_enabled":      input.ConnectorBindingEnabled,
		"connector_binding_stale":        input.ConnectorBindingStale,
		"connector_binding_healthy":      input.ConnectorBindingHealthy,
		"score":                          int64(score),
	}
	for name, values := range constants {
		activation[name] = cloneSlice(values)
	}
	return activation
}

func effectiveBusinessCriticality(value, suggested string) string {
	if value != "" && value != "unknown" && validBusinessCriticality(value) {
		return value
	}
	return suggested
}

func effectiveDataClassification(value, suggested string) string {
	if value != "" && value != "unknown" && validDataClassification(value) {
		return value
	}
	return suggested
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
