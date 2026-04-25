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

	var matchedPack *CompiledPack
	for i := range r.packs {
		if r.packs[i].Policy.Metadata.Domain != DomainSaaS || r.packs[i].Policy.Spec.Inputs.Schema != "saas_app_risk_input.v1" {
			continue
		}
		if matchedPack != nil {
			return SaaSResult{}, errors.New("multiple saas risk policy packs found for saas_app_risk_input.v1")
		}
		matchedPack = &r.packs[i]
	}
	if matchedPack == nil {
		return SaaSResult{}, errors.New("saas risk policy pack not found")
	}

	globalPack := *matchedPack
	scopedRules := r.matchingSaaSScopedRules(input)
	activation := saasActivation(input, globalPack.Policy.Spec.Constants, globalPack.Policy.Spec.Scoring.Base)
	businessCriticality, err := evaluateSuggestionRules(globalPack, globalPack.Policy.Spec.Suggestions.BusinessCriticality, activation)
	if err != nil {
		return SaaSResult{}, err
	}
	dataClassification, err := evaluateSuggestionRules(globalPack, globalPack.Policy.Spec.Suggestions.DataClassification, activation)
	if err != nil {
		return SaaSResult{}, err
	}

	if businessCriticality != "" {
		result.SuggestedBusinessCriticality = businessCriticality
	}
	if dataClassification != "" {
		result.SuggestedDataClassification = dataClassification
	}
	for _, scopedRule := range scopedRules {
		result.SuggestedBusinessCriticality = maxBusinessCriticality(result.SuggestedBusinessCriticality, scopedRule.Rule.Suggestions.BusinessCriticality)
		result.SuggestedDataClassification = maxDataClassification(result.SuggestedDataClassification, scopedRule.Rule.Suggestions.DataClassification)
	}
	input.EffectiveBusinessCriticality = effectiveBusinessCriticality(input.EffectiveBusinessCriticality, result.SuggestedBusinessCriticality)
	input.EffectiveDataClassification = effectiveDataClassification(input.EffectiveDataClassification, result.SuggestedDataClassification)
	result.EffectiveBusinessCriticality = input.EffectiveBusinessCriticality
	result.EffectiveDataClassification = input.EffectiveDataClassification

	score := globalPack.Policy.Spec.Scoring.Base
	for _, rule := range globalPack.Policy.Spec.Scoring.Rules {
		activation = saasActivation(input, globalPack.Policy.Spec.Constants, score)
		matched, err := globalPack.evaluateBool(rule.ID, activation)
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
				PolicyPackID:      globalPack.Policy.Metadata.ID,
				PolicyPackVersion: globalPack.Policy.Metadata.Version,
			})
		}
	}

	for _, scopedRule := range scopedRules {
		for _, rule := range scopedRule.Rule.Rules {
			activation = saasActivation(input, scopedRule.Pack.Policy.Spec.Constants, score)
			matched, err := scopedRule.Pack.evaluateBool(scopedRule.Rule.ID+"/"+rule.ID, activation)
			if err != nil {
				return SaaSResult{}, err
			}
			if !matched {
				continue
			}

			score += rule.ScoreDelta
			result.Signals = append(result.Signals, RiskSignal{
				ID:                rule.ID,
				Domain:            DomainSaaS,
				Severity:          rule.Severity,
				ScoreDelta:        rule.ScoreDelta,
				Title:             rule.Title,
				Evidence:          rule.Evidence,
				PolicyPackID:      scopedRule.Pack.Policy.Metadata.ID,
				PolicyPackVersion: scopedRule.Pack.Policy.Metadata.Version,
			})
		}
	}

	result.RiskScore = clampScore(score, globalPack.Policy.Spec.Scoring.Max)
	activation = saasActivation(input, globalPack.Policy.Spec.Constants, result.RiskScore)
	level, err := evaluateLevelRules(globalPack, globalPack.Policy.Spec.Levels, activation)
	if err != nil {
		return SaaSResult{}, err
	}
	if level != "" {
		result.RiskLevel = level
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

type matchedSaaSScopedRule struct {
	Pack CompiledPack
	Rule ScopedRule
}

func (r *Registry) matchingSaaSScopedRules(input SaaSInput) []matchedSaaSScopedRule {
	matches := make([]matchedSaaSScopedRule, 0, 2)
	for _, pack := range r.packs {
		if pack.Policy.Metadata.Domain != DomainSaaS {
			continue
		}
		for _, scopedRule := range pack.Policy.Spec.ScopedRules {
			if !scopedRule.Scope.App.matchesSaaSInput(input) {
				continue
			}
			matches = append(matches, matchedSaaSScopedRule{
				Pack: pack,
				Rule: scopedRule,
			})
		}
	}
	return matches
}

func (scope AppScope) matchesSaaSInput(input SaaSInput) bool {
	if scope.CanonicalKey != "" && scope.CanonicalKey != input.CanonicalKey {
		return false
	}
	if scope.PrimaryDomain != "" && scope.PrimaryDomain != input.PrimaryDomain {
		return false
	}
	if len(scope.DomainMatches) > 0 && !matchesDomainPatterns(input.PrimaryDomain, scope.DomainMatches) {
		return false
	}
	if scope.VendorName != "" && scope.VendorName != input.VendorName {
		return false
	}
	if scope.SourceKind != "" && scope.SourceKind != input.SourceKind {
		return false
	}
	if scope.SourceName != "" && scope.SourceName != input.SourceName {
		return false
	}
	if scope.Category != "" && scope.Category != input.Category {
		return false
	}
	return true
}

func matchesDomainPatterns(domain string, patterns []string) bool {
	for _, pattern := range patterns {
		if pattern == domain {
			return true
		}
		if strings.HasPrefix(pattern, "*.") {
			suffix := strings.TrimPrefix(pattern, "*")
			if domain == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(domain, suffix) {
				return true
			}
		}
		if strings.HasPrefix(pattern, ".") && strings.HasSuffix(domain, pattern) {
			return true
		}
	}
	return false
}

func saasActivation(input SaaSInput, constants map[string][]string, score int) map[string]any {
	activation := map[string]any{
		"canonical_key":                  input.CanonicalKey,
		"display_name":                   input.DisplayName,
		"primary_domain":                 input.PrimaryDomain,
		"vendor_name":                    input.VendorName,
		"source_kind":                    input.SourceKind,
		"source_name":                    input.SourceName,
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

func maxBusinessCriticality(values ...string) string {
	return maxRankedValue(businessCriticalityRank, values...)
}

func maxDataClassification(values ...string) string {
	return maxRankedValue(dataClassificationRank, values...)
}

func maxRankedValue(rank func(string) int, values ...string) string {
	maxRank := -1
	maxValue := ""
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if currentRank := rank(value); currentRank > maxRank {
			maxRank = currentRank
			maxValue = value
		}
	}
	return maxValue
}

func businessCriticalityRank(value string) int {
	switch value {
	case "low":
		return 1
	case "medium":
		return 2
	case "high":
		return 3
	case "critical":
		return 4
	default:
		return 0
	}
}

func dataClassificationRank(value string) int {
	switch value {
	case "public":
		return 1
	case "internal":
		return 2
	case "confidential":
		return 3
	case "restricted":
		return 4
	default:
		return 0
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
