package riskpolicy

import (
	"errors"
	"strings"
	"time"
)

type IdentityInput struct {
	IdentityID             int64
	PrincipalRef           string
	PrincipalType          string
	SourceKind             string
	SourceName             string
	DisplayName            string
	PrimaryEmail           string
	LastSeenAt             *time.Time
	OwnerPresence          string
	GovernanceState        string
	LinkedAssetsCount      int64
	LinkedCredentialsCount int64
	CredentialSignals      []string
	HasCriticalCredential  bool
	HasHighRiskCredential  bool
	HasExpiredCredential   bool
	HasExpiringCredential  bool
	HasUnusedCredential    bool
	HasStaleEvidence       bool
}

type IdentityResult struct {
	RiskLevel       string
	RiskRank        int
	RiskReasonCount int
	PolicyPacks     []PolicyPackRef
	Signals         []RiskSignal
}

func EvaluateIdentity(input IdentityInput) (IdentityResult, error) {
	registry, err := BuiltinRegistry()
	if err != nil {
		return IdentityResult{}, err
	}
	return registry.EvaluateIdentity(input)
}

func (r *Registry) EvaluateIdentity(input IdentityInput) (IdentityResult, error) {
	if r == nil {
		return IdentityResult{}, errors.New("risk policy registry is nil")
	}

	input = normalizeIdentityInput(input)
	result := IdentityResult{
		Signals: make([]RiskSignal, 0, 6),
	}
	defaultLevel := SeverityLow
	var matchedPack *CompiledPack
	for i := range r.packs {
		if r.packs[i].Policy.Metadata.Domain != DomainIdentity || r.packs[i].Policy.Spec.Inputs.Schema != "identity_risk_input.v1" {
			continue
		}
		if matchedPack != nil {
			return IdentityResult{}, errors.New("multiple identity risk policy packs found for identity_risk_input.v1")
		}
		matchedPack = &r.packs[i]
	}
	if matchedPack == nil {
		return IdentityResult{}, errors.New("identity risk policy pack not found")
	}

	pack := *matchedPack
	result.PolicyPacks = appendPolicyPackRef(result.PolicyPacks, pack.Policy.Metadata)
	if pack.Policy.Spec.Aggregation.RiskLevel.Default != "" {
		defaultLevel = pack.Policy.Spec.Aggregation.RiskLevel.Default
	}

	activation := identityActivation(input, pack.Policy.Spec.Constants)
	for _, rule := range pack.Policy.Spec.Rules {
		matched, err := pack.evaluateBool(rule.ID, activation)
		if err != nil {
			return IdentityResult{}, err
		}
		if !matched {
			continue
		}

		result.Signals = append(result.Signals, RiskSignal{
			ID:                rule.ID,
			Domain:            DomainIdentity,
			Severity:          rule.Severity,
			ScoreDelta:        rule.ScoreDelta,
			Title:             rule.Title,
			Evidence:          rule.Evidence,
			PolicyPackID:      pack.Policy.Metadata.ID,
			PolicyPackVersion: pack.Policy.Metadata.Version,
		})
		result.RiskLevel = MaxSeverity(result.RiskLevel, rule.Severity)
	}

	if result.RiskLevel == "" {
		result.RiskLevel = defaultLevel
	}
	if pack.Policy.Spec.Aggregation.RiskReasonCount.Strategy == "count_matching_rules" {
		result.RiskReasonCount = len(result.Signals)
	}
	result.RiskRank = SeverityRank(result.RiskLevel)
	return result, nil
}

func normalizeIdentityInput(input IdentityInput) IdentityInput {
	input.PrincipalRef = strings.TrimSpace(input.PrincipalRef)
	input.PrincipalType = strings.ToLower(strings.TrimSpace(input.PrincipalType))
	input.SourceKind = strings.ToLower(strings.TrimSpace(input.SourceKind))
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.DisplayName = strings.TrimSpace(input.DisplayName)
	input.PrimaryEmail = strings.ToLower(strings.TrimSpace(input.PrimaryEmail))
	input.LastSeenAt = normalizeTimePtr(input.LastSeenAt)
	input.OwnerPresence = strings.ToLower(strings.TrimSpace(input.OwnerPresence))
	if input.OwnerPresence == "" {
		input.OwnerPresence = "unknown"
	}
	input.GovernanceState = strings.ToLower(strings.TrimSpace(input.GovernanceState))
	if input.GovernanceState == "" {
		input.GovernanceState = "unreviewed"
	}
	for i := range input.CredentialSignals {
		input.CredentialSignals[i] = strings.ToLower(strings.TrimSpace(input.CredentialSignals[i]))
	}
	return input
}

func identityActivation(input IdentityInput, constants map[string][]string) map[string]any {
	activation := map[string]any{
		"identity_id":              input.IdentityID,
		"principal_ref":            input.PrincipalRef,
		"principal_type":           input.PrincipalType,
		"source_kind":              input.SourceKind,
		"source_name":              input.SourceName,
		"display_name":             input.DisplayName,
		"primary_email":            input.PrimaryEmail,
		"last_seen_at":             nullableTime(input.LastSeenAt),
		"owner_presence":           input.OwnerPresence,
		"governance_state":         input.GovernanceState,
		"linked_assets_count":      input.LinkedAssetsCount,
		"linked_credentials_count": input.LinkedCredentialsCount,
		"credential_signals":       cloneSlice(input.CredentialSignals),
		"has_critical_credential":  input.HasCriticalCredential,
		"has_high_risk_credential": input.HasHighRiskCredential,
		"has_expired_credential":   input.HasExpiredCredential,
		"has_expiring_credential":  input.HasExpiringCredential,
		"has_unused_credential":    input.HasUnusedCredential,
		"has_stale_evidence":       input.HasStaleEvidence,
	}
	for name, values := range constants {
		activation[name] = cloneSlice(values)
	}
	return activation
}
