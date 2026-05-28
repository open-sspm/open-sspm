package evaluator

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
		return IdentityResult{}, errors.New("policy registry is nil")
	}

	input = normalizeIdentityInput(input)
	packs := r.packsFor(DomainIdentity, "identity_risk_input.v1")
	if len(packs) == 0 {
		return IdentityResult{}, errors.New("identity policy pack not found")
	}

	entity := identityEntityInput(input)
	result := IdentityResult{
		RiskLevel: SeverityLow,
		Signals:   make([]RiskSignal, 0, 6),
	}
	for _, pack := range packs {
		evaluated, err := evaluateEntityPolicyPack(pack, entity)
		if err != nil {
			return IdentityResult{}, err
		}
		result.PolicyPacks = appendPolicyPackRef(result.PolicyPacks, pack.Policy.Metadata)
		result.Signals = appendEntitySignals(result.Signals, DomainIdentity, pack.Policy.Metadata, evaluated.Signals)
		if level := NormalizeSeverity(evaluated.RiskLevel); level != "" {
			result.RiskLevel = MaxSeverity(result.RiskLevel, level)
		}
	}
	if fromSignals := maxSignalSeverity(result.Signals); fromSignals != "" {
		result.RiskLevel = MaxSeverity(result.RiskLevel, fromSignals)
	}
	result.RiskReasonCount = len(result.Signals)
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

func identityEntityInput(input IdentityInput) map[string]any {
	return map[string]any{
		"identity_id":              input.IdentityID,
		"principal_ref":            input.PrincipalRef,
		"principal_type":           input.PrincipalType,
		"source_kind":              input.SourceKind,
		"source_name":              input.SourceName,
		"display_name":             input.DisplayName,
		"primary_email":            input.PrimaryEmail,
		"last_seen_at":             nullableTimeString(input.LastSeenAt),
		"owner_presence":           input.OwnerPresence,
		"governance_state":         input.GovernanceState,
		"linked_assets_count":      input.LinkedAssetsCount,
		"linked_credentials_count": input.LinkedCredentialsCount,
		"credential_signals":       append([]string(nil), input.CredentialSignals...),
		"has_critical_credential":  input.HasCriticalCredential,
		"has_high_risk_credential": input.HasHighRiskCredential,
		"has_expired_credential":   input.HasExpiredCredential,
		"has_expiring_credential":  input.HasExpiringCredential,
		"has_unused_credential":    input.HasUnusedCredential,
		"has_stale_evidence":       input.HasStaleEvidence,
	}
}
