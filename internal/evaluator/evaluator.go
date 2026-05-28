package evaluator

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
)

type RiskSignal struct {
	ID                string `json:"id"`
	Domain            Domain `json:"domain"`
	Severity          string `json:"severity"`
	Title             string `json:"title"`
	Evidence          string `json:"evidence,omitempty"`
	PolicyPackID      string `json:"policy_pack_id"`
	PolicyPackVersion string `json:"policy_pack_version"`
}

type PolicyPackRef struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

type CredentialInput struct {
	SourceKind            string
	SourceName            string
	CredentialKind        string
	Status                string
	ExpiresAt             *time.Time
	LastUsedAt            *time.Time
	CreatedAt             *time.Time
	CreatedByExternalID   string
	CreatedByDisplayName  string
	ApprovedByExternalID  string
	ApprovedByDisplayName string
	AssetRefKind          string
	AssetRefExternalID    string
	ScopeJSON             any
	EvaluatedAt           time.Time
}

type CredentialResult struct {
	RiskLevel   string
	RiskRank    int
	PolicyPacks []PolicyPackRef
	Signals     []RiskSignal
}

var (
	minRegoRFC3339NanoTime = time.Unix(0, math.MinInt64).UTC()
	maxRegoRFC3339NanoTime = time.Unix(0, math.MaxInt64).UTC()
)

func EvaluateCredential(input CredentialInput) (CredentialResult, error) {
	registry, err := BuiltinRegistry()
	if err != nil {
		return CredentialResult{}, err
	}
	return registry.EvaluateCredential(input)
}

func (r *Registry) EvaluateCredential(input CredentialInput) (CredentialResult, error) {
	if r == nil {
		return CredentialResult{}, errors.New("policy registry is nil")
	}

	input, err := normalizeCredentialInput(input)
	if err != nil {
		return CredentialResult{}, err
	}

	packs := r.packsFor(DomainCredential, "credential_risk_input.v1")
	if len(packs) == 0 {
		return CredentialResult{}, errors.New("credential policy pack not found")
	}

	entity := credentialEntityInput(input)
	result := CredentialResult{
		RiskLevel: SeverityLow,
		Signals:   make([]RiskSignal, 0, 4),
	}
	for _, pack := range packs {
		evaluated, err := evaluateEntityPolicyPack(pack, entity)
		if err != nil {
			return CredentialResult{}, err
		}
		result.PolicyPacks = appendPolicyPackRef(result.PolicyPacks, pack.Policy.Metadata)
		result.Signals = appendEntitySignals(result.Signals, DomainCredential, pack.Policy.Metadata, evaluated.Signals)
		if level := NormalizeSeverity(evaluated.RiskLevel); level != "" {
			result.RiskLevel = MaxSeverity(result.RiskLevel, level)
		}
	}
	if fromSignals := maxSignalSeverity(result.Signals); fromSignals != "" {
		result.RiskLevel = MaxSeverity(result.RiskLevel, fromSignals)
	}
	result.RiskRank = SeverityRank(result.RiskLevel)
	return result, nil
}

func normalizeCredentialInput(input CredentialInput) (CredentialInput, error) {
	input.SourceKind = strings.ToLower(strings.TrimSpace(input.SourceKind))
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.CredentialKind = strings.ToLower(strings.TrimSpace(input.CredentialKind))
	input.Status = strings.ToLower(strings.TrimSpace(input.Status))
	input.CreatedByExternalID = strings.TrimSpace(input.CreatedByExternalID)
	input.CreatedByDisplayName = strings.TrimSpace(input.CreatedByDisplayName)
	input.ApprovedByExternalID = strings.TrimSpace(input.ApprovedByExternalID)
	input.ApprovedByDisplayName = strings.TrimSpace(input.ApprovedByDisplayName)
	input.AssetRefKind = strings.ToLower(strings.TrimSpace(input.AssetRefKind))
	input.AssetRefExternalID = strings.TrimSpace(input.AssetRefExternalID)
	input.ExpiresAt = normalizeTimePtr(input.ExpiresAt)
	input.LastUsedAt = normalizeTimePtr(input.LastUsedAt)
	input.CreatedAt = normalizeTimePtr(input.CreatedAt)
	if input.EvaluatedAt.IsZero() || !validRegoRFC3339NanoTime(input.EvaluatedAt.UTC()) {
		input.EvaluatedAt = time.Now()
	}
	input.EvaluatedAt = input.EvaluatedAt.UTC()
	scopeJSON, err := normalizeScopeJSON(input.ScopeJSON)
	if err != nil {
		return CredentialInput{}, err
	}
	input.ScopeJSON = scopeJSON
	return input, nil
}

func credentialEntityInput(input CredentialInput) map[string]any {
	return map[string]any{
		"source_kind":              input.SourceKind,
		"source_name":              input.SourceName,
		"credential_kind":          input.CredentialKind,
		"status":                   input.Status,
		"expires_at":               nullableTimeString(input.ExpiresAt),
		"last_used_at":             nullableTimeString(input.LastUsedAt),
		"created_at":               nullableTimeString(input.CreatedAt),
		"created_by_external_id":   input.CreatedByExternalID,
		"created_by_display_name":  input.CreatedByDisplayName,
		"approved_by_external_id":  input.ApprovedByExternalID,
		"approved_by_display_name": input.ApprovedByDisplayName,
		"asset_ref_kind":           input.AssetRefKind,
		"asset_ref_external_id":    input.AssetRefExternalID,
		"scope_json":               input.ScopeJSON,
		"evaluated_at":             input.EvaluatedAt.Format(time.RFC3339Nano),
	}
}

func (r *Registry) packsFor(domain Domain, schema string) []CompiledPack {
	if r == nil {
		return nil
	}
	schema = strings.TrimSpace(schema)
	out := make([]CompiledPack, 0, 1)
	for _, pack := range r.packs {
		if pack.Policy.Metadata.Domain != domain {
			continue
		}
		if strings.TrimSpace(pack.Policy.Inputs.Schema) != schema {
			continue
		}
		out = append(out, pack)
	}
	return out
}

func evaluateEntityPolicyPack(pack CompiledPack, entity map[string]any) (osspecv2.EntityPolicyEvaluateResult, error) {
	result, err := pack.evaluator.Evaluate(pack.Policy, entity)
	if err != nil {
		return osspecv2.EntityPolicyEvaluateResult{}, fmt.Errorf("%s: evaluate Rego policy: %w", pack.Policy.Metadata.ID, err)
	}
	return result, nil
}

func appendEntitySignals(out []RiskSignal, domain Domain, metadata PolicyMetadata, signals []osspecv2.EntityPolicyTestSignal) []RiskSignal {
	// Entity policy signals mirror the Open SSPM spec shape; event signals may add evidence.
	for _, signal := range signals {
		out = append(out, RiskSignal{
			ID:                strings.TrimSpace(signal.ID),
			Domain:            domain,
			Severity:          NormalizeSeverity(signal.Severity),
			Title:             strings.TrimSpace(signal.Title),
			PolicyPackID:      metadata.ID,
			PolicyPackVersion: metadata.Version,
		})
	}
	return out
}

func appendPolicyPackRef(refs []PolicyPackRef, metadata PolicyMetadata) []PolicyPackRef {
	for _, ref := range refs {
		if ref.ID == metadata.ID && ref.Version == metadata.Version {
			return refs
		}
	}
	return append(refs, PolicyPackRef{
		ID:      metadata.ID,
		Version: metadata.Version,
	})
}

func maxSignalSeverity(signals []RiskSignal) string {
	maxSeverity := ""
	for _, signal := range signals {
		maxSeverity = MaxSeverity(maxSeverity, signal.Severity)
	}
	return maxSeverity
}

func normalizeTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	if !validRegoRFC3339NanoTime(normalized) {
		return nil
	}
	return &normalized
}

func nullableTimeString(value *time.Time) any {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	if !validRegoRFC3339NanoTime(normalized) {
		return nil
	}
	return normalized.Format(time.RFC3339Nano)
}

func validRegoRFC3339NanoTime(value time.Time) bool {
	return !value.IsZero() && !value.Before(minRegoRFC3339NanoTime) && !value.After(maxRegoRFC3339NanoTime)
}

func normalizeScopeJSON(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return map[string]any{}, nil
	case []byte:
		return decodeScopeJSON(v)
	case json.RawMessage:
		return decodeScopeJSON(v)
	default:
		return value, nil
	}
}

func decodeScopeJSON(data []byte) (any, error) {
	if len(data) == 0 {
		return map[string]any{}, nil
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, fmt.Errorf("decode credential scope_json: %w", err)
	}
	return decoded, nil
}
