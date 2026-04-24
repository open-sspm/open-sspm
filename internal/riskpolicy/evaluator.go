package riskpolicy

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type RiskSignal struct {
	ID                string
	Domain            Domain
	Severity          string
	ScoreDelta        int
	Title             string
	Evidence          string
	PolicyPackID      string
	PolicyPackVersion string
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
	RiskLevel string
	RiskRank  int
	Signals   []RiskSignal
}

func EvaluateCredential(input CredentialInput) (CredentialResult, error) {
	registry, err := BuiltinRegistry()
	if err != nil {
		return CredentialResult{}, err
	}
	return registry.EvaluateCredential(input)
}

func (r *Registry) EvaluateCredential(input CredentialInput) (CredentialResult, error) {
	if r == nil {
		return CredentialResult{}, errors.New("risk policy registry is nil")
	}

	input, err := normalizeCredentialInput(input)
	if err != nil {
		return CredentialResult{}, err
	}
	result := CredentialResult{
		Signals: make([]RiskSignal, 0, 4),
	}
	defaultLevel := SeverityLow
	foundPack := false

	for _, pack := range r.packs {
		if pack.Policy.Metadata.Domain != DomainCredential {
			continue
		}
		foundPack = true
		if pack.Policy.Spec.Aggregation.RiskLevel.Default != "" {
			defaultLevel = MaxSeverity(defaultLevel, pack.Policy.Spec.Aggregation.RiskLevel.Default)
		}

		activation := credentialActivation(input, pack.Policy.Spec.Constants)
		for _, rule := range pack.Policy.Spec.Rules {
			matched, err := pack.evaluateBool(rule.ID, activation)
			if err != nil {
				return CredentialResult{}, err
			}
			if !matched {
				continue
			}

			signal := RiskSignal{
				ID:                rule.ID,
				Domain:            DomainCredential,
				Severity:          rule.Severity,
				ScoreDelta:        rule.ScoreDelta,
				Title:             rule.Title,
				Evidence:          rule.Evidence,
				PolicyPackID:      pack.Policy.Metadata.ID,
				PolicyPackVersion: pack.Policy.Metadata.Version,
			}
			result.Signals = append(result.Signals, signal)
			result.RiskLevel = MaxSeverity(result.RiskLevel, rule.Severity)
		}
	}

	if !foundPack {
		return CredentialResult{}, errors.New("credential risk policy pack not found")
	}
	if result.RiskLevel == "" {
		result.RiskLevel = defaultLevel
	}
	result.RiskRank = SeverityRank(result.RiskLevel)
	return result, nil
}

func (pack CompiledPack) evaluateBool(ruleID string, activation map[string]any) (bool, error) {
	for _, expression := range pack.expressions {
		if expression.RuleID != ruleID {
			continue
		}
		value, _, err := expression.program.Eval(activation)
		if err != nil {
			return false, fmt.Errorf("%s: %s: evaluate %q: %w", pack.Policy.Metadata.ID, ruleID, expression.Expression, err)
		}
		matched, ok := value.Value().(bool)
		if !ok {
			return false, fmt.Errorf("%s: %s: evaluate %q: expected bool result, got %T", pack.Policy.Metadata.ID, ruleID, expression.Expression, value.Value())
		}
		return matched, nil
	}
	return false, fmt.Errorf("%s: compiled expression %q not found", pack.Policy.Metadata.ID, ruleID)
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
	if input.EvaluatedAt.IsZero() {
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

func credentialActivation(input CredentialInput, constants map[string][]string) map[string]any {
	activation := map[string]any{
		"source_kind":              input.SourceKind,
		"source_name":              input.SourceName,
		"credential_kind":          input.CredentialKind,
		"status":                   input.Status,
		"expires_at":               nullableTime(input.ExpiresAt),
		"last_used_at":             nullableTime(input.LastUsedAt),
		"created_at":               nullableTime(input.CreatedAt),
		"created_by_external_id":   input.CreatedByExternalID,
		"created_by_display_name":  input.CreatedByDisplayName,
		"approved_by_external_id":  input.ApprovedByExternalID,
		"approved_by_display_name": input.ApprovedByDisplayName,
		"asset_ref_kind":           input.AssetRefKind,
		"asset_ref_external_id":    input.AssetRefExternalID,
		"scope_json":               input.ScopeJSON,
		"evaluated_at":             input.EvaluatedAt,
	}
	for name, values := range constants {
		activation[name] = cloneSlice(values)
	}
	return activation
}

func normalizeTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
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
