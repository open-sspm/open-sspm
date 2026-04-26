package riskpolicy

import (
	"strings"
	"testing"
)

func TestLoadBuiltinPolicies(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}
	if got, want := registry.PackCount(), 4; got != want {
		t.Fatalf("PackCount() = %d, want %d", got, want)
	}
	if got := registry.CompiledExpressionCount(); got == 0 {
		t.Fatal("CompiledExpressionCount() = 0, want compiled expressions")
	}

	byID := make(map[string]PolicyPack)
	for _, pack := range registry.Packs() {
		byID[pack.Metadata.ID] = pack
	}
	metadataByID := make(map[string]PolicyMetadata)
	for _, metadata := range registry.PackMetadatas() {
		metadataByID[metadata.ID] = metadata
	}
	for _, id := range []string{
		"builtin-credential-risk",
		"builtin-saas-risk",
		"builtin-identity-risk",
		"builtin-saas-app-overrides",
	} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("missing built-in policy pack %q", id)
		}
		if metadata := metadataByID[id]; metadata.ID == "" || metadata.Version == "" || metadata.Domain == "" {
			t.Fatalf("missing built-in policy metadata %q: %+v", id, metadata)
		}
	}
}

func TestLoadDocumentsRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: bad
  version: 1.0.0
  domain: credential
spec:
  inputs:
    schema: credential_risk_input.v1
  unexpected: true
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want unknown field error")
	}
	if !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("LoadDocuments() error = %v, want unknown field error", err)
	}
}

func TestLoadDocumentsRejectsInvalidSeverity(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: bad
  version: 1.0.0
  domain: credential
spec:
  inputs:
    schema: credential_risk_input.v1
  rules:
    - id: invalid_severity
      severity: urgent
      when: "true"
      title: Invalid severity
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want invalid severity error")
	}
	if !strings.Contains(err.Error(), "severity") {
		t.Fatalf("LoadDocuments() error = %v, want severity error", err)
	}
}

func TestLoadDocumentsRejectsInvalidCEL(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: bad
  version: 1.0.0
  domain: credential
spec:
  inputs:
    schema: credential_risk_input.v1
  rules:
    - id: invalid_cel
      severity: high
      when: unknown_field == "x"
      title: Invalid CEL
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want CEL compile error")
	}
	if !strings.Contains(err.Error(), "invalid_cel") {
		t.Fatalf("LoadDocuments() error = %v, want rule id in error", err)
	}
}

func TestLoadDocumentsRejectsNonBooleanCEL(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: bad
  version: 1.0.0
  domain: saas
spec:
  inputs:
    schema: saas_app_risk_input.v1
  suggestions:
    business_criticality:
      - id: non_bool
        level: high
        when: actors_30d
      - id: default_low
        level: low
        when: "true"
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want non-boolean CEL error")
	}
	if !strings.Contains(err.Error(), "must return bool") {
		t.Fatalf("LoadDocuments() error = %v, want bool type error", err)
	}
}

func TestLoadDocumentsRejectsInvalidSuggestionLevels(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: bad
  version: 1.0.0
  domain: saas
spec:
  inputs:
    schema: saas_app_risk_input.v1
  suggestions:
    business_criticality:
      - id: invalid_business_criticality
        level: severe
        when: "true"
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want invalid suggestion level error")
	}
	if !strings.Contains(err.Error(), "severe") {
		t.Fatalf("LoadDocuments() error = %v, want invalid level in error", err)
	}
}

func TestLoadDocumentsRejectsUnknownSuggestionLevels(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: bad
  version: 1.0.0
  domain: saas
spec:
  inputs:
    schema: saas_app_risk_input.v1
  suggestions:
    data_classification:
      - id: invalid_unknown_data_classification
        level: unknown
        when: "true"
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want unknown suggestion level error")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("LoadDocuments() error = %v, want unknown level in error", err)
	}
}

func TestLoadDocumentsRejectsSuggestionRulesWithoutFallback(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: bad
  version: 1.0.0
  domain: saas
spec:
  inputs:
    schema: saas_app_risk_input.v1
  suggestions:
    business_criticality:
      - id: high_usage
        level: high
        when: actors_30d >= 50
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want deterministic fallback error")
	}
	if !strings.Contains(err.Error(), "deterministic fallback") {
		t.Fatalf("LoadDocuments() error = %v, want deterministic fallback error", err)
	}
}

func TestLoadDocumentsRejectsInvalidScopedSuggestionLevels(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: bad
  version: 1.0.0
  domain: saas
spec:
  scoped_rules:
    - id: invalid_scoped_suggestion
      scope:
        app:
          canonical_key: github
      suggestions:
        data_classification: restriced
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want invalid scoped suggestion level error")
	}
	if !strings.Contains(err.Error(), "restriced") {
		t.Fatalf("LoadDocuments() error = %v, want invalid scoped suggestion in error", err)
	}
}

func TestLoadDocumentsAllowsScopedRulesToReuseInnerRuleIDs(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"ok.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: ok
  version: 1.0.0
  domain: saas
spec:
  scoped_rules:
    - id: github_policy
      scope:
        app:
          canonical_key: github
      rules:
        - id: missing_owner
          severity: high
          when: owner_identity_id == 0
          title: GitHub app has no accountable owner
    - id: finance_policy
      scope:
        app:
          category: finance
      rules:
        - id: missing_owner
          severity: high
          when: owner_identity_id == 0
          title: Finance app has no accountable owner
`),
	})
	if err != nil {
		t.Fatalf("LoadDocuments() error = %v, want nil", err)
	}
}

func TestSeverityOrdering(t *testing.T) {
	t.Parallel()

	if got := MaxSeverity("low", "critical", "medium"); got != "critical" {
		t.Fatalf("MaxSeverity() = %q, want critical", got)
	}
	if got := MaxSeverity(); got != "" {
		t.Fatalf("MaxSeverity() = %q, want empty", got)
	}
	if got := MaxSeverity("urgent", ""); got != "" {
		t.Fatalf("MaxSeverity(invalid) = %q, want empty", got)
	}
	if got := MaxSeverity("low"); got != "low" {
		t.Fatalf("MaxSeverity(low) = %q, want low", got)
	}
	if ValidSeverity("urgent") {
		t.Fatal("ValidSeverity(\"urgent\") = true, want false")
	}
	if got := SeverityRank("HIGH"); got != 3 {
		t.Fatalf("SeverityRank(\"HIGH\") = %d, want 3", got)
	}
}
