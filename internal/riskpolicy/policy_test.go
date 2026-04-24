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
	for _, id := range []string{
		"builtin-credential-risk",
		"builtin-saas-risk",
		"builtin-identity-risk",
		"builtin-saas-app-overrides",
	} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("missing built-in policy pack %q", id)
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

func TestSeverityOrdering(t *testing.T) {
	t.Parallel()

	if got := MaxSeverity("low", "critical", "medium"); got != "critical" {
		t.Fatalf("MaxSeverity() = %q, want critical", got)
	}
	if ValidSeverity("urgent") {
		t.Fatal("ValidSeverity(\"urgent\") = true, want false")
	}
	if got := SeverityRank("HIGH"); got != 3 {
		t.Fatalf("SeverityRank(\"HIGH\") = %d, want 3", got)
	}
}
