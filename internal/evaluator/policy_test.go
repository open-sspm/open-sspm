package evaluator

import (
	"strings"
	"testing"

	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
)

const testEntityPolicyRego = `package opensspm.entity.test

result := {"risk_level": "low", "signals": []}`

func TestLoadBuiltinPolicies(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}
	if got, want := registry.PackCount(), 4; got != want {
		t.Fatalf("PackCount() = %d, want %d", got, want)
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
		"builtin.credential.risk",
		"builtin.saas.risk",
		"builtin.identity.risk",
		"builtin.saas.app_overrides",
	} {
		pack, ok := byID[id]
		if !ok {
			t.Fatalf("missing built-in policy pack %q", id)
		}
		if metadata := metadataByID[id]; metadata.ID == "" || metadata.Version == "" || metadata.Domain == "" {
			t.Fatalf("missing built-in policy metadata %q: %+v", id, metadata)
		}
		if pack.Policy.Engine != osspecv2.CheckEngine_REGO || pack.Policy.Query == "" || pack.Policy.Rego == "" {
			t.Fatalf("built-in policy %q is not a compiled Rego policy: %+v", id, pack.Policy)
		}
	}
}

func TestLoadPolicyPacksRejectsDuplicateNormalizedIDs(t *testing.T) {
	t.Parallel()

	pack := testPolicyPack("duplicate", DomainCredential, "credential_risk_input.v1")
	duplicate := pack
	duplicate.Metadata.ID = " duplicate "

	_, err := LoadPolicyPacks([]PolicyPack{pack, duplicate})
	if err == nil {
		t.Fatal("LoadPolicyPacks() error = nil, want duplicate ID error")
	}
	if !strings.Contains(err.Error(), `duplicate policy pack id "duplicate"`) {
		t.Fatalf("LoadPolicyPacks() error = %v, want normalized duplicate ID error", err)
	}
}

func TestLoadDocumentsRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
kind: opensspm.entity_policy_pack
schema_version: 2
entity_policy_pack:
  metadata:
    id: bad
    version: 1.0.0
    domain: credential
  spec:
    inputs:
      schema: credential_risk_input.v1
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want unknown field error")
	}
	if !strings.Contains(err.Error(), "field spec not found") {
		t.Fatalf("LoadDocuments() error = %v, want unknown spec field error", err)
	}
}

func TestLoadDocumentsRejectsMissingRego(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
kind: opensspm.entity_policy_pack
schema_version: 2
entity_policy_pack:
  metadata:
    id: bad
    version: 1.0.0
    domain: credential
  inputs:
    schema: credential_risk_input.v1
  policy:
    engine: rego
    package: opensspm.entity.bad
    query: data.opensspm.entity.bad.result
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want missing rego error")
	}
	if !strings.Contains(err.Error(), "policy.rego is required") {
		t.Fatalf("LoadDocuments() error = %v, want missing rego error", err)
	}
}

func TestLoadDocumentsRejectsInvalidRego(t *testing.T) {
	t.Parallel()

	_, err := LoadDocuments(map[string][]byte{
		"bad.yaml": []byte(`
kind: opensspm.entity_policy_pack
schema_version: 2
entity_policy_pack:
  metadata:
    id: bad
    version: 1.0.0
    domain: credential
  inputs:
    schema: credential_risk_input.v1
  policy:
    engine: rego
    package: opensspm.entity.bad
    query: data.opensspm.entity.bad.result
    rego: |
      package opensspm.entity.bad

      result := {"risk_level": "low", "signals": [
`),
	})
	if err == nil {
		t.Fatal("LoadDocuments() error = nil, want invalid rego error")
	}
	if !strings.Contains(err.Error(), "prepare Rego policy") {
		t.Fatalf("LoadDocuments() error = %v, want prepare Rego policy error", err)
	}
}

func TestLoadDocumentsRejectsNonRegoPolicy(t *testing.T) {
	t.Parallel()

	pack := testPolicyPack("bad", DomainCredential, "credential_risk_input.v1")
	pack.Policy.Engine = osspecv2.CheckEngine("unsupported")

	_, err := LoadPolicyPacks([]PolicyPack{pack})
	if err == nil {
		t.Fatal("LoadPolicyPacks() error = nil, want unsupported engine error")
	}
	if !strings.Contains(err.Error(), "policy.engine") {
		t.Fatalf("LoadPolicyPacks() error = %v, want policy.engine error", err)
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

func testPolicyPack(id string, domain Domain, schema string) PolicyPack {
	return PolicyPack{
		Metadata: PolicyMetadata{
			ID:      id,
			Version: "1.0.0",
			Domain:  domain,
		},
		Inputs: Inputs{Schema: schema},
		Policy: RegoPolicy{
			Engine:  osspecv2.CheckEngine_REGO,
			Package: "opensspm.entity.test",
			Query:   "data.opensspm.entity.test.result",
			Rego:    testEntityPolicyRego,
		},
	}
}
