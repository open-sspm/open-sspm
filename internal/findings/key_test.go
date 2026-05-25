package findings

import "testing"

func TestBuildKeyIsDeterministicAndScopeAware(t *testing.T) {
	base := FindingResult{
		Source:   SourceRef{Kind: "okta", Name: "example.okta.com"},
		Scope:    ScopeRef{Kind: "connector_instance", SourceKind: "okta", SourceName: "example.okta.com"},
		Entity:   EntityRef{Kind: "ruleset_scope", ID: "connector_instance:okta:example.okta.com"},
		Resource: ResourceRef{Kind: "rule", ID: "mfa-required"},
		Policy:   PolicyRef{BundleID: "cis.okta.idaas_stig.v2", ID: "mfa-required"},
	}

	first := BuildKey(base)
	second := BuildKey(base)
	if first == "" || first != second {
		t.Fatalf("BuildKey() = %q then %q, want stable non-empty key", first, second)
	}

	otherScope := base
	otherScope.Scope.SourceName = "other.okta.com"
	otherScope.Source.Name = "other.okta.com"
	if other := BuildKey(otherScope); other == first {
		t.Fatalf("BuildKey() did not include source/scope; both keys = %q", first)
	}
}
