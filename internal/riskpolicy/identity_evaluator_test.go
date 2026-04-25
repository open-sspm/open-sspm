package riskpolicy

import (
	"strings"
	"testing"
	"time"
)

func TestEvaluateIdentityGoldenCases(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}

	for _, tc := range identityGoldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := registry.EvaluateIdentity(tc.input)
			if err != nil {
				t.Fatalf("EvaluateIdentity() error = %v", err)
			}
			if result.RiskLevel != tc.wantLevel {
				t.Fatalf("RiskLevel = %q, want %q; signals=%+v", result.RiskLevel, tc.wantLevel, result.Signals)
			}
			if result.RiskRank != SeverityRank(tc.wantLevel) {
				t.Fatalf("RiskRank = %d, want %d", result.RiskRank, SeverityRank(tc.wantLevel))
			}
			if result.RiskReasonCount != tc.wantReasonCount {
				t.Fatalf("RiskReasonCount = %d, want %d; signals=%+v", result.RiskReasonCount, tc.wantReasonCount, result.Signals)
			}
			if len(result.PolicyPacks) != 1 || result.PolicyPacks[0].ID != "builtin-identity-risk" || result.PolicyPacks[0].Version == "" {
				t.Fatalf("PolicyPacks = %+v, want builtin identity pack metadata", result.PolicyPacks)
			}

			gotSignalIDs := make([]string, 0, len(result.Signals))
			for _, signal := range result.Signals {
				gotSignalIDs = append(gotSignalIDs, signal.ID)
				if signal.Domain != DomainIdentity {
					t.Fatalf("signal domain = %q, want identity", signal.Domain)
				}
				if signal.PolicyPackID == "" || signal.PolicyPackVersion == "" {
					t.Fatalf("signal missing source policy metadata: %+v", signal)
				}
			}
			if !sameStringSet(gotSignalIDs, tc.wantSignalIDs) {
				t.Fatalf("signal IDs = %v, want %v", gotSignalIDs, tc.wantSignalIDs)
			}
		})
	}
}

func TestEvaluateIdentityRejectsDuplicateGlobalPacks(t *testing.T) {
	t.Parallel()

	registry, err := LoadDocuments(map[string][]byte{
		"a.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: a
  version: 1.0.0
  domain: identity
spec:
  inputs:
    schema: identity_risk_input.v1
  rules:
    - id: low
      severity: low
      when: "true"
      title: Low risk
`),
		"b.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: b
  version: 1.0.0
  domain: identity
spec:
  inputs:
    schema: identity_risk_input.v1
  rules:
    - id: low
      severity: low
      when: "true"
      title: Low risk
`),
	})
	if err != nil {
		t.Fatalf("LoadDocuments() error = %v", err)
	}

	_, err = registry.EvaluateIdentity(IdentityInput{})
	if err == nil {
		t.Fatal("EvaluateIdentity() error = nil, want duplicate pack error")
	}
	if !strings.Contains(err.Error(), "multiple identity risk policy packs") {
		t.Fatalf("EvaluateIdentity() error = %v, want duplicate pack error", err)
	}
}

func TestEvaluateIdentityNormalizesInput(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}

	now := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)
	result, err := registry.EvaluateIdentity(IdentityInput{
		PrincipalType:        " SERVICE ",
		SourceKind:           " ENTRA ",
		SourceName:           " Tenant ",
		PrimaryEmail:         " SVC@example.COM ",
		LastSeenAt:           timePtr(now),
		OwnerPresence:        " UNKNOWN ",
		GovernanceState:      " REVIEWED ",
		CredentialSignals:    []string{" Linked_Expired_Credential "},
		HasExpiredCredential: true,
	})
	if err != nil {
		t.Fatalf("EvaluateIdentity() error = %v", err)
	}
	if result.RiskLevel != SeverityHigh {
		t.Fatalf("RiskLevel = %q, want high", result.RiskLevel)
	}
	if !sameStringSet(signalIDs(result.Signals), []string{"missing_accountable_owner", "linked_expired_credential"}) {
		t.Fatalf("signal IDs = %v, want owner and expired credential signals", signalIDs(result.Signals))
	}
}

type identityGoldenCase struct {
	name            string
	input           IdentityInput
	wantLevel       string
	wantReasonCount int
	wantSignalIDs   []string
}

func identityGoldenCases() []identityGoldenCase {
	base := IdentityInput{
		IdentityID:        42,
		PrincipalRef:      "identity-42",
		PrincipalType:     "service",
		SourceKind:        "entra",
		SourceName:        "tenant-1",
		DisplayName:       "svc-deploy",
		PrimaryEmail:      "svc-deploy@example.com",
		OwnerPresence:     "owned",
		GovernanceState:   "reviewed",
		LinkedAssetsCount: 1,
	}
	with := func(mutator func(*IdentityInput)) IdentityInput {
		input := base
		mutator(&input)
		return input
	}

	return []identityGoldenCase{
		{
			name: "critical linked credential",
			input: with(func(input *IdentityInput) {
				input.LinkedCredentialsCount = 1
				input.HasCriticalCredential = true
			}),
			wantLevel:       SeverityCritical,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"linked_critical_credential"},
		},
		{
			name: "critical linked credential with aggregate high flag",
			input: with(func(input *IdentityInput) {
				input.LinkedCredentialsCount = 1
				input.HasCriticalCredential = true
				input.HasHighRiskCredential = true
			}),
			wantLevel:       SeverityCritical,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"linked_critical_credential"},
		},
		{
			name: "unknown owner",
			input: with(func(input *IdentityInput) {
				input.OwnerPresence = "unknown"
			}),
			wantLevel:       SeverityHigh,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"missing_accountable_owner"},
		},
		{
			name: "high risk linked credential",
			input: with(func(input *IdentityInput) {
				input.LinkedCredentialsCount = 1
				input.HasHighRiskCredential = true
			}),
			wantLevel:       SeverityHigh,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"linked_high_risk_credential"},
		},
		{
			name: "expired linked credential",
			input: with(func(input *IdentityInput) {
				input.LinkedCredentialsCount = 1
				input.HasExpiredCredential = true
			}),
			wantLevel:       SeverityHigh,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"linked_expired_credential"},
		},
		{
			name: "unreviewed high risk credential",
			input: with(func(input *IdentityInput) {
				input.GovernanceState = "unreviewed"
				input.LinkedCredentialsCount = 1
				input.HasHighRiskCredential = true
			}),
			wantLevel:       SeverityHigh,
			wantReasonCount: 2,
			wantSignalIDs:   []string{"linked_high_risk_credential", "unreviewed_risky_principal"},
		},
		{
			name: "unreviewed expiring evidence is high",
			input: with(func(input *IdentityInput) {
				input.GovernanceState = "unreviewed"
				input.LinkedCredentialsCount = 1
				input.HasExpiringCredential = true
			}),
			wantLevel:       SeverityHigh,
			wantReasonCount: 2,
			wantSignalIDs:   []string{"unreviewed_risky_principal", "stale_or_unused_evidence"},
		},
		{
			name: "unreviewed unused evidence is high",
			input: with(func(input *IdentityInput) {
				input.GovernanceState = "unreviewed"
				input.LinkedCredentialsCount = 1
				input.HasUnusedCredential = true
			}),
			wantLevel:       SeverityHigh,
			wantReasonCount: 2,
			wantSignalIDs:   []string{"unreviewed_risky_principal", "stale_or_unused_evidence"},
		},
		{
			name: "reviewed expiring evidence is medium",
			input: with(func(input *IdentityInput) {
				input.LinkedCredentialsCount = 1
				input.HasExpiringCredential = true
			}),
			wantLevel:       SeverityMedium,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"stale_or_unused_evidence"},
		},
		{
			name: "reviewed unused evidence is medium",
			input: with(func(input *IdentityInput) {
				input.LinkedCredentialsCount = 1
				input.HasUnusedCredential = true
			}),
			wantLevel:       SeverityMedium,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"stale_or_unused_evidence"},
		},
		{
			name: "reviewed stale evidence is medium",
			input: with(func(input *IdentityInput) {
				input.HasStaleEvidence = true
			}),
			wantLevel:       SeverityMedium,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"stale_or_unused_evidence"},
		},
		{
			name: "in review expiring evidence remains medium",
			input: with(func(input *IdentityInput) {
				input.GovernanceState = "in_review"
				input.LinkedCredentialsCount = 1
				input.HasExpiringCredential = true
			}),
			wantLevel:       SeverityMedium,
			wantReasonCount: 1,
			wantSignalIDs:   []string{"stale_or_unused_evidence"},
		},
		{
			name:            "low risk owned principal",
			input:           base,
			wantLevel:       SeverityLow,
			wantReasonCount: 0,
			wantSignalIDs:   nil,
		},
	}
}

func signalIDs(signals []RiskSignal) []string {
	ids := make([]string, 0, len(signals))
	for _, signal := range signals {
		ids = append(ids, signal.ID)
	}
	return ids
}
