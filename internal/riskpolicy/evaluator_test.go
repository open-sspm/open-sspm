package riskpolicy

import (
	"testing"
	"time"
)

func TestEvaluateCredentialGoldenCases(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}
	for _, tc := range credentialGoldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := registry.EvaluateCredential(tc.input)
			if err != nil {
				t.Fatalf("EvaluateCredential() error = %v", err)
			}
			if result.RiskLevel != tc.wantLevel {
				t.Fatalf("RiskLevel = %q, want %q; signals=%+v", result.RiskLevel, tc.wantLevel, result.Signals)
			}
			if result.RiskRank != SeverityRank(tc.wantLevel) {
				t.Fatalf("RiskRank = %d, want %d", result.RiskRank, SeverityRank(tc.wantLevel))
			}
			if len(result.PolicyPacks) != 1 || result.PolicyPacks[0].ID != "builtin.credential.risk" {
				t.Fatalf("PolicyPacks = %+v, want builtin credential pack", result.PolicyPacks)
			}
			gotSignalIDs := make([]string, 0, len(result.Signals))
			for _, signal := range result.Signals {
				gotSignalIDs = append(gotSignalIDs, signal.ID)
				if signal.Domain != DomainCredential {
					t.Fatalf("signal domain = %q, want credential", signal.Domain)
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

func TestEvaluateCredentialRejectsMalformedScopeJSON(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}

	_, err = registry.EvaluateCredential(CredentialInput{
		CredentialKind:      "vault_token",
		CreatedByExternalID: "alice",
		ScopeJSON:           []byte(`{"broken":`),
		EvaluatedAt:         time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("EvaluateCredential() error = nil, want malformed scope_json error")
	}
}

type credentialGoldenCase struct {
	name          string
	input         CredentialInput
	wantLevel     string
	wantSignalIDs []string
}

func credentialGoldenCases() []credentialGoldenCase {
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	return []credentialGoldenCase{
		{
			name: "expired active credential",
			input: CredentialInput{
				Status:         "active",
				CredentialKind: "vault_token",
				ExpiresAt:      timePtr(now.Add(-24 * time.Hour)),
				EvaluatedAt:    now,
			},
			wantLevel:     SeverityCritical,
			wantSignalIDs: []string{"expired_active", "missing_creator"},
		},
		{
			name: "expired inactive credential",
			input: CredentialInput{
				Status:         "revoked",
				CredentialKind: "vault_token",
				ExpiresAt:      timePtr(now.Add(-24 * time.Hour)),
				EvaluatedAt:    now,
			},
			wantLevel:     SeverityHigh,
			wantSignalIDs: []string{"expired_inactive", "missing_creator"},
		},
		{
			name: "high privilege missing provenance",
			input: CredentialInput{
				Status:         "active",
				CredentialKind: "github_pat_fine_grained",
				EvaluatedAt:    now,
			},
			wantLevel:     SeverityCritical,
			wantSignalIDs: []string{"high_privilege_missing_provenance", "missing_creator"},
		},
		{
			name: "expiring within seven days",
			input: CredentialInput{
				Status:              "active",
				CredentialKind:      "vault_token",
				CreatedByExternalID: "alice",
				ExpiresAt:           timePtr(now.Add(2 * 24 * time.Hour)),
				EvaluatedAt:         now,
			},
			wantLevel:     SeverityHigh,
			wantSignalIDs: []string{"expiring_within_7_days"},
		},
		{
			name: "missing creator",
			input: CredentialInput{
				Status:         "active",
				CredentialKind: "vault_token",
				EvaluatedAt:    now,
			},
			wantLevel:     SeverityHigh,
			wantSignalIDs: []string{"missing_creator"},
		},
		{
			name: "unused over ninety days",
			input: CredentialInput{
				Status:              "active",
				CredentialKind:      "vault_token",
				CreatedByExternalID: "alice",
				LastUsedAt:          timePtr(now.Add(-91 * 24 * time.Hour)),
				EvaluatedAt:         now,
			},
			wantLevel:     SeverityHigh,
			wantSignalIDs: []string{"unused_over_90_days"},
		},
		{
			name: "expiring within thirty days",
			input: CredentialInput{
				Status:              "active",
				CredentialKind:      "vault_token",
				CreatedByExternalID: "alice",
				ExpiresAt:           timePtr(now.Add(14 * 24 * time.Hour)),
				EvaluatedAt:         now,
			},
			wantLevel:     SeverityMedium,
			wantSignalIDs: []string{"expiring_within_30_days"},
		},
		{
			name: "ordinary credential",
			input: CredentialInput{
				Status:              "active",
				CredentialKind:      "vault_token",
				CreatedByExternalID: "alice",
				EvaluatedAt:         now,
			},
			wantLevel: SeverityLow,
		},
	}
}

func timePtr(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}

func sameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[string]int, len(got))
	for _, value := range got {
		counts[value]++
	}
	for _, value := range want {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	return true
}
