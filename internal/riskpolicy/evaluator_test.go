package riskpolicy

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/testdb"
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
			if !sameStrings(gotSignalIDs, tc.wantSignalIDs) {
				t.Fatalf("signal IDs = %v, want %v", gotSignalIDs, tc.wantSignalIDs)
			}
		})
	}
}

func TestEvaluateCredentialMatchesSQLFunction(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_riskpolicy"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		for _, tc := range credentialGoldenCases() {
			t.Run(tc.name, func(t *testing.T) {
				result, err := registry.EvaluateCredential(tc.input)
				if err != nil {
					t.Fatalf("EvaluateCredential() error = %v", err)
				}

				var sqlLevel string
				err = pool.QueryRow(ctx, `
					SELECT credential_artifact_risk_level($1, $2, $3, $4, $5, $6, $7)
				`,
					tc.input.Status,
					tc.input.CredentialKind,
					nullableTime(tc.input.ExpiresAt),
					nullableTime(tc.input.LastUsedAt),
					tc.input.CreatedByExternalID,
					tc.input.ApprovedByExternalID,
					tc.input.EvaluatedAt,
				).Scan(&sqlLevel)
				if err != nil {
					t.Fatalf("credential_artifact_risk_level() error = %v", err)
				}
				if result.RiskLevel != sqlLevel {
					t.Fatalf("policy RiskLevel = %q, SQL risk_level = %q", result.RiskLevel, sqlLevel)
				}
			})
		}
	})
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

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
