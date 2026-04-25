package riskpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestEvaluateSaaSGoldenCases(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}

	for _, tc := range saasGoldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := registry.EvaluateSaaS(tc.input)
			if err != nil {
				t.Fatalf("EvaluateSaaS() error = %v", err)
			}
			if result.RiskScore != tc.wantScore {
				t.Fatalf("RiskScore = %d, want %d; result=%+v", result.RiskScore, tc.wantScore, result)
			}
			if result.RiskLevel != tc.wantLevel {
				t.Fatalf("RiskLevel = %q, want %q; result=%+v", result.RiskLevel, tc.wantLevel, result)
			}
			if result.RiskRank != SeverityRank(tc.wantLevel) {
				t.Fatalf("RiskRank = %d, want %d", result.RiskRank, SeverityRank(tc.wantLevel))
			}
			if result.SuggestedBusinessCriticality != tc.wantBusinessCriticality {
				t.Fatalf("SuggestedBusinessCriticality = %q, want %q", result.SuggestedBusinessCriticality, tc.wantBusinessCriticality)
			}
			if result.SuggestedDataClassification != tc.wantDataClassification {
				t.Fatalf("SuggestedDataClassification = %q, want %q", result.SuggestedDataClassification, tc.wantDataClassification)
			}
			if result.EffectiveBusinessCriticality != tc.wantEffectiveBusinessCriticality {
				t.Fatalf("EffectiveBusinessCriticality = %q, want %q", result.EffectiveBusinessCriticality, tc.wantEffectiveBusinessCriticality)
			}
			if result.EffectiveDataClassification != tc.wantEffectiveDataClassification {
				t.Fatalf("EffectiveDataClassification = %q, want %q", result.EffectiveDataClassification, tc.wantEffectiveDataClassification)
			}

			gotSignalIDs := make([]string, 0, len(result.Signals))
			for _, signal := range result.Signals {
				gotSignalIDs = append(gotSignalIDs, signal.ID)
				if signal.Domain != DomainSaaS {
					t.Fatalf("signal domain = %q, want saas", signal.Domain)
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

func TestEvaluateSaaSStoredRiskMatchesDiscoveryReadModelView(t *testing.T) {
	t.Parallel()

	registry, err := LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_saas_riskpolicy"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		ownerID := insertSaaSParityOwner(t, ctx, pool)
		now := time.Now().UTC()
		for i, tc := range saasParityCases() {
			t.Run(tc.name, func(t *testing.T) {
				appID := insertSaaSParityApp(t, ctx, pool, i, tc, ownerID, now)
				input := saasParityInput(i, tc, ownerID)

				result, err := registry.EvaluateSaaS(input)
				if err != nil {
					t.Fatalf("EvaluateSaaS() error = %v", err)
				}
				upsertSaaSParityRiskReadModel(t, ctx, pool, appID, result)

				row := getSaaSParityReadModel(t, ctx, pool, appID)
				if result.RiskScore != int(row.RiskScore) {
					t.Fatalf("policy RiskScore = %d, stored view risk_score = %d", result.RiskScore, row.RiskScore)
				}
				if result.RiskLevel != row.RiskLevel {
					t.Fatalf("policy RiskLevel = %q, stored view risk_level = %q", result.RiskLevel, row.RiskLevel)
				}
				if result.SuggestedBusinessCriticality != row.SuggestedBusinessCriticality {
					t.Fatalf("policy SuggestedBusinessCriticality = %q, stored view suggested_business_criticality = %q", result.SuggestedBusinessCriticality, row.SuggestedBusinessCriticality)
				}
				if result.SuggestedDataClassification != row.SuggestedDataClassification {
					t.Fatalf("policy SuggestedDataClassification = %q, stored view suggested_data_classification = %q", result.SuggestedDataClassification, row.SuggestedDataClassification)
				}
				if result.EffectiveBusinessCriticality != row.EffectiveBusinessCriticality {
					t.Fatalf("policy EffectiveBusinessCriticality = %q, stored view effective_business_criticality = %q", result.EffectiveBusinessCriticality, row.EffectiveBusinessCriticality)
				}
				if result.EffectiveDataClassification != row.EffectiveDataClassification {
					t.Fatalf("policy EffectiveDataClassification = %q, stored view effective_data_classification = %q", result.EffectiveDataClassification, row.EffectiveDataClassification)
				}
			})
		}
	})
}

func TestEvaluateSaaSRejectsDuplicateGlobalPacks(t *testing.T) {
	t.Parallel()

	registry, err := LoadDocuments(map[string][]byte{
		"a.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: a
  version: 1.0.0
  domain: saas
spec:
  inputs:
    schema: saas_app_risk_input.v1
  levels:
    - level: low
      when: "true"
`),
		"b.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: b
  version: 1.0.0
  domain: saas
spec:
  inputs:
    schema: saas_app_risk_input.v1
  levels:
    - level: low
      when: "true"
`),
	})
	if err != nil {
		t.Fatalf("LoadDocuments() error = %v", err)
	}

	_, err = registry.EvaluateSaaS(SaaSInput{})
	if err == nil {
		t.Fatal("EvaluateSaaS() error = nil, want duplicate pack error")
	}
	if !strings.Contains(err.Error(), "multiple saas risk policy packs") {
		t.Fatalf("EvaluateSaaS() error = %v, want duplicate pack error", err)
	}
}

func TestEvaluateSaaSAllowsDifferentScopedSignalsWithSameInnerRuleID(t *testing.T) {
	t.Parallel()

	registry, err := LoadDocuments(map[string][]byte{
		"global.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: global
  version: 1.0.0
  domain: saas
spec:
  inputs:
    schema: saas_app_risk_input.v1
  levels:
    - level: low
      when: "true"
`),
		"scoped.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: scoped
  version: 1.0.0
  domain: saas
spec:
  scoped_rules:
    - id: github_owner_policy
      scope:
        app:
          canonical_key: github
      rules:
        - id: missing_owner
          severity: high
          score_delta: 5
          when: owner_identity_id == 0
          title: GitHub app has no owner
    - id: github_security_policy
      scope:
        app:
          canonical_key: github
      rules:
        - id: missing_owner
          severity: medium
          score_delta: 7
          when: owner_identity_id == 0
          title: GitHub security review owner is missing
`),
	})
	if err != nil {
		t.Fatalf("LoadDocuments() error = %v", err)
	}

	result, err := registry.EvaluateSaaS(SaaSInput{
		CanonicalKey:    "github",
		OwnerIdentityID: 0,
	})
	if err != nil {
		t.Fatalf("EvaluateSaaS() error = %v", err)
	}
	if result.RiskScore != 12 {
		t.Fatalf("RiskScore = %d, want 12; result=%+v", result.RiskScore, result)
	}
	if len(result.Signals) != 2 {
		t.Fatalf("len(Signals) = %d, want 2; signals=%+v", len(result.Signals), result.Signals)
	}
	gotTitles := []string{result.Signals[0].Title, result.Signals[1].Title}
	wantTitles := []string{
		"GitHub app has no owner",
		"GitHub security review owner is missing",
	}
	if !sameStringSet(gotTitles, wantTitles) {
		t.Fatalf("signal titles = %v, want %v", gotTitles, wantTitles)
	}
}

func TestEvaluateSaaSNormalizesAWSIdentityCenterSourceKindForScopedRules(t *testing.T) {
	t.Parallel()

	registry, err := LoadDocuments(map[string][]byte{
		"global.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: global
  version: 1.0.0
  domain: saas
spec:
  inputs:
    schema: saas_app_risk_input.v1
  levels:
    - level: low
      when: "true"
`),
		"scoped.yaml": []byte(`
api_version: risk.open-sspm.io/v1
kind: RiskPolicyPack
metadata:
  id: scoped
  version: 1.0.0
  domain: saas
spec:
  scoped_rules:
    - id: aws_policy
      scope:
        app:
          source_kind: aws
      rules:
        - id: aws_scoped_signal
          severity: medium
          score_delta: 11
          when: "true"
          title: AWS app matched scoped source policy
`),
	})
	if err != nil {
		t.Fatalf("LoadDocuments() error = %v", err)
	}

	result, err := registry.EvaluateSaaS(SaaSInput{
		SourceKind: "aws_identity_center",
	})
	if err != nil {
		t.Fatalf("EvaluateSaaS() error = %v", err)
	}
	if result.RiskScore != 11 {
		t.Fatalf("RiskScore = %d, want 11; result=%+v", result.RiskScore, result)
	}
	if len(result.Signals) != 1 || result.Signals[0].ID != "aws_scoped_signal" {
		t.Fatalf("signals = %+v, want aws scoped signal", result.Signals)
	}
}

type saasGoldenCase struct {
	name                             string
	input                            SaaSInput
	wantScore                        int
	wantLevel                        string
	wantBusinessCriticality          string
	wantDataClassification           string
	wantEffectiveBusinessCriticality string
	wantEffectiveDataClassification  string
	wantSignalIDs                    []string
}

func saasGoldenCases() []saasGoldenCase {
	return []saasGoldenCase{
		{
			name: "unmanaged critical high usage privileged app",
			input: SaaSInput{
				Actors30d:          250,
				HasPrivilegedScope: true,
				ManagedState:       "unmanaged",
			},
			wantScore:                        100,
			wantLevel:                        SeverityCritical,
			wantBusinessCriticality:          "critical",
			wantDataClassification:           "restricted",
			wantEffectiveBusinessCriticality: "critical",
			wantEffectiveDataClassification:  "restricted",
			wantSignalIDs: []string{
				"unmanaged_app",
				"privileged_scopes",
				"missing_owner",
				"high_actor_count",
				"unmanaged_high_business_app",
				"unmanaged_sensitive_data_app",
			},
		},
		{
			name: "stale connector binding",
			input: SaaSInput{
				Actors30d:       3,
				ManagedState:    "unmanaged",
				ManagedReason:   "stale_sync",
				OwnerIdentityID: 42,
			},
			wantScore:                        45,
			wantLevel:                        SeverityMedium,
			wantBusinessCriticality:          "low",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "low",
			wantEffectiveDataClassification:  "internal",
			wantSignalIDs:                    []string{"unmanaged_app"},
		},
		{
			name: "unmanaged high usage app",
			input: SaaSInput{
				Actors30d:       60,
				ManagedState:    "unmanaged",
				OwnerIdentityID: 42,
			},
			wantScore:                        65,
			wantLevel:                        SeverityHigh,
			wantBusinessCriticality:          "high",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "high",
			wantEffectiveDataClassification:  "internal",
			wantSignalIDs: []string{
				"unmanaged_app",
				"high_actor_count",
				"unmanaged_high_business_app",
			},
		},
		{
			name: "unmanaged confidential app",
			input: SaaSInput{
				HasConfidentialScope: true,
				ManagedState:         "unmanaged",
				OwnerIdentityID:      42,
			},
			wantScore:                        50,
			wantLevel:                        SeverityMedium,
			wantBusinessCriticality:          "low",
			wantDataClassification:           "confidential",
			wantEffectiveBusinessCriticality: "low",
			wantEffectiveDataClassification:  "confidential",
			wantSignalIDs: []string{
				"unmanaged_app",
				"unmanaged_sensitive_data_app",
			},
		},
		{
			name: "managed app with privileged scope",
			input: SaaSInput{
				HasPrivilegedScope: true,
				ManagedState:       "managed",
				OwnerIdentityID:    42,
			},
			wantScore:                        20,
			wantLevel:                        SeverityLow,
			wantBusinessCriticality:          "high",
			wantDataClassification:           "restricted",
			wantEffectiveBusinessCriticality: "high",
			wantEffectiveDataClassification:  "restricted",
			wantSignalIDs:                    []string{"privileged_scopes"},
		},
		{
			name: "github missing owner matches scoped policy by domain",
			input: SaaSInput{
				CanonicalKey:    "domain:github.com",
				PrimaryDomain:   "github.com",
				ManagedState:    "managed",
				OwnerIdentityID: 0,
			},
			wantScore:                        35,
			wantLevel:                        SeverityMedium,
			wantBusinessCriticality:          "high",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "high",
			wantEffectiveDataClassification:  "internal",
			wantSignalIDs: []string{
				"missing_owner",
				"github_missing_owner",
			},
		},
		{
			name: "github missing owner matches scoped policy by vendor",
			input: SaaSInput{
				CanonicalKey:    "okta_app:acme.okta.com:00ogithub",
				VendorName:      "GitHub",
				ManagedState:    "managed",
				OwnerIdentityID: 0,
			},
			wantScore:                        35,
			wantLevel:                        SeverityMedium,
			wantBusinessCriticality:          "high",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "high",
			wantEffectiveDataClassification:  "internal",
			wantSignalIDs: []string{
				"missing_owner",
				"github_missing_owner",
			},
		},
		{
			name: "github domain and vendor do not double score scoped policy",
			input: SaaSInput{
				CanonicalKey:    "domain:github.com",
				PrimaryDomain:   "github.com",
				VendorName:      "GitHub",
				ManagedState:    "managed",
				OwnerIdentityID: 0,
			},
			wantScore:                        35,
			wantLevel:                        SeverityMedium,
			wantBusinessCriticality:          "high",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "high",
			wantEffectiveDataClassification:  "internal",
			wantSignalIDs: []string{
				"missing_owner",
				"github_missing_owner",
			},
		},
		{
			name: "category selectors are dormant until app catalog exists",
			input: SaaSInput{
				Category:        "finance",
				ManagedState:    "unmanaged",
				OwnerIdentityID: 42,
			},
			wantScore:                        45,
			wantLevel:                        SeverityMedium,
			wantBusinessCriticality:          "low",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "low",
			wantEffectiveDataClassification:  "internal",
			wantSignalIDs: []string{
				"unmanaged_app",
			},
		},
		{
			name: "github policy does not lower critical usage suggestion",
			input: SaaSInput{
				CanonicalKey:    "domain:github.com",
				PrimaryDomain:   "github.com",
				Actors30d:       250,
				ManagedState:    "managed",
				OwnerIdentityID: 42,
			},
			wantScore:                        10,
			wantLevel:                        SeverityLow,
			wantBusinessCriticality:          "critical",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "critical",
			wantEffectiveDataClassification:  "internal",
			wantSignalIDs:                    []string{"high_actor_count"},
		},
		{
			name: "governance override lowers effective criticality",
			input: SaaSInput{
				Actors30d:                    250,
				ManagedState:                 "unmanaged",
				OwnerIdentityID:              42,
				EffectiveBusinessCriticality: "low",
			},
			wantScore:                        55,
			wantLevel:                        SeverityMedium,
			wantBusinessCriticality:          "critical",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "low",
			wantEffectiveDataClassification:  "internal",
			wantSignalIDs: []string{
				"unmanaged_app",
				"high_actor_count",
			},
		},
		{
			name: "managed app with owner",
			input: SaaSInput{
				Actors30d:       5,
				ManagedState:    "managed",
				OwnerIdentityID: 42,
			},
			wantScore:                        0,
			wantLevel:                        SeverityLow,
			wantBusinessCriticality:          "low",
			wantDataClassification:           "internal",
			wantEffectiveBusinessCriticality: "low",
			wantEffectiveDataClassification:  "internal",
		},
	}
}

type saasParityCase struct {
	name                         string
	actors30d                    int64
	hasPrivilegedScope           bool
	hasConfidentialScope         bool
	bindingState                 string
	hasOwner                     bool
	effectiveBusinessCriticality string
	effectiveDataClassification  string
}

func saasParityCases() []saasParityCase {
	return []saasParityCase{
		{
			name:               "unmanaged critical high usage privileged app",
			actors30d:          250,
			hasPrivilegedScope: true,
		},
		{
			name:         "stale connector binding",
			actors30d:    3,
			bindingState: "stale",
			hasOwner:     true,
		},
		{
			name:         "connector disabled",
			actors30d:    3,
			bindingState: "disabled",
			hasOwner:     true,
		},
		{
			name:         "connector not configured",
			actors30d:    3,
			bindingState: "not_configured",
			hasOwner:     true,
		},
		{
			name:               "managed app with privileged scope",
			hasPrivilegedScope: true,
			bindingState:       "managed",
			hasOwner:           true,
		},
		{
			name:         "managed app with owner",
			actors30d:    5,
			bindingState: "managed",
			hasOwner:     true,
		},
	}
}

type saasParityReadModel struct {
	CanonicalKey                 string
	DisplayName                  string
	PrimaryDomain                string
	VendorName                   string
	Actors30d                    int64
	HasPrivilegedScope           bool
	HasConfidentialScope         bool
	ManagedState                 string
	ManagedReason                string
	OwnerIdentityID              int64
	GovernanceState              string
	ReviewDisposition            string
	EffectiveBusinessCriticality string
	EffectiveDataClassification  string
	ConnectorConfigured          bool
	ConnectorEnabled             bool
	ConnectorStale               bool
	ConnectorHealthy             bool
	RiskScore                    int32
	RiskLevel                    string
	SuggestedBusinessCriticality string
	SuggestedDataClassification  string
}

func saasParityInput(index int, tc saasParityCase, ownerID int64) SaaSInput {
	canonicalKey := fmt.Sprintf("saas-risk-parity-%d", index)
	input := SaaSInput{
		CanonicalKey:                 canonicalKey,
		DisplayName:                  fmt.Sprintf("SaaS Risk Parity %d", index),
		PrimaryDomain:                canonicalKey + ".example.com",
		VendorName:                   "Example",
		Actors30d:                    tc.actors30d,
		HasPrivilegedScope:           tc.hasPrivilegedScope,
		HasConfidentialScope:         tc.hasConfidentialScope,
		ManagedState:                 "unmanaged",
		ManagedReason:                "no_binding",
		GovernanceState:              "unreviewed",
		ReviewDisposition:            "unreviewed",
		EffectiveBusinessCriticality: configuredOrUnknown(tc.effectiveBusinessCriticality),
		EffectiveDataClassification:  configuredOrUnknown(tc.effectiveDataClassification),
	}
	if tc.hasOwner {
		input.OwnerIdentityID = ownerID
	}

	if tc.bindingState == "" {
		return input
	}

	input.SourceKind = "entra"
	input.SourceName = fmt.Sprintf("tenant-%d", index)
	input.ConnectorBindingConfigured = true
	input.ConnectorBindingEnabled = true
	switch tc.bindingState {
	case "disabled":
		input.ManagedReason = "connector_disabled"
		input.ConnectorBindingEnabled = false
	case "not_configured":
		input.ManagedReason = "connector_not_configured"
		input.ConnectorBindingConfigured = false
	case "stale":
		input.ManagedReason = "stale_sync"
		input.ConnectorBindingStale = true
	case "managed":
		input.ManagedState = "managed"
		input.ManagedReason = "active_binding_fresh_sync"
		input.ConnectorBindingHealthy = true
	}
	return input
}

func configuredOrUnknown(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unknown"
	}
	return value
}

func upsertSaaSParityRiskReadModel(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID int64, result SaaSResult) {
	t.Helper()

	policyPacksJSON, err := json.Marshal(result.PolicyPacks)
	if err != nil {
		t.Fatalf("marshal policy packs: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO saas_app_risk_read_models (
			saas_app_id,
			risk_score,
			risk_level,
			risk_rank,
			suggested_business_criticality,
			suggested_data_classification,
			effective_business_criticality,
			effective_data_classification,
			policy_packs_json
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (saas_app_id) DO UPDATE SET
			risk_score = EXCLUDED.risk_score,
			risk_level = EXCLUDED.risk_level,
			risk_rank = EXCLUDED.risk_rank,
			suggested_business_criticality = EXCLUDED.suggested_business_criticality,
			suggested_data_classification = EXCLUDED.suggested_data_classification,
			effective_business_criticality = EXCLUDED.effective_business_criticality,
			effective_data_classification = EXCLUDED.effective_data_classification,
			policy_packs_json = EXCLUDED.policy_packs_json,
			projection_refreshed_at = now()
	`, appID,
		result.RiskScore,
		result.RiskLevel,
		result.RiskRank,
		result.SuggestedBusinessCriticality,
		result.SuggestedDataClassification,
		result.EffectiveBusinessCriticality,
		result.EffectiveDataClassification,
		policyPacksJSON,
	)
	if err != nil {
		t.Fatalf("upsert saas app risk read model: %v", err)
	}
}

func insertSaaSParityOwner(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()

	var ownerID int64
	err := pool.QueryRow(ctx, `
		INSERT INTO identities (kind, display_name, primary_email)
		VALUES ('human', 'SaaS Risk Owner', 'saas-risk-owner@example.com')
		RETURNING id
	`).Scan(&ownerID)
	if err != nil {
		t.Fatalf("insert identity: %v", err)
	}
	return ownerID
}

func insertSaaSParityApp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, index int, tc saasParityCase, ownerID int64, now time.Time) int64 {
	t.Helper()

	canonicalKey := fmt.Sprintf("saas-risk-parity-%d", index)
	var appID int64
	err := pool.QueryRow(ctx, `
		INSERT INTO saas_apps (
			canonical_key,
			display_name,
			primary_domain,
			vendor_name,
			actors_30d,
			has_privileged_scope,
			has_confidential_scope
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`,
		canonicalKey,
		fmt.Sprintf("SaaS Risk Parity %d", index),
		canonicalKey+".example.com",
		"Example",
		tc.actors30d,
		tc.hasPrivilegedScope,
		tc.hasConfidentialScope,
	).Scan(&appID)
	if err != nil {
		t.Fatalf("insert saas app: %v", err)
	}

	if tc.hasOwner || tc.effectiveBusinessCriticality != "" || tc.effectiveDataClassification != "" {
		insertSaaSParityGovernance(t, ctx, pool, appID, ownerID, tc)
	}
	if tc.bindingState != "" {
		insertSaaSParityBinding(t, ctx, pool, appID, index, tc.bindingState, now)
	}
	return appID
}

func insertSaaSParityGovernance(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID, ownerID int64, tc saasParityCase) {
	t.Helper()

	businessCriticality := tc.effectiveBusinessCriticality
	if businessCriticality == "" {
		businessCriticality = "unknown"
	}
	dataClassification := tc.effectiveDataClassification
	if dataClassification == "" {
		dataClassification = "unknown"
	}
	var nullableOwnerID any
	if tc.hasOwner {
		nullableOwnerID = ownerID
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO governance_subject_overrides (
			subject_kind,
			subject_id,
			owner_identity_id,
			business_criticality,
			data_classification
		)
		VALUES ('saas_app', $1, $2, $3, $4)
	`, appID, nullableOwnerID, businessCriticality, dataClassification)
	if err != nil {
		t.Fatalf("insert governance override: %v", err)
	}
}

func insertSaaSParityBinding(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID int64, index int, bindingState string, now time.Time) {
	t.Helper()

	sourceName := fmt.Sprintf("tenant-%d", index)
	_, err := pool.Exec(ctx, `
		INSERT INTO saas_app_bindings (
			saas_app_id,
			connector_kind,
			connector_source_name,
			binding_source,
			confidence,
			is_primary
		)
		VALUES ($1, 'entra', $2, 'auto', 1, true)
	`, appID, sourceName)
	if err != nil {
		t.Fatalf("insert saas app binding: %v", err)
	}

	enabled := true
	configured := true
	lastSuccessAt := now
	freshUntilAt := now.Add(time.Hour)
	switch bindingState {
	case "disabled":
		enabled = false
	case "not_configured":
		configured = false
	case "stale":
		lastSuccessAt = now.Add(-2 * time.Hour)
		freshUntilAt = now.Add(-time.Hour)
	case "managed":
	default:
		t.Fatalf("unknown binding state %q", bindingState)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO connector_source_state (
			source_kind,
			source_name,
			enabled,
			configured,
			discovery_enabled,
			last_success_at,
			fresh_until_at
		)
		VALUES ('entra', $1, $2, $3, true, $4, $5)
	`, sourceName, enabled, configured, lastSuccessAt, freshUntilAt)
	if err != nil {
		t.Fatalf("insert connector source state: %v", err)
	}
}

func getSaaSParityReadModel(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID int64) saasParityReadModel {
	t.Helper()

	var row saasParityReadModel
	err := pool.QueryRow(ctx, `
		SELECT
			canonical_key,
			display_name,
			primary_domain,
			vendor_name,
			actors_30d,
			has_privileged_scope,
			has_confidential_scope,
			managed_state,
			managed_reason,
			owner_identity_id,
			governance_state,
			review_disposition,
			effective_business_criticality,
			effective_data_classification,
			connector_configured,
			connector_enabled,
			COALESCE(fresh_until_at < now(), false) AS connector_stale,
			managed_state = 'managed' AS connector_healthy,
			risk_score,
			risk_level,
			suggested_business_criticality,
			suggested_data_classification
		FROM discovery_app_read_models_v
		WHERE id = $1
	`, appID).Scan(
		&row.CanonicalKey,
		&row.DisplayName,
		&row.PrimaryDomain,
		&row.VendorName,
		&row.Actors30d,
		&row.HasPrivilegedScope,
		&row.HasConfidentialScope,
		&row.ManagedState,
		&row.ManagedReason,
		&row.OwnerIdentityID,
		&row.GovernanceState,
		&row.ReviewDisposition,
		&row.EffectiveBusinessCriticality,
		&row.EffectiveDataClassification,
		&row.ConnectorConfigured,
		&row.ConnectorEnabled,
		&row.ConnectorStale,
		&row.ConnectorHealthy,
		&row.RiskScore,
		&row.RiskLevel,
		&row.SuggestedBusinessCriticality,
		&row.SuggestedDataClassification,
	)
	if err != nil {
		t.Fatalf("query discovery app read model: %v", err)
	}
	return row
}
