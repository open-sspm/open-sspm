package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/findings"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

type fakeDatasets struct {
	data map[string][]any
	errs map[string]runtimev2.DatasetErrorKind
}

func (f *fakeDatasets) Capabilities(ctx context.Context) []runtimev2.DatasetRef {
	_ = ctx
	return nil
}

func (f *fakeDatasets) GetDataset(ctx context.Context, eval runtimev2.EvalContext, ref runtimev2.DatasetRef) runtimev2.DatasetResult {
	_, _ = ctx, eval

	key := ref.Dataset
	if f.errs != nil {
		if kind, ok := f.errs[key]; ok {
			return runtimev2.DatasetResult{Error: &runtimev2.DatasetError{Kind: kind}}
		}
	}

	rows, ok := f.data[key]
	if !ok {
		return runtimev2.DatasetResult{Error: &runtimev2.DatasetError{Kind: runtimev2.DatasetErrorKind_MISSING_DATASET}}
	}

	raw := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			return runtimev2.DatasetResult{Error: &runtimev2.DatasetError{Kind: runtimev2.DatasetErrorKind_ENGINE_ERROR, Message: err.Error()}}
		}
		raw = append(raw, json.RawMessage(b))
	}
	return runtimev2.DatasetResult{Rows: raw}
}

func TestEvalCheck_RegoPass(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:policies/sign-on": {
					map[string]any{"id": "a", "session": map[string]any{"max_idle_minutes": float64(10)}},
					map[string]any{"id": "b", "session": map[string]any{"max_idle_minutes": float64(15)}},
				},
			},
		},
	}

	rule := regoRule("Idle timeout", []string{"okta:policies/sign-on"}, idleTimeoutRego, map[string]any{"max_idle_minutes": float64(15)})

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "pass" {
		t.Fatalf("expected pass, got %q", ev.Status)
	}
}

func TestEvalCheck_RegoFail(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:policies/sign-on": {
					map[string]any{"id": "a", "session": map[string]any{"max_idle_minutes": float64(30)}},
				},
			},
		},
	}

	rule := regoRule("Idle timeout", []string{"okta:policies/sign-on"}, idleTimeoutRego, map[string]any{"max_idle_minutes": float64(15)})

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "fail" {
		t.Fatalf("expected fail, got %q", ev.Status)
	}
}

func TestEvalCheck_RegoCountComparePass(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:apps": {
					map[string]any{"id": "a", "active": true},
					map[string]any{"id": "b", "active": true},
					map[string]any{"id": "c", "active": false},
				},
			},
		},
	}

	rule := regoRule("Count active apps", []string{"okta:apps"}, activeAppsRego, nil)

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "pass" {
		t.Fatalf("expected pass, got %q", ev.Status)
	}
}

func TestEvalCheck_MissingDatasetIsPolicyUnknown(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{},
			errs: map[string]runtimev2.DatasetErrorKind{
				"missing": runtimev2.DatasetErrorKind_MISSING_DATASET,
			},
		},
	}

	rule := regoRule("Missing dataset", []string{"missing"}, datasetErrorRego, nil)

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "unknown" || ev.ErrorKind != "" {
		t.Fatalf("expected unknown policy result, got %q/%q", ev.Status, ev.ErrorKind)
	}
}

func TestEvalCheck_ManualRuleUnknown(t *testing.T) {
	e := &Engine{Datasets: &fakeDatasets{data: map[string][]any{}}}

	rule := osspecv2.Rule{
		Title:      "Manual control",
		Monitoring: osspecv2.Monitoring{Status: osspecv2.MonitoringStatus_MANUAL},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "unknown" {
		t.Fatalf("expected unknown, got %q", ev.Status)
	}
}

func TestOverrideLookupsFallbackToAggregateSourceName(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "rules_engine_overrides"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		e := &Engine{Q: q}
		ruleset, rule := seedEngineRule(t, ctx, q)
		scopeKind := "connector_instance"
		sourceKind := "okta"
		sourceName := "example.okta.com"

		if _, err := q.UpsertRulesetOverride(ctx, gen.UpsertRulesetOverrideParams{
			RulesetID:  ruleset.ID,
			ScopeKind:  scopeKind,
			SourceKind: sourceKind,
			SourceName: "",
			Enabled:    false,
		}); err != nil {
			t.Fatalf("UpsertRulesetOverride(aggregate) err = %v", err)
		}
		disabled, err := e.isRulesetDisabled(ctx, ruleset.ID, scopeKind, sourceKind, sourceName)
		if err != nil {
			t.Fatalf("isRulesetDisabled(aggregate fallback) err = %v", err)
		}
		if !disabled {
			t.Fatalf("ruleset override did not fall back from source %q to aggregate source", sourceName)
		}

		if _, err := q.UpsertRulesetOverride(ctx, gen.UpsertRulesetOverrideParams{
			RulesetID:  ruleset.ID,
			ScopeKind:  scopeKind,
			SourceKind: sourceKind,
			SourceName: sourceName,
			Enabled:    true,
		}); err != nil {
			t.Fatalf("UpsertRulesetOverride(exact) err = %v", err)
		}
		disabled, err = e.isRulesetDisabled(ctx, ruleset.ID, scopeKind, sourceKind, sourceName)
		if err != nil {
			t.Fatalf("isRulesetDisabled(exact) err = %v", err)
		}
		if disabled {
			t.Fatalf("exact ruleset override did not win over aggregate fallback")
		}

		if _, err := q.UpsertRuleOverride(ctx, gen.UpsertRuleOverrideParams{
			RuleID:     rule.ID,
			ScopeKind:  scopeKind,
			SourceKind: sourceKind,
			SourceName: "",
			Params:     []byte(`{"source":"aggregate"}`),
			Enabled:    false,
		}); err != nil {
			t.Fatalf("UpsertRuleOverride(aggregate) err = %v", err)
		}
		override, err := e.getRuleOverride(ctx, rule.ID, scopeKind, sourceKind, sourceName)
		if err != nil {
			t.Fatalf("getRuleOverride(aggregate fallback) err = %v", err)
		}
		if override == nil || override.Enabled {
			t.Fatalf("rule override did not fall back to disabled aggregate override: %#v", override)
		}

		if _, err := q.UpsertRuleOverride(ctx, gen.UpsertRuleOverrideParams{
			RuleID:     rule.ID,
			ScopeKind:  scopeKind,
			SourceKind: sourceKind,
			SourceName: sourceName,
			Params:     []byte(`{"source":"exact"}`),
			Enabled:    true,
		}); err != nil {
			t.Fatalf("UpsertRuleOverride(exact) err = %v", err)
		}
		override, err = e.getRuleOverride(ctx, rule.ID, scopeKind, sourceKind, sourceName)
		if err != nil {
			t.Fatalf("getRuleOverride(exact) err = %v", err)
		}
		if override == nil || !override.Enabled {
			t.Fatalf("exact rule override did not win over aggregate fallback: %#v", override)
		}

		if _, err := q.UpsertRuleAttestation(ctx, gen.UpsertRuleAttestationParams{
			RuleID:     rule.ID,
			ScopeKind:  scopeKind,
			SourceKind: sourceKind,
			SourceName: "",
			Status:     "accepted",
			Notes:      "aggregate",
			ExpiresAt:  pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		}); err != nil {
			t.Fatalf("UpsertRuleAttestation(aggregate) err = %v", err)
		}
		attestation, err := e.getActiveAttestation(ctx, rule.ID, scopeKind, sourceKind, sourceName, time.Now())
		if err != nil {
			t.Fatalf("getActiveAttestation(aggregate fallback) err = %v", err)
		}
		if attestation == nil || attestation.Notes != "aggregate" {
			t.Fatalf("attestation did not fall back to aggregate source: %#v", attestation)
		}

		if _, err := q.UpsertRuleAttestation(ctx, gen.UpsertRuleAttestationParams{
			RuleID:     rule.ID,
			ScopeKind:  scopeKind,
			SourceKind: sourceKind,
			SourceName: sourceName,
			Status:     "accepted",
			Notes:      "exact",
			ExpiresAt:  pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		}); err != nil {
			t.Fatalf("UpsertRuleAttestation(exact) err = %v", err)
		}
		attestation, err = e.getActiveAttestation(ctx, rule.ID, scopeKind, sourceKind, sourceName, time.Now())
		if err != nil {
			t.Fatalf("getActiveAttestation(exact) err = %v", err)
		}
		if attestation == nil || attestation.Notes != "exact" {
			t.Fatalf("exact attestation did not win over aggregate fallback: %#v", attestation)
		}
	})
}

func TestRuleFindingResultKeepsActionableNonPassStatusesOpen(t *testing.T) {
	ruleset := gen.Ruleset{Key: "cis.okta.idaas_stig.v2"}
	rule := gen.Rule{Key: "mfa-required", Title: "MFA required", Severity: "high"}
	evalCtx := Context{ScopeKind: "connector_instance", SourceKind: "okta", SourceName: "example.okta.com"}
	evaluatedAt := time.Date(2026, time.May, 21, 12, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		status string
		want   findings.Status
	}{
		{status: "fail", want: findings.StatusOpen},
		{status: "error", want: findings.StatusOpen},
		{status: "unknown", want: findings.StatusOpen},
		{status: "pass", want: findings.StatusResolved},
		{status: "not_applicable", want: findings.StatusResolved},
		{status: "skipped", want: findings.StatusResolved},
	} {
		got := ruleFindingResult(ruleset, rule, evalCtx, Evaluation{
			Status:          tc.status,
			EvidenceSummary: tc.status,
			EvidenceJSON:    []byte(`{}`),
		}, []string{}, evaluatedAt)
		if got.Status != tc.want {
			t.Fatalf("status %q mapped to %q, want %q", tc.status, got.Status, tc.want)
		}
	}
}

func TestWriteEvaluationRollsBackRuleRowsWhenFindingWriteFails(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "rules_engine_tx"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		ruleset, rule := seedEngineRule(t, ctx, q)
		badRule := rule
		badRule.Key = ""
		evaluatedAt := time.Date(2026, time.May, 21, 13, 0, 0, 0, time.UTC)
		e := &Engine{
			Q:  q,
			DB: pool,
			Now: func() time.Time {
				return evaluatedAt
			},
		}

		err := e.writeEvaluation(ctx, ruleset, badRule, Context{
			ScopeKind:  "connector_instance",
			SourceKind: "okta",
			SourceName: "example.okta.com",
		}, Evaluation{
			Status:          "fail",
			EvidenceSummary: "Rule failed",
			EvidenceJSON:    []byte(`{}`),
		})
		if err == nil {
			t.Fatalf("writeEvaluation() err = nil, want invalid finding error")
		}

		assertEngineCount(t, ctx, pool, `SELECT count(*) FROM rule_evaluations WHERE rule_id = $1`, 0, rule.ID)
		assertEngineCount(t, ctx, pool, `SELECT count(*) FROM rule_results_current WHERE rule_id = $1`, 0, rule.ID)
	})
}

func seedEngineRule(t *testing.T, ctx context.Context, q *gen.Queries) (gen.Ruleset, gen.Rule) {
	t.Helper()
	ruleset, err := q.UpsertRuleset(ctx, gen.UpsertRulesetParams{
		Key:            "cis.okta.idaas_stig.v2",
		Name:           "CIS Okta",
		Description:    "Okta STIG",
		Source:         "test",
		SourceVersion:  "v2",
		SourceDate:     pgtype.Date{Time: time.Date(2026, time.May, 1, 0, 0, 0, 0, time.UTC), Valid: true},
		ScopeKind:      "connector_instance",
		ConnectorKind:  pgtype.Text{String: "okta", Valid: true},
		Status:         "active",
		DefinitionHash: "test-hash",
		DefinitionJson: []byte(`{"key":"cis.okta.idaas_stig.v2"}`),
	})
	if err != nil {
		t.Fatalf("UpsertRuleset() err = %v", err)
	}
	rule, err := q.UpsertRule(ctx, gen.UpsertRuleParams{
		RulesetID:        ruleset.ID,
		Key:              "mfa-required",
		Title:            "MFA required",
		Summary:          "Summary",
		Category:         "identity",
		Severity:         "high",
		MonitoringStatus: "monitored",
		RequiredData:     []byte(`[]`),
		ExpectedParams:   []byte(`{}`),
		RuleVersion:      "v1",
		IsActive:         true,
		DefinitionJson:   []byte(`{"check":{"type":"manual"}}`),
	})
	if err != nil {
		t.Fatalf("UpsertRule() err = %v", err)
	}
	return ruleset, rule
}

func assertEngineCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, want int, args ...any) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("count query err = %v", err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
}

func regoRule(title string, requiredData []string, rego string, defaults map[string]any) osspecv2.Rule {
	rule := osspecv2.Rule{
		Key:          "rule",
		Title:        title,
		Monitoring:   osspecv2.Monitoring{Status: osspecv2.MonitoringStatus_AUTOMATED},
		RequiredData: requiredData,
		Check: &osspecv2.Check{
			Engine:  osspecv2.CheckEngine_REGO,
			Package: "opensspm.tests",
			Query:   "data.opensspm.tests.result",
			Rego:    rego,
		},
	}
	if defaults != nil {
		rule.Parameters = &osspecv2.Parameters{Defaults: defaults}
	}
	return rule
}

const idleTimeoutRego = `package opensspm.tests

rows := object.get(object.get(input.datasets, "okta:policies/sign-on", {}), "rows", [])
passed := [r |
  r := rows[_]
  object.get(object.get(r, "session", {}), "max_idle_minutes", 999999) <= input.params.max_idle_minutes
]

result := {
  "status": "pass",
  "selected_count": count(rows),
  "passed_count": count(passed),
  "count_value": count(passed),
} if {
  count(rows) > 0
  count(passed) == count(rows)
}

result := {
  "status": "fail",
  "selected_count": count(rows),
  "passed_count": count(passed),
  "count_value": count(passed),
} if {
  count(rows) > 0
  count(passed) != count(rows)
}`

const activeAppsRego = `package opensspm.tests

rows := object.get(object.get(input.datasets, "okta:apps", {}), "rows", [])
active := [r | r := rows[_]; object.get(r, "active", false) == true]

result := {
  "status": "pass",
  "selected_count": count(active),
  "passed_count": 1,
  "count_value": count(active),
  "target_value": 2,
} if {
  count(active) == 2
}

result := {
  "status": "fail",
  "selected_count": count(active),
  "passed_count": 0,
  "count_value": count(active),
  "target_value": 2,
} if {
  count(active) != 2
}`

const datasetErrorRego = `package opensspm.tests

result := {"status": "unknown", "reason_code": sprintf("dataset_%s", [kind])} if {
  err := object.get(object.get(input.datasets, "missing", {}), "error", null)
  err != null
  kind := object.get(err, "kind", "engine_error")
}`
