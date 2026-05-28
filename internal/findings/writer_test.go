package findings

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestWriterUpsertsFindingsAndDedupesLifecycleEvents(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "findings_writer"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		writer := NewWriter(q)
		evaluatedAt := time.Date(2026, time.May, 20, 12, 0, 0, 0, time.UTC)

		result := testFindingResult("fail", StatusOpen, evaluatedAt)
		if err := writer.Write(ctx, result); err != nil {
			t.Fatalf("Write(open) err = %v", err)
		}

		row, err := q.GetFindingByKey(ctx, result.Key)
		if err != nil {
			t.Fatalf("GetFindingByKey(open) err = %v", err)
		}
		if row.Status != string(StatusOpen) {
			t.Fatalf("status = %q, want %q", row.Status, StatusOpen)
		}
		if !row.FirstSeenAt.Valid || !row.FirstSeenAt.Time.Equal(evaluatedAt) {
			t.Fatalf("first_seen_at = %v, want %v", row.FirstSeenAt, evaluatedAt)
		}
		if got := countFindingEvents(t, ctx, pool, result.Key); got != 1 {
			t.Fatalf("finding events after first write = %d, want 1", got)
		}

		if err := writer.Write(ctx, result); err != nil {
			t.Fatalf("Write(duplicate open) err = %v", err)
		}
		if got := countFindingEvents(t, ctx, pool, result.Key); got != 1 {
			t.Fatalf("finding events after duplicate write = %d, want 1", got)
		}

		stillOpen := result
		stillOpen.EvaluatedAt = evaluatedAt.Add(30 * time.Minute)
		stillOpen.Summary = "Rule still failed"
		stillOpen.Evidence = "Rule still failed"
		if err := writer.Write(ctx, stillOpen); err != nil {
			t.Fatalf("Write(still open) err = %v", err)
		}
		if got := countFindingEvents(t, ctx, pool, result.Key); got != 1 {
			t.Fatalf("finding events after unchanged open write = %d, want 1", got)
		}

		resolvedAt := evaluatedAt.Add(time.Hour)
		resolved := result
		resolved.Status = StatusResolved
		resolved.Summary = "Rule passed"
		resolved.Evidence = "Rule passed"
		resolved.EvaluatedAt = resolvedAt
		resolved.Output = map[string]any{
			"rule_result": map[string]any{
				"status":      "pass",
				"sync_run_id": "43",
			},
		}
		if err := writer.Write(ctx, resolved); err != nil {
			t.Fatalf("Write(resolved) err = %v", err)
		}

		row, err = q.GetFindingByKey(ctx, result.Key)
		if err != nil {
			t.Fatalf("GetFindingByKey(resolved) err = %v", err)
		}
		if row.Status != string(StatusResolved) {
			t.Fatalf("status = %q, want %q", row.Status, StatusResolved)
		}
		if !row.FirstSeenAt.Valid || !row.FirstSeenAt.Time.Equal(evaluatedAt) {
			t.Fatalf("first_seen_at = %v, want preserved %v", row.FirstSeenAt, evaluatedAt)
		}
		if !row.LastSeenAt.Valid || !row.LastSeenAt.Time.Equal(resolvedAt) {
			t.Fatalf("last_seen_at = %v, want %v", row.LastSeenAt, resolvedAt)
		}
		if !row.ResolvedAt.Valid {
			t.Fatalf("resolved_at was not set")
		}
		if got := countFindingEvents(t, ctx, pool, result.Key); got != 2 {
			t.Fatalf("finding events after resolved write = %d, want 2", got)
		}
	})
}

func TestWriterPreservesActiveSuppressionOnEvaluationUpsert(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "findings_writer_suppression"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		writer := NewWriter(q)
		evaluatedAt := time.Date(2026, time.May, 20, 12, 0, 0, 0, time.UTC)
		result := testFindingResult("fail", StatusOpen, evaluatedAt)
		if err := writer.Write(ctx, result); err != nil {
			t.Fatalf("Write(open) err = %v", err)
		}

		suppressedUntil := evaluatedAt.Add(24 * time.Hour)
		suppressedAt := evaluatedAt.Add(10 * time.Minute)
		if _, err := pool.Exec(ctx, `
			UPDATE findings
			SET status = 'suppressed',
			    suppressed_until = $2,
			    suppression_reason = 'maintenance window',
			    suppressed_by = 'admin@example.com',
			    suppressed_at = $3
			WHERE finding_key = $1
		`, result.Key, suppressedUntil, suppressedAt); err != nil {
			t.Fatalf("suppress finding err = %v", err)
		}

		next := result
		next.EvaluatedAt = evaluatedAt.Add(time.Hour)
		next.Summary = "Rule still failed"
		next.Evidence = "Rule still failed"
		if err := writer.Write(ctx, next); err != nil {
			t.Fatalf("Write(open while suppressed) err = %v", err)
		}

		row, err := q.GetFindingByKey(ctx, result.Key)
		if err != nil {
			t.Fatalf("GetFindingByKey() err = %v", err)
		}
		if row.Status != string(StatusSuppressed) {
			t.Fatalf("status = %q, want %q", row.Status, StatusSuppressed)
		}
		if !row.SuppressedUntil.Valid || !row.SuppressedUntil.Time.Equal(suppressedUntil) {
			t.Fatalf("suppressed_until = %v, want %v", row.SuppressedUntil, suppressedUntil)
		}
		if row.SuppressionReason != "maintenance window" || row.SuppressedBy != "admin@example.com" {
			t.Fatalf("suppression fields = %q/%q, want preserved", row.SuppressionReason, row.SuppressedBy)
		}
		if !row.SuppressedAt.Valid || !row.SuppressedAt.Time.Equal(suppressedAt) {
			t.Fatalf("suppressed_at = %v, want %v", row.SuppressedAt, suppressedAt)
		}
		if got := countFindingEvents(t, ctx, pool, result.Key); got != 1 {
			t.Fatalf("finding events after suppressed evaluation = %d, want 1", got)
		}
	})
}

func TestListFindingRulesetCurrentByRulesetKeyReadsCanonicalFindings(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "findings_ruleset_readmodel"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		ruleset := seedFindingRuleset(t, ctx, q)
		seedFindingConnectorSource(t, ctx, q, "example.okta.com", true)
		rules := map[string]gen.Rule{}
		for _, key := range []string{
			"001-pass",
			"002-fail",
			"003-error",
			"004-not-applicable",
			"005-no-finding",
		} {
			rules[key] = seedFindingRule(t, ctx, q, ruleset.ID, key)
		}

		writer := NewWriter(q)
		evaluatedAt := time.Date(2026, time.May, 21, 9, 0, 0, 0, time.UTC)
		for _, tc := range []struct {
			key           string
			currentStatus string
			findingStatus Status
			errorKind     string
		}{
			{key: "001-pass", currentStatus: "pass", findingStatus: StatusResolved},
			{key: "002-fail", currentStatus: "fail", findingStatus: StatusOpen},
			{key: "003-error", currentStatus: "error", findingStatus: StatusOpen, errorKind: "engine_error"},
			{key: "004-not-applicable", currentStatus: "not_applicable", findingStatus: StatusResolved},
		} {
			result := testRulesetFindingResult(ruleset.Key, rules[tc.key], tc.currentStatus, tc.findingStatus, evaluatedAt, tc.errorKind)
			if err := writer.Write(ctx, result); err != nil {
				t.Fatalf("Write(%s) err = %v", tc.key, err)
			}
		}

		rows, err := q.ListFindingRulesetCurrentByRulesetKey(ctx, gen.ListFindingRulesetCurrentByRulesetKeyParams{
			ScopeKind:  "connector_instance",
			SourceKind: "okta",
			SourceName: "example.okta.com",
			Key:        ruleset.Key,
		})
		if err != nil {
			t.Fatalf("ListFindingRulesetCurrentByRulesetKey() err = %v", err)
		}
		if len(rows) != 5 {
			t.Fatalf("rows = %d, want 5", len(rows))
		}

		statusByRule := map[string]string{}
		errorKindByRule := map[string]string{}
		summaryByRule := map[string]string{}
		for _, row := range rows {
			statusByRule[row.Key] = row.CurrentStatus
			errorKindByRule[row.Key] = row.CurrentErrorKind
			summaryByRule[row.Key] = row.CurrentEvidenceSummary
		}

		wantStatuses := map[string]string{
			"001-pass":           "pass",
			"002-fail":           "fail",
			"003-error":          "error",
			"004-not-applicable": "not_applicable",
			"005-no-finding":     "unknown",
		}
		for key, want := range wantStatuses {
			if got := statusByRule[key]; got != want {
				t.Fatalf("status[%s] = %q, want %q", key, got, want)
			}
		}
		if got := errorKindByRule["003-error"]; got != "engine_error" {
			t.Fatalf("error kind = %q, want engine_error", got)
		}
		if got := summaryByRule["001-pass"]; got == "" {
			t.Fatalf("summary[001-pass] is empty, want canonical evidence summary")
		}
	})
}

func TestListFindingRulesetCurrentByRulesetKeyAggregatesAllConfiguredSources(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "findings_ruleset_all_configured"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		ruleset := seedFindingRuleset(t, ctx, q)
		rule := seedFindingRule(t, ctx, q, ruleset.ID, "001-aggregate")
		seedFindingConnectorSource(t, ctx, q, "example.okta.com", true)
		seedFindingConnectorSource(t, ctx, q, "second.okta.com", true)
		seedFindingConnectorSource(t, ctx, q, "retired.okta.com", false)

		writer := NewWriter(q)
		evaluatedAt := time.Date(2026, time.May, 21, 10, 0, 0, 0, time.UTC)
		example := testRulesetFindingResult(ruleset.Key, rule, "pass", StatusResolved, evaluatedAt, "")
		if err := writer.Write(ctx, example); err != nil {
			t.Fatalf("Write(example pass) err = %v", err)
		}
		second := withFindingSource(testRulesetFindingResult(ruleset.Key, rule, "fail", StatusOpen, evaluatedAt.Add(time.Minute), ""), "second.okta.com")
		if err := writer.Write(ctx, second); err != nil {
			t.Fatalf("Write(second fail) err = %v", err)
		}
		retired := withFindingSource(testRulesetFindingResult(ruleset.Key, rule, "error", StatusResolved, evaluatedAt.Add(2*time.Minute), "retired_error"), "retired.okta.com")
		if err := writer.Write(ctx, retired); err != nil {
			t.Fatalf("Write(retired error) err = %v", err)
		}

		rows, err := q.ListFindingRulesetCurrentByRulesetKey(ctx, gen.ListFindingRulesetCurrentByRulesetKeyParams{
			ScopeKind:  "connector_instance",
			SourceKind: "okta",
			SourceName: "",
			Key:        ruleset.Key,
		})
		if err != nil {
			t.Fatalf("ListFindingRulesetCurrentByRulesetKey(all configured) err = %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(rows))
		}
		if got := rows[0].CurrentStatus; got != "fail" {
			t.Fatalf("current_status = %q, want fail", got)
		}
		if got := rows[0].CurrentEvidenceSummary; got != "1 fail, 0 error, 0 unknown, 0 not applicable, 1 pass" {
			t.Fatalf("current_evidence_summary = %q", got)
		}
		if got := rows[0].CurrentErrorKind; got != "" {
			t.Fatalf("current_error_kind = %q, want empty retired source ignored", got)
		}
	})
}

func testFindingResult(currentStatus string, status Status, evaluatedAt time.Time) FindingResult {
	result := FindingResult{
		Status:            status,
		BaseSeverity:      "high",
		EffectiveSeverity: "high",
		SeveritySource:    SeveritySourcePolicy,
		Title:             "MFA required",
		Summary:           "Rule failed",
		Evidence:          "Rule failed",
		Source:            SourceRef{Kind: "okta", Name: "example.okta.com"},
		Scope: ScopeRef{
			Kind:       "connector_instance",
			SourceKind: "okta",
			SourceName: "example.okta.com",
		},
		Entity:   EntityRef{Kind: "ruleset_scope", ID: "connector_instance:okta:example.okta.com", Name: "example.okta.com"},
		Resource: ResourceRef{Kind: "rule", ID: "mfa-required", Name: "MFA required"},
		Policy: PolicyRef{
			BundleID:      "cis.okta.idaas_stig.v2",
			BundleVersion: "v2",
			ID:            "mfa-required",
			Title:         "MFA required",
			RuleID:        "mfa-required",
			RulesetID:     "cis.okta.idaas_stig.v2",
		},
		Output: map[string]any{
			"rule_result": map[string]any{
				"status":      currentStatus,
				"sync_run_id": "42",
			},
		},
		EvaluatedAt: evaluatedAt,
	}
	result.Key = BuildKey(result)
	return result
}

func testRulesetFindingResult(rulesetKey string, rule gen.Rule, currentStatus string, status Status, evaluatedAt time.Time, errorKind string) FindingResult {
	result := testFindingResult(currentStatus, status, evaluatedAt)
	result.Title = rule.Title
	result.Summary = "canonical " + currentStatus
	result.Evidence = result.Summary
	result.Resource = ResourceRef{Kind: "rule", ID: rule.Key, Name: rule.Title}
	result.Policy = PolicyRef{
		BundleID:      rulesetKey,
		BundleVersion: "v2",
		ID:            rule.Key,
		Title:         rule.Title,
		RuleID:        rule.Key,
		RulesetID:     rulesetKey,
	}
	result.Output = map[string]any{
		"rule_result": map[string]any{
			"status":      currentStatus,
			"error_kind":  errorKind,
			"sync_run_id": "42",
		},
	}
	result.Key = BuildKey(result)
	return result
}

func withFindingSource(result FindingResult, sourceName string) FindingResult {
	result.Source.Name = sourceName
	result.Scope.SourceName = sourceName
	result.Entity.ID = "connector_instance:okta:" + sourceName
	result.Entity.Name = sourceName
	result.Key = BuildKey(result)
	return result
}

func seedFindingRuleset(t *testing.T, ctx context.Context, q *gen.Queries) gen.Ruleset {
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
	return ruleset
}

func seedFindingRule(t *testing.T, ctx context.Context, q *gen.Queries, rulesetID int64, key string) gen.Rule {
	t.Helper()
	rule, err := q.UpsertRule(ctx, gen.UpsertRuleParams{
		RulesetID:        rulesetID,
		Key:              key,
		Title:            "Rule " + key,
		Summary:          "Summary " + key,
		Category:         "identity",
		Severity:         "medium",
		MonitoringStatus: "monitored",
		RequiredData:     []byte(`[]`),
		ExpectedParams:   []byte(`{}`),
		RuleVersion:      "v1",
		IsActive:         true,
		DefinitionJson:   []byte(`{"check":{"type":"manual"}}`),
	})
	if err != nil {
		t.Fatalf("UpsertRule(%s) err = %v", key, err)
	}
	return rule
}

func seedFindingConnectorSource(t *testing.T, ctx context.Context, q *gen.Queries, sourceName string, configured bool) {
	t.Helper()
	if err := q.UpsertConnectorSourceState(ctx, gen.UpsertConnectorSourceStateParams{
		SourceKind:       "okta",
		SourceName:       sourceName,
		Enabled:          configured,
		Configured:       configured,
		DiscoveryEnabled: configured,
	}); err != nil {
		t.Fatalf("UpsertConnectorSourceState(%s) err = %v", sourceName, err)
	}
}

func countFindingEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, findingKey string) int64 {
	t.Helper()
	var count int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM finding_events WHERE finding_key = $1`, findingKey).Scan(&count); err != nil {
		t.Fatalf("count finding_events err = %v", err)
	}
	return count
}
