package handlers

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/findings"
)

func TestHandleFindingsRulesetUsesCanonicalFindingsReadmodel(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "example.okta.com",
			Token:  "okta-token",
		})
		ruleset := seedRulesetForFindingsHandler(t, ctx, q)
		passRule := seedRuleForFindingsHandler(t, ctx, q, ruleset.ID, "001-pass")
		seedRuleForFindingsHandler(t, ctx, q, ruleset.ID, "002-no-finding")

		evaluatedAt := time.Date(2026, time.May, 21, 9, 0, 0, 0, time.UTC)
		if err := findings.NewWriter(q).Write(ctx, canonicalRuleFindingForHandler(ruleset.Key, passRule, "pass", findings.StatusResolved, evaluatedAt)); err != nil {
			t.Fatalf("Write(canonical pass) err = %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/findings/rulesets/"+ruleset.Key)
		c.Request().Header.Set("HX-Request", "true")
		c.Request().Header.Set("HX-Target", "rules-card")
		c.SetPathValues(echo.PathValues{{Name: "rulesetKey", Value: ruleset.Key}})

		if err := h.HandleFindingsRuleset(c); err != nil {
			t.Fatalf("HandleFindingsRuleset() err = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		for _, want := range []string{"Rule 001-pass", "canonical pass", "Rule 002-no-finding"} {
			if !strings.Contains(body, want) {
				t.Fatalf("body missing %q:\n%s", want, body)
			}
		}
		data, err := h.buildFindingsRulesetViewData(ctx, c, ruleset, nil)
		if err != nil {
			t.Fatalf("buildFindingsRulesetViewData() err = %v", err)
		}
		statusByRule := map[string]string{}
		for _, rule := range data.Rules {
			statusByRule[rule.Key] = rule.Status
		}
		if got := statusByRule["001-pass"]; got != "pass" {
			t.Fatalf("status[001-pass] = %q, want pass", got)
		}
		if got := statusByRule["002-no-finding"]; got != "unknown" {
			t.Fatalf("status[002-no-finding] = %q, want unknown", got)
		}
	})
}

func TestHandleFindingsRulesetAggregatesAllConfiguredSources(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "example.okta.com",
			Token:  "okta-token",
		})
		seedFindingsHandlerConnectorSource(t, ctx, q, "second.okta.com", true)
		seedFindingsHandlerConnectorSource(t, ctx, q, "retired.okta.com", false)

		ruleset := seedRulesetForFindingsHandler(t, ctx, q)
		rule := seedRuleForFindingsHandler(t, ctx, q, ruleset.ID, "001-aggregate")

		evaluatedAt := time.Date(2026, time.May, 21, 9, 0, 0, 0, time.UTC)
		writer := findings.NewWriter(q)
		if err := writer.Write(ctx, canonicalRuleFindingForHandler(ruleset.Key, rule, "pass", findings.StatusResolved, evaluatedAt)); err != nil {
			t.Fatalf("Write(example pass) err = %v", err)
		}
		if err := writer.Write(ctx, canonicalRuleFindingForHandlerSource(ruleset.Key, rule, "second.okta.com", "fail", findings.StatusOpen, evaluatedAt.Add(time.Minute))); err != nil {
			t.Fatalf("Write(second fail) err = %v", err)
		}
		if err := writer.Write(ctx, canonicalRuleFindingForHandlerSource(ruleset.Key, rule, "retired.okta.com", "error", findings.StatusResolved, evaluatedAt.Add(2*time.Minute))); err != nil {
			t.Fatalf("Write(retired error) err = %v", err)
		}

		c, _ := newTestContext(http.MethodGet, "http://example.com/findings/rulesets/"+ruleset.Key)
		data, err := h.buildFindingsRulesetViewData(ctx, c, ruleset, nil)
		if err != nil {
			t.Fatalf("buildFindingsRulesetViewData() err = %v", err)
		}
		if data.SourceName != "" {
			t.Fatalf("SourceName = %q, want empty all-configured scope", data.SourceName)
		}
		if len(data.Rules) != 1 {
			t.Fatalf("rules = %d, want 1", len(data.Rules))
		}
		if got := data.Rules[0].Status; got != "fail" {
			t.Fatalf("rule status = %q, want fail", got)
		}
		if got := data.Rules[0].EvidenceSummary; got != "1 fail, 0 error, 0 unknown, 0 not applicable, 1 pass" {
			t.Fatalf("evidence summary = %q", got)
		}
	})
}

func TestHandleFindingsRuleUsesConcreteConnectorSourceScope(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "example.okta.com",
			Token:  "okta-token",
		})

		ruleset := seedRulesetForFindingsHandler(t, ctx, q)
		rule := seedRuleForFindingsHandler(t, ctx, q, ruleset.ID, "001-detail")

		evaluatedAt := time.Date(2026, time.May, 21, 9, 0, 0, 0, time.UTC)
		canonical := canonicalRuleFindingForHandler(ruleset.Key, rule, "fail", findings.StatusOpen, evaluatedAt)
		canonical.Output["evidence"] = map[string]any{
			"schema_version": 1,
			"check": map[string]any{
				"type": "manual",
			},
		}
		if err := findings.NewWriter(q).Write(ctx, canonical); err != nil {
			t.Fatalf("Write(canonical detail) err = %v", err)
		}
		if _, err := q.UpsertRuleOverride(ctx, gen.UpsertRuleOverrideParams{
			RuleID:     rule.ID,
			ScopeKind:  "connector_instance",
			SourceKind: "okta",
			SourceName: "example.okta.com",
			Params:     []byte(`{"source":"exact"}`),
			Enabled:    false,
		}); err != nil {
			t.Fatalf("UpsertRuleOverride() err = %v", err)
		}
		if _, err := q.UpsertRuleAttestation(ctx, gen.UpsertRuleAttestationParams{
			RuleID:     rule.ID,
			ScopeKind:  "connector_instance",
			SourceKind: "okta",
			SourceName: "example.okta.com",
			Status:     "not_applicable",
			Notes:      "source-specific attestation",
			ExpiresAt:  pgtype.Timestamptz{Time: evaluatedAt.Add(24 * time.Hour), Valid: true},
		}); err != nil {
			t.Fatalf("UpsertRuleAttestation() err = %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/findings/rulesets/"+ruleset.Key+"/rules/"+rule.Key)
		c.SetPathValues(echo.PathValues{
			{Name: "rulesetKey", Value: ruleset.Key},
			{Name: "ruleKey", Value: rule.Key},
		})

		if err := h.HandleFindingsRule(c); err != nil {
			t.Fatalf("HandleFindingsRule() err = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		for _, want := range []string{"example.okta.com", "canonical fail", "source-specific attestation"} {
			if !strings.Contains(body, want) {
				t.Fatalf("body missing %q:\n%s", want, body)
			}
		}
		if strings.Contains(body, "Unknown") {
			t.Fatalf("rule detail fell back to unknown status:\n%s", body)
		}

		scope, err := h.findingsConcreteScopeForRuleset(ctx, ruleset)
		if err != nil {
			t.Fatalf("findingsConcreteScopeForRuleset() err = %v", err)
		}
		if scope.SourceName != "example.okta.com" {
			t.Fatalf("concrete SourceName = %q, want example.okta.com", scope.SourceName)
		}

		row, err := q.GetFindingRuleCurrentByRulesetKeyAndRuleKey(ctx, gen.GetFindingRuleCurrentByRulesetKeyAndRuleKeyParams{
			RulesetKey: ruleset.Key,
			RuleKey:    rule.Key,
			ScopeKind:  scope.ScopeKind,
			SourceKind: scope.SourceKind,
			SourceName: scope.SourceName,
		})
		if err != nil {
			t.Fatalf("GetFindingRuleCurrentByRulesetKeyAndRuleKey() err = %v", err)
		}
		data, err := h.buildFindingsRuleViewData(ctx, c, ruleset, row, scope, nil)
		if err != nil {
			t.Fatalf("buildFindingsRuleViewData() err = %v", err)
		}
		if data.CurrentStatus != "fail" {
			t.Fatalf("CurrentStatus = %q, want fail", data.CurrentStatus)
		}
		if data.Evidence.CheckType != "manual" {
			t.Fatalf("Evidence.CheckType = %q, want manual", data.Evidence.CheckType)
		}
		if data.RuleOverride.Enabled {
			t.Fatalf("RuleOverride.Enabled = true, want false")
		}
		if !strings.Contains(data.RuleOverride.CurrentParamsPretty, `"source": "exact"`) {
			t.Fatalf("CurrentParamsPretty = %q, want exact override", data.RuleOverride.CurrentParamsPretty)
		}
		if data.Attestation.Notes != "source-specific attestation" {
			t.Fatalf("Attestation.Notes = %q", data.Attestation.Notes)
		}
	})
}

func TestHandleFindingsRuleAttestationWritesConcreteConnectorSource(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "example.okta.com",
			Token:  "okta-token",
		})

		ruleset := seedRulesetForFindingsHandler(t, ctx, q)
		rule := seedRuleForFindingsHandler(t, ctx, q, ruleset.ID, "001-attestation")

		c, rec := newTestContext(http.MethodPost, "http://example.com/findings/rulesets/"+ruleset.Key+"/rules/"+rule.Key+"/attestation")
		c.Request().Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
		c.Request().Body = http.NoBody
		c.Request().PostForm = url.Values{
			"status":     {"fail"},
			"expires_at": {""},
			"notes":      {"saved under concrete source"},
		}
		c.SetPathValues(echo.PathValues{
			{Name: "rulesetKey", Value: ruleset.Key},
			{Name: "ruleKey", Value: rule.Key},
		})

		if err := h.HandleFindingsRuleAttestation(c); err != nil {
			t.Fatalf("HandleFindingsRuleAttestation() err = %v", err)
		}
		if rec.Code < 300 || rec.Code > 399 {
			t.Fatalf("status = %d, want redirect; body=%s", rec.Code, rec.Body.String())
		}

		var concreteCount, emptyCount int
		if err := pool.QueryRow(ctx, `
			SELECT
			  count(*) FILTER (WHERE source_name = 'example.okta.com')::int,
			  count(*) FILTER (WHERE source_name = '')::int
			FROM rule_attestations
			WHERE rule_id = $1
			  AND scope_kind = 'connector_instance'
			  AND source_kind = 'okta'
		`, rule.ID).Scan(&concreteCount, &emptyCount); err != nil {
			t.Fatalf("count attestations: %v", err)
		}
		if concreteCount != 1 || emptyCount != 0 {
			t.Fatalf("attestation counts concrete=%d empty=%d, want concrete=1 empty=0", concreteCount, emptyCount)
		}
	})
}

func seedRulesetForFindingsHandler(t *testing.T, ctx context.Context, q *gen.Queries) gen.Ruleset {
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
		DefinitionHash: "handler-test",
		DefinitionJson: []byte(`{"key":"cis.okta.idaas_stig.v2"}`),
	})
	if err != nil {
		t.Fatalf("UpsertRuleset() err = %v", err)
	}
	return ruleset
}

func seedRuleForFindingsHandler(t *testing.T, ctx context.Context, q *gen.Queries, rulesetID int64, key string) gen.Rule {
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

func canonicalRuleFindingForHandler(rulesetKey string, rule gen.Rule, currentStatus string, status findings.Status, evaluatedAt time.Time) findings.FindingResult {
	return canonicalRuleFindingForHandlerSource(rulesetKey, rule, "example.okta.com", currentStatus, status, evaluatedAt)
}

func canonicalRuleFindingForHandlerSource(rulesetKey string, rule gen.Rule, sourceName string, currentStatus string, status findings.Status, evaluatedAt time.Time) findings.FindingResult {
	result := findings.FindingResult{
		Status:            status,
		BaseSeverity:      strings.TrimSpace(rule.Severity),
		EffectiveSeverity: strings.TrimSpace(rule.Severity),
		SeveritySource:    findings.SeveritySourcePolicy,
		Title:             strings.TrimSpace(rule.Title),
		Summary:           "canonical " + currentStatus,
		Evidence:          "canonical " + currentStatus,
		Source:            findings.SourceRef{Kind: "okta", Name: sourceName},
		Scope: findings.ScopeRef{
			Kind:       "connector_instance",
			SourceKind: "okta",
			SourceName: sourceName,
		},
		Entity:   findings.EntityRef{Kind: "ruleset_scope", ID: "connector_instance:okta:" + sourceName, Name: sourceName},
		Resource: findings.ResourceRef{Kind: "rule", ID: rule.Key, Name: rule.Title},
		Policy: findings.PolicyRef{
			BundleID:      rulesetKey,
			BundleVersion: "v2",
			ID:            rule.Key,
			Title:         rule.Title,
			RuleID:        rule.Key,
			RulesetID:     rulesetKey,
		},
		Output: map[string]any{
			"rule_result": map[string]any{
				"status":      currentStatus,
				"sync_run_id": "42",
			},
		},
		EvaluatedAt: evaluatedAt,
	}
	result.Key = findings.BuildKey(result)
	return result
}

func seedFindingsHandlerConnectorSource(t *testing.T, ctx context.Context, q *gen.Queries, sourceName string, configured bool) {
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
