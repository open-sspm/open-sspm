package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

type Evaluation struct {
	Status              string
	ErrorKind           string
	EvidenceSummary     string
	EvidenceJSON        []byte
	AffectedResourceIDs []string
}

func (e *Engine) Run(ctx context.Context, evalCtx Context) error {
	if e == nil || e.Q == nil {
		return errors.New("engine: missing db queries")
	}

	scopeKind := strings.TrimSpace(evalCtx.ScopeKind)
	if scopeKind == "" {
		return errors.New("engine: scope kind is required")
	}

	sourceKind := strings.TrimSpace(evalCtx.SourceKind)
	sourceName := strings.TrimSpace(evalCtx.SourceName)

	rulesets, err := e.Q.ListRulesets(ctx)
	if err != nil {
		return err
	}

	var errs []error
	for _, rs := range rulesets {
		if strings.TrimSpace(rs.Status) != "active" {
			continue
		}

		if strings.TrimSpace(rs.ScopeKind) != scopeKind {
			continue
		}

		if scopeKind == "connector_instance" {
			if !rs.ConnectorKind.Valid || strings.TrimSpace(rs.ConnectorKind.String) == "" {
				continue
			}
			if strings.TrimSpace(rs.ConnectorKind.String) != sourceKind {
				continue
			}
		}

		disabled, err := e.isRulesetDisabled(ctx, rs.ID, scopeKind, sourceKind, sourceName)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: ruleset override: %w", strings.TrimSpace(rs.Key), err))
			continue
		}
		if disabled {
			continue
		}

		rules, err := e.Q.ListActiveRulesByRulesetID(ctx, rs.ID)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: list rules: %w", strings.TrimSpace(rs.Key), err))
			continue
		}

		for _, rule := range rules {
			ev, err := e.EvaluateRule(ctx, rs, rule, evalCtx)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s/%s: %w", strings.TrimSpace(rs.Key), strings.TrimSpace(rule.Key), err))
				continue
			}
			if ev == nil {
				continue
			}
			if err := e.writeEvaluation(ctx, rule.ID, evalCtx, *ev); err != nil {
				errs = append(errs, fmt.Errorf("%s/%s: write evaluation: %w", strings.TrimSpace(rs.Key), strings.TrimSpace(rule.Key), err))
				continue
			}
		}
	}

	return errors.Join(errs...)
}

func (e *Engine) EvaluateRule(ctx context.Context, ruleset gen.Ruleset, rule gen.Rule, evalCtx Context) (*Evaluation, error) {
	start := time.Now()
	ev, err := e.evaluateRuleInternal(ctx, ruleset, rule, evalCtx)
	metrics.RuleEvaluationDuration.WithLabelValues(ruleset.Key).Observe(time.Since(start).Seconds())

	status := "error"
	if err == nil && ev != nil {
		status = ev.Status
	} else if err == nil && ev == nil {
		status = "skipped"
	}
	metrics.RuleEvaluationsTotal.WithLabelValues(ruleset.Key, status).Inc()

	return ev, err
}

func (e *Engine) evaluateRuleInternal(ctx context.Context, ruleset gen.Ruleset, rule gen.Rule, evalCtx Context) (*Evaluation, error) {
	if e == nil {
		return nil, errors.New("engine: nil")
	}

	var def osspecv2.Rule
	if err := json.Unmarshal(rule.DefinitionJson, &def); err != nil {
		return &Evaluation{
			Status:          "error",
			ErrorKind:       "engine_error",
			EvidenceSummary: "Invalid rule definition_json",
			EvidenceJSON: mustJSON(map[string]any{
				"schema_version": 1,
				"rule": map[string]any{
					"ruleset_key": strings.TrimSpace(ruleset.Key),
					"rule_key":    strings.TrimSpace(rule.Key),
				},
				"result": map[string]any{
					"status":     "error",
					"error_kind": "engine_error",
				},
				"error": "invalid definition_json",
			}),
			AffectedResourceIDs: []string{},
		}, nil
	}

	now := evalCtx.EvaluatedAt
	if now.IsZero() {
		if e.Now != nil {
			now = e.Now()
		} else {
			now = time.Now()
		}
	}

	scopeKind := strings.TrimSpace(evalCtx.ScopeKind)
	sourceKind := strings.TrimSpace(evalCtx.SourceKind)
	sourceName := strings.TrimSpace(evalCtx.SourceName)

	override, err := e.getRuleOverride(ctx, rule.ID, scopeKind, sourceKind, sourceName)
	if err != nil {
		return nil, err
	}

	if override != nil && !override.Enabled {
		return &Evaluation{
			Status:          "not_applicable",
			ErrorKind:       "",
			EvidenceSummary: "Not applicable (disabled by override)",
			EvidenceJSON: mustJSON(map[string]any{
				"schema_version": 1,
				"rule": map[string]any{
					"ruleset_key": strings.TrimSpace(ruleset.Key),
					"rule_key":    strings.TrimSpace(rule.Key),
				},
				"check": checkSummary(def.Check),
				"result": map[string]any{
					"status": "not_applicable",
				},
				"override": map[string]any{
					"enabled": false,
				},
			}),
			AffectedResourceIDs: []string{},
		}, nil
	}

	att, err := e.getActiveAttestation(ctx, rule.ID, scopeKind, sourceKind, sourceName, now)
	if err != nil {
		return nil, err
	}
	if att != nil {
		status := strings.TrimSpace(att.Status)
		if status == "" {
			status = "unknown"
		}
		ev := map[string]any{
			"schema_version": 1,
			"rule": map[string]any{
				"ruleset_key": strings.TrimSpace(ruleset.Key),
				"rule_key":    strings.TrimSpace(rule.Key),
			},
			"check": checkSummary(def.Check),
			"result": map[string]any{
				"status": status,
			},
			"attestation": map[string]any{
				"status":     strings.TrimSpace(att.Status),
				"notes":      strings.TrimSpace(att.Notes),
				"expires_at": pgTimeString(att.ExpiresAt),
			},
		}
		return &Evaluation{
			Status:              status,
			ErrorKind:           "",
			EvidenceSummary:     fmt.Sprintf("Attested: %s", status),
			EvidenceJSON:        mustJSON(ev),
			AffectedResourceIDs: []string{},
		}, nil
	}

	defaultParams := map[string]any{}
	if def.Parameters != nil && def.Parameters.Defaults != nil {
		defaultParams = def.Parameters.Defaults
	}

	overrideParams := map[string]any{}
	if override != nil && len(override.Params) > 0 {
		overrideParams, err = parseJSONObject(override.Params)
		if err != nil {
			return &Evaluation{
				Status:          "error",
				ErrorKind:       "invalid_params",
				EvidenceSummary: "Invalid override params",
				EvidenceJSON: mustJSON(map[string]any{
					"schema_version": 1,
					"rule": map[string]any{
						"ruleset_key": strings.TrimSpace(ruleset.Key),
						"rule_key":    strings.TrimSpace(rule.Key),
					},
					"check": checkSummary(def.Check),
					"result": map[string]any{
						"status":     "error",
						"error_kind": "invalid_params",
					},
				}),
				AffectedResourceIDs: []string{},
			}, nil
		}
	}

	effectiveParams := deepMerge(defaultParams, overrideParams)
	if def.Parameters != nil && def.Parameters.Schema != nil {
		if err := validateParams(effectiveParams, def.Parameters.Schema); err != nil {
			return &Evaluation{
				Status:          "error",
				ErrorKind:       "invalid_params",
				EvidenceSummary: "Invalid parameters",
				EvidenceJSON: mustJSON(map[string]any{
					"schema_version": 1,
					"rule": map[string]any{
						"ruleset_key": strings.TrimSpace(ruleset.Key),
						"rule_key":    strings.TrimSpace(rule.Key),
					},
					"check":  checkSummary(def.Check),
					"params": effectiveParams,
					"result": map[string]any{
						"status":     "error",
						"error_kind": "invalid_params",
					},
					"error": err.Error(),
				}),
				AffectedResourceIDs: []string{},
			}, nil
		}
	}

	out, err := e.evalCheck(ctx, strings.TrimSpace(ruleset.Key), strings.TrimSpace(rule.Key), evalCtx, def, effectiveParams)
	if err != nil {
		return &Evaluation{
			Status:          "error",
			ErrorKind:       "engine_error",
			EvidenceSummary: "Engine error",
			EvidenceJSON: mustJSON(map[string]any{
				"schema_version": 1,
				"rule": map[string]any{
					"ruleset_key": strings.TrimSpace(ruleset.Key),
					"rule_key":    strings.TrimSpace(rule.Key),
				},
				"check":  checkSummary(def.Check),
				"params": effectiveParams,
				"result": map[string]any{
					"status":     "error",
					"error_kind": "engine_error",
				},
				"error": err.Error(),
			}),
			AffectedResourceIDs: []string{},
		}, nil
	}

	return out, nil
}

func (e *Engine) writeEvaluation(ctx context.Context, ruleID int64, evalCtx Context, ev Evaluation) error {
	now := evalCtx.EvaluatedAt
	if now.IsZero() {
		if e.Now != nil {
			now = e.Now()
		} else {
			now = time.Now()
		}
	}

	evaluatedAt := pgtype.Timestamptz{Time: now, Valid: true}
	var syncRunID pgtype.Int8
	if evalCtx.SyncRunID != nil && *evalCtx.SyncRunID > 0 {
		syncRunID = pgtype.Int8{Int64: *evalCtx.SyncRunID, Valid: true}
	}

	affected := ev.AffectedResourceIDs
	if affected == nil {
		affected = []string{}
	}

	if _, err := e.Q.InsertRuleEvaluation(ctx, gen.InsertRuleEvaluationParams{
		RuleID:              ruleID,
		ScopeKind:           strings.TrimSpace(evalCtx.ScopeKind),
		SourceKind:          strings.TrimSpace(evalCtx.SourceKind),
		SourceName:          strings.TrimSpace(evalCtx.SourceName),
		Status:              strings.TrimSpace(ev.Status),
		SyncRunID:           syncRunID,
		EvaluatedAt:         evaluatedAt,
		EvidenceSummary:     strings.TrimSpace(ev.EvidenceSummary),
		EvidenceJson:        ev.EvidenceJSON,
		AffectedResourceIds: affected,
		ErrorKind:           strings.TrimSpace(ev.ErrorKind),
	}); err != nil {
		return err
	}

	_, err := e.Q.UpsertRuleResultCurrent(ctx, gen.UpsertRuleResultCurrentParams{
		RuleID:              ruleID,
		ScopeKind:           strings.TrimSpace(evalCtx.ScopeKind),
		SourceKind:          strings.TrimSpace(evalCtx.SourceKind),
		SourceName:          strings.TrimSpace(evalCtx.SourceName),
		Status:              strings.TrimSpace(ev.Status),
		EvaluatedAt:         evaluatedAt,
		SyncRunID:           syncRunID,
		EvidenceSummary:     strings.TrimSpace(ev.EvidenceSummary),
		EvidenceJson:        ev.EvidenceJSON,
		AffectedResourceIds: affected,
		ErrorKind:           strings.TrimSpace(ev.ErrorKind),
	})
	return err
}

func (e *Engine) isRulesetDisabled(ctx context.Context, rulesetID int64, scopeKind, sourceKind, sourceName string) (bool, error) {
	row, err := e.Q.GetRulesetOverride(ctx, gen.GetRulesetOverrideParams{
		RulesetID:  rulesetID,
		ScopeKind:  scopeKind,
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return !row.Enabled, nil
}

func (e *Engine) getRuleOverride(ctx context.Context, ruleID int64, scopeKind, sourceKind, sourceName string) (*gen.RuleOverride, error) {
	row, err := e.Q.GetRuleOverride(ctx, gen.GetRuleOverrideParams{
		RuleID:     ruleID,
		ScopeKind:  scopeKind,
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &row, nil
}

func (e *Engine) getActiveAttestation(ctx context.Context, ruleID int64, scopeKind, sourceKind, sourceName string, now time.Time) (*gen.RuleAttestation, error) {
	row, err := e.Q.GetRuleAttestation(ctx, gen.GetRuleAttestationParams{
		RuleID:     ruleID,
		ScopeKind:  scopeKind,
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	if row.ExpiresAt.Valid && now.After(row.ExpiresAt.Time) {
		return nil, nil
	}
	return &row, nil
}

type selectionCounts struct {
	Selected int `json:"selected"`
	Passed   int `json:"passed"`
}

func (e *Engine) evalCheck(ctx context.Context, rulesetKey, ruleKey string, evalCtx Context, rule osspecv2.Rule, params map[string]any) (*Evaluation, error) {
	datasets, err := e.buildEvaluateDatasets(ctx, evalCtx, rule)
	if err != nil {
		return nil, err
	}

	result, evalErr := osspecv2.EvaluateRule(&rule, osspecv2.EvaluateInput{
		Datasets: datasets,
		Params:   params,
	})

	status, errorKind := normalizeEvaluateOutcome(result, evalErr)
	summary := buildEvaluationSummary(strings.TrimSpace(rule.Title), status, result)

	resultPayload := map[string]any{
		"status": status,
	}
	if reason := strings.TrimSpace(result.ReasonCode); reason != "" {
		resultPayload["reason_code"] = reason
	}
	if errorKind != "" {
		resultPayload["error_kind"] = errorKind
	}
	resultPayload["selected_count"] = result.SelectedCount
	resultPayload["passed_count"] = result.PassedCount
	resultPayload["count_value"] = result.CountValue
	if result.TargetValue != nil {
		resultPayload["target_value"] = *result.TargetValue
	}

	envelope := map[string]any{
		"schema_version": 1,
		"rule": map[string]any{
			"ruleset_key": rulesetKey,
			"rule_key":    ruleKey,
		},
		"check":     checkSummary(rule.Check),
		"params":    params,
		"result":    resultPayload,
		"selection": selectionCounts{Selected: result.SelectedCount, Passed: result.PassedCount},
	}
	if evalErr != nil {
		envelope["error"] = evalErr.Error()
	}

	return &Evaluation{
		Status:              status,
		ErrorKind:           errorKind,
		EvidenceSummary:     summary,
		EvidenceJSON:        mustJSON(envelope),
		AffectedResourceIDs: []string{},
	}, nil
}

func (e *Engine) buildEvaluateDatasets(ctx context.Context, evalCtx Context, rule osspecv2.Rule) (map[string]osspecv2.DatasetInput, error) {
	keys := requiredDatasetsForRule(rule)
	if len(keys) == 0 {
		return map[string]osspecv2.DatasetInput{}, nil
	}
	if e.Datasets == nil {
		return nil, errors.New("engine: missing dataset provider")
	}

	runtimeEval := runtimev2.EvalContext{
		ScopeKind: runtimev2.ScopeKind(strings.TrimSpace(evalCtx.ScopeKind)),
	}
	if runtimeEval.ScopeKind == runtimev2.ScopeKind_CONNECTOR_INSTANCE {
		runtimeEval.ConnectorKind = strings.TrimSpace(evalCtx.SourceKind)
		runtimeEval.ConnectorInstance = strings.TrimSpace(evalCtx.SourceName)
	}

	out := make(map[string]osspecv2.DatasetInput, len(keys))
	for _, key := range keys {
		res := e.Datasets.GetDataset(ctx, runtimeEval, runtimev2.DatasetRef{Dataset: key, Version: 1})
		if res.Error != nil {
			out[key] = osspecv2.DatasetInput{
				Error: &osspecv2.DatasetInputError{
					Kind:    osspecv2.DatasetErrorKind(res.Error.Kind),
					Message: strings.TrimSpace(res.Error.Message),
				},
			}
			continue
		}

		rows := make([]any, 0, len(res.Rows))
		for i, raw := range res.Rows {
			if len(raw) == 0 {
				continue
			}
			var v any
			if err := json.Unmarshal(raw, &v); err != nil {
				return nil, fmt.Errorf("decode dataset %s row %d: %w", key, i, err)
			}
			rows = append(rows, v)
		}
		out[key] = osspecv2.DatasetInput{Rows: rows}
	}

	return out, nil
}

func requiredDatasetsForRule(rule osspecv2.Rule) []string {
	seen := map[string]struct{}{}
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		seen[v] = struct{}{}
	}

	for _, dataset := range rule.RequiredData {
		add(dataset)
	}
	if rule.Check != nil && rule.Check.Plan != nil {
		add(rule.Check.Plan.Dataset)
	}

	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func normalizeEvaluateOutcome(result osspecv2.EvaluateResult, evalErr error) (string, string) {
	status := strings.TrimSpace(strings.ToLower(string(result.Status)))
	switch status {
	case "pass", "fail", "unknown":
	default:
		status = "unknown"
	}

	if evalErr == nil {
		return status, ""
	}

	reason := strings.TrimSpace(result.ReasonCode)
	if reason == "missing_param" {
		return "error", "invalid_params"
	}
	if strings.HasPrefix(reason, "dataset_") {
		kind := strings.TrimPrefix(reason, "dataset_")
		if kind == "" {
			kind = "engine_error"
		}
		return "error", kind
	}

	return "error", "engine_error"
}

func buildEvaluationSummary(ruleTitle, status string, result osspecv2.EvaluateResult) string {
	title := strings.TrimSpace(ruleTitle)
	if title == "" {
		title = "Rule"
	}

	switch status {
	case "pass":
		if result.SelectedCount > 0 {
			return fmt.Sprintf("%s (%d/%d selected passed)", title, result.PassedCount, result.SelectedCount)
		}
		if result.TargetValue != nil {
			return fmt.Sprintf("%s (count=%d target=%d)", title, result.CountValue, *result.TargetValue)
		}
	case "fail":
		if result.SelectedCount > 0 {
			failed := result.SelectedCount - result.PassedCount
			if failed < 0 {
				failed = 0
			}
			return fmt.Sprintf("%s (%d/%d selected failed)", title, failed, result.SelectedCount)
		}
		if result.TargetValue != nil {
			return fmt.Sprintf("%s (count=%d target=%d)", title, result.CountValue, *result.TargetValue)
		}
	case "unknown":
		if strings.TrimSpace(result.ReasonCode) == "manual_rule" {
			return "Manual check requires attestation"
		}
	case "error":
		return "Engine error"
	}

	return title
}

func checkSummary(check *osspecv2.Check) map[string]any {
	if check == nil {
		return map[string]any{}
	}

	out := map[string]any{}
	engine := strings.TrimSpace(string(check.Engine))
	if engine != "" {
		out["engine"] = engine
	}
	if expr := strings.TrimSpace(check.Expression); expr != "" {
		out["expression"] = expr
	}

	if check.Plan != nil {
		plan := map[string]any{}
		if t := strings.TrimSpace(check.Plan.Type); t != "" {
			plan["type"] = t
			out["type"] = t
		}
		if dataset := strings.TrimSpace(check.Plan.Dataset); dataset != "" {
			plan["dataset"] = dataset
			out["dataset"] = dataset
		}
		if expr := strings.TrimSpace(check.Plan.WhereExpression); expr != "" {
			plan["where_expression"] = expr
		}
		if expr := strings.TrimSpace(check.Plan.AssertExpression); expr != "" {
			plan["assert_expression"] = expr
		}
		if check.Plan.Expect != nil {
			plan["expect"] = map[string]any{
				"match":        strings.TrimSpace(check.Plan.Expect.Match),
				"min_selected": check.Plan.Expect.MinSelected,
				"on_empty":     strings.TrimSpace(check.Plan.Expect.OnEmpty),
			}
		}
		if check.Plan.Compare != nil {
			plan["compare"] = map[string]any{
				"op":    strings.TrimSpace(check.Plan.Compare.Op),
				"value": check.Plan.Compare.Value,
			}
		}
		if v := strings.TrimSpace(check.Plan.OnMissingDataset); v != "" {
			plan["on_missing_dataset"] = v
		}
		if v := strings.TrimSpace(check.Plan.OnPermissionDenied); v != "" {
			plan["on_permission_denied"] = v
		}
		if v := strings.TrimSpace(check.Plan.OnSyncError); v != "" {
			plan["on_sync_error"] = v
		}
		out["plan"] = plan
	}

	if _, ok := out["type"]; !ok {
		switch engine {
		case string(osspecv2.CheckEngine_CEL):
			out["type"] = "cel.expression"
		case string(osspecv2.CheckEngine_CEL_PLAN):
			out["type"] = "cel.plan"
		}
	}

	return out
}

func pgTimeString(ts pgtype.Timestamptz) string {
	if !ts.Valid {
		return ""
	}
	return ts.Time.UTC().Format(time.RFC3339)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"schema_version":1,"result":{"status":"error","error_kind":"engine_error"}}`)
	}
	return b
}
