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
	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
	osspecv1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v1"
	"github.com/open-sspm/open-sspm/internal/db/gen"
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
	if e == nil {
		return nil, errors.New("engine: nil")
	}

	var def osspecv1.Rule
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

	// Only evaluate v1 rules that provide a check.
	if def.Check == nil {
		return nil, nil
	}
	if strings.TrimSpace(string(def.Check.Type)) != "manual.attestation" && e.Datasets == nil {
		return nil, errors.New("engine: missing dataset provider")
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
				"check": map[string]any{
					"type": strings.TrimSpace(string(def.Check.Type)),
				},
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
			"check": map[string]any{
				"type": strings.TrimSpace(string(def.Check.Type)),
			},
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
					"check": map[string]any{
						"type": strings.TrimSpace(string(def.Check.Type)),
					},
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
					"check": map[string]any{
						"type": strings.TrimSpace(string(def.Check.Type)),
					},
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
		var paramErr ParamError
		if errors.As(err, &paramErr) {
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
					"check": map[string]any{
						"type": strings.TrimSpace(string(def.Check.Type)),
					},
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
				"check": map[string]any{
					"type": strings.TrimSpace(string(def.Check.Type)),
				},
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
	Total    int `json:"total"`
	Selected int `json:"selected"`
}

type violation struct {
	ResourceID string `json:"resource_id"`
	Display    string `json:"display,omitempty"`
}

func (e *Engine) evalCheck(ctx context.Context, rulesetKey, ruleKey string, evalCtx Context, rule osspecv1.Rule, params map[string]any) (*Evaluation, error) {
	check := rule.Check
	if check == nil {
		return nil, nil
	}

	switch strings.TrimSpace(string(check.Type)) {
	case "manual.attestation":
		return &Evaluation{
			Status:          "unknown",
			EvidenceSummary: "Manual check requires attestation",
			EvidenceJSON: mustJSON(map[string]any{
				"schema_version": 1,
				"rule": map[string]any{
					"ruleset_key": rulesetKey,
					"rule_key":    ruleKey,
				},
				"check": map[string]any{
					"type": "manual.attestation",
				},
				"params": params,
				"result": map[string]any{
					"status": "unknown",
				},
			}),
			AffectedResourceIDs: []string{},
		}, nil
	case "dataset.field_compare":
		return e.evalDatasetFieldCompare(ctx, rulesetKey, ruleKey, evalCtx, rule, params)
	case "dataset.count_compare":
		return e.evalDatasetCountCompare(ctx, rulesetKey, ruleKey, evalCtx, rule, params)
	case "dataset.join_count_compare":
		return e.evalDatasetJoinCountCompare(ctx, rulesetKey, ruleKey, evalCtx, rule, params)
	default:
		return nil, fmt.Errorf("unsupported check.type %q", strings.TrimSpace(string(check.Type)))
	}
}

func (e *Engine) evalDatasetFieldCompare(ctx context.Context, rulesetKey, ruleKey string, evalCtx Context, rule osspecv1.Rule, params map[string]any) (*Evaluation, error) {
	check := rule.Check
	if check == nil {
		return nil, nil
	}
	if check.Assert == nil {
		return nil, errors.New("field_compare requires assert")
	}

	rows, ev, err := e.getDatasetOrResult(ctx, rulesetKey, ruleKey, evalCtx, *check, params)
	if ev != nil || err != nil {
		return ev, err
	}

	selected := make([]any, 0, len(rows))
	for _, row := range rows {
		ok, err := evalAllWhere(row, check.Where, params)
		if err != nil {
			return nil, err
		}
		if ok {
			selected = append(selected, row)
		}
	}

	minSelected := 0
	onEmpty := "unknown"
	match := "all"
	if check.Expect != nil {
		if check.Expect.MinSelected > 0 {
			minSelected = check.Expect.MinSelected
		}
		if strings.TrimSpace(string(check.Expect.OnEmpty)) != "" {
			onEmpty = strings.TrimSpace(string(check.Expect.OnEmpty))
		}
		if strings.TrimSpace(string(check.Expect.Match)) != "" {
			match = strings.TrimSpace(string(check.Expect.Match))
		}
	}

	selection := selectionCounts{Total: len(rows), Selected: len(selected)}
	if len(selected) == 0 || len(selected) < minSelected {
		status := onEmpty
		errorKind := ""
		if status == "" {
			status = "unknown"
		}
		if status == "error" {
			errorKind = "engine_error"
		}
		result := resultObject(status, errorKind)
		return &Evaluation{
			Status:          status,
			ErrorKind:       errorKind,
			EvidenceSummary: fmt.Sprintf("No matching records (selected=%d, min_selected=%d)", selection.Selected, minSelected),
			EvidenceJSON: mustJSON(map[string]any{
				"schema_version": 1,
				"rule": map[string]any{
					"ruleset_key": rulesetKey,
					"rule_key":    ruleKey,
				},
				"check":     checkSummary(*check),
				"params":    params,
				"result":    result,
				"selection": selection,
			}),
			AffectedResourceIDs: []string{},
		}, nil
	}

	var (
		passCount int
		failures  []violation
	)

	idPath := ""
	displayPath := ""
	if rule.Evidence != nil && rule.Evidence.AffectedResources != nil {
		idPath = strings.TrimSpace(rule.Evidence.AffectedResources.IDField)
		displayPath = strings.TrimSpace(rule.Evidence.AffectedResources.DisplayField)
	}
	if idPath == "" {
		idPath = "/id"
	}

	for _, row := range selected {
		ok, err := evalSinglePredicate(row, check.Assert.Path, string(check.Assert.Op), predicateValue{
			Value:      check.Assert.Value,
			ValueParam: check.Assert.ValueParam,
		}, params)
		if err != nil {
			return nil, err
		}
		if ok {
			passCount++
			continue
		}

		id := extractStringByPointer(row, idPath)
		display := ""
		if displayPath != "" {
			display = extractStringByPointer(row, displayPath)
		}
		failures = append(failures, violation{ResourceID: id, Display: display})
	}

	selectedCount := len(selected)
	pass := false
	switch match {
	case "any":
		pass = passCount > 0
	case "none":
		pass = passCount == 0
	default: // all
		pass = passCount == selectedCount
	}

	sort.SliceStable(failures, func(i, j int) bool {
		if failures[i].ResourceID != failures[j].ResourceID {
			return failures[i].ResourceID < failures[j].ResourceID
		}
		return failures[i].Display < failures[j].Display
	})

	violationsTruncated := false
	if len(failures) > 100 {
		failures = failures[:100]
		violationsTruncated = true
	}

	status := "fail"
	if pass {
		status = "pass"
	}

	affectedIDs := make([]string, 0, len(failures))
	for _, v := range failures {
		if strings.TrimSpace(v.ResourceID) == "" {
			continue
		}
		affectedIDs = append(affectedIDs, v.ResourceID)
	}

	summary := fmt.Sprintf("%s (%d/%d selected passed)", strings.TrimSpace(rule.Title), passCount, selectedCount)
	if status == "fail" {
		summary = fmt.Sprintf("%s (%d/%d selected failed)", strings.TrimSpace(rule.Title), len(failures), selectedCount)
	}

	envelope := map[string]any{
		"schema_version": 1,
		"rule": map[string]any{
			"ruleset_key": rulesetKey,
			"rule_key":    ruleKey,
		},
		"check":                checkSummary(*check),
		"params":               params,
		"result":               map[string]any{"status": status},
		"selection":            selection,
		"violations":           failures,
		"violations_truncated": violationsTruncated,
	}

	return &Evaluation{
		Status:              status,
		ErrorKind:           "",
		EvidenceSummary:     summary,
		EvidenceJSON:        mustJSON(envelope),
		AffectedResourceIDs: affectedIDs,
	}, nil
}

func (e *Engine) evalDatasetCountCompare(ctx context.Context, rulesetKey, ruleKey string, evalCtx Context, rule osspecv1.Rule, params map[string]any) (*Evaluation, error) {
	check := rule.Check
	if check == nil {
		return nil, nil
	}
	if check.Compare == nil {
		return nil, errors.New("count_compare requires compare")
	}

	rows, ev, err := e.getDatasetOrResult(ctx, rulesetKey, ruleKey, evalCtx, *check, params)
	if ev != nil || err != nil {
		return ev, err
	}

	var selected int
	for _, row := range rows {
		ok, err := evalAllWhere(row, check.Where, params)
		if err != nil {
			return nil, err
		}
		if ok {
			selected++
		}
	}

	wantAny := any(nil)
	if check.Compare.ValueParam != "" {
		v, err := resolvePredicateValue(predicateValue{ValueParam: check.Compare.ValueParam}, params)
		if err != nil {
			return nil, err
		}
		wantAny = v
	} else if check.Compare.Value != nil {
		wantAny = float64(*check.Compare.Value)
	}

	wantF, ok := asFloat(wantAny)
	if !ok {
		return nil, fmt.Errorf("compare value must be number, got %T", wantAny)
	}

	okCmp, err := evalOp(float64(selected), strings.TrimSpace(string(check.Compare.Op)), wantF)
	if err != nil {
		return nil, err
	}

	status := "fail"
	if okCmp {
		status = "pass"
	}

	selection := selectionCounts{Total: len(rows), Selected: selected}
	summary := fmt.Sprintf("%s (count=%d)", strings.TrimSpace(rule.Title), selected)

	return &Evaluation{
		Status:          status,
		EvidenceSummary: summary,
		EvidenceJSON: mustJSON(map[string]any{
			"schema_version": 1,
			"rule": map[string]any{
				"ruleset_key": rulesetKey,
				"rule_key":    ruleKey,
			},
			"check":  checkSummary(*check),
			"params": params,
			"result": map[string]any{
				"status": status,
			},
			"selection": selection,
			"compare": map[string]any{
				"op":    strings.TrimSpace(string(check.Compare.Op)),
				"value": wantF,
			},
		}),
		AffectedResourceIDs: []string{},
	}, nil
}

func (e *Engine) evalDatasetJoinCountCompare(ctx context.Context, rulesetKey, ruleKey string, evalCtx Context, rule osspecv1.Rule, params map[string]any) (*Evaluation, error) {
	check := rule.Check
	if check == nil {
		return nil, nil
	}
	if check.Left == nil || check.Right == nil {
		return nil, errors.New("join_count_compare requires left and right")
	}
	if check.Compare == nil {
		return nil, errors.New("join_count_compare requires compare")
	}

	leftRows, ev, err := e.getDatasetOrResult(ctx, rulesetKey, ruleKey, evalCtx, osspecv1.Check{
		Type:               check.Type,
		Dataset:            check.Left.Dataset,
		DatasetVersion:     check.DatasetVersion,
		OnMissingDataset:   check.OnMissingDataset,
		OnPermissionDenied: check.OnPermissionDenied,
		OnSyncError:        check.OnSyncError,
	}, params)
	if ev != nil || err != nil {
		return ev, err
	}
	rightRows, ev, err := e.getDatasetOrResult(ctx, rulesetKey, ruleKey, evalCtx, osspecv1.Check{
		Type:               check.Type,
		Dataset:            check.Right.Dataset,
		DatasetVersion:     check.DatasetVersion,
		OnMissingDataset:   check.OnMissingDataset,
		OnPermissionDenied: check.OnPermissionDenied,
		OnSyncError:        check.OnSyncError,
	}, params)
	if ev != nil || err != nil {
		return ev, err
	}

	rightByKey := make(map[string][]any, len(rightRows))
	for _, row := range rightRows {
		k := extractStringByPointer(row, check.Right.KeyPath)
		if strings.TrimSpace(k) == "" {
			continue
		}
		rightByKey[k] = append(rightByKey[k], row)
	}

	onUnmatchedLeft := strings.TrimSpace(string(check.OnUnmatchedLeft))
	if onUnmatchedLeft == "" {
		onUnmatchedLeft = "ignore"
	}

	type joined struct {
		left  any
		right any
	}

	var (
		joinedRows    []joined
		unmatchedLeft int
	)
	for _, l := range leftRows {
		k := extractStringByPointer(l, check.Left.KeyPath)
		matches := rightByKey[k]
		if len(matches) == 0 {
			unmatchedLeft++
			switch onUnmatchedLeft {
			case "count":
				joinedRows = append(joinedRows, joined{left: l, right: nil})
			case "error":
				// defer until we know if any unmatched exists.
			default: // ignore
			}
			continue
		}
		for _, r := range matches {
			joinedRows = append(joinedRows, joined{left: l, right: r})
		}
	}

	if onUnmatchedLeft == "error" && unmatchedLeft > 0 {
		return &Evaluation{
			Status:          "error",
			ErrorKind:       "join_unmatched",
			EvidenceSummary: fmt.Sprintf("Join failed: %d unmatched left rows", unmatchedLeft),
			EvidenceJSON: mustJSON(map[string]any{
				"schema_version": 1,
				"rule": map[string]any{
					"ruleset_key": rulesetKey,
					"rule_key":    ruleKey,
				},
				"check":  checkSummary(*check),
				"params": params,
				"result": map[string]any{
					"status":     "error",
					"error_kind": "join_unmatched",
				},
				"join": map[string]any{
					"unmatched_left": unmatchedLeft,
				},
			}),
			AffectedResourceIDs: []string{},
		}, nil
	}

	var selected int
	for _, j := range joinedRows {
		ok, err := evalAllWhereJoin(j.left, j.right, check.Where, params)
		if err != nil {
			return nil, err
		}
		if ok {
			selected++
		}
	}

	wantAny := any(nil)
	if check.Compare.ValueParam != "" {
		v, err := resolvePredicateValue(predicateValue{ValueParam: check.Compare.ValueParam}, params)
		if err != nil {
			return nil, err
		}
		wantAny = v
	} else if check.Compare.Value != nil {
		wantAny = float64(*check.Compare.Value)
	}

	wantF, ok := asFloat(wantAny)
	if !ok {
		return nil, fmt.Errorf("compare value must be number, got %T", wantAny)
	}

	okCmp, err := evalOp(float64(selected), strings.TrimSpace(string(check.Compare.Op)), wantF)
	if err != nil {
		return nil, err
	}
	status := "fail"
	if okCmp {
		status = "pass"
	}

	selection := selectionCounts{Total: len(joinedRows), Selected: selected}
	summary := fmt.Sprintf("%s (joined_count=%d)", strings.TrimSpace(rule.Title), selected)

	return &Evaluation{
		Status:          status,
		EvidenceSummary: summary,
		EvidenceJSON: mustJSON(map[string]any{
			"schema_version": 1,
			"rule": map[string]any{
				"ruleset_key": rulesetKey,
				"rule_key":    ruleKey,
			},
			"check":     checkSummary(*check),
			"params":    params,
			"result":    resultObject(status, ""),
			"selection": selection,
			"compare": map[string]any{
				"op":    strings.TrimSpace(string(check.Compare.Op)),
				"value": wantF,
			},
			"join": map[string]any{
				"on_unmatched_left": onUnmatchedLeft,
				"unmatched_left":    unmatchedLeft,
			},
		}),
		AffectedResourceIDs: []string{},
	}, nil
}

func evalAllWhere(row any, where []osspecv1.Predicate, params map[string]any) (bool, error) {
	for _, clause := range where {
		path := strings.TrimSpace(clause.Path)
		if path == "" {
			return false, errors.New("where.path is required")
		}
		ok, err := evalSinglePredicate(row, path, string(clause.Op), predicateValue{Value: clause.Value, ValueParam: clause.ValueParam}, params)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evalAllWhereJoin(left any, right any, where []osspecv1.Predicate, params map[string]any) (bool, error) {
	for _, clause := range where {
		lp := strings.TrimSpace(clause.LeftPath)
		rp := strings.TrimSpace(clause.RightPath)
		if lp != "" && rp != "" {
			return false, errors.New("where clause cannot set both left_path and right_path")
		}

		target := left
		path := lp
		if rp != "" {
			target = right
			path = rp
		}

		if strings.TrimSpace(path) == "" {
			return false, errors.New("where.left_path or where.right_path is required")
		}

		// Spec: predicates using right_path evaluate as false when right is null.
		if target == nil {
			return false, nil
		}

		ok, err := evalSinglePredicate(target, path, string(clause.Op), predicateValue{Value: clause.Value, ValueParam: clause.ValueParam}, params)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (e *Engine) getDatasetOrResult(ctx context.Context, rulesetKey, ruleKey string, evalCtx Context, check osspecv1.Check, params map[string]any) ([]any, *Evaluation, error) {
	ds := strings.TrimSpace(check.Dataset)
	if ds == "" {
		return nil, nil, errors.New("dataset key is required")
	}

	version := 1
	if check.DatasetVersion > 0 {
		version = check.DatasetVersion
	}

	runtimeEval := runtimev1.EvalContext{
		ScopeKind: runtimev1.ScopeKind(strings.TrimSpace(evalCtx.ScopeKind)),
	}
	if runtimeEval.ScopeKind == runtimev1.ScopeKind_CONNECTOR_INSTANCE {
		runtimeEval.ConnectorKind = strings.TrimSpace(evalCtx.SourceKind)
		runtimeEval.ConnectorInstance = strings.TrimSpace(evalCtx.SourceName)
	}

	res := e.Datasets.GetDataset(ctx, runtimeEval, runtimev1.DatasetRef{Dataset: ds, Version: version})
	if res.Error != nil {
		status := "unknown"
		errorKind := strings.TrimSpace(string(res.Error.Kind))

		switch res.Error.Kind {
		case runtimev1.DatasetErrorKind_MISSING_INTEGRATION:
			status = "unknown"
		case runtimev1.DatasetErrorKind_MISSING_DATASET:
			status = statusFromPolicy(string(check.OnMissingDataset), "unknown")
		case runtimev1.DatasetErrorKind_PERMISSION_DENIED:
			status = statusFromPolicy(string(check.OnPermissionDenied), "unknown")
		case runtimev1.DatasetErrorKind_SYNC_FAILED:
			status = statusFromPolicy(string(check.OnSyncError), "error")
		case runtimev1.DatasetErrorKind_ENGINE_ERROR:
			status = "error"
			if errorKind == "" {
				errorKind = "engine_error"
			}
		default:
			status = "error"
			errorKind = "engine_error"
		}

		return nil, &Evaluation{
			Status:          status,
			ErrorKind:       errorKind,
			EvidenceSummary: fmt.Sprintf("Dataset error: %s", errorKind),
			EvidenceJSON: mustJSON(map[string]any{
				"schema_version": 1,
				"rule": map[string]any{
					"ruleset_key": rulesetKey,
					"rule_key":    ruleKey,
				},
				"check":  checkSummary(check),
				"params": params,
				"result": map[string]any{
					"status":     status,
					"error_kind": errorKind,
				},
				"error": strings.TrimSpace(res.Error.Message),
			}),
			AffectedResourceIDs: []string{},
		}, nil
	}

	if res.Rows == nil {
		return []any{}, nil, nil
	}

	rows := make([]any, 0, len(res.Rows))
	for i, raw := range res.Rows {
		if len(raw) == 0 {
			continue
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, &Evaluation{
				Status:          "error",
				ErrorKind:       "engine_error",
				EvidenceSummary: "Invalid dataset row JSON",
				EvidenceJSON: mustJSON(map[string]any{
					"schema_version": 1,
					"rule": map[string]any{
						"ruleset_key": rulesetKey,
						"rule_key":    ruleKey,
					},
					"check":  checkSummary(check),
					"params": params,
					"result": map[string]any{
						"status":     "error",
						"error_kind": "engine_error",
					},
					"error": fmt.Sprintf("row %d: %v", i, err),
				}),
				AffectedResourceIDs: []string{},
			}, nil
		}
		rows = append(rows, v)
	}

	return rows, nil, nil
}

func statusFromPolicy(policy string, def string) string {
	p := strings.TrimSpace(policy)
	if p == "" {
		return def
	}
	switch p {
	case "unknown", "error":
		return p
	default:
		return def
	}
}

func checkSummary(check osspecv1.Check) map[string]any {
	out := map[string]any{
		"type": strings.TrimSpace(string(check.Type)),
	}
	if strings.TrimSpace(check.Dataset) != "" {
		out["dataset"] = strings.TrimSpace(check.Dataset)
	}
	if check.DatasetVersion > 0 {
		out["dataset_version"] = check.DatasetVersion
	}
	if check.Left != nil {
		out["left"] = map[string]any{
			"dataset":  strings.TrimSpace(check.Left.Dataset),
			"key_path": strings.TrimSpace(check.Left.KeyPath),
		}
	}
	if check.Right != nil {
		out["right"] = map[string]any{
			"dataset":  strings.TrimSpace(check.Right.Dataset),
			"key_path": strings.TrimSpace(check.Right.KeyPath),
		}
	}
	return out
}

func extractStringByPointer(doc any, pointer string) string {
	v, ok, _ := getByJSONPointer(doc, pointer)
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", t))
	}
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

func resultObject(status string, errorKind string) map[string]any {
	out := map[string]any{
		"status": strings.TrimSpace(status),
	}
	if strings.TrimSpace(errorKind) != "" {
		out["error_kind"] = strings.TrimSpace(errorKind)
	}
	return out
}
