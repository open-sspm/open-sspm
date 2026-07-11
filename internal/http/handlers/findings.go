package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/connectorui"
	"github.com/open-sspm/open-sspm/internal/http/events"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

func (h *Handlers) HandleFindings(c *echo.Context) error {
	ctx := c.Request().Context()

	layout, _, err := h.LayoutData(ctx, c, "Findings")
	if err != nil {
		return h.RenderError(c, err)
	}

	rows, err := h.Q.ListRulesets(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.FindingsRulesetItem, 0, len(rows))
	for _, rs := range rows {
		if strings.TrimSpace(rs.Status) != "active" {
			continue
		}

		connectorKind := ""
		if rs.ConnectorKind.Valid {
			connectorKind = strings.TrimSpace(rs.ConnectorKind.String)
		}

		items = append(items, viewmodels.FindingsRulesetItem{
			Key:           strings.TrimSpace(rs.Key),
			Name:          strings.TrimSpace(rs.Name),
			Description:   strings.TrimSpace(rs.Description),
			ScopeKind:     strings.TrimSpace(rs.ScopeKind),
			ConnectorKind: connectorKind,
			Status:        strings.TrimSpace(rs.Status),
			Source:        strings.TrimSpace(rs.Source),
			SourceVersion: strings.TrimSpace(rs.SourceVersion),
			Href:          "/findings/rulesets/" + strings.TrimSpace(rs.Key),
		})
	}

	data := viewmodels.FindingsRulesetsViewData{
		Layout:      layout,
		Rulesets:    items,
		HasRulesets: len(items) > 0,
	}

	return h.RenderComponent(c, views.FindingsPage(data))
}

func ruleSeverityRank(severity string) int {
	s := strings.ToUpper(strings.TrimSpace(severity))
	switch s {
	case "CAT I", "CAT 1":
		return 1
	case "CAT II", "CAT 2":
		return 2
	case "CAT III", "CAT 3":
		return 3
	case "CRITICAL":
		return 1
	case "HIGH":
		return 2
	case "MEDIUM":
		return 3
	case "LOW":
		return 4
	case "INFO", "INFORMATIONAL":
		return 5
	default:
		return 99
	}
}

func (h *Handlers) HandleFindingsRuleset(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()

	rulesetKey := strings.TrimSpace(c.Param("rulesetKey"))
	if rulesetKey == "" {
		return RenderNotFound(c)
	}

	rs, err := h.Q.GetRulesetByKey(ctx, rulesetKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	data, err := h.buildFindingsRulesetViewData(ctx, c, rs, nil)
	if err != nil {
		return h.RenderError(c, err)
	}

	if isHX(c) && isHXTarget(c, "rules-card") {
		return h.RenderComponent(c, views.FindingsRulesetRulesCard(data))
	}
	return h.RenderComponent(c, views.FindingsRulesetPage(data))
}

func (h *Handlers) buildFindingsRulesetViewData(ctx context.Context, c *echo.Context, rs gen.Ruleset, alert *viewmodels.AlertViewData) (viewmodels.FindingsRulesetViewData, error) {
	layout, _, err := h.LayoutData(ctx, c, strings.TrimSpace(rs.Name))
	if err != nil {
		return viewmodels.FindingsRulesetViewData{}, err
	}

	scope, err := h.findingsScopeForRuleset(ctx, rs)
	if err != nil {
		return viewmodels.FindingsRulesetViewData{}, err
	}

	overrideScope, err := h.findingsConcreteScopeForRuleset(ctx, rs)
	if err != nil {
		return viewmodels.FindingsRulesetViewData{}, err
	}

	overrideEnabled, overrideExists, err := h.getRulesetOverride(ctx, rs.ID, overrideScope)
	if err != nil {
		return viewmodels.FindingsRulesetViewData{}, err
	}

	statusFilter := normalizeRuleStatusFilter(c.QueryParam("status"))
	severityFilter := normalizeSeverityFilter(c.QueryParam("severity"))
	monitoringFilter := normalizeMonitoringFilter(c.QueryParam("monitoring"))

	ruleRows, err := h.Q.ListFindingRulesetCurrentByRulesetKey(ctx, gen.ListFindingRulesetCurrentByRulesetKeyParams{
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
		Key:        strings.TrimSpace(rs.Key),
	})
	if err != nil {
		return viewmodels.FindingsRulesetViewData{}, err
	}

	items := make([]viewmodels.FindingsRuleItem, 0, len(ruleRows))
	for _, row := range ruleRows {
		evaluatedAt := formatTimeTable(row.CurrentEvaluatedAt)

		item := viewmodels.FindingsRuleItem{
			Key:              strings.TrimSpace(row.Key),
			Severity:         strings.TrimSpace(row.Severity),
			Title:            strings.TrimSpace(row.Title),
			Summary:          strings.TrimSpace(row.Summary),
			MonitoringStatus: strings.TrimSpace(row.MonitoringStatus),
			Status:           strings.TrimSpace(row.CurrentStatus),
			EvaluatedAt:      evaluatedAt,
			EvidenceSummary:  strings.TrimSpace(row.CurrentEvidenceSummary),
			ErrorKind:        strings.TrimSpace(row.CurrentErrorKind),
			Href:             "/findings/rulesets/" + strings.TrimSpace(rs.Key) + "/rules/" + strings.TrimSpace(row.Key),
		}

		if statusFilter != "" && strings.ToLower(strings.TrimSpace(item.Status)) != statusFilter {
			continue
		}
		if severityFilter != "" && strings.ToLower(strings.TrimSpace(item.Severity)) != severityFilter {
			continue
		}
		if monitoringFilter != "" && strings.ToLower(strings.TrimSpace(item.MonitoringStatus)) != monitoringFilter {
			continue
		}

		items = append(items, item)
	}

	sort.SliceStable(items, func(i, j int) bool {
		ri := ruleSeverityRank(items[i].Severity)
		rj := ruleSeverityRank(items[j].Severity)
		if ri != rj {
			return ri < rj
		}
		return items[i].Key < items[j].Key
	})

	connectorKind := ""
	if rs.ConnectorKind.Valid {
		connectorKind = strings.TrimSpace(rs.ConnectorKind.String)
	}

	data := viewmodels.FindingsRulesetViewData{
		Layout: layout,
		Ruleset: viewmodels.FindingsRulesetItem{
			Key:           strings.TrimSpace(rs.Key),
			Name:          strings.TrimSpace(rs.Name),
			Description:   strings.TrimSpace(rs.Description),
			ScopeKind:     strings.TrimSpace(rs.ScopeKind),
			ConnectorKind: connectorKind,
			Status:        strings.TrimSpace(rs.Status),
			Source:        strings.TrimSpace(rs.Source),
			SourceVersion: strings.TrimSpace(rs.SourceVersion),
			Href:          "/findings/rulesets/" + strings.TrimSpace(rs.Key),
		},
		SourceName:        scope.SourceName,
		ConnectorHintHref: scope.ConnectorHintHref,
		OverrideExists:    overrideExists,
		OverrideEnabled:   overrideEnabled,
		StatusFilter:      statusFilter,
		SeverityFilter:    severityFilter,
		MonitoringFilter:  monitoringFilter,
		Rules:             items,
		HasRules:          len(items) > 0,
		Alert:             alert,
	}
	return data, nil
}

type findingsScope struct {
	ScopeKind         string
	SourceKind        string
	SourceName        string
	ConnectorHintHref string
}

func (h *Handlers) findingsScopeForRuleset(ctx context.Context, rs gen.Ruleset) (findingsScope, error) {
	return h.findingsScopeForRulesetMode(ctx, rs, false)
}

func (h *Handlers) findingsConcreteScopeForRuleset(ctx context.Context, rs gen.Ruleset) (findingsScope, error) {
	return h.findingsScopeForRulesetMode(ctx, rs, true)
}

func (h *Handlers) findingsScopeForRulesetMode(ctx context.Context, rs gen.Ruleset, concreteSource bool) (findingsScope, error) {
	scopeKind := strings.TrimSpace(rs.ScopeKind)
	switch scopeKind {
	case "global":
		return findingsScope{ScopeKind: "global"}, nil
	case "connector_instance":
		connectorKind := ""
		if rs.ConnectorKind.Valid {
			connectorKind = strings.TrimSpace(rs.ConnectorKind.String)
		}
		if connectorKind == "" {
			return findingsScope{ScopeKind: "connector_instance"}, nil
		}

		hintHref := connectorui.SettingsHrefForKind(connectorKind)
		sourceName := ""

		if h.Registry != nil {
			states, err := h.Registry.LoadStates(ctx, h.Q)
			if err != nil {
				return findingsScope{}, err
			}
			for _, st := range states {
				if strings.EqualFold(strings.TrimSpace(st.Definition.Kind()), connectorKind) {
					if concreteSource {
						sourceName = strings.TrimSpace(st.SourceName)
					}
					break
				}
			}
		}
		if concreteSource && sourceName == "" && h.Pool != nil {
			err := h.Pool.QueryRow(ctx, `
				WITH configured_sources AS (
				  SELECT source_name, count(*) OVER () AS source_count
				  FROM connector_source_state
				  WHERE configured
				    AND source_kind = $1
				)
				SELECT source_name
				FROM configured_sources
				WHERE source_count = 1
			`, connectorKind).Scan(&sourceName)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return findingsScope{}, err
			}
			sourceName = strings.TrimSpace(sourceName)
		}

		return findingsScope{
			ScopeKind:         "connector_instance",
			SourceKind:        connectorKind,
			SourceName:        sourceName,
			ConnectorHintHref: hintHref,
		}, nil
	default:
		return findingsScope{}, fmt.Errorf("unsupported ruleset scope_kind %q", scopeKind)
	}
}

func (h *Handlers) getRulesetOverride(ctx context.Context, rulesetID int64, scope findingsScope) (enabled bool, exists bool, err error) {
	row, err := h.Q.GetRulesetOverride(ctx, gen.GetRulesetOverrideParams{
		RulesetID:  rulesetID,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return true, false, nil
		}
		return false, false, err
	}
	return row.Enabled, true, nil
}

func (h *Handlers) HandleFindingsRulesetOverride(c *echo.Context) error {
	ctx := c.Request().Context()
	rulesetKey := strings.TrimSpace(c.Param("rulesetKey"))
	if rulesetKey == "" {
		return RenderNotFound(c)
	}

	rs, err := h.Q.GetRulesetByKey(ctx, rulesetKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	scope, err := h.findingsConcreteScopeForRuleset(ctx, rs)
	if err != nil {
		return h.RenderError(c, err)
	}

	enabled := ParseBoolForm(c.FormValue("enabled"))
	if _, err := h.Q.UpsertRulesetOverride(ctx, gen.UpsertRulesetOverrideParams{
		RulesetID:  rs.ID,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
		Enabled:    enabled,
	}); err != nil {
		return h.RenderError(c, err)
	}

	toast := viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Ruleset updated",
		Description: "Ruleset override saved.",
	}
	if isHX(c) {
		setResponseToast(c, toast)
		addHXTrigger(c, events.FindingsRulesetChanged, map[string]string{"ruleset": rulesetKey})

		if returnRuleKey := strings.TrimSpace(c.FormValue("return_rule_key")); returnRuleKey != "" {
			return h.renderFindingsRuleMutationResponse(c, rs, scope, returnRuleKey, nil)
		}

		data, err := h.buildFindingsRulesetViewData(ctx, c, rs, nil)
		if err != nil {
			return h.RenderError(c, err)
		}
		return h.RenderComponent(c, views.FindingsRulesetMutationResponse(data))
	}

	return redirectWithFlash(c, "/findings/rulesets/"+rulesetKey, toast)
}

func (h *Handlers) HandleFindingsRule(c *echo.Context) error {
	ctx := c.Request().Context()

	rulesetKey := strings.TrimSpace(c.Param("rulesetKey"))
	ruleKey := strings.TrimSpace(c.Param("ruleKey"))
	if rulesetKey == "" || ruleKey == "" {
		return RenderNotFound(c)
	}

	rs, err := h.Q.GetRulesetByKey(ctx, rulesetKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	scope, err := h.findingsConcreteScopeForRuleset(ctx, rs)
	if err != nil {
		return h.RenderError(c, err)
	}

	r, err := h.Q.GetFindingRuleCurrentByRulesetKeyAndRuleKey(ctx, gen.GetFindingRuleCurrentByRulesetKeyAndRuleKeyParams{
		RulesetKey: rulesetKey,
		RuleKey:    ruleKey,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	data, err := h.buildFindingsRuleViewData(ctx, c, rs, r, scope, nil)
	if err != nil {
		return h.RenderError(c, err)
	}
	return h.RenderComponent(c, views.FindingsRulePage(data))
}

func (h *Handlers) renderFindingsRuleMutationResponse(c *echo.Context, rs gen.Ruleset, scope findingsScope, ruleKey string, alert *viewmodels.AlertViewData) error {
	ctx := c.Request().Context()
	rulesetKey := strings.TrimSpace(rs.Key)
	ruleKey = strings.TrimSpace(ruleKey)
	if rulesetKey == "" || ruleKey == "" {
		return RenderNotFound(c)
	}

	r, err := h.Q.GetFindingRuleCurrentByRulesetKeyAndRuleKey(ctx, gen.GetFindingRuleCurrentByRulesetKeyAndRuleKeyParams{
		RulesetKey: rulesetKey,
		RuleKey:    ruleKey,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	data, err := h.buildFindingsRuleViewData(ctx, c, rs, r, scope, alert)
	if err != nil {
		return h.RenderError(c, err)
	}
	return h.RenderComponent(c, views.FindingsRuleMutationResponse(data))
}

func (h *Handlers) HandleFindingsRuleOverride(c *echo.Context) error {
	ctx := c.Request().Context()

	rulesetKey := strings.TrimSpace(c.Param("rulesetKey"))
	ruleKey := strings.TrimSpace(c.Param("ruleKey"))
	if rulesetKey == "" || ruleKey == "" {
		return RenderNotFound(c)
	}

	rs, err := h.Q.GetRulesetByKey(ctx, rulesetKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	scope, err := h.findingsConcreteScopeForRuleset(ctx, rs)
	if err != nil {
		return h.RenderError(c, err)
	}

	r, err := h.Q.GetFindingRuleCurrentByRulesetKeyAndRuleKey(ctx, gen.GetFindingRuleCurrentByRulesetKeyAndRuleKeyParams{
		RulesetKey: rulesetKey,
		RuleKey:    ruleKey,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	defaults := parseRuleParameters(r.DefinitionJson)
	params, err := parseOverrideParamsFromForm(c, defaults)
	if err != nil {
		return h.renderRuleWithAlert(c, rs, r, scope, viewmodels.AlertViewData{Title: "Invalid override params", Message: err.Error(), Destructive: true})
	}

	if err := engine.ValidateParamOverrides(defaults, params); err != nil {
		return h.renderRuleWithAlert(c, rs, r, scope, viewmodels.AlertViewData{Title: "Invalid parameters", Message: err.Error(), Destructive: true})
	}

	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return h.RenderError(c, err)
	}

	enabled := ParseBoolForm(c.FormValue("enabled"))
	if _, err := h.Q.UpsertRuleOverride(ctx, gen.UpsertRuleOverrideParams{
		RuleID:     r.ID,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
		Params:     paramsJSON,
		Enabled:    enabled,
	}); err != nil {
		return h.RenderError(c, err)
	}

	toast := viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Rule override saved",
		Description: "The rule override was updated.",
	}
	if isHX(c) {
		setResponseToast(c, toast)
		addHXTrigger(c, events.FindingsRuleChanged, map[string]string{"ruleset": rulesetKey, "rule": ruleKey})
		return h.renderFindingsRuleMutationResponse(c, rs, scope, ruleKey, nil)
	}

	return redirectWithFlash(c, "/findings/rulesets/"+rulesetKey+"/rules/"+ruleKey, toast)
}

func (h *Handlers) HandleFindingsRuleAttestation(c *echo.Context) error {
	ctx := c.Request().Context()

	rulesetKey := strings.TrimSpace(c.Param("rulesetKey"))
	ruleKey := strings.TrimSpace(c.Param("ruleKey"))
	if rulesetKey == "" || ruleKey == "" {
		return RenderNotFound(c)
	}

	rs, err := h.Q.GetRulesetByKey(ctx, rulesetKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	scope, err := h.findingsConcreteScopeForRuleset(ctx, rs)
	if err != nil {
		return h.RenderError(c, err)
	}

	r, err := h.Q.GetFindingRuleCurrentByRulesetKeyAndRuleKey(ctx, gen.GetFindingRuleCurrentByRulesetKeyAndRuleKeyParams{
		RulesetKey: rulesetKey,
		RuleKey:    ruleKey,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	status := strings.ToLower(strings.TrimSpace(c.FormValue("status")))
	switch status {
	case "pass", "fail", "not_applicable":
	default:
		return h.renderRuleWithAlert(c, rs, r, scope, viewmodels.AlertViewData{Title: "Invalid attestation status", Message: "Status must be pass, fail, or not_applicable.", Destructive: true})
	}

	expiresAt, err := parseDatetimeLocal(c.FormValue("expires_at"))
	if err != nil {
		return h.renderRuleWithAlert(c, rs, r, scope, viewmodels.AlertViewData{Title: "Invalid expiry", Message: err.Error(), Destructive: true})
	}

	notes := strings.TrimSpace(c.FormValue("notes"))

	if _, err := h.Q.UpsertRuleAttestation(ctx, gen.UpsertRuleAttestationParams{
		RuleID:     r.ID,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
		Status:     status,
		Notes:      notes,
		ExpiresAt:  expiresAt,
	}); err != nil {
		return h.RenderError(c, err)
	}

	toast := viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Attestation saved",
		Description: "Manual attestation updated.",
	}
	if isHX(c) {
		setResponseToast(c, toast)
		addHXTrigger(c, events.FindingsRuleChanged, map[string]string{"ruleset": rulesetKey, "rule": ruleKey})
		return h.renderFindingsRuleMutationResponse(c, rs, scope, ruleKey, nil)
	}

	return redirectWithFlash(c, "/findings/rulesets/"+rulesetKey+"/rules/"+ruleKey, toast)
}

func (h *Handlers) getRuleOverride(ctx context.Context, ruleID int64, scope findingsScope) (*gen.RuleOverride, error) {
	row, err := h.Q.GetRuleOverride(ctx, gen.GetRuleOverrideParams{
		RuleID:     ruleID,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &row, nil
}

func (h *Handlers) getRuleAttestation(ctx context.Context, ruleID int64, scope findingsScope) (viewmodels.FindingsRuleAttestationViewData, error) {
	row, err := h.Q.GetRuleAttestation(ctx, gen.GetRuleAttestationParams{
		RuleID:     ruleID,
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return viewmodels.FindingsRuleAttestationViewData{Status: "pass"}, nil
		}
		return viewmodels.FindingsRuleAttestationViewData{}, err
	}

	expiresInput := ""
	expiresDisplay := viewmodels.TimeDisplay{}
	if row.ExpiresAt.Valid {
		expiresInput = row.ExpiresAt.Time.Format("2006-01-02T15:04")
		expiresDisplay = calendarDateDisplay(row.ExpiresAt)
	}

	return viewmodels.FindingsRuleAttestationViewData{
		Status:         strings.ToLower(strings.TrimSpace(row.Status)),
		Notes:          strings.TrimSpace(row.Notes),
		ExpiresAtInput: expiresInput,
		ExpiresAt:      expiresDisplay,
	}, nil
}

func formatTimeTable(t pgtype.Timestamptz) viewmodels.TimeDisplay {
	return calendarDateDisplayOrEmpty(t)
}

func normalizeRuleStatusFilter(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "all":
		return ""
	case "pass", "fail", "unknown", "error", "not_applicable":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ""
	}
}

func normalizeSeverityFilter(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "all":
		return ""
	case "critical", "high", "medium", "low", "info":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ""
	}
}

func normalizeMonitoringFilter(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "all":
		return ""
	case "automated", "partial", "manual", "unsupported":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ""
	}
}

func parseRuleParameters(definitionJSON []byte) map[string]any {
	var r osspecv2.Rule
	if err := json.Unmarshal(definitionJSON, &r); err != nil {
		return nil
	}
	return r.Parameters
}

func parseEvidence(evidenceJSON []byte) viewmodels.FindingsEvidenceViewData {
	out := viewmodels.FindingsEvidenceViewData{}
	out.RawPretty = prettyJSON(evidenceJSON)

	var payload map[string]any
	if err := json.Unmarshal(evidenceJSON, &payload); err != nil {
		return out
	}

	schemaVersion := intFromAny(payload["schema_version"])
	if schemaVersion != 1 {
		return out
	}

	out.IsEnvelopeV1 = true

	check, _ := payload["check"].(map[string]any)
	out.CheckType = strings.TrimSpace(stringFromAny(check["type"]))
	out.Dataset = strings.TrimSpace(stringFromAny(check["dataset"]))

	if params, ok := payload["params"].(map[string]any); ok {
		b, _ := json.MarshalIndent(params, "", "  ")
		out.ParamsPretty = string(b)
	}

	if sel, ok := payload["selection"].(map[string]any); ok {
		out.SelectionTotal = intFromAny(sel["total"])
		if out.SelectionTotal == 0 {
			out.SelectionTotal = intFromAny(sel["selected"])
		}

		out.SelectionSelected = intFromAny(sel["passed"])
		if out.SelectionSelected == 0 {
			out.SelectionSelected = intFromAny(sel["selected"])
		}
	}

	if v, ok := payload["violations"].([]any); ok {
		out.Violations = make([]viewmodels.FindingsEvidenceViolation, 0, len(v))
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out.Violations = append(out.Violations, viewmodels.FindingsEvidenceViolation{
				ResourceID: strings.TrimSpace(stringFromAny(m["resource_id"])),
				Display:    strings.TrimSpace(stringFromAny(m["display"])),
			})
		}
	}

	if b, ok := payload["violations_truncated"].(bool); ok {
		out.ViolationsTruncated = b
	}

	return out
}

func prettyJSON(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return strings.TrimSpace(string(b))
	}
	pretty, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return strings.TrimSpace(string(b))
	}
	return string(pretty)
}

func stringFromAny(v any) string {
	s, _ := v.(string)
	return s
}

func intFromAny(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	default:
		return 0
	}
}

func parseOverrideParamsFromForm(c *echo.Context, defaults map[string]any) (map[string]any, error) {
	params := make(map[string]any)
	for key, defaultValue := range defaults {
		formKey := "param_" + key
		raw := strings.TrimSpace(c.FormValue(formKey))
		if raw == "" {
			continue
		}

		switch parameterType(defaultValue) {
		case "string":
			params[key] = raw
		case "number":
			var value any
			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				return nil, fmt.Errorf("%s must be number", key)
			}
			params[key] = value
		case "boolean":
			switch strings.ToLower(raw) {
			case "true":
				params[key] = true
			case "false":
				params[key] = false
			default:
				return nil, fmt.Errorf("%s must be true/false", key)
			}
		case "json":
			var value any
			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				return nil, fmt.Errorf("%s must be valid JSON", key)
			}
			params[key] = value
		}
	}

	return params, nil
}

func buildRuleOverrideView(defaults map[string]any, override *gen.RuleOverride) viewmodels.FindingsRuleOverrideViewData {
	out := viewmodels.FindingsRuleOverrideViewData{
		Enabled: true,
	}

	overrideParams := map[string]any{}
	if override != nil {
		out.Enabled = override.Enabled
		out.CurrentParamsPretty = prettyJSON(override.Params)
		_ = json.Unmarshal(override.Params, &overrideParams)
	}

	if len(defaults) == 0 {
		return out
	}

	out.HasParameters = true
	keys := make([]string, 0, len(defaults))
	for k := range defaults {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out.Fields = make([]viewmodels.FindingsParamField, 0, len(keys))
	for _, key := range keys {
		field := viewmodels.FindingsParamField{
			Key:           key,
			Type:          parameterType(defaults[key]),
			DefaultValue:  formatParameterValue(defaults[key]),
			OverrideValue: "",
		}

		if v, ok := overrideParams[key]; ok {
			switch vv := v.(type) {
			case bool:
				if vv {
					field.OverrideValue = "true"
				} else {
					field.OverrideValue = "false"
				}
			default:
				field.OverrideValue = formatParameterValue(vv)
			}
		}

		if field.DefaultValue == "<nil>" {
			field.DefaultValue = ""
		}

		out.Fields = append(out.Fields, field)
	}

	return out
}

func parameterType(value any) string {
	switch value.(type) {
	case string:
		return "string"
	case bool:
		return "boolean"
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return "number"
	default:
		return "json"
	}
}

func formatParameterValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(b)
}

func parseDatetimeLocal(v string) (pgtype.Timestamptz, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return pgtype.Timestamptz{}, nil
	}
	t, err := time.ParseInLocation("2006-01-02T15:04", v, time.Local)
	if err != nil {
		return pgtype.Timestamptz{}, err
	}
	return pgtype.Timestamptz{Time: t, Valid: true}, nil
}

func (h *Handlers) buildFindingsRuleViewData(ctx context.Context, c *echo.Context, rs gen.Ruleset, r gen.GetFindingRuleCurrentByRulesetKeyAndRuleKeyRow, scope findingsScope, alert *viewmodels.AlertViewData) (viewmodels.FindingsRuleViewData, error) {
	layout, _, err := h.LayoutData(ctx, c, strings.TrimSpace(rs.Name))
	if err != nil {
		return viewmodels.FindingsRuleViewData{}, err
	}

	rulesetOverrideEnabled, _, err := h.getRulesetOverride(ctx, rs.ID, scope)
	if err != nil {
		return viewmodels.FindingsRuleViewData{}, err
	}

	ruleOverride, err := h.getRuleOverride(ctx, r.ID, scope)
	if err != nil {
		return viewmodels.FindingsRuleViewData{}, err
	}

	attestation, err := h.getRuleAttestation(ctx, r.ID, scope)
	if err != nil {
		return viewmodels.FindingsRuleViewData{}, err
	}

	defaults := parseRuleParameters(r.DefinitionJson)
	evidence := parseEvidence(r.CurrentEvidenceJson)

	connectorKind := ""
	if rs.ConnectorKind.Valid {
		connectorKind = strings.TrimSpace(rs.ConnectorKind.String)
	}

	return viewmodels.FindingsRuleViewData{
		Layout: layout,
		Ruleset: viewmodels.FindingsRulesetItem{
			Key:           strings.TrimSpace(rs.Key),
			Name:          strings.TrimSpace(rs.Name),
			Description:   strings.TrimSpace(rs.Description),
			ScopeKind:     strings.TrimSpace(rs.ScopeKind),
			ConnectorKind: connectorKind,
			Status:        strings.TrimSpace(rs.Status),
			Source:        strings.TrimSpace(rs.Source),
			SourceVersion: strings.TrimSpace(rs.SourceVersion),
			Href:          "/findings/rulesets/" + strings.TrimSpace(rs.Key),
		},
		SourceName:             scope.SourceName,
		RuleKey:                strings.TrimSpace(r.Key),
		RuleTitle:              strings.TrimSpace(r.Title),
		RuleSummary:            strings.TrimSpace(r.Summary),
		RuleSeverity:           strings.TrimSpace(r.Severity),
		MonitoringStatus:       strings.TrimSpace(r.MonitoringStatus),
		MonitoringReason:       strings.TrimSpace(r.MonitoringReason),
		CurrentStatus:          strings.TrimSpace(r.CurrentStatus),
		CurrentErrorKind:       strings.TrimSpace(r.CurrentErrorKind),
		EvidenceSummary:        strings.TrimSpace(r.CurrentEvidenceSummary),
		Evidence:               evidence,
		CurrentEvaluatedAt:     formatTimeTable(r.CurrentEvaluatedAt),
		RulesetOverrideEnabled: rulesetOverrideEnabled,
		RuleOverride:           buildRuleOverrideView(defaults, ruleOverride),
		Attestation:            attestation,
		Alert:                  alert,
	}, nil
}

func (h *Handlers) renderRuleWithAlert(c *echo.Context, rs gen.Ruleset, r gen.GetFindingRuleCurrentByRulesetKeyAndRuleKeyRow, scope findingsScope, alert viewmodels.AlertViewData) error {
	ctx := c.Request().Context()

	data, err := h.buildFindingsRuleViewData(ctx, c, rs, r, scope, &alert)
	if err != nil {
		return h.RenderError(c, err)
	}
	if isHX(c) {
		return h.RenderComponentStatus(c, http.StatusUnprocessableEntity, views.FindingsRuleMutationResponse(data))
	}
	return h.RenderComponent(c, views.FindingsRulePage(data))
}
