package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v4"
	osspecv1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v1"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

func (h *Handlers) HandleFindings(c echo.Context) error {
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

func (h *Handlers) HandleFindingsRuleset(c echo.Context) error {
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

	layout, _, err := h.LayoutData(ctx, c, strings.TrimSpace(rs.Name))
	if err != nil {
		return h.RenderError(c, err)
	}

	scope, err := h.findingsScopeForRuleset(ctx, rs)
	if err != nil {
		return h.RenderError(c, err)
	}

	overrideEnabled, overrideExists, err := h.getRulesetOverride(ctx, rs.ID, scope)
	if err != nil {
		return h.RenderError(c, err)
	}

	statusFilter := normalizeRuleStatusFilter(c.QueryParam("status"))
	severityFilter := normalizeSeverityFilter(c.QueryParam("severity"))
	monitoringFilter := normalizeMonitoringFilter(c.QueryParam("monitoring"))

	ruleRows, err := h.Q.ListActiveRulesWithCurrentResultsByRulesetKey(ctx, gen.ListActiveRulesWithCurrentResultsByRulesetKeyParams{
		Key:        strings.TrimSpace(rs.Key),
		ScopeKind:  scope.ScopeKind,
		SourceKind: scope.SourceKind,
		SourceName: scope.SourceName,
	})
	if err != nil {
		return h.RenderError(c, err)
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
			Href:             "/findings/rulesets/" + rulesetKey + "/rules/" + strings.TrimSpace(row.Key),
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

	meta := parseRulesetMetadata(rs.DefinitionJson)

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
		SourceName:              scope.SourceName,
		ConnectorHintHref:       scope.ConnectorHintHref,
		Tags:                    meta.Tags,
		References:              meta.References,
		FrameworkMappings:       meta.FrameworkMappings,
		HasMetadata:             meta.HasMetadata,
		OverrideExists:          overrideExists,
		OverrideEnabled:         overrideEnabled,
		StatusFilter:            statusFilter,
		SeverityFilter:          severityFilter,
		MonitoringFilter:        monitoringFilter,
		Rules:                   items,
		HasRules:                len(items) > 0,
	}

	return h.RenderComponent(c, views.FindingsRulesetPage(data))
}

type findingsScope struct {
	ScopeKind         string
	SourceKind        string
	SourceName        string
	ConnectorHintHref string
}

func (h *Handlers) findingsScopeForRuleset(ctx context.Context, rs gen.Ruleset) (findingsScope, error) {
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

		var (
			sourceName string
			hintHref   string
		)

		if h.Registry != nil {
			states, err := h.Registry.LoadStates(ctx, h.Q)
			if err != nil {
				return findingsScope{}, err
			}
			for _, st := range states {
				if strings.EqualFold(strings.TrimSpace(st.Definition.Kind()), connectorKind) {
					sourceName = strings.TrimSpace(st.SourceName)
					hintHref = strings.TrimSpace(st.Definition.SettingsHref())
					break
				}
			}
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

func (h *Handlers) HandleFindingsRulesetOverride(c echo.Context) error {
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

	scope, err := h.findingsScopeForRuleset(ctx, rs)
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

	return c.Redirect(http.StatusSeeOther, "/findings/rulesets/"+rulesetKey)
}

func (h *Handlers) HandleFindingsRule(c echo.Context) error {
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

	scope, err := h.findingsScopeForRuleset(ctx, rs)
	if err != nil {
		return h.RenderError(c, err)
	}

	layout, _, err := h.LayoutData(ctx, c, strings.TrimSpace(rs.Name))
	if err != nil {
		return h.RenderError(c, err)
	}

	r, err := h.Q.GetRuleWithCurrentResultByRulesetKeyAndRuleKey(ctx, gen.GetRuleWithCurrentResultByRulesetKeyAndRuleKeyParams{
		Key:        rulesetKey,
		Key_2:      ruleKey,
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

	rulesetOverrideEnabled, _, err := h.getRulesetOverride(ctx, rs.ID, scope)
	if err != nil {
		return h.RenderError(c, err)
	}

	ruleOverride, err := h.getRuleOverride(ctx, r.ID, scope)
	if err != nil {
		return h.RenderError(c, err)
	}

	attestation, err := h.getRuleAttestation(ctx, r.ID, scope)
	if err != nil {
		return h.RenderError(c, err)
	}

	def := parseRuleDefinition(r.DefinitionJson)
	evidence := parseEvidence(r.CurrentEvidenceJson)

	connectorKind := ""
	if rs.ConnectorKind.Valid {
		connectorKind = strings.TrimSpace(rs.ConnectorKind.String)
	}

	data := viewmodels.FindingsRuleViewData{
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
		SourceName:              scope.SourceName,
		RuleKey:                 strings.TrimSpace(r.Key),
		RuleTitle:               strings.TrimSpace(r.Title),
		RuleSummary:             strings.TrimSpace(r.Summary),
		RuleSeverity:            strings.TrimSpace(r.Severity),
		MonitoringStatus:        strings.TrimSpace(r.MonitoringStatus),
		MonitoringReason:        strings.TrimSpace(r.MonitoringReason),
		RemediationInstructions: strings.TrimSpace(def.RemediationInstructions),
		RemediationRisks:        strings.TrimSpace(def.RemediationRisks),
		RemediationEffort:       strings.TrimSpace(def.RemediationEffort),
		CurrentStatus:           strings.TrimSpace(r.CurrentStatus),
		CurrentErrorKind:        strings.TrimSpace(r.CurrentErrorKind),
		EvidenceSummary:         strings.TrimSpace(r.CurrentEvidenceSummary),
		Evidence:                evidence,
		CurrentEvaluatedAt:      formatTimeRFC3339(r.CurrentEvaluatedAt),
		RulesetOverrideEnabled:  rulesetOverrideEnabled,
		RuleOverride:            buildRuleOverrideView(def.ParamSchema, def.ParamDefaults, ruleOverride),
		Attestation:             attestation,
	}

	return h.RenderComponent(c, views.FindingsRulePage(data))
}

func (h *Handlers) HandleFindingsRuleOverride(c echo.Context) error {
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

	scope, err := h.findingsScopeForRuleset(ctx, rs)
	if err != nil {
		return h.RenderError(c, err)
	}

	r, err := h.Q.GetRuleWithCurrentResultByRulesetKeyAndRuleKey(ctx, gen.GetRuleWithCurrentResultByRulesetKeyAndRuleKeyParams{
		Key:        rulesetKey,
		Key_2:      ruleKey,
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

	def := parseRuleDefinition(r.DefinitionJson)
	params, err := parseOverrideParamsFromForm(c, def.ParamSchema)
	if err != nil {
		return h.renderRuleWithAlert(c, rs, r, scope, viewmodels.FindingsAlert{Title: "Invalid override params", Message: err.Error(), Destructive: true})
	}

	if len(def.ParamSchema) > 0 {
		if err := engine.ValidateParams(params, def.ParamSchema); err != nil {
			return h.renderRuleWithAlert(c, rs, r, scope, viewmodels.FindingsAlert{Title: "Invalid parameters", Message: err.Error(), Destructive: true})
		}
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

	return c.Redirect(http.StatusSeeOther, "/findings/rulesets/"+rulesetKey+"/rules/"+ruleKey)
}

func (h *Handlers) HandleFindingsRuleAttestation(c echo.Context) error {
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

	scope, err := h.findingsScopeForRuleset(ctx, rs)
	if err != nil {
		return h.RenderError(c, err)
	}

	r, err := h.Q.GetRuleWithCurrentResultByRulesetKeyAndRuleKey(ctx, gen.GetRuleWithCurrentResultByRulesetKeyAndRuleKeyParams{
		Key:        rulesetKey,
		Key_2:      ruleKey,
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
		return h.renderRuleWithAlert(c, rs, r, scope, viewmodels.FindingsAlert{Title: "Invalid attestation status", Message: "Status must be pass, fail, or not_applicable.", Destructive: true})
	}

	expiresAt, err := parseDatetimeLocal(c.FormValue("expires_at"))
	if err != nil {
		return h.renderRuleWithAlert(c, rs, r, scope, viewmodels.FindingsAlert{Title: "Invalid expiry", Message: err.Error(), Destructive: true})
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

	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Attestation saved",
		Description: "Manual attestation updated.",
	})

	return c.Redirect(http.StatusSeeOther, "/findings/rulesets/"+rulesetKey+"/rules/"+ruleKey)
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

	expires := ""
	if row.ExpiresAt.Valid {
		expires = row.ExpiresAt.Time.Format("2006-01-02T15:04")
	}

	return viewmodels.FindingsRuleAttestationViewData{
		Status:    strings.ToLower(strings.TrimSpace(row.Status)),
		Notes:     strings.TrimSpace(row.Notes),
		ExpiresAt: expires,
	}, nil
}

func formatTimeRFC3339(t pgtype.Timestamptz) string {
	if !t.Valid {
		return ""
	}
	return t.Time.Format(time.RFC3339)
}

func formatTimeTable(t pgtype.Timestamptz) string {
	if !t.Valid {
		return ""
	}
	return t.Time.Format("02-01-2006 15:04")
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

type rulesetMeta struct {
	Tags              []string
	References        []viewmodels.FindingsReferenceItem
	FrameworkMappings []viewmodels.FindingsFrameworkMappingItem
	HasMetadata       bool
}

func parseRulesetMetadata(definitionJSON []byte) rulesetMeta {
	var doc osspecv1.RulesetDoc
	if err := json.Unmarshal(definitionJSON, &doc); err != nil {
		return rulesetMeta{}
	}

	rs := doc.Ruleset
	meta := rulesetMeta{}

	if len(rs.Tags) > 0 {
		meta.Tags = append([]string(nil), rs.Tags...)
		meta.HasMetadata = true
	}
	if len(rs.References) > 0 {
		meta.HasMetadata = true
		meta.References = make([]viewmodels.FindingsReferenceItem, 0, len(rs.References))
		for _, r := range rs.References {
			meta.References = append(meta.References, viewmodels.FindingsReferenceItem{
				Title: strings.TrimSpace(r.Title),
				URL:   strings.TrimSpace(r.URL),
				Type:  strings.TrimSpace(string(r.Type)),
			})
		}
	}
	if len(rs.FrameworkMappings) > 0 {
		meta.HasMetadata = true
		meta.FrameworkMappings = make([]viewmodels.FindingsFrameworkMappingItem, 0, len(rs.FrameworkMappings))
		for _, m := range rs.FrameworkMappings {
			meta.FrameworkMappings = append(meta.FrameworkMappings, viewmodels.FindingsFrameworkMappingItem{
				Framework:   strings.TrimSpace(m.Framework),
				Control:     strings.TrimSpace(m.Control),
				Coverage:    strings.TrimSpace(string(m.Coverage)),
			})
		}
	}

	return meta
}

type ruleMeta struct {
	RemediationInstructions string
	RemediationRisks        string
	RemediationEffort       string
	ParamSchema             map[string]osspecv1.ParameterSchema
	ParamDefaults           map[string]any
}

func parseRuleDefinition(definitionJSON []byte) ruleMeta {
	var r osspecv1.Rule
	if err := json.Unmarshal(definitionJSON, &r); err != nil {
		return ruleMeta{}
	}

	out := ruleMeta{}

	if r.Remediation != nil {
		out.RemediationInstructions = strings.TrimSpace(r.Remediation.Instructions)
		out.RemediationRisks = strings.TrimSpace(r.Remediation.Risks)
		out.RemediationEffort = strings.TrimSpace(string(r.Remediation.Effort))
	}

	if r.Parameters != nil {
		if r.Parameters.Schema != nil {
			out.ParamSchema = r.Parameters.Schema
		}
		if r.Parameters.Defaults != nil {
			out.ParamDefaults = r.Parameters.Defaults
		}
	}

	return out
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

	if left, ok := check["left"].(map[string]any); ok {
		out.Left.Dataset = strings.TrimSpace(stringFromAny(left["dataset"]))
	}
	if right, ok := check["right"].(map[string]any); ok {
		out.Right.Dataset = strings.TrimSpace(stringFromAny(right["dataset"]))
	}

	if params, ok := payload["params"].(map[string]any); ok {
		b, _ := json.MarshalIndent(params, "", "  ")
		out.ParamsPretty = string(b)
	}

	if sel, ok := payload["selection"].(map[string]any); ok {
		out.SelectionTotal = intFromAny(sel["total"])
		out.SelectionSelected = intFromAny(sel["selected"])
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

func parseOverrideParamsFromForm(c echo.Context, schema map[string]osspecv1.ParameterSchema) (map[string]any, error) {
	params := make(map[string]any)
	for key, sch := range schema {
		formKey := "param_" + key
		raw := strings.TrimSpace(c.FormValue(formKey))
		if raw == "" {
			continue
		}

		switch strings.TrimSpace(sch.Type) {
		case "string":
			params[key] = raw
		case "integer":
			n, err := strconv.Atoi(raw)
			if err != nil {
				return nil, fmt.Errorf("%s must be integer", key)
			}
			params[key] = n
		case "number":
			f, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return nil, fmt.Errorf("%s must be number", key)
			}
			params[key] = f
		case "boolean":
			switch strings.ToLower(raw) {
			case "true":
				params[key] = true
			case "false":
				params[key] = false
			default:
				return nil, fmt.Errorf("%s must be true/false", key)
			}
		default:
			return nil, fmt.Errorf("%s has unsupported type %q", key, sch.Type)
		}
	}

	return params, nil
}

func buildRuleOverrideView(schema map[string]osspecv1.ParameterSchema, defaults map[string]any, override *gen.RuleOverride) viewmodels.FindingsRuleOverrideViewData {
	out := viewmodels.FindingsRuleOverrideViewData{
		Enabled: true,
	}

	overrideParams := map[string]any{}
	if override != nil {
		out.Enabled = override.Enabled
		out.CurrentParamsPretty = prettyJSON(override.Params)
		_ = json.Unmarshal(override.Params, &overrideParams)
	}

	if len(schema) == 0 {
		return out
	}

	out.HasSchema = true
	keys := make([]string, 0, len(schema))
	for k := range schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out.Fields = make([]viewmodels.FindingsParamField, 0, len(keys))
	for _, key := range keys {
		sch := schema[key]
		field := viewmodels.FindingsParamField{
			Key:           key,
			Type:          strings.TrimSpace(sch.Type),
			Description:   strings.TrimSpace(sch.Description),
			DefaultValue:  fmt.Sprintf("%v", defaults[key]),
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
				field.OverrideValue = fmt.Sprintf("%v", vv)
			}
		}

		if field.DefaultValue == "<nil>" {
			field.DefaultValue = ""
		}

		out.Fields = append(out.Fields, field)
	}

	return out
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

func (h *Handlers) renderRuleWithAlert(c echo.Context, rs gen.Ruleset, r gen.GetRuleWithCurrentResultByRulesetKeyAndRuleKeyRow, scope findingsScope, alert viewmodels.FindingsAlert) error {
	ctx := c.Request().Context()

	layout, _, err := h.LayoutData(ctx, c, strings.TrimSpace(rs.Name))
	if err != nil {
		return h.RenderError(c, err)
	}

	rulesetOverrideEnabled, _, err := h.getRulesetOverride(ctx, rs.ID, scope)
	if err != nil {
		return h.RenderError(c, err)
	}

	ruleOverride, err := h.getRuleOverride(ctx, r.ID, scope)
	if err != nil {
		return h.RenderError(c, err)
	}

	attestation, err := h.getRuleAttestation(ctx, r.ID, scope)
	if err != nil {
		return h.RenderError(c, err)
	}

	def := parseRuleDefinition(r.DefinitionJson)
	evidence := parseEvidence(r.CurrentEvidenceJson)

	connectorKind := ""
	if rs.ConnectorKind.Valid {
		connectorKind = strings.TrimSpace(rs.ConnectorKind.String)
	}

	data := viewmodels.FindingsRuleViewData{
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
		SourceName:              scope.SourceName,
		RuleKey:                 strings.TrimSpace(r.Key),
		RuleTitle:               strings.TrimSpace(r.Title),
		RuleSummary:             strings.TrimSpace(r.Summary),
		RuleSeverity:            strings.TrimSpace(r.Severity),
		MonitoringStatus:        strings.TrimSpace(r.MonitoringStatus),
		MonitoringReason:        strings.TrimSpace(r.MonitoringReason),
		RemediationInstructions: strings.TrimSpace(def.RemediationInstructions),
		RemediationRisks:        strings.TrimSpace(def.RemediationRisks),
		RemediationEffort:       strings.TrimSpace(def.RemediationEffort),
		CurrentStatus:           strings.TrimSpace(r.CurrentStatus),
		CurrentErrorKind:        strings.TrimSpace(r.CurrentErrorKind),
		EvidenceSummary:         strings.TrimSpace(r.CurrentEvidenceSummary),
		Evidence:                evidence,
		CurrentEvaluatedAt:      formatTimeRFC3339(r.CurrentEvaluatedAt),
		RulesetOverrideEnabled:  rulesetOverrideEnabled,
		RuleOverride:            buildRuleOverrideView(def.ParamSchema, def.ParamDefaults, ruleOverride),
		Attestation:             attestation,
		Alert:                   &alert,
	}

	return h.RenderComponent(c, views.FindingsRulePage(data))
}
