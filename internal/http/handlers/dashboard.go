package handlers

import (
	"context"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"
	connregistry "github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleDashboard renders the dashboard page.
func (h *Handlers) HandleDashboard(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Posture")
	if err != nil {
		return h.RenderError(c, err)
	}

	identityCount, err := h.dashboardIdentityCount(ctx, stateView)
	if err != nil {
		return h.RenderError(c, err)
	}

	discoveryAppCount, err := h.dashboardDiscoveryAppCount(ctx, stateView)
	if err != nil {
		return h.RenderError(c, err)
	}

	appAssetCount, err := h.dashboardAppAssetCount(ctx, stateView)
	if err != nil {
		return h.RenderError(c, err)
	}

	credentialsAttention, err := h.dashboardCredentialsAttention(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}

	unreviewedDiscoveryApps, err := h.dashboardUnreviewedDiscoveryAppCount(ctx, stateView)
	if err != nil {
		return h.RenderError(c, err)
	}

	suspendedHumanIdentities, err := h.dashboardSuspendedHumanIdentityCount(ctx, stateView)
	if err != nil {
		return h.RenderError(c, err)
	}

	sourceNameByKind := map[string]string{}
	var connectorStates []connregistry.ConnectorState
	if h.Registry != nil {
		connectorStates, err = h.Registry.LoadStates(ctx, h.Q)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, st := range connectorStates {
			sourceNameByKind[strings.ToLower(strings.TrimSpace(st.Definition.Kind()))] = strings.TrimSpace(st.SourceName)
		}
	}

	connectorHealth, err := buildConnectorHealthViewData(h.Cfg, h.Q, ctx, connectorStates, h.Syncer != nil)
	if err != nil {
		return h.RenderError(c, err)
	}

	rulesets, err := h.Q.ListRulesets(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}

	frameworkPosture := make([]viewmodels.DashboardFrameworkPostureItem, 0, 5)
	findingSeverity := viewmodels.DashboardFindingSeverity{}
	for _, rs := range rulesets {
		if strings.TrimSpace(rs.Status) != "active" {
			continue
		}

		scopeKind := strings.TrimSpace(rs.ScopeKind)
		var sourceKind, sourceName string
		switch scopeKind {
		case "global":
		case "connector_instance":
			if rs.ConnectorKind.Valid {
				sourceKind = strings.TrimSpace(rs.ConnectorKind.String)
			}
			if sourceKind == "" {
				continue
			}
			sourceName = sourceNameByKind[strings.ToLower(sourceKind)]
		default:
			continue
		}

		rows, err := h.Q.ListFindingRulesetCurrentByRulesetKey(ctx, gen.ListFindingRulesetCurrentByRulesetKeyParams{
			ScopeKind:  scopeKind,
			SourceKind: sourceKind,
			SourceName: sourceName,
			Key:        strings.TrimSpace(rs.Key),
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		counts := dashboardFrameworkPostureCounts(rows)
		dashboardAccumulateFindingSeverity(&findingSeverity, rows)
		if counts.TotalRules == 0 || counts.EvaluatedRules == 0 {
			continue
		}

		name := strings.TrimSpace(rs.Name)
		if name == "" {
			name = strings.TrimSpace(rs.Key)
		}

		frameworkPosture = append(frameworkPosture, viewmodels.DashboardFrameworkPostureItem{
			Key:         strings.TrimSpace(rs.Key),
			Name:        name,
			PassedCount: counts.PassedRules,
			TotalCount:  counts.TotalRules,
			PassPercent: dashboardPercent(counts.PassedRules, counts.TotalRules),
			BadgeLabel:  dashboardFrameworkBadgeLabel(name),
			Href:        "/findings/rulesets/" + strings.TrimSpace(rs.Key),
		})
	}

	sort.SliceStable(frameworkPosture, func(i, j int) bool {
		if frameworkPosture[i].PassPercent != frameworkPosture[j].PassPercent {
			return frameworkPosture[i].PassPercent < frameworkPosture[j].PassPercent
		}
		if frameworkPosture[i].TotalCount != frameworkPosture[j].TotalCount {
			return frameworkPosture[i].TotalCount > frameworkPosture[j].TotalCount
		}
		return frameworkPosture[i].Name < frameworkPosture[j].Name
	})
	if len(frameworkPosture) > 5 {
		frameworkPosture = frameworkPosture[:5]
	}

	data := viewmodels.DashboardViewData{
		Layout:                     layout,
		IdentityCount:              identityCount,
		DiscoveryAppCount:          discoveryAppCount,
		AppAssetCount:              appAssetCount,
		CredentialsAttention:       credentialsAttention,
		UnreviewedDiscoveryApps:    unreviewedDiscoveryApps,
		SuspendedHumanIdentities:   suspendedHumanIdentities,
		ConnectorsNeedingAttention: int64(connectorHealth.NeedsAttentionCount),
		FindingSeverity:            findingSeverity,
		FrameworkPosture:           frameworkPosture,
	}

	if isHX(c) && isHXTarget(c, "dashboard-content") {
		return h.RenderComponent(c, views.DashboardContent(data))
	}
	return h.RenderComponent(c, views.DashboardPage(data))
}

func dashboardAccumulateFindingSeverity(summary *viewmodels.DashboardFindingSeverity, rows []gen.ListFindingRulesetCurrentByRulesetKeyRow) {
	for _, row := range rows {
		if !row.CurrentEvaluatedAt.Valid {
			continue
		}
		summary.Evaluated++
		if strings.TrimSpace(row.CurrentStatus) != "fail" {
			continue
		}
		summary.Open++
		switch strings.ToUpper(strings.TrimSpace(row.Severity)) {
		case "CRITICAL", "CAT I", "CAT 1":
			summary.Critical++
		case "HIGH", "CAT II", "CAT 2":
			summary.High++
		case "MEDIUM", "CAT III", "CAT 3":
			summary.Medium++
		case "LOW":
			summary.Low++
		default:
			summary.Informational++
		}
	}
}

type dashboardPostureCounts struct {
	TotalRules     int64
	PassedRules    int64
	EvaluatedRules int64
}

func dashboardFrameworkPostureCounts(rows []gen.ListFindingRulesetCurrentByRulesetKeyRow) dashboardPostureCounts {
	counts := dashboardPostureCounts{TotalRules: int64(len(rows))}
	for _, row := range rows {
		if !row.CurrentEvaluatedAt.Valid {
			continue
		}
		counts.EvaluatedRules++
		if strings.TrimSpace(row.CurrentStatus) == "pass" {
			counts.PassedRules++
		}
	}
	return counts
}

func (h *Handlers) dashboardIdentityCount(ctx context.Context, stateView connectorStateView) (int64, error) {
	sourcePairs := availableIdentitySourcePairs(stateView)
	if len(sourcePairs) == 0 {
		return 0, nil
	}

	configuredKinds, configuredNames := identityConfiguredSourcePairs(sourcePairs)
	return h.Q.CountIdentitiesInventoryByFilters(ctx, gen.CountIdentitiesInventoryByFiltersParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
	})
}

func (h *Handlers) dashboardDiscoveryAppCount(ctx context.Context, stateView connectorStateView) (int64, error) {
	sourceOptions := discoverySourceOptions(stateView)
	if len(sourceOptions) == 0 {
		return 0, nil
	}

	// Empty source filters count across all configured, discovery-enabled sources; SQL enforces that scope.
	return h.Q.CountSaaSAppsByFilters(ctx, gen.CountSaaSAppsByFiltersParams{})
}

func (h *Handlers) dashboardUnreviewedDiscoveryAppCount(ctx context.Context, stateView connectorStateView) (int64, error) {
	sourceOptions := discoverySourceOptions(stateView)
	if len(sourceOptions) == 0 {
		return 0, nil
	}

	// Empty source filters count across all configured, discovery-enabled sources; SQL enforces that scope.
	return h.Q.CountSaaSAppsByFilters(ctx, gen.CountSaaSAppsByFiltersParams{
		ReviewDisposition: "unreviewed",
	})
}

func (h *Handlers) dashboardCredentialsAttention(ctx context.Context) (viewmodels.DashboardCredentialsAttention, error) {
	summary, err := h.Q.SummarizeCredentialAttentionForEnabledSources(ctx)
	if err != nil {
		return viewmodels.DashboardCredentialsAttention{}, err
	}

	return viewmodels.DashboardCredentialsAttention{
		Total:    summary.Critical + summary.High,
		Critical: summary.Critical,
		High:     summary.High,
	}, nil
}

func (h *Handlers) dashboardSuspendedHumanIdentityCount(ctx context.Context, stateView connectorStateView) (int64, error) {
	sourcePairs := availableIdentitySourcePairs(stateView)
	if len(sourcePairs) == 0 {
		return 0, nil
	}

	configuredKinds, configuredNames := identityConfiguredSourcePairs(sourcePairs)
	summary, err := h.Q.SummarizeIdentitiesInventoryByFilters(ctx, gen.SummarizeIdentitiesInventoryByFiltersParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
		IdentityType:          "human",
	})
	if err != nil {
		return 0, err
	}
	return summary.SuspendedCount, nil
}

func (h *Handlers) dashboardAppAssetCount(ctx context.Context, stateView connectorStateView) (int64, error) {
	sourcePairs := configuredProgrammaticSources(stateView)
	if len(sourcePairs) == 0 {
		return 0, nil
	}

	configuredKinds, configuredNames := programmaticConfiguredSourcePairs(sourcePairs)
	return h.Q.CountAppAssetsBySourcesAndQueryAndKind(ctx, gen.CountAppAssetsBySourcesAndQueryAndKindParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
	})
}

func dashboardFrameworkBadgeLabel(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "FW"
	}

	upper := strings.ToUpper(name)
	if strings.HasPrefix(upper, "CIS") {
		return "CIS"
	}

	parts := strings.Fields(upper)
	if len(parts) >= 2 {
		return safePrefix(parts[0], 1) + safePrefix(parts[1], 1)
	}
	if len(parts) == 1 {
		return safePrefix(parts[0], 2)
	}
	return "FW"
}

func safePrefix(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(strings.TrimSpace(s))
	if len(r) == 0 {
		return ""
	}
	if len(r) < n {
		n = len(r)
	}
	return string(r[:n])
}

func dashboardPercent(numerator, denominator int64) int {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	percent := int((numerator * 100) / denominator)
	if percent > 100 {
		return 100
	}
	return percent
}

// HandleHealthz returns a simple health check response.
func (h *Handlers) HandleHealthz(c *echo.Context) error {
	return c.String(200, "ok")
}
