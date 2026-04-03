package handlers

import (
	"context"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleDashboard renders the dashboard page.
func (h *Handlers) HandleDashboard(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Dashboard")
	if err != nil {
		return h.RenderError(c, err)
	}

	identityCount, err := h.dashboardIdentityCount(ctx, snap)
	if err != nil {
		return h.RenderError(c, err)
	}

	discoveryAppCount, err := h.dashboardDiscoveryAppCount(ctx, snap)
	if err != nil {
		return h.RenderError(c, err)
	}

	appAssetCount, err := h.dashboardAppAssetCount(ctx, snap)
	if err != nil {
		return h.RenderError(c, err)
	}

	sourceNameByKind := map[string]string{}
	if h.Registry != nil {
		states, err := h.Registry.LoadStates(ctx, h.Q)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, st := range states {
			sourceNameByKind[strings.ToLower(strings.TrimSpace(st.Definition.Kind()))] = strings.TrimSpace(st.SourceName)
		}
	}

	rulesets, err := h.Q.ListRulesets(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}

	frameworkPosture := make([]viewmodels.DashboardFrameworkPostureItem, 0, 5)
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

		counts, err := h.Q.GetRulesetPostureCounts(ctx, gen.GetRulesetPostureCountsParams{
			RulesetID:  rs.ID,
			ScopeKind:  scopeKind,
			SourceKind: sourceKind,
			SourceName: sourceName,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		if counts.TotalRules == 0 || counts.EvaluatedRules == 0 {
			continue
		}

		passPercent := 0
		if counts.TotalRules > 0 {
			passPercent = max(int((counts.PassedRules*100)/counts.TotalRules), 0)
			if passPercent > 100 {
				passPercent = 100
			}
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
			PassPercent: passPercent,
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
		Layout:            layout,
		IdentityCount:     identityCount,
		DiscoveryAppCount: discoveryAppCount,
		AppAssetCount:     appAssetCount,
		FrameworkPosture:  frameworkPosture,
	}

	return h.RenderComponent(c, views.DashboardPage(data))
}

func (h *Handlers) dashboardIdentityCount(ctx context.Context, snap ConnectorSnapshot) (int64, error) {
	sourcePairs := availableIdentitySourcePairs(snap)
	if len(sourcePairs) == 0 {
		return 0, nil
	}

	configuredKinds, configuredNames := identityConfiguredSourcePairs(sourcePairs)
	return h.Q.CountIdentitiesInventoryByFilters(ctx, gen.CountIdentitiesInventoryByFiltersParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
	})
}

func (h *Handlers) dashboardDiscoveryAppCount(ctx context.Context, snap ConnectorSnapshot) (int64, error) {
	sourceOptions := discoverySourceOptions(snap)
	if len(sourceOptions) == 0 {
		return 0, nil
	}

	configuredKinds, configuredNames := discoveryConfiguredSourcePairs(sourceOptions)
	return h.Q.CountSaaSAppsByFilters(ctx, gen.CountSaaSAppsByFiltersParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
	})
}

func (h *Handlers) dashboardAppAssetCount(ctx context.Context, snap ConnectorSnapshot) (int64, error) {
	sourcePairs := configuredProgrammaticSources(snap)
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

// HandleHealthz returns a simple health check response.
func (h *Handlers) HandleHealthz(c *echo.Context) error {
	return c.String(200, "ok")
}
