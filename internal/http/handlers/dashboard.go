package handlers

import (
	"context"
	"math"
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
	layout, stateView, err := h.LayoutData(ctx, c, "Dashboard")
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

	relationshipGraph, err := h.dashboardRelationshipGraph(ctx, stateView, identityCount)
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
		RelationshipGraph: relationshipGraph,
		FrameworkPosture:  frameworkPosture,
	}

	return h.RenderComponent(c, views.DashboardPage(data))
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

	return h.Q.CountSaaSAppsByFilters(ctx, gen.CountSaaSAppsByFiltersParams{})
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

func (h *Handlers) dashboardRelationshipGraph(ctx context.Context, stateView connectorStateView, identityCount int64) (viewmodels.DashboardRelationshipGraph, error) {
	graph := viewmodels.DashboardRelationshipGraph{
		CenterX:       50,
		CenterY:       50,
		IdentityCount: identityCount,
	}

	sourcePairs := availableIdentitySourcePairs(stateView)
	if len(sourcePairs) == 0 {
		return graph, nil
	}

	configuredKinds, configuredNames := identityConfiguredSourcePairs(sourcePairs)
	sourceRows, err := h.Q.ListDashboardSourceAccountSummaries(ctx, gen.ListDashboardSourceAccountSummariesParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
	})
	if err != nil {
		return graph, err
	}

	bucketRows, err := h.Q.ListDashboardPrivilegedAccessBuckets(ctx, gen.ListDashboardPrivilegedAccessBucketsParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
		BucketLimit:           2,
	})
	if err != nil {
		return graph, err
	}

	bucketsBySource := make(map[string][]viewmodels.DashboardGraphBucket)
	for _, row := range bucketRows {
		label := strings.TrimSpace(row.BucketLabel)
		if label == "" {
			continue
		}
		key := dashboardSourceGraphKey(row.SourceKind, row.SourceName)
		bucketsBySource[key] = append(bucketsBySource[key], viewmodels.DashboardGraphBucket{
			Label:            label,
			Severity:         strings.TrimSpace(row.Severity),
			AffectedCount:    row.AffectedCount,
			EntitlementCount: row.EntitlementCount,
		})
	}

	const (
		radius   = 37.0
		minNodeX = 16
		maxNodeX = 84
		minNodeY = 22
		maxNodeY = 78
	)
	totalSources := len(sourceRows)
	if totalSources == 0 {
		return graph, nil
	}

	graph.Sources = make([]viewmodels.DashboardGraphSourceNode, 0, totalSources)
	for idx, row := range sourceRows {
		angle := -math.Pi / 2
		if totalSources > 1 {
			angle += (2 * math.Pi * float64(idx)) / float64(totalSources)
		}
		x := clampInt(int(math.Round(float64(graph.CenterX)+radius*math.Cos(angle))), minNodeX, maxNodeX)
		y := clampInt(int(math.Round(float64(graph.CenterY)+radius*math.Sin(angle))), minNodeY, maxNodeY)
		kind := strings.TrimSpace(row.SourceKind)
		sourceName := strings.TrimSpace(row.SourceName)

		graph.AccountCount += row.AccountCount
		graph.Sources = append(graph.Sources, viewmodels.DashboardGraphSourceNode{
			Kind:                  kind,
			SourceName:            sourceName,
			Label:                 sourcePrimaryLabel(kind),
			Href:                  dashboardSourceHref(kind),
			X:                     x,
			Y:                     y,
			IdentityCount:         row.IdentityCount,
			AccountCount:          row.AccountCount,
			ManagedAccountCount:   row.ManagedAccountCount,
			UnmanagedAccountCount: row.UnmanagedAccountCount,
			CoveragePercent:       dashboardPercent(row.ManagedAccountCount, row.AccountCount),
			Tone:                  dashboardGraphTone(kind),
			Buckets:               bucketsBySource[dashboardSourceGraphKey(kind, sourceName)],
		})
	}

	return graph, nil
}

func dashboardSourceGraphKey(sourceKind, sourceName string) string {
	return strings.ToLower(strings.TrimSpace(sourceKind)) + "\x00" + strings.ToLower(strings.TrimSpace(sourceName))
}

func dashboardPercent(numerator, denominator int64) int {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	percent := int((numerator * 100) / denominator)
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

func dashboardGraphTone(kind string) string {
	switch NormalizeConnectorKind(kind) {
	case "okta":
		return "okta"
	case "entra":
		return "entra"
	case "google_workspace":
		return "google"
	case "github":
		return "github"
	case "datadog":
		return "datadog"
	case "aws_identity_center":
		return "aws"
	case "vault":
		return "vault"
	default:
		return "default"
	}
}

func dashboardSourceHref(kind string) string {
	switch NormalizeConnectorKind(kind) {
	case "okta":
		return "/accounts/okta"
	case "entra":
		return "/accounts/entra"
	case "google_workspace":
		return "/accounts/google-workspace"
	case "github":
		return "/accounts/github"
	case "datadog":
		return "/accounts/datadog"
	case "aws_identity_center":
		return "/accounts/aws"
	default:
		return ""
	}
}

func clampInt(v, minValue, maxValue int) int {
	if v < minValue {
		return minValue
	}
	if v > maxValue {
		return maxValue
	}
	return v
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
