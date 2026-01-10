package handlers

import (
	"sort"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleDashboard renders the dashboard page.
func (h *Handlers) HandleDashboard(c echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Dashboard")
	if err != nil {
		return h.RenderError(c, err)
	}
	oktaCount, err := h.Q.CountIdPUsers(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	activeUserCount, err := h.Q.CountIdPUsersByQueryAndState(ctx, gen.CountIdPUsersByQueryAndStateParams{
		Query: "",
		State: "active",
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	appCount, err := h.Q.CountOktaApps(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	connectedAppCount, err := h.Q.CountConnectedOktaApps(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}

	var ghCount int64
	var ddCount int64
	var matched int64
	var unmatched int64

	if snap.GitHubEnabled && snap.GitHubConfigured {
		ghCount, err = h.Q.CountAppUsersBySource(ctx, gen.CountAppUsersBySourceParams{SourceKind: "github", SourceName: snap.GitHub.Org})
		if err != nil {
			return h.RenderError(c, err)
		}
		matched, err = h.Q.CountMatchedAppUsersBySource(ctx, gen.CountMatchedAppUsersBySourceParams{SourceKind: "github", SourceName: snap.GitHub.Org})
		if err != nil {
			return h.RenderError(c, err)
		}
		unmatched, err = h.Q.CountUnmatchedAppUsersBySource(ctx, gen.CountUnmatchedAppUsersBySourceParams{SourceKind: "github", SourceName: snap.GitHub.Org})
		if err != nil {
			return h.RenderError(c, err)
		}
	}

	if snap.DatadogEnabled && snap.DatadogConfigured {
		ddCount, err = h.Q.CountAppUsersBySource(ctx, gen.CountAppUsersBySourceParams{SourceKind: "datadog", SourceName: snap.Datadog.Site})
		if err != nil {
			return h.RenderError(c, err)
		}
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
			passPercent = int((counts.PassedRules * 100) / counts.TotalRules)
			if passPercent < 0 {
				passPercent = 0
			}
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
		ActiveUserCount:   activeUserCount,
		AppCount:          appCount,
		ConnectedAppCount: connectedAppCount,
		OktaCount:         oktaCount,
		GitHubCount:       ghCount,
		DatadogCount:      ddCount,
		MatchedCount:      matched,
		UnmatchedCount:    unmatched,
		FrameworkPosture:  frameworkPosture,
	}

	return h.RenderComponent(c, views.DashboardPage(data))
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
func (h *Handlers) HandleHealthz(c echo.Context) error {
	return c.String(200, "ok")
}
