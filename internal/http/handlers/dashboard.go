package handlers

import (
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleDashboard renders the dashboard page.
func (h *Handlers) HandleDashboard(c echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Dashboard", "")
	if err != nil {
		return h.RenderError(c, err)
	}
	oktaCount, err := h.Q.CountIdPUsers(ctx)
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

	commandUsersRaw, err := h.Q.ListIdPUsersForCommand(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	commandUsers := make([]viewmodels.DashboardCommandUserItem, 0, len(commandUsersRaw))
	for _, u := range commandUsersRaw {
		status := strings.TrimSpace(u.Status)
		if status == "" {
			status = "—"
		}
		commandUsers = append(commandUsers, viewmodels.DashboardCommandUserItem{
			ID:          u.ID,
			Email:       strings.TrimSpace(u.Email),
			DisplayName: strings.TrimSpace(u.DisplayName),
			Status:      status,
		})
	}

	commandAppsRaw, err := h.Q.ListOktaAppsForCommand(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	commandApps := make([]viewmodels.DashboardCommandAppItem, 0, len(commandAppsRaw))
	for _, app := range commandAppsRaw {
		label := strings.TrimSpace(app.Label)
		if label == "" {
			label = strings.TrimSpace(app.ExternalID)
		}
		status := strings.TrimSpace(app.Status)
		if status == "" {
			status = "—"
		}
		commandApps = append(commandApps, viewmodels.DashboardCommandAppItem{
			ExternalID: strings.TrimSpace(app.ExternalID),
			Label:      label,
			Name:       strings.TrimSpace(app.Name),
			Status:     status,
		})
	}

	data := viewmodels.DashboardViewData{
		Layout:         layout,
		OktaCount:      oktaCount,
		GitHubCount:    ghCount,
		DatadogCount:   ddCount,
		MatchedCount:   matched,
		UnmatchedCount: unmatched,
		CommandUsers:   commandUsers,
		CommandApps:    commandApps,
	}

	return h.RenderComponent(c, views.DashboardPage(data))
}

// HandleHealthz returns a simple health check response.
func (h *Handlers) HandleHealthz(c echo.Context) error {
	return c.String(200, "ok")
}
