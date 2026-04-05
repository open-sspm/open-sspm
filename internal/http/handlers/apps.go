package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleApps renders the apps list page.
func (h *Handlers) HandleApps(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Assigned Apps")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	queryState := querystate.ParseBasicListQuery("/assigned-apps", c.Request().URL.Query(), querystate.BasicListOptions{})
	page := queryState.Page

	var totalCount int64
	if queryState.Q == "" {
		totalCount, err = h.Q.CountOktaApps(ctx)
		if err != nil {
			return h.RenderError(c, err)
		}
	} else {
		totalCount, err = h.Q.CountOktaAppsByQuery(ctx, queryState.Q)
		if err != nil {
			return h.RenderError(c, err)
		}
	}

	pagination := newPaginatedListState(totalCount, page, perPage)
	makeItem := func(externalID, label, name, status, signOnMode, integrationKind string) viewmodels.AppListItem {
		label = strings.TrimSpace(label)
		if label == "" {
			label = strings.TrimSpace(externalID)
		}
		status = strings.TrimSpace(status)
		if status == "" {
			status = "—"
		}
		signOnMode = strings.TrimSpace(signOnMode)
		if signOnMode == "" {
			signOnMode = "—"
		}
		integratedHref := IntegratedAppHref(integrationKind)
		suggestedKind := ""
		if integratedHref == "" {
			labelLower := strings.ToLower(label)
			nameLower := strings.ToLower(strings.TrimSpace(name))
			switch {
			case strings.Contains(labelLower, "github"), strings.Contains(nameLower, "github"):
				suggestedKind = configstore.KindGitHub
			case strings.Contains(labelLower, "datadog"), strings.Contains(nameLower, "datadog"):
				suggestedKind = configstore.KindDatadog
			}
		}
		return viewmodels.AppListItem{
			ExternalID:     strings.TrimSpace(externalID),
			Label:          label,
			Name:           strings.TrimSpace(name),
			Status:         status,
			SignOnMode:     signOnMode,
			IntegratedHref: integratedHref,
			SuggestedKind:  suggestedKind,
		}
	}

	items := make([]viewmodels.AppListItem, 0, perPage)
	if queryState.Q == "" {
		apps, err := h.Q.ListOktaAppsPage(ctx, gen.ListOktaAppsPageParams{
			PageLimit:  int32(perPage),
			PageOffset: int32(pagination.Offset()),
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		items = make([]viewmodels.AppListItem, 0, len(apps))
		for _, app := range apps {
			items = append(items, makeItem(app.ExternalID, app.Label, app.Name, app.Status, app.SignOnMode, app.IntegrationKind))
		}
	} else {
		apps, err := h.Q.ListOktaAppsPageByQuery(ctx, gen.ListOktaAppsPageByQueryParams{
			Query:      queryState.Q,
			PageLimit:  int32(perPage),
			PageOffset: int32(pagination.Offset()),
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		items = make([]viewmodels.AppListItem, 0, len(apps))
		for _, app := range apps {
			items = append(items, makeItem(app.ExternalID, app.Label, app.Name, app.Status, app.SignOnMode, app.IntegrationKind))
		}
	}

	data := viewmodels.AppsViewData{
		PaginatedListPageData: pagination.PageData(layout, len(items), func() string {
			if queryState.HasFilters() {
				return "No assigned apps match the current search."
			}
			return "No assigned apps have been synced yet. Run a sync to discover assignments."
		}(), ""),
		Apps:    items,
		Query:   queryState,
		HasApps: len(items) > 0,
	}

	if isHX(c) && isHXTarget(c, "apps-results") {
		return h.RenderComponent(c, views.AppsPageResults(data))
	}
	return h.RenderComponent(c, views.AppsPage(data))
}

// HandleOktaAppShow renders the Okta app detail page.
func (h *Handlers) HandleOktaAppShow(c *echo.Context) error {
	oktaAppExternalID := strings.TrimSpace(c.Param("externalID"))
	if oktaAppExternalID == "" {
		oktaAppExternalID = strings.Trim(c.Param("*"), "/")
	}
	if oktaAppExternalID == "" {
		return RenderNotFound(c)
	}
	oktaAppExternalID = strings.TrimPrefix(path.Clean("/"+oktaAppExternalID), "/")
	if oktaAppExternalID == "" || oktaAppExternalID == "." || strings.Contains(oktaAppExternalID, "/") {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	app, err := h.Q.GetOktaAppByExternalIDWithIntegration(ctx, oktaAppExternalID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	integratedHref := IntegratedAppHref(app.IntegrationKind)
	if integratedHref != "" {
		return c.Redirect(http.StatusSeeOther, integratedHref)
	}

	layout, _, err := h.LayoutData(ctx, c, "Assigned App")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	queryState := querystate.ParseBasicListQuery("/assigned-apps/"+strings.TrimSpace(app.ExternalID), c.Request().URL.Query(), querystate.BasicListOptions{
		NormalizeState: normalizeActiveInactiveState,
	})
	page := queryState.Page

	totalCount, err := h.Q.CountOktaAppAssignedAccountsByQuery(ctx, gen.CountOktaAppAssignedAccountsByQueryParams{
		OktaAppID: app.ID,
		State:     queryState.State,
		Query:     queryState.Q,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination := newPaginatedListState(totalCount, page, perPage)
	assignments, err := h.Q.ListOktaAppAssignedAccountsPageByQuery(ctx, gen.ListOktaAppAssignedAccountsPageByQueryParams{
		OktaAppID:  app.ID,
		State:      queryState.State,
		Query:      queryState.Q,
		PageOffset: int32(pagination.Offset()),
		PageLimit:  int32(perPage),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	oktaAccountIDs := make([]int64, 0, len(assignments))
	for _, row := range assignments {
		oktaAccountIDs = append(oktaAccountIDs, row.OktaAccountID)
	}

	grantingGroups := make(map[int64][]string)
	if len(oktaAccountIDs) > 0 {
		rows, err := h.Q.ListOktaAppGrantingGroupsForOktaAccounts(ctx, gen.ListOktaAppGrantingGroupsForOktaAccountsParams{
			OktaAppID:      app.ID,
			OktaAccountIds: oktaAccountIDs,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, row := range rows {
			name := strings.TrimSpace(row.OktaGroupName)
			if name == "" {
				name = strings.TrimSpace(row.OktaGroupExternalID)
			}
			if name == "" {
				name = "(unknown)"
			}
			grantingGroups[row.OktaAccountID] = append(grantingGroups[row.OktaAccountID], name)
		}
	}

	label := strings.TrimSpace(app.Label)
	if label == "" {
		label = strings.TrimSpace(app.ExternalID)
	}
	status := strings.TrimSpace(app.Status)
	if status == "" {
		status = "—"
	}
	signOnMode := strings.TrimSpace(app.SignOnMode)
	if signOnMode == "" {
		signOnMode = "—"
	}

	items := make([]viewmodels.OktaAppAssignedAccountView, 0, len(assignments))
	for _, assignment := range assignments {
		accountName := strings.TrimSpace(assignment.OktaAccountDisplayName)
		if accountName == "" {
			accountName = strings.TrimSpace(assignment.OktaAccountEmail)
		}
		if accountName == "" {
			accountName = strings.TrimSpace(assignment.OktaAccountExternalID)
		}
		if accountName == "" {
			accountName = "—"
		}
		accountStatus := strings.TrimSpace(assignment.OktaAccountStatus)
		if accountStatus == "" {
			accountStatus = "—"
		}

		assignedVia := "Unknown"
		scope := strings.ToUpper(strings.TrimSpace(assignment.Scope))
		if scope == "USER" {
			assignedVia = "Direct"
		} else if scope == "GROUP" {
			assignedVia = "Group"
		}

		var groups []string
		if scope == "GROUP" {
			groups = append(groups, grantingGroups[assignment.OktaAccountID]...)
			sort.Strings(groups)
			if len(groups) == 0 {
				groups = []string{"(unknown)"}
			}
		}

		items = append(items, viewmodels.OktaAppAssignedAccountView{
			OktaAccountID:         assignment.OktaAccountID,
			AccountHref:           fmt.Sprintf("/accounts/okta/%d", assignment.OktaAccountID),
			AccountDisplayName:    accountName,
			AccountEmail:          strings.TrimSpace(assignment.OktaAccountEmail),
			OktaAccountExternalID: strings.TrimSpace(assignment.OktaAccountExternalID),
			OktaAccountStatus:     accountStatus,
			AssignedVia:           assignedVia,
			Groups:                groups,
			Permissions:           SummarizeProfilePermissions(assignment.ProfileJson),
		})
	}

	data := viewmodels.OktaAppShowViewData{
		PaginatedListPageData: pagination.PageData(layout, len(items), func() string {
			if queryState.HasFilters() {
				return "No assigned users match the current search."
			}
			return "No Okta users are assigned to this app."
		}(), ""),
		App: viewmodels.OktaAppSummaryView{
			ExternalID: strings.TrimSpace(app.ExternalID),
			Label:      label,
			Name:       strings.TrimSpace(app.Name),
			Status:     status,
			SignOnMode: signOnMode,
		},
		Accounts:    items,
		Query:       queryState,
		HasAccounts: len(items) > 0,
	}

	return h.RenderComponent(c, views.OktaAppShowPage(data))
}

// HandleAppsMap handles mapping an Okta app to an integration.
func (h *Handlers) HandleAppsMap(c *echo.Context) error {
	ctx := c.Request().Context()
	kind := NormalizeConnectorKind(c.FormValue("integration_kind"))
	oktaAppExternalID := strings.TrimSpace(c.FormValue("okta_app_external_id"))

	switch kind {
	case configstore.KindGitHub, configstore.KindDatadog:
	default:
		return RenderNotFound(c)
	}

	if oktaAppExternalID == "" {
		if err := h.Q.DeleteIntegrationOktaAppMap(ctx, kind); err != nil {
			return h.RenderError(c, err)
		}
		return c.Redirect(http.StatusSeeOther, "/assigned-apps")
	}

	if err := h.Q.UpsertIntegrationOktaAppMap(ctx, gen.UpsertIntegrationOktaAppMapParams{
		IntegrationKind:   kind,
		OktaAppExternalID: oktaAppExternalID,
	}); err != nil {
		return h.RenderError(c, err)
	}

	return c.Redirect(http.StatusSeeOther, "/assigned-apps")
}
