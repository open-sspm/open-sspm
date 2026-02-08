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
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleApps renders the apps list page.
func (h *Handlers) HandleApps(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Apps")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)

	var totalCount int64
	if query == "" {
		totalCount, err = h.Q.CountOktaApps(ctx)
		if err != nil {
			return h.RenderError(c, err)
		}
	} else {
		totalCount, err = h.Q.CountOktaAppsByQuery(ctx, query)
		if err != nil {
			return h.RenderError(c, err)
		}
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
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
	if query == "" {
		apps, err := h.Q.ListOktaAppsPage(ctx, gen.ListOktaAppsPageParams{
			PageLimit:  int32(perPage),
			PageOffset: int32(offset),
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
			Query:      query,
			PageLimit:  int32(perPage),
			PageOffset: int32(offset),
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		items = make([]viewmodels.AppListItem, 0, len(apps))
		for _, app := range apps {
			items = append(items, makeItem(app.ExternalID, app.Label, app.Name, app.Status, app.SignOnMode, app.IntegrationKind))
		}
	}

	showingCount := len(items)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	data := viewmodels.AppsViewData{
		Layout:       layout,
		Apps:         items,
		Query:        query,
		ShowingCount: showingCount,
		ShowingFrom:  showingFrom,
		ShowingTo:    showingTo,
		TotalCount:   totalCount,
		Page:         page,
		PerPage:      perPage,
		TotalPages:   totalPages,
		HasApps:      showingCount > 0,
		EmptyStateMsg: func() string {
			if query != "" {
				return "No Okta apps match the current search."
			}
			return "No Okta apps have been synced yet. Run a sync to discover assignments."
		}(),
	}

	if isHX(c) && isHXTarget(c, "apps-results") {
		return h.RenderComponent(c, views.AppsPageResults(data))
	}
	return h.RenderComponent(c, views.AppsPage(data))
}

// HandleOktaAppShow renders the Okta app detail page.
func (h *Handlers) HandleOktaAppShow(c *echo.Context) error {
	oktaAppExternalID := strings.Trim(c.Param("*"), "/")
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

	layout, _, err := h.LayoutData(ctx, c, "Okta App")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	state := strings.ToLower(strings.TrimSpace(c.QueryParam("state")))
	switch state {
	case "active", "inactive":
	default:
		state = ""
	}
	page := parsePageParam(c)

	totalCount, err := h.Q.CountOktaAppUserAssignmentsByQuery(ctx, gen.CountOktaAppUserAssignmentsByQueryParams{
		OktaAppID: app.ID,
		State:     state,
		Query:     query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	assignments, err := h.Q.ListOktaAppUserAssignmentsPageByQuery(ctx, gen.ListOktaAppUserAssignmentsPageByQueryParams{
		OktaAppID:  app.ID,
		State:      state,
		Query:      query,
		PageOffset: int32(offset),
		PageLimit:  int32(perPage),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	idpUserIDs := make([]int64, 0, len(assignments))
	for _, row := range assignments {
		idpUserIDs = append(idpUserIDs, row.IdpUserID)
	}

	grantingGroups := make(map[int64][]string)
	if len(idpUserIDs) > 0 {
		rows, err := h.Q.ListOktaAppGrantingGroupsForIdpUsers(ctx, gen.ListOktaAppGrantingGroupsForIdpUsersParams{
			OktaAppID:  app.ID,
			IdpUserIds: idpUserIDs,
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
			grantingGroups[row.IdpUserID] = append(grantingGroups[row.IdpUserID], name)
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

	items := make([]viewmodels.OktaAppAssignedUserView, 0, len(assignments))
	for _, assignment := range assignments {
		userName := strings.TrimSpace(assignment.IdpUserDisplayName)
		if userName == "" {
			userName = strings.TrimSpace(assignment.IdpUserEmail)
		}
		if userName == "" {
			userName = strings.TrimSpace(assignment.IdpUserExternalID)
		}
		if userName == "" {
			userName = "—"
		}
		userStatus := strings.TrimSpace(assignment.IdpUserStatus)
		if userStatus == "" {
			userStatus = "—"
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
			groups = append(groups, grantingGroups[assignment.IdpUserID]...)
			sort.Strings(groups)
			if len(groups) == 0 {
				groups = []string{"(unknown)"}
			}
		}

		items = append(items, viewmodels.OktaAppAssignedUserView{
			IdpUserID:       assignment.IdpUserID,
			UserHref:        fmt.Sprintf("/idp-users/%d", assignment.IdpUserID),
			UserDisplayName: userName,
			UserEmail:       strings.TrimSpace(assignment.IdpUserEmail),
			UserExternalID:  strings.TrimSpace(assignment.IdpUserExternalID),
			UserStatus:      userStatus,
			AssignedVia:     assignedVia,
			Groups:          groups,
			Permissions:     SummarizeProfilePermissions(assignment.ProfileJson),
		})
	}

	showingCount := len(items)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	data := viewmodels.OktaAppShowViewData{
		Layout: layout,
		App: viewmodels.OktaAppSummaryView{
			ExternalID: strings.TrimSpace(app.ExternalID),
			Label:      label,
			Name:       strings.TrimSpace(app.Name),
			Status:     status,
			SignOnMode: signOnMode,
		},
		Users:        items,
		Query:        query,
		State:        state,
		ShowingCount: showingCount,
		ShowingFrom:  showingFrom,
		ShowingTo:    showingTo,
		TotalCount:   totalCount,
		Page:         page,
		PerPage:      perPage,
		TotalPages:   totalPages,
		HasUsers:     showingCount > 0,
		EmptyStateMsg: func() string {
			if query != "" || state != "" {
				return "No assigned users match the current search."
			}
			return "No Okta users are assigned to this app."
		}(),
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
		return c.Redirect(http.StatusSeeOther, "/apps")
	}

	if err := h.Q.UpsertIntegrationOktaAppMap(ctx, gen.UpsertIntegrationOktaAppMapParams{
		IntegrationKind:   kind,
		OktaAppExternalID: oktaAppExternalID,
	}); err != nil {
		return h.RenderError(c, err)
	}

	return c.Redirect(http.StatusSeeOther, "/apps")
}
