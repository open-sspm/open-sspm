package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleIdpUsers renders the IdP users list page.
func (h *Handlers) HandleIdpUsers(c echo.Context) error {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Okta Users")
	if err != nil {
		return h.RenderError(c, err)
	}
	const perPage = 20

	query := strings.TrimSpace(c.QueryParam("q"))
	state := strings.ToLower(strings.TrimSpace(c.QueryParam("state")))
	if state == "" {
		state = strings.ToLower(strings.TrimSpace(c.QueryParam("status")))
	}
	switch state {
	case "active", "inactive":
	default:
		state = ""
	}
	page := 1
	if rawPage := strings.TrimSpace(c.QueryParam("page")); rawPage != "" {
		if parsed, err := strconv.Atoi(rawPage); err == nil && parsed > 0 {
			page = parsed
		}
	}

	totalCount, err := h.Q.CountIdPUsersByQueryAndState(ctx, gen.CountIdPUsersByQueryAndStateParams{
		Query: query,
		State: state,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	totalPages := int((totalCount + perPage - 1) / perPage)
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}

	offset := (page - 1) * perPage
	users, err := h.Q.ListIdPUsersPageByQueryAndState(ctx, gen.ListIdPUsersPageByQueryAndStateParams{
		Query:      query,
		State:      state,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	showingCount := len(users)
	showingFrom := 0
	showingTo := 0
	if totalCount > 0 && showingCount > 0 {
		showingFrom = offset + 1
		showingTo = offset + showingCount
		if int64(showingTo) > totalCount {
			showingTo = int(totalCount)
		}
	}

	emptyState := "No Okta users synced yet."
	if query != "" || state != "" {
		emptyState = "No Okta users match the current search."
	}

	data := viewmodels.IdPUsersViewData{
		Layout:        layout,
		Users:         users,
		Query:         query,
		State:         state,
		ShowingCount:  showingCount,
		ShowingFrom:   showingFrom,
		ShowingTo:     showingTo,
		TotalCount:    totalCount,
		Page:          page,
		PerPage:       perPage,
		TotalPages:    totalPages,
		HasUsers:      showingCount > 0,
		EmptyStateMsg: emptyState,
	}

	return h.RenderComponent(c, views.IdPUsersPage(data))
}

// HandleIdpUserShow renders the IdP user detail page.
func (h *Handlers) HandleIdpUserShow(c echo.Context) error {
	idStr := strings.Trim(c.Param("*"), "/")
	if idStr == "" {
		return RenderNotFound(c)
	}
	id, err := strconv.ParseInt(path.Clean("/" + idStr)[1:], 10, 64)
	if err != nil {
		return RenderNotFound(c)
	}
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Okta User")
	if err != nil {
		return h.RenderError(c, err)
	}
	user, err := h.Q.GetIdPUser(ctx, id)
	if err != nil {
		return RenderNotFound(c)
	}
	linked, err := h.Q.ListLinkedAppUsersForIdPUser(ctx, id)
	if err != nil {
		return h.RenderError(c, err)
	}
	var linkedApps []viewmodels.LinkedAppView
	for _, app := range linked {
		ents, err := h.Q.ListEntitlementsForAppUser(ctx, app.ID)
		if err != nil {
			return h.RenderError(c, err)
		}
		linkedApps = append(linkedApps, viewmodels.LinkedAppView{AppUser: app, Entitlements: ents})
	}

	assignments, err := h.Q.ListOktaUserAppAssignmentsForIdpUser(ctx, user.ID)
	if err != nil {
		return h.RenderError(c, err)
	}
	userGroups, err := h.Q.ListOktaGroupsForIdpUser(ctx, user.ID)
	if err != nil {
		return h.RenderError(c, err)
	}

	groupNames := make(map[int64]string)
	for _, group := range userGroups {
		name := group.Name
		if name == "" {
			name = group.ExternalID
		}
		groupNames[group.ID] = name
	}

	appIDSet := make(map[int64]struct{})
	for _, assignment := range assignments {
		appIDSet[assignment.OktaAppID] = struct{}{}
	}
	appIDs := make([]int64, 0, len(appIDSet))
	for appID := range appIDSet {
		appIDs = append(appIDs, appID)
	}

	appGroupAssignments := []gen.ListOktaAppGroupAssignmentsByAppIDsRow{}
	if len(appIDs) > 0 {
		appGroupAssignments, err = h.Q.ListOktaAppGroupAssignmentsByAppIDs(ctx, appIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
	}

	appGroupIDs := make(map[int64][]int64)
	for _, row := range appGroupAssignments {
		appGroupIDs[row.OktaAppID] = append(appGroupIDs[row.OktaAppID], row.OktaGroupID)
	}

	var oktaAssignments []viewmodels.OktaAssignmentView
	for _, assignment := range assignments {
		appLabel := assignment.AppLabel
		if appLabel == "" {
			appLabel = assignment.OktaAppExternalID
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
			for _, groupID := range appGroupIDs[assignment.OktaAppID] {
				if name, ok := groupNames[groupID]; ok && name != "" {
					groups = append(groups, name)
				}
			}
			sort.Strings(groups)
			if len(groups) == 0 {
				groups = []string{"(unknown)"}
			}
		}
		oktaAssignments = append(oktaAssignments, viewmodels.OktaAssignmentView{
			AppLabel: appLabel,
			AppName:  assignment.AppName,
			AppHref: func() string {
				href := IntegratedAppHref(assignment.IntegrationKind)
				if href == "" {
					if externalID := strings.TrimSpace(assignment.OktaAppExternalID); externalID != "" {
						href = "/apps/" + externalID
					}
				}
				return href
			}(),
			AssignedVia: assignedVia,
			Groups:      groups,
			Permissions: SummarizeProfilePermissions(assignment.ProfileJson),
		})
	}

	data := viewmodels.IdPUserShowViewData{
		Layout:          layout,
		User:            user,
		OktaAssignments: oktaAssignments,
		OktaAppCount:    len(oktaAssignments),
		LinkedApps:      linkedApps,
		LinkedAppsCount: len(linkedApps),
		HasLinkedApps:   len(linkedApps) > 0,
	}

	return h.RenderComponent(c, views.IdPUserShowPage(data))
}

// HandleGitHubUsers renders the GitHub users page.
func (h *Handlers) HandleGitHubUsers(c echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "GitHub Users")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := 1
	if rawPage := strings.TrimSpace(c.QueryParam("page")); rawPage != "" {
		if parsed, err := strconv.Atoi(rawPage); err == nil && parsed > 0 {
			page = parsed
		}
	}

	if !snap.GitHubConfigured || !snap.GitHubEnabled {
		message := "GitHub is not configured yet. Add credentials in Connectors."
		if snap.GitHubConfigured && !snap.GitHubEnabled {
			message = "GitHub sync is disabled. Enable it in Connectors."
		}
		totalPages := 1
		data := viewmodels.GitHubUsersViewData{
			Layout:         layout,
			Users:          nil,
			Query:          query,
			ShowingCount:   0,
			ShowingFrom:    0,
			ShowingTo:      0,
			TotalCount:     0,
			Page:           1,
			PerPage:        perPage,
			TotalPages:     totalPages,
			HasUsers:       false,
			EmptyStateMsg:  message,
			EmptyStateHref: "/settings/connectors?open=github",
		}
		return h.RenderComponent(c, views.GitHubUsersPage(data))
	}

	totalCount, err := h.Q.CountAppUsersWithLinkBySourceAndQuery(ctx, gen.CountAppUsersWithLinkBySourceAndQueryParams{
		SourceKind: "github",
		SourceName: snap.GitHub.Org,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	totalPages := int((totalCount + perPage - 1) / perPage)
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}

	offset := (page - 1) * perPage
	users, err := h.Q.ListAppUsersWithLinkPageBySourceAndQuery(ctx, gen.ListAppUsersWithLinkPageBySourceAndQueryParams{
		SourceKind: "github",
		SourceName: snap.GitHub.Org,
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	showingCount := len(users)
	showingFrom := 0
	showingTo := 0
	if totalCount > 0 && showingCount > 0 {
		showingFrom = offset + 1
		showingTo = offset + showingCount
		if int64(showingTo) > totalCount {
			showingTo = int(totalCount)
		}
	}

	emptyState := "No GitHub users synced yet."
	if query != "" {
		emptyState = "No GitHub users match the current search."
	}

	data := viewmodels.GitHubUsersViewData{
		Layout:         layout,
		Users:          users,
		Query:          query,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     totalCount,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		HasUsers:       showingCount > 0,
		EmptyStateMsg:  emptyState,
		EmptyStateHref: "/settings/connectors?open=github",
	}

	return h.RenderComponent(c, views.GitHubUsersPage(data))
}

// HandleDatadogUsers renders the Datadog users page.
func (h *Handlers) HandleDatadogUsers(c echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Datadog Users")
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
	page := 1
	if rawPage := strings.TrimSpace(c.QueryParam("page")); rawPage != "" {
		if parsed, err := strconv.Atoi(rawPage); err == nil && parsed > 0 {
			page = parsed
		}
	}

	if !snap.DatadogConfigured || !snap.DatadogEnabled {
		message := "Datadog is not configured yet. Add credentials in Connectors."
		if snap.DatadogConfigured && !snap.DatadogEnabled {
			message = "Datadog sync is disabled. Enable it in Connectors."
		}
		totalPages := 1
		data := viewmodels.DatadogUsersViewData{
			Layout:         layout,
			Users:          nil,
			Query:          query,
			State:          state,
			ShowingCount:   0,
			ShowingFrom:    0,
			ShowingTo:      0,
			TotalCount:     0,
			Page:           1,
			PerPage:        perPage,
			TotalPages:     totalPages,
			HasUsers:       false,
			EmptyStateMsg:  message,
			EmptyStateHref: "/settings/connectors?open=datadog",
		}
		return h.RenderComponent(c, views.DatadogUsersPage(data))
	}

	totalCount, err := h.Q.CountAppUsersBySourceAndQueryAndState(ctx, gen.CountAppUsersBySourceAndQueryAndStateParams{
		SourceKind: "datadog",
		SourceName: snap.Datadog.Site,
		Query:      query,
		State:      state,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	totalPages := int((totalCount + perPage - 1) / perPage)
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}

	offset := (page - 1) * perPage
	users, err := h.Q.ListAppUsersPageBySourceAndQueryAndState(ctx, gen.ListAppUsersPageBySourceAndQueryAndStateParams{
		SourceKind: "datadog",
		SourceName: snap.Datadog.Site,
		Query:      query,
		State:      state,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	appUserIDs := make([]int64, 0, len(users))
	for _, user := range users {
		appUserIDs = append(appUserIDs, user.ID)
	}

	rolesByUserID := make(map[int64][]string)
	if len(appUserIDs) > 0 {
		roles, err := h.Q.ListEntitlementResourcesByAppUserIDsAndKind(ctx, gen.ListEntitlementResourcesByAppUserIDsAndKindParams{
			AppUserIds: appUserIDs,
			EntKind:    "datadog_role",
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, row := range roles {
			name := strings.TrimSpace(row.Resource)
			if name == "" {
				continue
			}
			rolesByUserID[row.AppUserID] = append(rolesByUserID[row.AppUserID], name)
		}
	}

	items := make([]viewmodels.DatadogUserListItem, 0, len(users))
	for _, user := range users {
		userName := strings.TrimSpace(user.DisplayName)
		if userName == "" {
			userName = strings.TrimSpace(user.Email)
		}
		if userName == "" {
			userName = strings.TrimSpace(user.ExternalID)
		}
		status := datadogUserStatus(user.ID, user.ExternalID, user.RawJson)

		roles := rolesByUserID[user.ID]
		sort.Strings(roles)
		roles = DedupeStrings(roles)
		rolesDisplay := strings.Join(roles, ", ")

		items = append(items, viewmodels.DatadogUserListItem{
			UserName:     userName,
			Status:       status,
			RolesDisplay: rolesDisplay,
		})
	}

	showingCount := len(items)
	showingFrom := 0
	showingTo := 0
	if totalCount > 0 && showingCount > 0 {
		showingFrom = offset + 1
		showingTo = offset + showingCount
		if int64(showingTo) > totalCount {
			showingTo = int(totalCount)
		}
	}

	emptyState := "No Datadog users synced yet."
	if query != "" || state != "" {
		emptyState = "No Datadog users match the current search."
	}

	data := viewmodels.DatadogUsersViewData{
		Layout:         layout,
		Users:          items,
		Query:          query,
		State:          state,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     totalCount,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		HasUsers:       showingCount > 0,
		EmptyStateMsg:  emptyState,
		EmptyStateHref: "/settings/connectors?open=datadog",
	}

	return h.RenderComponent(c, views.DatadogUsersPage(data))
}

// HandleUnmatchedGitHub renders the unmatched GitHub accounts page.
func (h *Handlers) HandleUnmatchedGitHub(c echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Unmatched GitHub Accounts")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := 1
	if rawPage := strings.TrimSpace(c.QueryParam("page")); rawPage != "" {
		if parsed, err := strconv.Atoi(rawPage); err == nil && parsed > 0 {
			page = parsed
		}
	}

	if !snap.GitHubConfigured || !snap.GitHubEnabled {
		message := "GitHub is not configured yet. Add credentials in Connectors."
		if snap.GitHubConfigured && !snap.GitHubEnabled {
			message = "GitHub sync is disabled. Enable it in Connectors."
		}
		data := viewmodels.UnmatchedGitHubViewData{
			Layout:         layout,
			Users:          nil,
			Query:          query,
			ShowingCount:   0,
			ShowingFrom:    0,
			ShowingTo:      0,
			TotalCount:     0,
			Page:           1,
			PerPage:        perPage,
			TotalPages:     1,
			HasUsers:       false,
			EmptyStateMsg:  message,
			EmptyStateHref: "/settings/connectors?open=github",
		}
		return h.RenderComponent(c, views.UnmatchedGitHubPage(data))
	}

	org := strings.Trim(c.Param("*"), "/")
	if org == "" {
		return RenderNotFound(c)
	}
	if org != snap.GitHub.Org {
		return c.String(http.StatusNotFound, "unknown org")
	}

	totalCount, err := h.Q.CountUnmatchedAppUsersBySourceAndQuery(ctx, gen.CountUnmatchedAppUsersBySourceAndQueryParams{
		SourceKind: "github",
		SourceName: org,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	totalPages := int((totalCount + perPage - 1) / perPage)
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}

	offset := (page - 1) * perPage
	users, err := h.Q.ListUnmatchedAppUsersPageBySourceAndQuery(ctx, gen.ListUnmatchedAppUsersPageBySourceAndQueryParams{
		SourceKind: "github",
		SourceName: org,
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	showingCount := len(users)
	showingFrom := 0
	showingTo := 0
	if totalCount > 0 && showingCount > 0 {
		showingFrom = offset + 1
		showingTo = offset + showingCount
		if int64(showingTo) > totalCount {
			showingTo = int(totalCount)
		}
	}

	emptyState := "No unmatched GitHub accounts."
	if query != "" {
		emptyState = "No unmatched GitHub accounts match the current search."
	}

	data := viewmodels.UnmatchedGitHubViewData{
		Layout:         layout,
		Users:          users,
		Query:          query,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     totalCount,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		HasUsers:       showingCount > 0,
		EmptyStateMsg:  emptyState,
		EmptyStateHref: "/settings/connectors?open=github",
	}

	return h.RenderComponent(c, views.UnmatchedGitHubPage(data))
}

// HandleUnmatchedDatadog renders the unmatched Datadog accounts page.
func (h *Handlers) HandleUnmatchedDatadog(c echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Unmatched Datadog Accounts")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := 1
	if rawPage := strings.TrimSpace(c.QueryParam("page")); rawPage != "" {
		if parsed, err := strconv.Atoi(rawPage); err == nil && parsed > 0 {
			page = parsed
		}
	}

	if !snap.DatadogConfigured || !snap.DatadogEnabled {
		message := "Datadog is not configured yet. Add credentials in Connectors."
		if snap.DatadogConfigured && !snap.DatadogEnabled {
			message = "Datadog sync is disabled. Enable it in Connectors."
		}
		data := viewmodels.UnmatchedDatadogViewData{
			Layout:         layout,
			Users:          nil,
			Query:          query,
			ShowingCount:   0,
			ShowingFrom:    0,
			ShowingTo:      0,
			TotalCount:     0,
			Page:           1,
			PerPage:        perPage,
			TotalPages:     1,
			HasUsers:       false,
			EmptyStateMsg:  message,
			EmptyStateHref: "/settings/connectors?open=datadog",
		}
		return h.RenderComponent(c, views.UnmatchedDatadogPage(data))
	}

	site := strings.Trim(c.Param("*"), "/")
	if site == "" {
		return RenderNotFound(c)
	}
	if site != snap.Datadog.Site {
		return c.String(http.StatusNotFound, "unknown site")
	}

	totalCount, err := h.Q.CountUnmatchedAppUsersBySourceAndQuery(ctx, gen.CountUnmatchedAppUsersBySourceAndQueryParams{
		SourceKind: "datadog",
		SourceName: site,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	totalPages := int((totalCount + perPage - 1) / perPage)
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}

	offset := (page - 1) * perPage
	users, err := h.Q.ListUnmatchedAppUsersPageBySourceAndQuery(ctx, gen.ListUnmatchedAppUsersPageBySourceAndQueryParams{
		SourceKind: "datadog",
		SourceName: site,
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	showingCount := len(users)
	showingFrom := 0
	showingTo := 0
	if totalCount > 0 && showingCount > 0 {
		showingFrom = offset + 1
		showingTo = offset + showingCount
		if int64(showingTo) > totalCount {
			showingTo = int(totalCount)
		}
	}

	emptyState := "No unmatched Datadog accounts."
	if query != "" {
		emptyState = "No unmatched Datadog accounts match the current search."
	}

	data := viewmodels.UnmatchedDatadogViewData{
		Layout:         layout,
		Users:          users,
		Query:          query,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     totalCount,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		HasUsers:       showingCount > 0,
		EmptyStateMsg:  emptyState,
		EmptyStateHref: "/settings/connectors?open=datadog",
	}

	return h.RenderComponent(c, views.UnmatchedDatadogPage(data))
}

// HandleCreateLink creates an identity link between IdP and app users.
func (h *Handlers) HandleCreateLink(c echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	idpID, err := strconv.ParseInt(c.FormValue("idp_user_id"), 10, 64)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid idp_user_id")
	}
	appID, err := strconv.ParseInt(c.FormValue("app_user_id"), 10, 64)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid app_user_id")
	}
	reason := c.FormValue("reason")
	if reason == "" {
		reason = "manual"
	}
	_, err = h.Q.CreateIdentityLink(c.Request().Context(), gen.CreateIdentityLinkParams{
		IdpUserID:  idpID,
		AppUserID:  appID,
		LinkReason: reason,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	redirect := c.Request().Header.Get("Referer")
	if redirect == "" {
		snap, err := h.LoadConnectorSnapshot(c.Request().Context())
		if err == nil && snap.GitHub.Org != "" {
			redirect = fmt.Sprintf("/unmatched/github/%s", snap.GitHub.Org)
		} else {
			redirect = "/settings/connectors?open=github"
		}
	}
	return c.Redirect(http.StatusSeeOther, redirect)
}

// HandleIdpUserAccessTree handles the access tree API endpoint.
func (h *Handlers) HandleIdpUserAccessTree(c echo.Context) error {
	id, err := strconv.ParseInt(strings.TrimSpace(c.Param("id")), 10, 64)
	if err != nil || id <= 0 {
		return jsonTreeError(c, http.StatusBadRequest, "invalid idp user id")
	}

	ctx := c.Request().Context()
	_, err = h.Q.GetIdPUser(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return jsonTreeError(c, http.StatusNotFound, "idp user not found")
		}
		return jsonTreeError(c, http.StatusInternalServerError, "internal error")
	}

	nodeID := strings.TrimSpace(c.QueryParam("node"))
	if nodeID == "" {
		nodeID = "root"
	}

	response := accessTreeResponse{Parent: nodeID}
	switch {
	case nodeID == "root":
		response.Nodes = []accessTreeNode{{
			ID:          "apps",
			Label:       "Apps",
			HasChildren: true,
		}}
		return c.JSON(http.StatusOK, response)
	case nodeID == "apps":
		assignments, err := h.Q.ListOktaUserAppAssignmentsForIdpUser(ctx, id)
		if err != nil {
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}
		nodes := make([]accessTreeNode, 0, len(assignments))
		for _, assignment := range assignments {
			externalID := strings.TrimSpace(assignment.OktaAppExternalID)
			if externalID == "" {
				continue
			}
			label := strings.TrimSpace(assignment.AppLabel)
			if label == "" {
				label = externalID
			}
			subLabel := strings.TrimSpace(assignment.AppName)

			var badges []string
			if status := strings.TrimSpace(assignment.AppStatus); status != "" {
				badges = append(badges, status)
			}
			if mode := strings.TrimSpace(assignment.AppSignOnMode); mode != "" {
				badges = append(badges, mode)
			}

			scope := strings.ToUpper(strings.TrimSpace(assignment.Scope))
			switch scope {
			case "USER":
				badges = append(badges, "Direct")
			case "GROUP":
				badges = append(badges, "Group")
			case "":
			default:
				badges = append(badges, "Unknown")
			}

			href := IntegratedAppHref(assignment.IntegrationKind)
			if href == "" {
				href = "/apps/" + externalID
			}

			nodes = append(nodes, accessTreeNode{
				ID:          "app:" + externalID,
				Label:       label,
				SubLabel:    subLabel,
				Badges:      badges,
				HasChildren: true,
				Href:        href,
			})
		}
		if len(nodes) == 0 {
			nodes = []accessTreeNode{{
				ID:          "apps-empty",
				Label:       "No assigned apps found.",
				HasChildren: false,
			}}
		}
		response.Nodes = nodes
		return c.JSON(http.StatusOK, response)
	case strings.HasPrefix(nodeID, "app:"):
		externalID := strings.TrimPrefix(nodeID, "app:")
		externalID = strings.TrimSpace(externalID)
		if externalID == "" {
			return jsonTreeError(c, http.StatusBadRequest, "invalid app node")
		}
		_, err := h.Q.GetOktaUserAppAssignmentForIdpUserByOktaAppExternalID(ctx, gen.GetOktaUserAppAssignmentForIdpUserByOktaAppExternalIDParams{
			IdpUserID:         id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return jsonTreeError(c, http.StatusNotFound, "app assignment not found")
			}
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}
		response.Nodes = []accessTreeNode{
			{ID: "appattrs:" + externalID, Label: "Attributes", HasChildren: true},
			{ID: "appgroups:" + externalID, Label: "Groups", HasChildren: true},
			{ID: "appperms:" + externalID, Label: "Permissions", HasChildren: true},
		}
		return c.JSON(http.StatusOK, response)
	case strings.HasPrefix(nodeID, "appattrs:"):
		externalID := strings.TrimSpace(strings.TrimPrefix(nodeID, "appattrs:"))
		if externalID == "" {
			return jsonTreeError(c, http.StatusBadRequest, "invalid app attributes node")
		}
		assignment, err := h.Q.GetOktaUserAppAssignmentForIdpUserByOktaAppExternalID(ctx, gen.GetOktaUserAppAssignmentForIdpUserByOktaAppExternalIDParams{
			IdpUserID:         id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return jsonTreeError(c, http.StatusNotFound, "app assignment not found")
			}
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}

		assignedVia := "Unknown"
		scope := strings.ToUpper(strings.TrimSpace(assignment.Scope))
		switch scope {
		case "USER":
			assignedVia = "Direct"
		case "GROUP":
			assignedVia = "Group"
		}

		nodes := []accessTreeNode{
			{ID: "attr:status:" + externalID, Label: "Status", SubLabel: strings.TrimSpace(assignment.AppStatus), HasChildren: false},
			{ID: "attr:signon:" + externalID, Label: "Sign-on", SubLabel: strings.TrimSpace(assignment.AppSignOnMode), HasChildren: false},
			{ID: "attr:assignedvia:" + externalID, Label: "Assigned via", SubLabel: assignedVia, HasChildren: false},
			{ID: "attr:externalid:" + externalID, Label: "Okta app external id", SubLabel: externalID, HasChildren: false},
		}

		if kind := strings.TrimSpace(assignment.IntegrationKind); kind != "" {
			label := strings.TrimSpace(ConnectorDisplayName(kind))
			if label == "" {
				label = kind
			}
			nodes = append(nodes, accessTreeNode{
				ID:          "attr:integration:" + externalID,
				Label:       "Integration",
				SubLabel:    label,
				HasChildren: false,
				Href:        IntegratedAppHref(kind),
			})
		}

		response.Nodes = nodes
		return c.JSON(http.StatusOK, response)
	case strings.HasPrefix(nodeID, "appgroups:"):
		externalID := strings.TrimSpace(strings.TrimPrefix(nodeID, "appgroups:"))
		if externalID == "" {
			return jsonTreeError(c, http.StatusBadRequest, "invalid app groups node")
		}
		_, err := h.Q.GetOktaUserAppAssignmentForIdpUserByOktaAppExternalID(ctx, gen.GetOktaUserAppAssignmentForIdpUserByOktaAppExternalIDParams{
			IdpUserID:         id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return jsonTreeError(c, http.StatusNotFound, "app assignment not found")
			}
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}

		groups, err := h.Q.ListOktaAppGrantingGroupsForIdpUserByOktaAppExternalID(ctx, gen.ListOktaAppGrantingGroupsForIdpUserByOktaAppExternalIDParams{
			IdpUserID:         id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}
		if len(groups) == 0 {
			response.Nodes = []accessTreeNode{{ID: "groups-empty:" + externalID, Label: "No granting groups found.", HasChildren: false}}
			return c.JSON(http.StatusOK, response)
		}

		nodes := make([]accessTreeNode, 0, len(groups))
		for _, group := range groups {
			name := strings.TrimSpace(group.OktaGroupName)
			ext := strings.TrimSpace(group.OktaGroupExternalID)
			if name == "" {
				name = ext
			}
			nodes = append(nodes, accessTreeNode{
				ID:          "group:" + ext,
				Label:       name,
				SubLabel:    ext,
				HasChildren: false,
			})
		}
		response.Nodes = nodes
		return c.JSON(http.StatusOK, response)
	case strings.HasPrefix(nodeID, "appperms:"):
		externalID := strings.TrimSpace(strings.TrimPrefix(nodeID, "appperms:"))
		if externalID == "" {
			return jsonTreeError(c, http.StatusBadRequest, "invalid app permissions node")
		}
		assignment, err := h.Q.GetOktaUserAppAssignmentForIdpUserByOktaAppExternalID(ctx, gen.GetOktaUserAppAssignmentForIdpUserByOktaAppExternalIDParams{
			IdpUserID:         id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return jsonTreeError(c, http.StatusNotFound, "app assignment not found")
			}
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}

		perms := SummarizeProfilePermissions(assignment.ProfileJson)
		if len(perms) == 0 {
			response.Nodes = []accessTreeNode{{ID: "perms-empty:" + externalID, Label: "No permissions found.", HasChildren: false}}
			return c.JSON(http.StatusOK, response)
		}

		nodes := make([]accessTreeNode, 0, len(perms))
		for i, perm := range perms {
			text := strings.TrimSpace(perm.Text)
			if text == "" {
				continue
			}
			nodes = append(nodes, accessTreeNode{
				ID:          "perm:" + externalID + ":" + strconv.Itoa(i),
				Label:       text,
				HasChildren: false,
			})
		}
		if len(nodes) == 0 {
			nodes = []accessTreeNode{{ID: "perms-empty:" + externalID, Label: "No permissions found.", HasChildren: false}}
		}
		response.Nodes = nodes
		return c.JSON(http.StatusOK, response)
	default:
		return jsonTreeError(c, http.StatusBadRequest, "unknown node")
	}
}

// Access tree helper types and functions.
type accessTreeNode struct {
	ID          string   `json:"id"`
	Label       string   `json:"label"`
	SubLabel    string   `json:"subLabel,omitempty"`
	Badges      []string `json:"badges,omitempty"`
	HasChildren bool     `json:"hasChildren"`
	Href        string   `json:"href,omitempty"`
}

type accessTreeResponse struct {
	Parent string           `json:"parent"`
	Nodes  []accessTreeNode `json:"nodes"`
}

type accessTreeError struct {
	Error string `json:"error"`
}

func jsonTreeError(c echo.Context, status int, message string) error {
	return c.JSON(status, accessTreeError{Error: message})
}

func datadogUserStatus(appUserID int64, externalID string, rawJSON []byte) string {
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rawJSON, &payload); err != nil {
		log.Printf("datadog: app_user_id=%d external_id=%s: unable to parse user raw_json: %v", appUserID, strings.TrimSpace(externalID), err)
		return ""
	}
	return strings.TrimSpace(payload.Status)
}
