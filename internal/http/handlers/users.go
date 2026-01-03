package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/accessgraph"
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
	linkedIDs := make([]int64, 0, len(linked))
	for _, app := range linked {
		linkedIDs = append(linkedIDs, app.ID)
	}
	entitlementsByAppUserID := make(map[int64][]gen.Entitlement, len(linked))
	if len(linkedIDs) > 0 {
		ents, err := h.Q.ListEntitlementsForAppUserIDs(ctx, linkedIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, ent := range ents {
			entitlementsByAppUserID[ent.AppUserID] = append(entitlementsByAppUserID[ent.AppUserID], ent)
		}
	}
	var linkedApps []viewmodels.LinkedAppView
	for _, app := range linked {
		entitlementViews := make([]viewmodels.LinkedEntitlementView, 0, len(entitlementsByAppUserID[app.ID]))
		for _, ent := range entitlementsByAppUserID[app.ID] {
			resourceKind, resourceID, ok := accessgraph.ParseCanonicalResourceRef(ent.Resource)
			if !ok {
				resourceID = strings.TrimSpace(ent.Resource)
			}
			resourceHref := ""
			if ok {
				resourceHref = accessgraph.BuildResourceHref(app.SourceKind, app.SourceName, resourceKind, resourceID)
			}
			resourceLabel := accessgraph.DisplayResourceLabel(ent.Resource, ent.RawJson)
			if strings.TrimSpace(resourceLabel) == "" {
				if ok {
					resourceLabel = resourceID
				} else {
					resourceLabel = strings.TrimSpace(ent.Resource)
				}
			}
			entitlementViews = append(entitlementViews, viewmodels.LinkedEntitlementView{
				Kind:          strings.TrimSpace(ent.Kind),
				ResourceKind:  resourceKind,
				ResourceID:    resourceID,
				ResourceLabel: resourceLabel,
				ResourceHref:  resourceHref,
				Permission:    strings.TrimSpace(ent.Permission),
			})
		}
		linkedApps = append(linkedApps, viewmodels.LinkedAppView{AppUser: app, Entitlements: entitlementViews})
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
		ents, err := h.Q.ListEntitlementsForAppUserIDs(ctx, appUserIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, ent := range ents {
			if strings.TrimSpace(ent.Kind) != "datadog_role" {
				continue
			}
			label := strings.TrimSpace(accessgraph.DisplayResourceLabel(ent.Resource, ent.RawJson))
			if label == "" {
				continue
			}
			rolesByUserID[ent.AppUserID] = append(rolesByUserID[ent.AppUserID], label)
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

	encodeNodePart := func(v string) string {
		return url.PathEscape(v)
	}
	decodeNodePart := func(v string) (string, error) {
		return url.PathUnescape(v)
	}

	response := accessTreeResponse{Parent: nodeID}
	switch {
	case nodeID == "root":
		response.Nodes = []accessTreeNode{
			{ID: "connector:okta", Label: "Okta", HasChildren: true},
			{ID: "connector:github", Label: "GitHub", HasChildren: true},
			{ID: "connector:datadog", Label: "Datadog", HasChildren: true},
			{ID: "connector:aws", Label: "AWS Identity Center", HasChildren: true},
		}
		return c.JSON(http.StatusOK, response)
	case nodeID == "connector:okta":
		response.Nodes = []accessTreeNode{{ID: "okta_apps", Label: "Apps", HasChildren: true}}
		return c.JSON(http.StatusOK, response)
	case nodeID == "okta_apps":
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
	case nodeID == "connector:github" || nodeID == "connector:datadog" || nodeID == "connector:aws":
		sourceKind := strings.TrimSpace(strings.TrimPrefix(nodeID, "connector:"))
		if sourceKind == "" {
			return jsonTreeError(c, http.StatusBadRequest, "invalid connector node")
		}

		linked, err := h.Q.ListLinkedAppUsersForIdPUser(ctx, id)
		if err != nil {
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}

		sourceCounts := make(map[string]int)
		for _, app := range linked {
			if strings.EqualFold(strings.TrimSpace(app.SourceKind), sourceKind) {
				sourceCounts[app.SourceName]++
			}
		}

		sourceNames := make([]string, 0, len(sourceCounts))
		for name := range sourceCounts {
			sourceNames = append(sourceNames, name)
		}
		sort.Strings(sourceNames)

		nodes := make([]accessTreeNode, 0, len(sourceNames))
		for _, name := range sourceNames {
			count := sourceCounts[name]
			badge := "1 account"
			if count != 1 {
				badge = fmt.Sprintf("%d accounts", count)
			}
			label := strings.TrimSpace(name)
			if label == "" {
				label = "(unknown)"
			}
			nodes = append(nodes, accessTreeNode{
				ID:          "inst:" + strings.ToLower(sourceKind) + ":" + encodeNodePart(name),
				Label:       label,
				Badges:      []string{badge},
				HasChildren: true,
			})
		}
		if len(nodes) == 0 {
			nodes = []accessTreeNode{{
				ID:          sourceKind + "-empty",
				Label:       "No linked accounts found.",
				HasChildren: false,
			}}
		}
		response.Nodes = nodes
		return c.JSON(http.StatusOK, response)
	case strings.HasPrefix(nodeID, "inst:"):
		raw := strings.TrimSpace(strings.TrimPrefix(nodeID, "inst:"))
		parts := strings.SplitN(raw, ":", 2)
		if len(parts) != 2 {
			return jsonTreeError(c, http.StatusBadRequest, "invalid instance node")
		}
		sourceKind := strings.TrimSpace(parts[0])
		sourceName, err := decodeNodePart(parts[1])
		if err != nil {
			return jsonTreeError(c, http.StatusBadRequest, "invalid instance node")
		}
		if sourceKind == "" || sourceName == "" {
			return jsonTreeError(c, http.StatusBadRequest, "invalid instance node")
		}

		linked, err := h.Q.ListLinkedAppUsersForIdPUser(ctx, id)
		if err != nil {
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}

		nodes := make([]accessTreeNode, 0, len(linked))
		for _, app := range linked {
			if !strings.EqualFold(strings.TrimSpace(app.SourceKind), sourceKind) {
				continue
			}
			if app.SourceName != sourceName {
				continue
			}
			label := strings.TrimSpace(app.DisplayName)
			if label == "" {
				label = strings.TrimSpace(app.ExternalID)
			}
			if label == "" {
				label = "(unknown)"
			}

			subLabel := strings.TrimSpace(app.Email)
			if subLabel == "" {
				subLabel = strings.TrimSpace(app.ExternalID)
			}

			nodes = append(nodes, accessTreeNode{
				ID:          "appuser:" + strconv.FormatInt(app.ID, 10),
				Label:       label,
				SubLabel:    subLabel,
				HasChildren: true,
			})
		}
		if len(nodes) == 0 {
			nodes = []accessTreeNode{{ID: "inst-empty:" + raw, Label: "No linked accounts found.", HasChildren: false}}
		}
		response.Nodes = nodes
		return c.JSON(http.StatusOK, response)
	case strings.HasPrefix(nodeID, "appuser:"):
		rawID := strings.TrimSpace(strings.TrimPrefix(nodeID, "appuser:"))
		appUserID, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil || appUserID <= 0 {
			return jsonTreeError(c, http.StatusBadRequest, "invalid app user node")
		}

		link, err := h.Q.GetIdentityLinkByAppUser(ctx, appUserID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return jsonTreeError(c, http.StatusNotFound, "app user not linked")
			}
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}
		if link.IdpUserID != id {
			return jsonTreeError(c, http.StatusNotFound, "app user not linked")
		}

		appUser, err := h.Q.GetAppUser(ctx, appUserID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return jsonTreeError(c, http.StatusNotFound, "app user not found")
			}
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}

		ents, err := h.Q.ListEntitlementsForAppUser(ctx, appUserID)
		if err != nil {
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}

		type resourceGroup struct {
			resourceKind string
			externalID   string
			label        string
			count        int
		}

		resourceGroups := make(map[string]*resourceGroup)
		var unmapped []accessTreeNode

		for _, ent := range ents {
			resourceKind, externalID, ok := accessgraph.ParseCanonicalResourceRef(ent.Resource)
			if !ok {
				label := strings.TrimSpace(ent.Resource)
				if label == "" {
					label = strings.TrimSpace(ent.Kind)
				}
				if label == "" {
					label = "(unknown)"
				}
				var badges []string
				if kind := strings.TrimSpace(ent.Kind); kind != "" {
					badges = append(badges, kind)
				}
				if perm := strings.TrimSpace(ent.Permission); perm != "" {
					badges = append(badges, perm)
				}
				unmapped = append(unmapped, accessTreeNode{
					ID:          "ent:" + strconv.FormatInt(ent.ID, 10),
					Label:       label,
					Badges:      badges,
					HasChildren: false,
				})
				continue
			}

			key := resourceKind + "\x00" + externalID
			if existing := resourceGroups[key]; existing != nil {
				existing.count++
				continue
			}

			label := accessgraph.DisplayResourceLabel(ent.Resource, ent.RawJson)
			if strings.TrimSpace(label) == "" {
				label = externalID
			}
			resourceGroups[key] = &resourceGroup{
				resourceKind: resourceKind,
				externalID:   externalID,
				label:        label,
				count:        1,
			}
		}

		keys := make([]string, 0, len(resourceGroups))
		for key := range resourceGroups {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			left := resourceGroups[keys[i]]
			right := resourceGroups[keys[j]]
			if left.resourceKind == right.resourceKind {
				return strings.ToLower(left.label) < strings.ToLower(right.label)
			}
			return left.resourceKind < right.resourceKind
		})

		nodes := make([]accessTreeNode, 0, len(keys)+len(unmapped))
		for _, key := range keys {
			group := resourceGroups[key]
			if group == nil {
				continue
			}
			var badges []string
			if kindLabel := humanizeResourceKind(group.resourceKind); kindLabel != "" {
				badges = append(badges, kindLabel)
			}
			if group.count > 1 {
				badges = append(badges, fmt.Sprintf("%d entitlements", group.count))
			}

			subLabel := ""
			if group.label != group.externalID {
				subLabel = group.externalID
			}

			nodes = append(nodes, accessTreeNode{
				ID:          "res:" + strconv.FormatInt(appUserID, 10) + ":" + group.resourceKind + ":" + encodeNodePart(group.externalID),
				Label:       group.label,
				SubLabel:    subLabel,
				Badges:      badges,
				HasChildren: true,
				Href:        accessgraph.BuildResourceHref(appUser.SourceKind, appUser.SourceName, group.resourceKind, group.externalID),
			})
		}

		if len(unmapped) > 0 {
			sort.Slice(unmapped, func(i, j int) bool {
				return strings.ToLower(unmapped[i].Label) < strings.ToLower(unmapped[j].Label)
			})
			nodes = append(nodes, unmapped...)
		}

		if len(nodes) == 0 {
			nodes = []accessTreeNode{{ID: "entitlements-empty:" + rawID, Label: "No entitlements found.", HasChildren: false}}
		}
		response.Nodes = nodes
		return c.JSON(http.StatusOK, response)
	case strings.HasPrefix(nodeID, "res:"):
		raw := strings.TrimSpace(strings.TrimPrefix(nodeID, "res:"))
		parts := strings.SplitN(raw, ":", 3)
		if len(parts) != 3 {
			return jsonTreeError(c, http.StatusBadRequest, "invalid resource node")
		}
		appUserID, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || appUserID <= 0 {
			return jsonTreeError(c, http.StatusBadRequest, "invalid resource node")
		}
		resourceKind := strings.TrimSpace(parts[1])
		externalID, err := decodeNodePart(parts[2])
		if err != nil || resourceKind == "" || externalID == "" {
			return jsonTreeError(c, http.StatusBadRequest, "invalid resource node")
		}

		link, err := h.Q.GetIdentityLinkByAppUser(ctx, appUserID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return jsonTreeError(c, http.StatusNotFound, "app user not linked")
			}
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}
		if link.IdpUserID != id {
			return jsonTreeError(c, http.StatusNotFound, "app user not linked")
		}

		ents, err := h.Q.ListEntitlementsForAppUser(ctx, appUserID)
		if err != nil {
			return jsonTreeError(c, http.StatusInternalServerError, "internal error")
		}

		nodes := make([]accessTreeNode, 0, len(ents))
		for _, ent := range ents {
			kind, eid, ok := accessgraph.ParseCanonicalResourceRef(ent.Resource)
			if !ok {
				continue
			}
			if kind != resourceKind || eid != externalID {
				continue
			}
			label := strings.TrimSpace(ent.Permission)
			if label == "" {
				label = "(no permission)"
			}
			subLabel := strings.TrimSpace(ent.Kind)
			if subLabel == "" {
				subLabel = strings.TrimSpace(ent.Resource)
			}
			nodes = append(nodes, accessTreeNode{
				ID:          "ent:" + strconv.FormatInt(ent.ID, 10),
				Label:       label,
				SubLabel:    subLabel,
				HasChildren: false,
			})
		}

		if len(nodes) == 0 {
			nodes = []accessTreeNode{{ID: "perms-empty:" + raw, Label: "No permissions found.", HasChildren: false}}
		} else {
			sort.Slice(nodes, func(i, j int) bool {
				return strings.ToLower(nodes[i].Label) < strings.ToLower(nodes[j].Label)
			})
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
		slog.Warn("datadog user raw_json parse failed", "app_user_id", appUserID, "external_id", strings.TrimSpace(externalID), "err", err)
		return ""
	}
	return strings.TrimSpace(payload.Status)
}
