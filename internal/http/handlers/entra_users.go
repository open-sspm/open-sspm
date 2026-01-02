package handlers

import (
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleEntraUsers(c echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Microsoft Entra ID Users")
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

	sourceName := strings.TrimSpace(snap.Entra.TenantID)

	if !snap.EntraConfigured || !snap.EntraEnabled {
		message := "Microsoft Entra ID is not configured yet. Add settings in Connectors."
		if snap.EntraConfigured && !snap.EntraEnabled {
			message = "Microsoft Entra ID sync is disabled. Enable it in Connectors."
		}
		data := viewmodels.EntraUsersViewData{
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
			EmptyStateHref: "/settings/connectors?open=entra",
		}
		return h.RenderComponent(c, views.EntraUsersPage(data))
	}

	totalCount, err := h.Q.CountAppUsersWithLinkBySourceAndQuery(ctx, gen.CountAppUsersWithLinkBySourceAndQueryParams{
		SourceKind: "entra",
		SourceName: sourceName,
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
		SourceKind: "entra",
		SourceName: sourceName,
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

	appUserIDs := make([]int64, 0, len(users))
	for _, user := range users {
		appUserIDs = append(appUserIDs, user.ID)
	}

	directoryRoleCounts := make(map[int64]int)
	enterpriseAppCounts := make(map[int64]int)
	if len(appUserIDs) > 0 {
		rows, err := h.Q.ListEntitlementResourcesByAppUserIDsAndKind(ctx, gen.ListEntitlementResourcesByAppUserIDsAndKindParams{
			AppUserIds: appUserIDs,
			EntKind:    "entra_directory_role",
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		directoryRoleCounts = countUniqueEntitlementResources(rows)

		rows, err = h.Q.ListEntitlementResourcesByAppUserIDsAndKind(ctx, gen.ListEntitlementResourcesByAppUserIDsAndKindParams{
			AppUserIds: appUserIDs,
			EntKind:    "entra_app_role",
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		enterpriseAppCounts = countUniqueEntitlementResources(rows)
	}

	items := make([]viewmodels.EntraUserListItem, 0, len(users))
	for _, user := range users {
		userName := strings.TrimSpace(user.DisplayName)
		if userName == "" {
			userName = strings.TrimSpace(user.Email)
		}
		if userName == "" {
			userName = strings.TrimSpace(user.ExternalID)
		}
		items = append(items, viewmodels.EntraUserListItem{
			ID:                 user.ID,
			ExternalID:         strings.TrimSpace(user.ExternalID),
			Email:              strings.TrimSpace(user.Email),
			DisplayName:        userName,
			IdpUserID:          user.IdpUserID,
			DirectoryRoleCount: directoryRoleCounts[user.ID],
			EnterpriseAppCount: enterpriseAppCounts[user.ID],
		})
	}

	emptyState := "No Microsoft Entra ID users synced yet."
	if query != "" {
		emptyState = "No Microsoft Entra ID users match the current search."
	}

	data := viewmodels.EntraUsersViewData{
		Layout:         layout,
		Users:          items,
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
		EmptyStateHref: "/settings/connectors?open=entra",
	}

	return h.RenderComponent(c, views.EntraUsersPage(data))
}

func (h *Handlers) HandleUnmatchedEntra(c echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Unmatched Microsoft Entra ID Users")
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

	sourceName := strings.TrimSpace(snap.Entra.TenantID)

	if !snap.EntraConfigured || !snap.EntraEnabled {
		message := "Microsoft Entra ID is not configured yet. Add settings in Connectors."
		if snap.EntraConfigured && !snap.EntraEnabled {
			message = "Microsoft Entra ID sync is disabled. Enable it in Connectors."
		}
		data := viewmodels.UnmatchedEntraViewData{
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
			EmptyStateHref: "/settings/connectors?open=entra",
		}
		return h.RenderComponent(c, views.UnmatchedEntraPage(data))
	}

	totalCount, err := h.Q.CountUnmatchedAppUsersBySourceAndQuery(ctx, gen.CountUnmatchedAppUsersBySourceAndQueryParams{
		SourceKind: "entra",
		SourceName: sourceName,
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
		SourceKind: "entra",
		SourceName: sourceName,
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

	emptyState := "No unmatched Microsoft Entra ID users."
	if query != "" {
		emptyState = "No unmatched Microsoft Entra ID users match the current search."
	}

	data := viewmodels.UnmatchedEntraViewData{
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
		EmptyStateHref: "/settings/connectors?open=entra",
	}

	return h.RenderComponent(c, views.UnmatchedEntraPage(data))
}

func countUniqueEntitlementResources(rows []gen.ListEntitlementResourcesByAppUserIDsAndKindRow) map[int64]int {
	counts := make(map[int64]int)
	var lastUserID int64
	lastResource := ""
	for _, row := range rows {
		resource := strings.TrimSpace(row.Resource)
		if resource == "" {
			continue
		}
		if row.AppUserID != lastUserID {
			lastUserID = row.AppUserID
			lastResource = ""
		}
		if resource == lastResource {
			continue
		}
		lastResource = resource
		counts[row.AppUserID]++
	}
	return counts
}
