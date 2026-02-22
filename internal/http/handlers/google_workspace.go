package handlers

import (
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleGoogleWorkspaceUsers(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Google Workspace Users")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)
	sourceName := strings.TrimSpace(snap.GoogleWorkspace.CustomerID)

	if !snap.GoogleWorkspaceConfigured || !snap.GoogleWorkspaceEnabled {
		message := connectorUnavailableMessage("Google Workspace", snap.GoogleWorkspaceConfigured, snap.GoogleWorkspaceEnabled)
		data := viewmodels.GoogleWorkspaceUsersViewData{
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
			EmptyStateHref: "/settings/connectors?open=google_workspace",
		}
		return h.RenderComponent(c, views.GoogleWorkspaceUsersPage(data))
	}

	totalCount, err := h.Q.CountGoogleWorkspaceUsersBySourceAndQuery(ctx, gen.CountGoogleWorkspaceUsersBySourceAndQueryParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	users, err := h.Q.ListGoogleWorkspaceUsersPageBySourceAndQuery(ctx, gen.ListGoogleWorkspaceUsersPageBySourceAndQueryParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		Query:      query,
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

	groupCounts := map[int64]int{}
	adminRoleCounts := map[int64]int{}
	if len(appUserIDs) > 0 {
		rows, err := h.Q.ListEntitlementResourcesByAppUserIDsAndKind(ctx, gen.ListEntitlementResourcesByAppUserIDsAndKindParams{
			AppUserIds: appUserIDs,
			EntKind:    "google_group_member",
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		groupCounts = countUniqueEntitlementResources(rows)

		rows, err = h.Q.ListEntitlementResourcesByAppUserIDsAndKind(ctx, gen.ListEntitlementResourcesByAppUserIDsAndKindParams{
			AppUserIds: appUserIDs,
			EntKind:    "google_admin_role",
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		adminRoleCounts = countUniqueEntitlementResources(rows)
	}

	items := make([]viewmodels.GoogleWorkspaceUserListItem, 0, len(users))
	for _, user := range users {
		displayName := strings.TrimSpace(user.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(user.Email)
		}
		if displayName == "" {
			displayName = strings.TrimSpace(user.ExternalID)
		}

		items = append(items, viewmodels.GoogleWorkspaceUserListItem{
			ID:             user.ID,
			ExternalID:     strings.TrimSpace(user.ExternalID),
			Email:          strings.TrimSpace(user.Email),
			DisplayName:    displayName,
			IdpUserID:      user.IdpUserID,
			GroupCount:     groupCounts[user.ID],
			AdminRoleCount: adminRoleCounts[user.ID],
		})
	}

	showingCount := len(items)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	emptyState := "No Google Workspace users synced yet."
	if query != "" {
		emptyState = "No Google Workspace users match the current search."
	}

	data := viewmodels.GoogleWorkspaceUsersViewData{
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
		EmptyStateHref: "/settings/connectors?open=google_workspace",
	}

	return h.RenderComponent(c, views.GoogleWorkspaceUsersPage(data))
}

func (h *Handlers) HandleGoogleWorkspaceGroups(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Google Workspace Groups")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)
	sourceName := strings.TrimSpace(snap.GoogleWorkspace.CustomerID)

	if !snap.GoogleWorkspaceConfigured || !snap.GoogleWorkspaceEnabled {
		message := connectorUnavailableMessage("Google Workspace", snap.GoogleWorkspaceConfigured, snap.GoogleWorkspaceEnabled)
		data := viewmodels.GoogleWorkspaceGroupsViewData{
			Layout:         layout,
			Groups:         nil,
			Query:          query,
			ShowingCount:   0,
			ShowingFrom:    0,
			ShowingTo:      0,
			TotalCount:     0,
			Page:           1,
			PerPage:        perPage,
			TotalPages:     1,
			HasGroups:      false,
			EmptyStateMsg:  message,
			EmptyStateHref: "/settings/connectors?open=google_workspace",
		}
		return h.RenderComponent(c, views.GoogleWorkspaceGroupsPage(data))
	}

	totalCount, err := h.Q.CountGoogleWorkspaceGroupsBySourceAndQuery(ctx, gen.CountGoogleWorkspaceGroupsBySourceAndQueryParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	groups, err := h.Q.ListGoogleWorkspaceGroupsPageBySourceAndQuery(ctx, gen.ListGoogleWorkspaceGroupsPageBySourceAndQueryParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	externalIDs := make([]string, 0, len(groups))
	for _, group := range groups {
		externalIDs = append(externalIDs, strings.TrimSpace(group.ExternalID))
	}

	type membershipCounts struct {
		members  int
		owners   int
		managers int
	}
	countsByGroupExternalID := map[string]membershipCounts{}
	if len(externalIDs) > 0 {
		rows, err := h.Q.ListGoogleWorkspaceGroupMemberCountsByGroupExternalIDs(ctx, externalIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, row := range rows {
			externalID := strings.TrimSpace(row.GroupExternalID)
			countsByGroupExternalID[externalID] = membershipCounts{
				members:  int(row.MemberCount),
				owners:   int(row.OwnerCount),
				managers: int(row.ManagerCount),
			}
		}
	}

	items := make([]viewmodels.GoogleWorkspaceGroupListItem, 0, len(groups))
	for _, group := range groups {
		externalID := strings.TrimSpace(group.ExternalID)
		displayName := strings.TrimSpace(group.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(group.Email)
		}
		if displayName == "" {
			displayName = externalID
		}
		counts := countsByGroupExternalID[externalID]
		items = append(items, viewmodels.GoogleWorkspaceGroupListItem{
			ID:           group.ID,
			ExternalID:   externalID,
			Email:        strings.TrimSpace(group.Email),
			DisplayName:  displayName,
			MemberCount:  counts.members,
			OwnerCount:   counts.owners,
			ManagerCount: counts.managers,
		})
	}

	showingCount := len(items)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	emptyState := "No Google Workspace groups synced yet."
	if query != "" {
		emptyState = "No Google Workspace groups match the current search."
	}

	data := viewmodels.GoogleWorkspaceGroupsViewData{
		Layout:         layout,
		Groups:         items,
		Query:          query,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     totalCount,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		HasGroups:      showingCount > 0,
		EmptyStateMsg:  emptyState,
		EmptyStateHref: "/settings/connectors?open=google_workspace",
	}

	return h.RenderComponent(c, views.GoogleWorkspaceGroupsPage(data))
}

func (h *Handlers) HandleGoogleWorkspaceOAuthApps(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Google Workspace OAuth Apps")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)
	sourceName := strings.TrimSpace(snap.GoogleWorkspace.CustomerID)

	if !snap.GoogleWorkspaceConfigured || !snap.GoogleWorkspaceEnabled {
		message := connectorUnavailableMessage("Google Workspace", snap.GoogleWorkspaceConfigured, snap.GoogleWorkspaceEnabled)
		data := viewmodels.GoogleWorkspaceOAuthAppsViewData{
			Layout:         layout,
			Apps:           nil,
			Query:          query,
			ShowingCount:   0,
			ShowingFrom:    0,
			ShowingTo:      0,
			TotalCount:     0,
			Page:           1,
			PerPage:        perPage,
			TotalPages:     1,
			HasApps:        false,
			EmptyStateMsg:  message,
			EmptyStateHref: "/settings/connectors?open=google_workspace",
		}
		return h.RenderComponent(c, views.GoogleWorkspaceOAuthAppsPage(data))
	}

	totalCount, err := h.Q.CountAppAssetsBySourceAndQueryAndKind(ctx, gen.CountAppAssetsBySourceAndQueryAndKindParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		AssetKind:  "google_oauth_client",
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	assets, err := h.Q.ListAppAssetsPageBySourceAndQueryAndKind(ctx, gen.ListAppAssetsPageBySourceAndQueryAndKindParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		AssetKind:  "google_oauth_client",
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	assetIDs := make([]int64, 0, len(assets))
	assetRefKinds := make([]string, 0, len(assets))
	assetRefExternalIDs := make([]string, 0, len(assets))
	refToAssetID := map[string]int64{}
	for _, asset := range assets {
		assetIDs = append(assetIDs, asset.ID)
		assetRefKind := "google_oauth_client"
		assetRefExternalID := "google_oauth_client:" + strings.TrimSpace(asset.ExternalID)
		assetRefKinds = append(assetRefKinds, assetRefKind)
		assetRefExternalIDs = append(assetRefExternalIDs, assetRefExternalID)
		refToAssetID[credentialSourceRefKey(configstore.KindGoogleWorkspace, sourceName, assetRefKind, assetRefExternalID)] = asset.ID
	}

	ownerCounts := map[int64]int{}
	if len(assetIDs) > 0 {
		owners, err := h.Q.ListAppAssetOwnersByAssetIDs(ctx, assetIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, owner := range owners {
			ownerCounts[owner.AppAssetID]++
		}
	}

	grantCounts := map[int64]int{}
	if len(assetRefKinds) > 0 {
		rows, err := h.Q.ListCredentialArtifactCountsByAssetRef(ctx, gen.ListCredentialArtifactCountsByAssetRefParams{
			SourceKind:          configstore.KindGoogleWorkspace,
			SourceName:          sourceName,
			AssetRefKinds:       assetRefKinds,
			AssetRefExternalIds: assetRefExternalIDs,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, row := range rows {
			assetID, ok := refToAssetID[credentialSourceRefKey(configstore.KindGoogleWorkspace, sourceName, row.AssetRefKind, row.AssetRefExternalID)]
			if !ok {
				continue
			}
			grantCounts[assetID] = int(row.CredentialCount)
		}
	}

	items := make([]viewmodels.GoogleWorkspaceOAuthAppListItem, 0, len(assets))
	for _, asset := range assets {
		displayName := strings.TrimSpace(asset.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(asset.ExternalID)
		}
		if displayName == "" {
			displayName = "OAuth app"
		}
		items = append(items, viewmodels.GoogleWorkspaceOAuthAppListItem{
			ID:          asset.ID,
			ExternalID:  strings.TrimSpace(asset.ExternalID),
			DisplayName: displayName,
			Status:      fallbackDash(strings.TrimSpace(asset.Status)),
			OwnerCount:  ownerCounts[asset.ID],
			GrantCount:  grantCounts[asset.ID],
			LastSeenAt:  formatProgrammaticDate(asset.LastObservedAt),
		})
	}

	showingCount := len(items)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	emptyState := "No Google Workspace OAuth apps synced yet."
	if query != "" {
		emptyState = "No Google Workspace OAuth apps match the current search."
	}

	data := viewmodels.GoogleWorkspaceOAuthAppsViewData{
		Layout:         layout,
		Apps:           items,
		Query:          query,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     totalCount,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		HasApps:        showingCount > 0,
		EmptyStateMsg:  emptyState,
		EmptyStateHref: "/settings/connectors?open=google_workspace",
	}

	return h.RenderComponent(c, views.GoogleWorkspaceOAuthAppsPage(data))
}

func (h *Handlers) HandleUnmatchedGoogleWorkspace(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Unmanaged Google Workspace Users")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)
	sourceName := strings.TrimSpace(snap.GoogleWorkspace.CustomerID)

	if !snap.GoogleWorkspaceConfigured || !snap.GoogleWorkspaceEnabled {
		message := connectorUnavailableMessage("Google Workspace", snap.GoogleWorkspaceConfigured, snap.GoogleWorkspaceEnabled)
		data := viewmodels.UnmatchedGoogleWorkspaceViewData{
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
			EmptyStateHref: "/settings/connectors?open=google_workspace",
		}
		return h.RenderComponent(c, views.UnmatchedGoogleWorkspacePage(data))
	}

	totalCount, err := h.Q.CountUnmatchedGoogleWorkspaceUsersBySourceAndQuery(ctx, gen.CountUnmatchedGoogleWorkspaceUsersBySourceAndQueryParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	users, err := h.Q.ListUnmatchedGoogleWorkspaceUsersPageBySourceAndQuery(ctx, gen.ListUnmatchedGoogleWorkspaceUsersPageBySourceAndQueryParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	showingCount := len(users)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	emptyState := "No unmanaged Google Workspace users."
	if query != "" {
		emptyState = "No unmanaged Google Workspace users match the current search."
	}

	data := viewmodels.UnmatchedGoogleWorkspaceViewData{
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
		EmptyStateHref: "/settings/connectors?open=google_workspace",
	}

	return h.RenderComponent(c, views.UnmatchedGoogleWorkspacePage(data))
}
