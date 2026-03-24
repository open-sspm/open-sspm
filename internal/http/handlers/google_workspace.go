package handlers

import (
	"context"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleGoogleWorkspaceUsers(c *echo.Context) error {
	inventory, err := h.buildSourceAccountInventoryPage(c, sourceAccountInventoryOptions{
		Title:              "Google Workspace Users",
		ConnectorName:      "Google Workspace",
		EmptyStateHref:     "/settings/connectors?open=google_workspace",
		SyncedEmptyState:   "No Google Workspace users synced yet.",
		FilteredEmptyState: "No Google Workspace users match the current search.",
		IsConfigured: func(snap ConnectorSnapshot) bool {
			return snap.GoogleWorkspaceConfigured
		},
		IsEnabled: func(snap ConnectorSnapshot) bool {
			return snap.GoogleWorkspaceEnabled
		},
		Count: func(ctx context.Context, snap ConnectorSnapshot, query string) (int64, error) {
			return h.Q.CountSourceAccountsBySourceAndQuery(ctx, gen.CountSourceAccountsBySourceAndQueryParams{
				SourceKind:     configstore.KindGoogleWorkspace,
				SourceName:     strings.TrimSpace(snap.GoogleWorkspace.CustomerID),
				EntityCategory: registry.EntityCategoryUser,
				Query:          query,
			})
		},
		List: func(ctx context.Context, snap ConnectorSnapshot, query string, offset, limit int) ([]sourceAccountInventoryAccount, error) {
			users, err := h.Q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, gen.ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
				SourceKind:            configstore.KindGoogleWorkspace,
				SourceName:            strings.TrimSpace(snap.GoogleWorkspace.CustomerID),
				EntityCategory:        registry.EntityCategoryUser,
				Query:                 query,
				PageLimit:             int32(limit),
				PageOffset:            int32(offset),
				DistinctResourceKind1: "google_group_member",
				DistinctResourceKind2: "google_admin_role",
			})
			if err != nil {
				return nil, err
			}

			accounts := make([]sourceAccountInventoryAccount, 0, len(users))
			for _, user := range users {
				accounts = append(accounts, sourceAccountInventoryAccount{
					ID:          user.ID,
					ExternalID:  strings.TrimSpace(user.ExternalID),
					Email:       strings.TrimSpace(user.Email),
					DisplayName: sourceAccountInventoryDisplayName(user.DisplayName, user.Email, user.ExternalID),
					IdentityID:  user.IdentityID,
					Summary: sourceAccountInventorySummary{
						DistinctResourceCount1: int(user.DistinctResourceCount1),
						DistinctResourceCount2: int(user.DistinctResourceCount2),
					},
				})
			}
			return accounts, nil
		},
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.GoogleWorkspaceUserListItem, 0, len(inventory.Accounts))
	for _, user := range inventory.Accounts {
		items = append(items, viewmodels.GoogleWorkspaceUserListItem{
			ID:             user.ID,
			ExternalID:     user.ExternalID,
			Email:          user.Email,
			DisplayName:    user.DisplayName,
			IdentityID:     user.IdentityID,
			GroupCount:     user.Summary.DistinctResourceCount1,
			AdminRoleCount: user.Summary.DistinctResourceCount2,
		})
	}

	data := viewmodels.GoogleWorkspaceUsersViewData{
		SourceAccountInventoryPageData: inventory.PageData,
		Users:                          items,
		HasUsers:                       inventory.PageData.HasAccounts,
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
	query := strings.TrimSpace(c.QueryParam("q"))
	target := "/connected-apps"
	if query != "" {
		target = connectedAppsListURL(query, "", 1)
	}
	return c.Redirect(http.StatusMovedPermanently, target)
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

	totalCount, err := h.Q.CountUnlinkedSourceAccountsBySourceAndQuery(ctx, gen.CountUnlinkedSourceAccountsBySourceAndQueryParams{
		SourceKind:     configstore.KindGoogleWorkspace,
		SourceName:     sourceName,
		EntityCategory: registry.EntityCategoryUser,
		Query:          query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	users, err := h.Q.ListUnlinkedSourceAccountsPageBySourceAndQuery(ctx, gen.ListUnlinkedSourceAccountsPageBySourceAndQueryParams{
		SourceKind:     configstore.KindGoogleWorkspace,
		SourceName:     sourceName,
		EntityCategory: registry.EntityCategoryUser,
		Query:          query,
		PageLimit:      int32(perPage),
		PageOffset:     int32(offset),
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
