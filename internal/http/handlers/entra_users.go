package handlers

import (
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleEntraUsers(c *echo.Context) error {
	queries := h.sourceAccountInventoryQueries(sourceAccountInventoryQueryOptions{
		SourceKind:            "entra",
		EntityCategory:        registry.EntityCategoryUser,
		DistinctResourceKind1: "entra_directory_role",
		DistinctResourceKind2: "entra_app_role",
	})
	inventory, err := h.buildSourceAccountInventoryPage(c, sourceAccountInventoryOptions{
		Title:              "Microsoft Entra ID Users",
		BasePath:           "/accounts/entra",
		ConnectorName:      "Microsoft Entra ID",
		ConnectorKind:      "entra",
		EmptyStateHref:     "/settings/connectors?open=entra",
		SyncedEmptyState:   "No Microsoft Entra ID users synced yet.",
		FilteredEmptyState: "No Microsoft Entra ID users match the current search.",
		UnavailableMessageFn: func(configured, enabled bool) string {
			if configured && !enabled {
				return "Microsoft Entra ID sync is disabled. Enable it in Connectors."
			}
			return "Microsoft Entra ID is not configured yet. Add settings in Connectors."
		},
		Count: queries.Count,
		List:  queries.List,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := mapSourceAccountInventoryItems(inventory.Accounts, func(user sourceAccountInventoryAccount) viewmodels.EntraUserListItem {
		return viewmodels.EntraUserListItem{
			ID:                 user.ID,
			ExternalID:         user.ExternalID,
			Email:              user.Email,
			DisplayName:        user.DisplayName,
			IdentityID:         user.IdentityID,
			DirectoryRoleCount: user.Summary.DistinctResourceCount1,
			EnterpriseAppCount: user.Summary.DistinctResourceCount2,
		}
	})

	data := viewmodels.EntraUsersViewData{
		SourceAccountInventoryPageData: inventory.PageData,
		Users:                          items,
		HasUsers:                       inventory.PageData.HasAccounts,
	}

	return h.renderListWithHX(c, "entra-users-results", views.EntraUsersPageResults(data), views.EntraUsersPage(data))
}

func (h *Handlers) HandleUnmatchedEntra(c *echo.Context) error {
	unmatched, err := h.buildUnmatchedSourceAccountsPage(c, unmatchedSourceAccountOptions{
		Title:              "Unlinked Microsoft Entra ID Users",
		BasePath:           "/accounts/unlinked/entra",
		ConnectorName:      "Microsoft Entra ID",
		ConnectorKind:      "entra",
		SourceKind:         "entra",
		EntityCategory:     registry.EntityCategoryUser,
		EmptyStateHref:     "/settings/connectors?open=entra",
		SyncedEmptyState:   "No unlinked Microsoft Entra ID users.",
		FilteredEmptyState: "No unlinked Microsoft Entra ID users match the current search.",
		UnavailableMessageFn: func(configured, enabled bool) string {
			if configured && !enabled {
				return "Microsoft Entra ID sync is disabled. Enable it in Connectors."
			}
			return "Microsoft Entra ID is not configured yet. Add settings in Connectors."
		},
		ResolveSourceName: func(_ *echo.Context, configuredSourceName string) (string, error) {
			return configuredSourceName, nil
		},
	})
	if err != nil {
		return h.renderUnmatchedSourceAccountsError(c, err)
	}

	data := viewmodels.UnmatchedEntraViewData{
		UnmatchedSourceAccountsPageData: unmatched.PageData,
	}

	return h.renderListWithHX(c, "unmatched-entra-results", views.UnmatchedEntraPageResults(data), views.UnmatchedEntraPage(data))
}
