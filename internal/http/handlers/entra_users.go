package handlers

import (
	"context"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleEntraUsers(c *echo.Context) error {
	inventory, err := h.buildSourceAccountInventoryPage(c, sourceAccountInventoryOptions{
		Title:              "Microsoft Entra ID Users",
		ConnectorName:      "Microsoft Entra ID",
		ConnectorKind:      "entra",
		SourceKind:         querySourceKind("entra"),
		EmptyStateHref:     "/settings/connectors?open=entra",
		SyncedEmptyState:   "No Microsoft Entra ID users synced yet.",
		FilteredEmptyState: "No Microsoft Entra ID users match the current search.",
		UnavailableMessageFn: func(configured, enabled bool) string {
			if configured && !enabled {
				return "Microsoft Entra ID sync is disabled. Enable it in Connectors."
			}
			return "Microsoft Entra ID is not configured yet. Add settings in Connectors."
		},
		Count: func(ctx context.Context, sourceName, query string) (int64, error) {
			return h.Q.CountSourceAccountsBySourceAndQuery(ctx, gen.CountSourceAccountsBySourceAndQueryParams{
				SourceKind:     "entra",
				SourceName:     sourceName,
				EntityCategory: registry.EntityCategoryUser,
				Query:          query,
			})
		},
		List: func(ctx context.Context, sourceName, query string, offset, limit int) ([]sourceAccountInventoryAccount, error) {
			users, err := h.Q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, gen.ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
				SourceKind:            "entra",
				SourceName:            sourceName,
				EntityCategory:        registry.EntityCategoryUser,
				Query:                 query,
				PageLimit:             int32(limit),
				PageOffset:            int32(offset),
				DistinctResourceKind1: "entra_directory_role",
				DistinctResourceKind2: "entra_app_role",
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

	items := make([]viewmodels.EntraUserListItem, 0, len(inventory.Accounts))
	for _, user := range inventory.Accounts {
		items = append(items, viewmodels.EntraUserListItem{
			ID:                 user.ID,
			ExternalID:         user.ExternalID,
			Email:              user.Email,
			DisplayName:        user.DisplayName,
			IdentityID:         user.IdentityID,
			DirectoryRoleCount: user.Summary.DistinctResourceCount1,
			EnterpriseAppCount: user.Summary.DistinctResourceCount2,
		})
	}

	data := viewmodels.EntraUsersViewData{
		SourceAccountInventoryPageData: inventory.PageData,
		Users:                          items,
		HasUsers:                       inventory.PageData.HasAccounts,
	}

	return h.RenderComponent(c, views.EntraUsersPage(data))
}

func (h *Handlers) HandleUnmatchedEntra(c *echo.Context) error {
	unmatched, err := h.buildUnmatchedSourceAccountsPage(c, unmatchedSourceAccountOptions{
		Title:              "Unlinked Microsoft Entra ID Users",
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

	return h.RenderComponent(c, views.UnmatchedEntraPage(data))
}
