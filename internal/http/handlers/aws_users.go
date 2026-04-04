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

func (h *Handlers) HandleAWSUsers(c *echo.Context) error {
	inventory, err := h.buildSourceAccountInventoryPage(c, sourceAccountInventoryOptions{
		Title:              "AWS Identity Center Users",
		ConnectorName:      "AWS Identity Center",
		ConnectorKind:      "aws_identity_center",
		SourceKind:         querySourceKind("aws_identity_center"),
		EmptyStateHref:     "/settings/connectors?open=aws_identity_center",
		SyncedEmptyState:   "No AWS Identity Center users synced yet.",
		FilteredEmptyState: "No AWS Identity Center users match the current search.",
		UnavailableMessageFn: func(configured, enabled bool) string {
			if configured && !enabled {
				return "AWS Identity Center sync is disabled. Enable it in Connectors."
			}
			return "AWS Identity Center is not configured yet. Add settings in Connectors."
		},
		Count: func(ctx context.Context, sourceName, query string) (int64, error) {
			return h.Q.CountSourceAccountsBySourceAndQuery(ctx, gen.CountSourceAccountsBySourceAndQueryParams{
				SourceKind:     "aws",
				SourceName:     sourceName,
				EntityCategory: registry.EntityCategoryUser,
				Query:          query,
			})
		},
		List: func(ctx context.Context, sourceName, query string, offset, limit int) ([]sourceAccountInventoryAccount, error) {
			users, err := h.Q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, gen.ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
				SourceKind:            "aws",
				SourceName:            sourceName,
				EntityCategory:        registry.EntityCategoryUser,
				Query:                 query,
				PageLimit:             int32(limit),
				PageOffset:            int32(offset),
				DistinctResourceKind1: "aws_permission_set",
				EntitlementKind1:      "aws_permission_set",
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
						EntitlementCount1:      int(user.EntitlementCount1),
					},
				})
			}
			return accounts, nil
		},
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.AWSUserListItem, 0, len(inventory.Accounts))
	for _, user := range inventory.Accounts {
		items = append(items, viewmodels.AWSUserListItem{
			ID:              user.ID,
			ExternalID:      user.ExternalID,
			Email:           user.Email,
			DisplayName:     user.DisplayName,
			IdentityID:      user.IdentityID,
			AccountCount:    user.Summary.DistinctResourceCount1,
			AssignmentCount: user.Summary.EntitlementCount1,
		})
	}

	data := viewmodels.AWSUsersViewData{
		SourceAccountInventoryPageData: inventory.PageData,
		Users:                          items,
		HasUsers:                       inventory.PageData.HasAccounts,
	}

	return h.RenderComponent(c, views.AWSUsersPage(data))
}

func (h *Handlers) HandleUnmatchedAWS(c *echo.Context) error {
	unmatched, err := h.buildUnmatchedSourceAccountsPage(c, unmatchedSourceAccountOptions{
		Title:              "Unlinked AWS Identity Center Users",
		ConnectorName:      "AWS Identity Center",
		ConnectorKind:      "aws_identity_center",
		SourceKind:         "aws",
		EntityCategory:     registry.EntityCategoryUser,
		EmptyStateHref:     "/settings/connectors?open=aws_identity_center",
		SyncedEmptyState:   "No unlinked AWS Identity Center users.",
		FilteredEmptyState: "No unlinked AWS Identity Center users match the current search.",
		UnavailableMessageFn: func(configured, enabled bool) string {
			if configured && !enabled {
				return "AWS Identity Center sync is disabled. Enable it in Connectors."
			}
			return "AWS Identity Center is not configured yet. Add settings in Connectors."
		},
		ResolveSourceName: func(_ *echo.Context, configuredSourceName string) (string, error) {
			return configuredSourceName, nil
		},
	})
	if err != nil {
		return h.renderUnmatchedSourceAccountsError(c, err)
	}

	data := viewmodels.UnmatchedAWSViewData{
		UnmatchedSourceAccountsPageData: unmatched.PageData,
	}

	return h.RenderComponent(c, views.UnmatchedAWSPage(data))
}
