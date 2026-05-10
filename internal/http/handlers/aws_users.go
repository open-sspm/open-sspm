package handlers

import (
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleAWSUsers(c *echo.Context) error {
	queries := h.sourceAccountInventoryQueries(sourceAccountInventoryQueryOptions{
		SourceKind:            "aws",
		EntityCategory:        registry.EntityCategoryUser,
		DistinctResourceKind1: "aws_permission_set",
		EntitlementKind1:      "aws_permission_set",
	})
	inventory, err := h.buildSourceAccountInventoryPage(c, sourceAccountInventoryOptions{
		Title:              "AWS Identity Center Users",
		BasePath:           "/accounts/aws",
		ConnectorName:      "AWS Identity Center",
		ConnectorKind:      "aws_identity_center",
		EmptyStateHref:     "/settings/connectors?open=aws_identity_center",
		SyncedEmptyState:   "No AWS Identity Center users synced yet.",
		FilteredEmptyState: "No AWS Identity Center users match the current search.",
		UnavailableMessageFn: func(configured, enabled bool) string {
			if configured && !enabled {
				return "AWS Identity Center sync is disabled. Enable it in Connectors."
			}
			return "AWS Identity Center is not configured yet. Add settings in Connectors."
		},
		Count: queries.Count,
		List:  queries.List,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := mapSourceAccountInventoryItems(inventory.Accounts, func(user sourceAccountInventoryAccount) viewmodels.AWSUserListItem {
		return viewmodels.AWSUserListItem{
			ID:              user.ID,
			ExternalID:      user.ExternalID,
			Email:           user.Email,
			DisplayName:     user.DisplayName,
			IdentityID:      user.IdentityID,
			AccountCount:    user.Summary.DistinctResourceCount1,
			AssignmentCount: user.Summary.EntitlementCount1,
		}
	})

	data := viewmodels.AWSUsersViewData{
		SourceAccountInventoryPageData: inventory.PageData,
		Users:                          items,
		HasUsers:                       inventory.PageData.HasAccounts,
	}

	return h.renderListWithHX(c, "aws-users-results", views.AWSUsersPageResults(data), views.AWSUsersPage(data))
}

func (h *Handlers) HandleUnmatchedAWS(c *echo.Context) error {
	unmatched, err := h.buildUnmatchedSourceAccountsPage(c, unmatchedSourceAccountOptions{
		Title:              "Unlinked AWS Identity Center Users",
		BasePath:           "/accounts/unlinked/aws",
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

	return h.renderListWithHX(c, "unmatched-aws-results", views.UnmatchedAWSPageResults(data), views.UnmatchedAWSPage(data))
}
