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
		EmptyStateHref:     "/settings/connectors?open=aws_identity_center",
		SyncedEmptyState:   "No AWS Identity Center users synced yet.",
		FilteredEmptyState: "No AWS Identity Center users match the current search.",
		IsConfigured: func(snap ConnectorSnapshot) bool {
			return snap.AWSIdentityCenterConfigured
		},
		IsEnabled: func(snap ConnectorSnapshot) bool {
			return snap.AWSIdentityCenterEnabled
		},
		UnavailableMessageFn: func(snap ConnectorSnapshot) string {
			if snap.AWSIdentityCenterConfigured && !snap.AWSIdentityCenterEnabled {
				return "AWS Identity Center sync is disabled. Enable it in Connectors."
			}
			return "AWS Identity Center is not configured yet. Add settings in Connectors."
		},
		Count: func(ctx context.Context, snap ConnectorSnapshot, query string) (int64, error) {
			sourceName := strings.TrimSpace(snap.AWSIdentityCenter.Name)
			if sourceName == "" {
				sourceName = strings.TrimSpace(snap.AWSIdentityCenter.Region)
			}
			return h.Q.CountSourceAccountsBySourceAndQuery(ctx, gen.CountSourceAccountsBySourceAndQueryParams{
				SourceKind:     "aws",
				SourceName:     sourceName,
				EntityCategory: registry.EntityCategoryUser,
				Query:          query,
			})
		},
		List: func(ctx context.Context, snap ConnectorSnapshot, query string, offset, limit int) ([]sourceAccountInventoryAccount, error) {
			sourceName := strings.TrimSpace(snap.AWSIdentityCenter.Name)
			if sourceName == "" {
				sourceName = strings.TrimSpace(snap.AWSIdentityCenter.Region)
			}
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
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Unmanaged AWS Identity Center Users")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)

	sourceName := strings.TrimSpace(snap.AWSIdentityCenter.Name)
	if sourceName == "" {
		sourceName = strings.TrimSpace(snap.AWSIdentityCenter.Region)
	}

	if !snap.AWSIdentityCenterConfigured || !snap.AWSIdentityCenterEnabled {
		message := "AWS Identity Center is not configured yet. Add settings in Connectors."
		if snap.AWSIdentityCenterConfigured && !snap.AWSIdentityCenterEnabled {
			message = "AWS Identity Center sync is disabled. Enable it in Connectors."
		}
		data := viewmodels.UnmatchedAWSViewData{
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
			EmptyStateHref: "/settings/connectors?open=aws_identity_center",
		}
		return h.RenderComponent(c, views.UnmatchedAWSPage(data))
	}

	totalCount, err := h.Q.CountUnlinkedSourceAccountsBySourceAndQuery(ctx, gen.CountUnlinkedSourceAccountsBySourceAndQueryParams{
		SourceKind:     "aws",
		SourceName:     sourceName,
		EntityCategory: registry.EntityCategoryUser,
		Query:          query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	users, err := h.Q.ListUnlinkedSourceAccountsPageBySourceAndQuery(ctx, gen.ListUnlinkedSourceAccountsPageBySourceAndQueryParams{
		SourceKind:     "aws",
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

	emptyState := "No unmanaged AWS Identity Center users."
	if query != "" {
		emptyState = "No unmanaged AWS Identity Center users match the current search."
	}

	data := viewmodels.UnmatchedAWSViewData{
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
		EmptyStateHref: "/settings/connectors?open=aws_identity_center",
	}

	return h.RenderComponent(c, views.UnmatchedAWSPage(data))
}
