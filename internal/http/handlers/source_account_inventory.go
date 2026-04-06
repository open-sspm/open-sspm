package handlers

import (
	"context"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

const sourceAccountInventoryPerPage = 20

type sourceAccountInventoryAccount struct {
	ID          int64
	ExternalID  string
	Email       string
	DisplayName string
	IdentityID  int64
	Summary     sourceAccountInventorySummary
}

type sourceAccountInventorySummary struct {
	DistinctResourceCount1 int
	DistinctResourceCount2 int
	EntitlementCount1      int
}

type sourceAccountInventoryResult struct {
	PageData viewmodels.SourceAccountInventoryPageData
	Accounts []sourceAccountInventoryAccount
}

type sourceAccountInventoryOptions struct {
	Title                string
	BasePath             string
	ConnectorName        string
	ConnectorKind        string
	EmptyStateHref       string
	SyncedEmptyState     string
	FilteredEmptyState   string
	UnavailableMessageFn func(configured, enabled bool) string
	Count                func(ctx context.Context, sourceName, query string) (int64, error)
	List                 func(ctx context.Context, sourceName, query string, offset, limit int) ([]sourceAccountInventoryAccount, error)
}

type sourceAccountInventoryQueryOptions struct {
	SourceKind            string
	EntityCategory        string
	DistinctResourceKind1 string
	DistinctResourceKind2 string
	EntitlementKind1      string
}

func (h *Handlers) buildSourceAccountInventoryPage(c *echo.Context, opts sourceAccountInventoryOptions) (sourceAccountInventoryResult, error) {
	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, opts.Title)
	if err != nil {
		return sourceAccountInventoryResult{}, err
	}

	query := querystate.ParseBasicListQuery(opts.BasePath, c.Request().URL.Query(), querystate.BasicListOptions{})
	pagination := newPaginatedListState(0, query.Page, sourceAccountInventoryPerPage)
	configured := stateView.Configured(opts.ConnectorKind)
	enabled := stateView.Enabled(opts.ConnectorKind)
	sourceName := stateView.SourceName(opts.ConnectorKind)
	if !configured || !enabled || sourceName == "" {
		return sourceAccountInventoryResult{
			PageData: viewmodels.SourceAccountInventoryPageData{
				PaginatedListPageData: pagination.PageData(layout, 0, opts.unavailableMessage(configured, enabled), opts.EmptyStateHref),
				Query:                 query,
			},
		}, nil
	}

	totalCount, err := opts.Count(ctx, sourceName, query.Q)
	if err != nil {
		return sourceAccountInventoryResult{}, err
	}

	pagination = newPaginatedListState(totalCount, query.Page, sourceAccountInventoryPerPage)
	accounts, err := opts.List(ctx, sourceName, query.Q, pagination.Offset(), sourceAccountInventoryPerPage)
	if err != nil {
		return sourceAccountInventoryResult{}, err
	}

	emptyState := opts.SyncedEmptyState
	if query.HasFilters() {
		emptyState = opts.FilteredEmptyState
	}

	return sourceAccountInventoryResult{
		PageData: viewmodels.SourceAccountInventoryPageData{
			PaginatedListPageData: pagination.PageData(layout, len(accounts), emptyState, opts.EmptyStateHref),
			Query:                 query,
			HasAccounts:           len(accounts) > 0,
		},
		Accounts: accounts,
	}, nil
}

func (h *Handlers) sourceAccountInventoryQueries(opts sourceAccountInventoryQueryOptions) sourceAccountInventoryOptions {
	return sourceAccountInventoryOptions{
		Count: func(ctx context.Context, sourceName, query string) (int64, error) {
			return h.Q.CountSourceAccountsBySourceAndQuery(ctx, gen.CountSourceAccountsBySourceAndQueryParams{
				SourceKind:     opts.SourceKind,
				SourceName:     sourceName,
				EntityCategory: opts.EntityCategory,
				Query:          query,
			})
		},
		List: func(ctx context.Context, sourceName, query string, offset, limit int) ([]sourceAccountInventoryAccount, error) {
			rows, err := h.Q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, gen.ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
				SourceKind:            opts.SourceKind,
				SourceName:            sourceName,
				EntityCategory:        opts.EntityCategory,
				Query:                 query,
				PageLimit:             int32(limit),
				PageOffset:            int32(offset),
				DistinctResourceKind1: opts.DistinctResourceKind1,
				DistinctResourceKind2: opts.DistinctResourceKind2,
				EntitlementKind1:      opts.EntitlementKind1,
			})
			if err != nil {
				return nil, err
			}
			return buildSourceAccountInventoryAccounts(rows), nil
		},
	}
}

func buildSourceAccountInventoryAccounts(rows []gen.ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsRow) []sourceAccountInventoryAccount {
	accounts := make([]sourceAccountInventoryAccount, 0, len(rows))
	for _, row := range rows {
		accounts = append(accounts, sourceAccountInventoryAccount{
			ID:          row.ID,
			ExternalID:  strings.TrimSpace(row.ExternalID),
			Email:       strings.TrimSpace(row.Email),
			DisplayName: sourceAccountInventoryDisplayName(row.DisplayName, row.Email, row.ExternalID),
			IdentityID:  row.IdentityID,
			Summary: sourceAccountInventorySummary{
				DistinctResourceCount1: int(row.DistinctResourceCount1),
				DistinctResourceCount2: int(row.DistinctResourceCount2),
				EntitlementCount1:      int(row.EntitlementCount1),
			},
		})
	}
	return accounts
}

func mapSourceAccountInventoryItems[T any](accounts []sourceAccountInventoryAccount, mapItem func(sourceAccountInventoryAccount) T) []T {
	items := make([]T, 0, len(accounts))
	for _, account := range accounts {
		items = append(items, mapItem(account))
	}
	return items
}

func (opts sourceAccountInventoryOptions) unavailableMessage(configured, enabled bool) string {
	if opts.UnavailableMessageFn != nil {
		return opts.UnavailableMessageFn(configured, enabled)
	}
	return connectorUnavailableMessage(opts.ConnectorName, configured, enabled)
}

func sourceAccountInventoryDisplayName(displayName, email, externalID string) string {
	displayName = strings.TrimSpace(displayName)
	if displayName != "" {
		return displayName
	}
	email = strings.TrimSpace(email)
	if email != "" {
		return email
	}
	return strings.TrimSpace(externalID)
}
