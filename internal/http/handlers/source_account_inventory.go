package handlers

import (
	"context"
	"strings"

	"github.com/labstack/echo/v5"
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
	ConnectorName        string
	EmptyStateHref       string
	SyncedEmptyState     string
	FilteredEmptyState   string
	IsConfigured         func(snap ConnectorSnapshot) bool
	IsEnabled            func(snap ConnectorSnapshot) bool
	UnavailableMessageFn func(snap ConnectorSnapshot) string
	Count                func(ctx context.Context, snap ConnectorSnapshot, query string) (int64, error)
	List                 func(ctx context.Context, snap ConnectorSnapshot, query string, offset, limit int) ([]sourceAccountInventoryAccount, error)
}

func (h *Handlers) buildSourceAccountInventoryPage(c *echo.Context, opts sourceAccountInventoryOptions) (sourceAccountInventoryResult, error) {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, opts.Title)
	if err != nil {
		return sourceAccountInventoryResult{}, err
	}

	query := strings.TrimSpace(c.QueryParam("q"))
	pagination := newPaginatedListState(0, parsePageParam(c), sourceAccountInventoryPerPage)
	if !opts.IsConfigured(snap) || !opts.IsEnabled(snap) {
		return sourceAccountInventoryResult{
			PageData: viewmodels.SourceAccountInventoryPageData{
				PaginatedListPageData: pagination.PageData(layout, 0, opts.unavailableMessage(snap), opts.EmptyStateHref),
				Query:                 query,
			},
		}, nil
	}

	totalCount, err := opts.Count(ctx, snap, query)
	if err != nil {
		return sourceAccountInventoryResult{}, err
	}

	pagination = newPaginatedListState(totalCount, parsePageParam(c), sourceAccountInventoryPerPage)
	accounts, err := opts.List(ctx, snap, query, pagination.Offset(), sourceAccountInventoryPerPage)
	if err != nil {
		return sourceAccountInventoryResult{}, err
	}

	emptyState := opts.SyncedEmptyState
	if query != "" {
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

func (opts sourceAccountInventoryOptions) unavailableMessage(snap ConnectorSnapshot) string {
	if opts.UnavailableMessageFn != nil {
		return opts.UnavailableMessageFn(snap)
	}
	return connectorUnavailableMessage(opts.ConnectorName, opts.IsConfigured(snap), opts.IsEnabled(snap))
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
