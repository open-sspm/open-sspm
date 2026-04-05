package handlers

import (
	"context"
	"strings"

	"github.com/labstack/echo/v5"
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
	SourceKind           string
	EmptyStateHref       string
	SyncedEmptyState     string
	FilteredEmptyState   string
	UnavailableMessageFn func(configured, enabled bool) string
	Count                func(ctx context.Context, sourceName, query string) (int64, error)
	List                 func(ctx context.Context, sourceName, query string, offset, limit int) ([]sourceAccountInventoryAccount, error)
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
