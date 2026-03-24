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
	if !opts.IsConfigured(snap) || !opts.IsEnabled(snap) {
		return sourceAccountInventoryResult{
			PageData: viewmodels.SourceAccountInventoryPageData{
				Layout:         layout,
				Query:          query,
				Page:           1,
				PerPage:        sourceAccountInventoryPerPage,
				TotalPages:     1,
				EmptyStateMsg:  opts.unavailableMessage(snap),
				EmptyStateHref: opts.EmptyStateHref,
			},
		}, nil
	}

	totalCount, err := opts.Count(ctx, snap, query)
	if err != nil {
		return sourceAccountInventoryResult{}, err
	}

	page := parsePageParam(c)
	page, totalPages, offset := paginate(totalCount, page, sourceAccountInventoryPerPage)
	accounts, err := opts.List(ctx, snap, query, offset, sourceAccountInventoryPerPage)
	if err != nil {
		return sourceAccountInventoryResult{}, err
	}

	showingCount := len(accounts)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)
	emptyState := opts.SyncedEmptyState
	if query != "" {
		emptyState = opts.FilteredEmptyState
	}

	return sourceAccountInventoryResult{
		PageData: viewmodels.SourceAccountInventoryPageData{
			Layout:         layout,
			Query:          query,
			ShowingCount:   showingCount,
			ShowingFrom:    showingFrom,
			ShowingTo:      showingTo,
			TotalCount:     totalCount,
			Page:           page,
			PerPage:        sourceAccountInventoryPerPage,
			TotalPages:     totalPages,
			HasAccounts:    showingCount > 0,
			EmptyStateMsg:  emptyState,
			EmptyStateHref: opts.EmptyStateHref,
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
