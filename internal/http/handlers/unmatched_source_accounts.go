package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

const unmatchedSourceAccountsPerPage = 20

var errUnmatchedSourceAccountNotFound = errors.New("unmatched source account not found")

type unmatchedSourceNameError string

func (e unmatchedSourceNameError) Error() string {
	return string(e)
}

type unmatchedSourceAccountsResult struct {
	PageData viewmodels.UnmatchedSourceAccountsPageData
}

type unmatchedSourceAccountOptions struct {
	Title                string
	BasePath             string
	ConnectorName        string
	ConnectorKind        string
	SourceKind           string
	EntityCategory       string
	EmptyStateHref       string
	SyncedEmptyState     string
	FilteredEmptyState   string
	UnavailableMessageFn func(configured, enabled bool) string
	ResolveSourceName    func(c *echo.Context, configuredSourceName string) (string, error)
}

func (h *Handlers) buildUnmatchedSourceAccountsPage(c *echo.Context, opts unmatchedSourceAccountOptions) (unmatchedSourceAccountsResult, error) {
	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, opts.Title)
	if err != nil {
		return unmatchedSourceAccountsResult{}, err
	}

	query := querystate.ParseBasicListQuery(opts.BasePath, c.Request().URL.Query(), querystate.BasicListOptions{})
	pagination := newPaginatedListState(0, query.Page, unmatchedSourceAccountsPerPage)
	configured := stateView.Configured(opts.ConnectorKind)
	enabled := stateView.Enabled(opts.ConnectorKind)
	configuredSourceName := stateView.SourceName(opts.ConnectorKind)
	if !configured || !enabled || configuredSourceName == "" {
		return unmatchedSourceAccountsResult{
			PageData: viewmodels.UnmatchedSourceAccountsPageData{
				PaginatedListPageData: pagination.PageData(layout, 0, opts.unavailableMessage(configured, enabled), opts.EmptyStateHref),
				Query:                 query,
				HasUsers:              false,
			},
		}, nil
	}

	sourceName, err := opts.resolveSourceName(c, configuredSourceName)
	if err != nil {
		return unmatchedSourceAccountsResult{}, err
	}

	totalCount, err := h.countUnmatchedSourceAccounts(ctx, opts, sourceName, query.Q)
	if err != nil {
		return unmatchedSourceAccountsResult{}, err
	}

	pagination = newPaginatedListState(totalCount, query.Page, unmatchedSourceAccountsPerPage)
	users, err := h.listUnmatchedSourceAccounts(ctx, opts, sourceName, query.Q, pagination.Offset(), unmatchedSourceAccountsPerPage)
	if err != nil {
		return unmatchedSourceAccountsResult{}, err
	}

	emptyState := opts.SyncedEmptyState
	if query.HasFilters() {
		emptyState = opts.FilteredEmptyState
	}

	return unmatchedSourceAccountsResult{
		PageData: viewmodels.UnmatchedSourceAccountsPageData{
			PaginatedListPageData: pagination.PageData(layout, len(users), emptyState, opts.EmptyStateHref),
			Users:                 users,
			Query:                 query,
			HasUsers:              len(users) > 0,
		},
	}, nil
}

func (h *Handlers) countUnmatchedSourceAccounts(ctx context.Context, opts unmatchedSourceAccountOptions, sourceName, query string) (int64, error) {
	return h.Q.CountUnlinkedSourceAccountsBySourceAndQuery(ctx, gen.CountUnlinkedSourceAccountsBySourceAndQueryParams{
		SourceKind:     opts.SourceKind,
		SourceName:     sourceName,
		EntityCategory: opts.EntityCategory,
		Query:          query,
	})
}

func (h *Handlers) listUnmatchedSourceAccounts(ctx context.Context, opts unmatchedSourceAccountOptions, sourceName, query string, offset, limit int) ([]gen.Account, error) {
	return h.Q.ListUnlinkedSourceAccountsPageBySourceAndQuery(ctx, gen.ListUnlinkedSourceAccountsPageBySourceAndQueryParams{
		SourceKind:     opts.SourceKind,
		SourceName:     sourceName,
		EntityCategory: opts.EntityCategory,
		Query:          query,
		PageLimit:      int32(limit),
		PageOffset:     int32(offset),
	})
}

func (h *Handlers) renderUnmatchedSourceAccountsError(c *echo.Context, err error) error {
	if errors.Is(err, errUnmatchedSourceAccountNotFound) {
		return RenderNotFound(c)
	}

	var sourceErr unmatchedSourceNameError
	if errors.As(err, &sourceErr) {
		return c.String(http.StatusNotFound, sourceErr.Error())
	}

	return h.RenderError(c, err)
}

func (opts unmatchedSourceAccountOptions) unavailableMessage(configured, enabled bool) string {
	if opts.UnavailableMessageFn != nil {
		return opts.UnavailableMessageFn(configured, enabled)
	}
	return connectorUnavailableMessage(opts.ConnectorName, configured, enabled)
}

func (opts unmatchedSourceAccountOptions) resolveSourceName(c *echo.Context, configuredSourceName string) (string, error) {
	if opts.ResolveSourceName == nil {
		return configuredSourceName, nil
	}
	return opts.ResolveSourceName(c, configuredSourceName)
}

func routeParamOrWildcard(c *echo.Context, name string) string {
	value := strings.TrimSpace(c.Param(name))
	if value != "" {
		return value
	}
	return strings.Trim(c.Param("*"), "/")
}
