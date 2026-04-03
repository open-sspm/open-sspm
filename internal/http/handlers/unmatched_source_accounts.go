package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
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
	ConnectorName        string
	SourceKind           string
	EntityCategory       string
	EmptyStateHref       string
	SyncedEmptyState     string
	FilteredEmptyState   string
	IsConfigured         func(snap ConnectorSnapshot) bool
	IsEnabled            func(snap ConnectorSnapshot) bool
	UnavailableMessageFn func(snap ConnectorSnapshot) string
	ResolveSourceName    func(c *echo.Context, snap ConnectorSnapshot) (string, error)
}

func (h *Handlers) buildUnmatchedSourceAccountsPage(c *echo.Context, opts unmatchedSourceAccountOptions) (unmatchedSourceAccountsResult, error) {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, opts.Title)
	if err != nil {
		return unmatchedSourceAccountsResult{}, err
	}

	query := strings.TrimSpace(c.QueryParam("q"))
	pagination := newPaginatedListState(0, parsePageParam(c), unmatchedSourceAccountsPerPage)
	if !opts.IsConfigured(snap) || !opts.IsEnabled(snap) {
		return unmatchedSourceAccountsResult{
			PageData: viewmodels.UnmatchedSourceAccountsPageData{
				PaginatedListPageData: pagination.PageData(layout, 0, opts.unavailableMessage(snap), opts.EmptyStateHref),
				Query:                 query,
				HasUsers:              false,
			},
		}, nil
	}

	sourceName, err := opts.resolveSourceName(c, snap)
	if err != nil {
		return unmatchedSourceAccountsResult{}, err
	}

	totalCount, err := h.countUnmatchedSourceAccounts(ctx, opts, sourceName, query)
	if err != nil {
		return unmatchedSourceAccountsResult{}, err
	}

	pagination = newPaginatedListState(totalCount, parsePageParam(c), unmatchedSourceAccountsPerPage)
	users, err := h.listUnmatchedSourceAccounts(ctx, opts, sourceName, query, pagination.Offset(), unmatchedSourceAccountsPerPage)
	if err != nil {
		return unmatchedSourceAccountsResult{}, err
	}

	emptyState := opts.SyncedEmptyState
	if query != "" {
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

func (opts unmatchedSourceAccountOptions) unavailableMessage(snap ConnectorSnapshot) string {
	if opts.UnavailableMessageFn != nil {
		return opts.UnavailableMessageFn(snap)
	}
	return connectorUnavailableMessage(opts.ConnectorName, opts.IsConfigured(snap), opts.IsEnabled(snap))
}

func (opts unmatchedSourceAccountOptions) resolveSourceName(c *echo.Context, snap ConnectorSnapshot) (string, error) {
	if opts.ResolveSourceName == nil {
		return "", nil
	}
	return opts.ResolveSourceName(c, snap)
}

func routeParamOrWildcard(c *echo.Context, name string) string {
	value := strings.TrimSpace(c.Param(name))
	if value != "" {
		return value
	}
	return strings.Trim(c.Param("*"), "/")
}
