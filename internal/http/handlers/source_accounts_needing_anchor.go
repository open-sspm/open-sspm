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

const sourceAccountsNeedingAnchorPerPage = 20

var errSourceAccountNeedsAnchorNotFound = errors.New("source account needing anchor not found")

type sourceAccountNeedsAnchorNameError string

func (e sourceAccountNeedsAnchorNameError) Error() string {
	return string(e)
}

type sourceAccountsNeedingAnchorResult struct {
	PageData viewmodels.SourceAccountsNeedingAnchorPageData
}

type sourceAccountsNeedingAnchorOptions struct {
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

func (h *Handlers) buildSourceAccountsNeedingAnchorPage(c *echo.Context, opts sourceAccountsNeedingAnchorOptions) (sourceAccountsNeedingAnchorResult, error) {
	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, opts.Title)
	if err != nil {
		return sourceAccountsNeedingAnchorResult{}, err
	}

	query := querystate.ParseBasicListQuery(opts.BasePath, c.Request().URL.Query(), querystate.BasicListOptions{})
	pagination := newPaginatedListState(0, query.Page, sourceAccountsNeedingAnchorPerPage)
	configured := stateView.Configured(opts.ConnectorKind)
	enabled := stateView.Enabled(opts.ConnectorKind)
	configuredSourceName := stateView.SourceName(opts.ConnectorKind)
	if !configured || !enabled || configuredSourceName == "" {
		return sourceAccountsNeedingAnchorResult{
			PageData: viewmodels.SourceAccountsNeedingAnchorPageData{
				PaginatedListPageData: pagination.PageData(layout, 0, opts.unavailableMessage(configured, enabled), opts.EmptyStateHref),
				Query:                 query,
				HasUsers:              false,
			},
		}, nil
	}

	sourceName, err := opts.resolveSourceName(c, configuredSourceName)
	if err != nil {
		return sourceAccountsNeedingAnchorResult{}, err
	}

	totalCount, err := h.countSourceAccountsNeedingAnchor(ctx, opts, sourceName, query.Q)
	if err != nil {
		return sourceAccountsNeedingAnchorResult{}, err
	}

	pagination = newPaginatedListState(totalCount, query.Page, sourceAccountsNeedingAnchorPerPage)
	users, err := h.listSourceAccountsNeedingAnchor(ctx, opts, sourceName, query.Q, pagination.Offset(), sourceAccountsNeedingAnchorPerPage)
	if err != nil {
		return sourceAccountsNeedingAnchorResult{}, err
	}

	emptyState := opts.SyncedEmptyState
	if query.HasFilters() {
		emptyState = opts.FilteredEmptyState
	}

	return sourceAccountsNeedingAnchorResult{
		PageData: viewmodels.SourceAccountsNeedingAnchorPageData{
			PaginatedListPageData: pagination.PageData(layout, len(users), emptyState, opts.EmptyStateHref),
			Users:                 users,
			Query:                 query,
			HasUsers:              len(users) > 0,
		},
	}, nil
}

func (h *Handlers) countSourceAccountsNeedingAnchor(ctx context.Context, opts sourceAccountsNeedingAnchorOptions, sourceName, query string) (int64, error) {
	return h.Q.CountSourceAccountsNeedingAnchorBySourceAndQuery(ctx, gen.CountSourceAccountsNeedingAnchorBySourceAndQueryParams{
		SourceKind:     opts.SourceKind,
		SourceName:     sourceName,
		EntityCategory: opts.EntityCategory,
		Query:          query,
	})
}

func (h *Handlers) listSourceAccountsNeedingAnchor(ctx context.Context, opts sourceAccountsNeedingAnchorOptions, sourceName, query string, offset, limit int) ([]gen.Account, error) {
	return h.Q.ListSourceAccountsNeedingAnchorPageBySourceAndQuery(ctx, gen.ListSourceAccountsNeedingAnchorPageBySourceAndQueryParams{
		SourceKind:     opts.SourceKind,
		SourceName:     sourceName,
		EntityCategory: opts.EntityCategory,
		Query:          query,
		PageLimit:      int32(limit),
		PageOffset:     int32(offset),
	})
}

func (h *Handlers) renderSourceAccountsNeedingAnchorError(c *echo.Context, err error) error {
	if errors.Is(err, errSourceAccountNeedsAnchorNotFound) {
		return RenderNotFound(c)
	}

	var sourceErr sourceAccountNeedsAnchorNameError
	if errors.As(err, &sourceErr) {
		return c.String(http.StatusNotFound, sourceErr.Error())
	}

	return h.RenderError(c, err)
}

func (opts sourceAccountsNeedingAnchorOptions) unavailableMessage(configured, enabled bool) string {
	if opts.UnavailableMessageFn != nil {
		return opts.UnavailableMessageFn(configured, enabled)
	}
	return connectorUnavailableMessage(opts.ConnectorName, configured, enabled)
}

func (opts sourceAccountsNeedingAnchorOptions) resolveSourceName(c *echo.Context, configuredSourceName string) (string, error) {
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
