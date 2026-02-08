package handlers

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleIdentities(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Identities")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)

	totalCount, err := h.Q.CountIdentitiesByQuery(ctx, query)
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	identities, err := h.Q.ListIdentitiesPageByQuery(ctx, gen.ListIdentitiesPageByQueryParams{
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	showingCount := len(identities)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	emptyState := "No identities found yet."
	if query != "" {
		emptyState = "No identities match the current search."
	}

	data := viewmodels.IdentitiesViewData{
		Layout:        layout,
		Identities:    identities,
		Query:         query,
		ShowingCount:  showingCount,
		ShowingFrom:   showingFrom,
		ShowingTo:     showingTo,
		TotalCount:    totalCount,
		Page:          page,
		PerPage:       perPage,
		TotalPages:    totalPages,
		HasIdentities: showingCount > 0,
		EmptyStateMsg: emptyState,
	}

	if isHX(c) && isHXTarget(c, "identities-results") {
		return h.RenderComponent(c, views.IdentitiesPageResults(data))
	}
	return h.RenderComponent(c, views.IdentitiesPage(data))
}

func (h *Handlers) HandleIdentityShow(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Identity")
	if err != nil {
		return h.RenderError(c, err)
	}

	id, err := strconv.ParseInt(strings.TrimSpace(c.Param("id")), 10, 64)
	if err != nil || id <= 0 {
		return c.String(http.StatusBadRequest, "invalid identity id")
	}

	summary, err := h.Q.GetIdentitySummaryByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return c.String(http.StatusNotFound, "identity not found")
		}
		return h.RenderError(c, err)
	}

	accounts, err := h.Q.ListLinkedAccountsForIdentity(ctx, id)
	if err != nil {
		return h.RenderError(c, err)
	}

	entitlementsByAccountID := make(map[int64]int, len(accounts))
	if len(accounts) > 0 {
		accountIDs := make([]int64, 0, len(accounts))
		for _, account := range accounts {
			accountIDs = append(accountIDs, account.ID)
		}
		entitlements, err := h.Q.ListEntitlementsForAppUserIDs(ctx, accountIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, entitlement := range entitlements {
			entitlementsByAccountID[entitlement.AppUserID]++
		}
	}

	linkedAccounts := make([]viewmodels.IdentityLinkedAccountView, 0, len(accounts))
	for _, account := range accounts {
		linkedAccounts = append(linkedAccounts, viewmodels.IdentityLinkedAccountView{
			Account:          account,
			EntitlementCount: entitlementsByAccountID[account.ID],
			DetailHref:       linkedAccountDetailHref(account),
		})
	}

	programmaticAccessHref := ""
	if email := strings.TrimSpace(summary.PrimaryEmail); email != "" {
		programmaticAccessHref = "/credentials?q=" + url.QueryEscape(email)
	}

	return h.RenderComponent(c, views.IdentityShowPage(viewmodels.IdentityShowViewData{
		Layout:                 layout,
		Identity:               summary,
		LinkedAccounts:         linkedAccounts,
		ProgrammaticAccessHref: programmaticAccessHref,
		HasLinkedAccounts:      len(linkedAccounts) > 0,
	}))
}

func linkedAccountDetailHref(account gen.Account) string {
	sourceKind := strings.ToLower(strings.TrimSpace(account.SourceKind))
	externalID := strings.TrimSpace(account.ExternalID)

	switch sourceKind {
	case "okta":
		return "/idp-users/" + strconv.FormatInt(account.ID, 10)
	case "github":
		return listAccountHref("/github-users", externalID)
	case "datadog":
		return listAccountHref("/datadog-users", externalID)
	case "entra":
		return listAccountHref("/entra-users", externalID)
	case "aws":
		return listAccountHref("/aws-users", externalID)
	default:
		return ""
	}
}

func listAccountHref(basePath, externalID string) string {
	externalID = strings.TrimSpace(externalID)
	if externalID == "" {
		return basePath
	}
	return basePath + "?q=" + url.QueryEscape(externalID)
}
