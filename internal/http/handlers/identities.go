package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleIdentities(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Identities")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	sourcePairs := availableIdentitySourcePairs(stateView)
	sourceKindOptions := identitySourceKindOptions(sourcePairs)
	queryState := querystate.ParseIdentitiesQuery(c.Request().URL.Query(), programmaticQuerySources(sourcePairs))
	sourceNameOptions := identitySourceNameOptions(queryState.Source.Kind, sourcePairs)
	page := queryState.Page
	pagination := newPaginatedListState(0, page, perPage)

	data := viewmodels.IdentitiesViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No identities found yet.", ""),
		Items:                 nil,
		Sources:               sourceKindOptions,
		SourceNameOptions:     sourceNameOptions,
		Query:                 queryState,
	}
	renderIdentities := func() error {
		if isHX(c) && isHXTarget(c, "identities-results") {
			return h.RenderComponent(c, views.IdentitiesPageResults(data))
		}
		return h.RenderComponent(c, views.IdentitiesPage(data))
	}

	if len(sourcePairs) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "Configure and enable identity connectors to populate inventory."
		return renderIdentities()
	}

	configuredSourceKinds, configuredSourceNames := identityConfiguredSourcePairs(sourcePairs)
	totalCount, err := h.Q.CountIdentitiesInventoryByFilters(ctx, gen.CountIdentitiesInventoryByFiltersParams{
		ManagedState:          queryState.ManagedState,
		PrivilegedOnly:        queryState.PrivilegedOnly,
		Status:                queryState.Status,
		ActivityState:         queryState.ActivityState,
		LinkQuality:           queryState.LinkQuality,
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
		Query:                 queryState.Q,
		IdentityType:          queryState.IdentityType,
		SourceKind:            queryState.Source.Kind,
		SourceName:            queryState.Source.Name,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination = newPaginatedListState(totalCount, page, perPage)
	rows, err := h.Q.ListIdentitiesInventoryPageByFilters(ctx, gen.ListIdentitiesInventoryPageByFiltersParams{
		ManagedState:          queryState.ManagedState,
		PrivilegedOnly:        queryState.PrivilegedOnly,
		Status:                queryState.Status,
		ActivityState:         queryState.ActivityState,
		LinkQuality:           queryState.LinkQuality,
		SortBy:                queryState.SortBy,
		SortDir:               queryState.SortDir,
		PageOffset:            int32(pagination.Offset()),
		PageLimit:             int32(perPage),
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
		Query:                 queryState.Q,
		IdentityType:          queryState.IdentityType,
		SourceKind:            queryState.Source.Kind,
		SourceName:            queryState.Source.Name,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.IdentityListItem, 0, len(rows))
	for _, row := range rows {
		linkReason := strings.TrimSpace(row.LinkReason)
		if linkReason == "" {
			linkReason = "—"
		}
		items = append(items, viewmodels.IdentityListItem{
			ID:                row.ID,
			Initials:          identityInitials(row.DisplayName, row.PrimaryEmail),
			NamePrimary:       identityNamePrimary(row.DisplayName, row.PrimaryEmail, row.ID),
			NameSecondary:     identityNameSecondary(row.DisplayName, row.PrimaryEmail),
			IdentityType:      strings.TrimSpace(row.IdentityType),
			Managed:           row.Managed,
			SourceKind:        strings.TrimSpace(row.SourceKind),
			SourceName:        strings.TrimSpace(row.SourceName),
			IntegrationsCount: row.IntegrationCount,
			PrivilegedRoles:   row.PrivilegedRoles,
			Status:            strings.TrimSpace(row.Status),
			ActivityState:     strings.TrimSpace(row.ActivityState),
			LastSeenOn:        identityCalendarDate(row.LastSeenAt),
			LastSeenRelative:  identityRelativeDate(row.LastSeenAt),
			FirstSeenOn:       identityCalendarDate(row.FirstSeenAt),
			LinkQuality:       strings.TrimSpace(row.LinkQuality),
			LinkReason:        linkReason,
			MinLinkConfidence: row.MinLinkConfidence,
			RowState:          strings.TrimSpace(row.RowState),
		})
	}

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No identities found yet.", "")
	data.HasIdentities = len(items) > 0
	if queryState.HasFilters() {
		data.PaginatedListPageData.EmptyStateMsg = "No identities match the current filters."
	}

	return renderIdentities()
}

func availableIdentitySourcePairs(stateView connectorStateView) []viewmodels.ProgrammaticSourceOption {
	out := make([]viewmodels.ProgrammaticSourceOption, 0, 7)
	appendSource := func(kind, sourceName string) {
		kind = querySourceKind(kind)
		sourceName = strings.TrimSpace(sourceName)
		if kind == "" || sourceName == "" {
			return
		}
		out = append(out, viewmodels.ProgrammaticSourceOption{
			SourceKind: kind,
			SourceName: sourceName,
			Label:      sourcePrimaryLabel(kind),
		})
	}

	okta := stateView.Okta()
	if okta.Configured() {
		appendSource("okta", okta.SourceName())
	}
	entra := stateView.Entra()
	if entra.Configured() {
		appendSource("entra", entra.SourceName())
	}
	google := stateView.GoogleWorkspace()
	if google.Configured() {
		appendSource(configstore.KindGoogleWorkspace, google.SourceName())
	}
	github := stateView.GitHub()
	if github.Configured() {
		appendSource("github", github.SourceName())
	}
	datadog := stateView.Datadog()
	if datadog.Configured() {
		appendSource("datadog", datadog.SourceName())
	}
	aws := stateView.AWSIdentityCenter()
	if aws.Configured() {
		appendSource(configstore.KindAWSIdentityCenter, aws.SourceName())
	}
	vault := stateView.Vault()
	if vault.Configured() {
		appendSource("vault", vault.SourceName())
	}

	seen := map[string]struct{}{}
	deduped := make([]viewmodels.ProgrammaticSourceOption, 0, len(out))
	for _, source := range out {
		key := source.SourceKind + "\x00" + strings.ToLower(source.SourceName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, source)
	}
	sort.Slice(deduped, func(i, j int) bool {
		if deduped[i].SourceKind == deduped[j].SourceKind {
			return strings.ToLower(deduped[i].SourceName) < strings.ToLower(deduped[j].SourceName)
		}
		return deduped[i].SourceKind < deduped[j].SourceKind
	})
	return deduped
}

func identitySourceKindOptions(sourcePairs []viewmodels.ProgrammaticSourceOption) []viewmodels.ProgrammaticSourceOption {
	seen := map[string]struct{}{}
	out := make([]viewmodels.ProgrammaticSourceOption, 0, len(sourcePairs))
	for _, source := range sourcePairs {
		kind := NormalizeConnectorKind(source.SourceKind)
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		out = append(out, viewmodels.ProgrammaticSourceOption{
			SourceKind: kind,
			Label:      sourcePrimaryLabel(kind),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Label < out[j].Label
	})
	return out
}

func identitySourceNameOptions(selectedSourceKind string, sourcePairs []viewmodels.ProgrammaticSourceOption) []viewmodels.ProgrammaticSourceOption {
	selectedSourceKind = NormalizeConnectorKind(selectedSourceKind)
	out := make([]viewmodels.ProgrammaticSourceOption, 0, len(sourcePairs))
	seenNames := map[string]struct{}{}
	for _, source := range sourcePairs {
		kind := NormalizeConnectorKind(source.SourceKind)
		if selectedSourceKind != "" && kind != selectedSourceKind {
			continue
		}
		if selectedSourceKind == "" {
			nameKey := strings.ToLower(strings.TrimSpace(source.SourceName))
			if nameKey == "" {
				continue
			}
			if _, ok := seenNames[nameKey]; ok {
				continue
			}
			seenNames[nameKey] = struct{}{}
		}
		out = append(out, viewmodels.ProgrammaticSourceOption{
			SourceKind: kind,
			SourceName: source.SourceName,
			Label:      source.SourceName,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i].SourceName) < strings.ToLower(out[j].SourceName)
	})
	return out
}

func identityConfiguredSourcePairs(sourcePairs []viewmodels.ProgrammaticSourceOption) ([]string, []string) {
	kinds := make([]string, 0, len(sourcePairs))
	names := make([]string, 0, len(sourcePairs))
	for _, source := range sourcePairs {
		kinds = append(kinds, source.SourceKind)
		names = append(names, source.SourceName)
	}
	return kinds, names
}

func identityNamePrimary(displayName, primaryEmail string, id int64) string {
	displayName = strings.TrimSpace(displayName)
	if displayName != "" {
		return displayName
	}

	primaryEmail = strings.TrimSpace(primaryEmail)
	if primaryEmail != "" {
		return primaryEmail
	}

	return fmt.Sprintf("Identity %d", id)
}

func identityNameSecondary(displayName, primaryEmail string) string {
	displayName = strings.TrimSpace(displayName)
	primaryEmail = strings.TrimSpace(primaryEmail)

	if displayName == "" || primaryEmail == "" {
		return ""
	}
	if strings.EqualFold(displayName, primaryEmail) {
		return ""
	}
	return primaryEmail
}

func identityCalendarDate(value pgtype.Timestamptz) string {
	if !value.Valid {
		return "—"
	}
	return value.Time.UTC().Format("Jan 2, 2006")
}

func identityRelativeDate(value pgtype.Timestamptz) string {
	if !value.Valid {
		return ""
	}
	days := int(time.Since(value.Time).Hours() / 24)
	switch {
	case days <= 0:
		return "today"
	case days == 1:
		return "1d ago"
	case days < 365:
		return strconv.Itoa(days) + "d ago"
	default:
		return strconv.Itoa(days/365) + "y ago"
	}
}

func identityInitials(displayName, primaryEmail string) string {
	name := strings.TrimSpace(displayName)
	if name == "" {
		name = strings.TrimSpace(primaryEmail)
	}
	if name == "" {
		return "?"
	}
	words := strings.Fields(name)
	r0, _ := utf8.DecodeRuneInString(words[0])
	if len(words) >= 2 {
		r1, _ := utf8.DecodeRuneInString(words[1])
		return strings.ToUpper(string(r0) + string(r1))
	}
	return strings.ToUpper(string(r0))
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
		entitlements, err := h.Q.ListEntitlementsForAccountIDs(ctx, accountIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, entitlement := range entitlements {
			entitlementsByAccountID[entitlement.AccountID]++
		}
	}

	linkedAccounts := make([]viewmodels.IdentityLinkedAccountView, 0, len(accounts))
	totalEntitlements := 0
	for _, account := range accounts {
		totalEntitlements += entitlementsByAccountID[account.ID]
		linkedAccounts = append(linkedAccounts, viewmodels.IdentityLinkedAccountView{
			Account:          account,
			EntitlementCount: entitlementsByAccountID[account.ID],
			DetailHref:       linkedAccountDetailHref(account),
		})
	}

	nonHumanAccessHref := ""
	if email := strings.TrimSpace(summary.PrimaryEmail); email != "" {
		nonHumanAccessHref = "/non-human-access?q=" + url.QueryEscape(email)
	}

	h.trackNonHumanAccessOutboundClick(c, "identity", summary.ID)

	return h.RenderComponent(c, views.IdentityShowPage(viewmodels.IdentityShowViewData{
		Layout:             layout,
		Identity:           summary,
		NamePrimary:        identityNamePrimary(summary.DisplayName, summary.PrimaryEmail, summary.ID),
		NameSecondary:      identityNameSecondary(summary.DisplayName, summary.PrimaryEmail),
		CreatedOn:          identityCalendarDate(summary.CreatedAt),
		UpdatedOn:          identityCalendarDate(summary.UpdatedAt),
		TotalEntitlements:  totalEntitlements,
		LinkedAccounts:     linkedAccounts,
		NonHumanAccessHref: nonHumanAccessHref,
		HasLinkedAccounts:  len(linkedAccounts) > 0,
	}))
}

func linkedAccountDetailHref(account gen.Account) string {
	sourceKind := strings.ToLower(strings.TrimSpace(account.SourceKind))
	externalID := strings.TrimSpace(account.ExternalID)

	switch sourceKind {
	case "okta":
		return "/accounts/okta/" + strconv.FormatInt(account.ID, 10)
	case "github":
		return listAccountHref("/accounts/github", externalID)
	case "datadog":
		return listAccountHref("/accounts/datadog", externalID)
	case "entra":
		return listAccountHref("/accounts/entra", externalID)
	case "aws":
		return listAccountHref("/accounts/aws", externalID)
	case "google_workspace":
		return listAccountHref("/accounts/google-workspace", externalID)
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
