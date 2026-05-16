package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/accessgraph"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	identitydomain "github.com/open-sspm/open-sspm/internal/identitydetail"
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
	queryState.IdentityType = "human"
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

	summaryRow, err := h.Q.SummarizeIdentitiesInventoryByFilters(ctx, gen.SummarizeIdentitiesInventoryByFiltersParams{
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
	data.Summary = viewmodels.IdentitiesSummary{
		Total:               summaryRow.TotalCount,
		ActionRequired:      summaryRow.ActionRequiredCount,
		Review:              summaryRow.ReviewCount,
		Privileged:          summaryRow.PrivilegedCount,
		PrivilegedUnmanaged: summaryRow.PrivilegedUnmanagedCount,
		StalePrivileged:     summaryRow.StalePrivilegedCount,
		Unmanaged:           summaryRow.UnmanagedCount,
		Suspended:           summaryRow.SuspendedCount,
		Stale:               summaryRow.StaleCount,
	}

	listParams := func(offset int32) gen.ListIdentitiesInventoryPageByFiltersParams {
		return gen.ListIdentitiesInventoryPageByFiltersParams{
			ManagedState:          queryState.ManagedState,
			PrivilegedOnly:        queryState.PrivilegedOnly,
			Status:                queryState.Status,
			ActivityState:         queryState.ActivityState,
			RowState:              queryState.RowState,
			SortBy:                queryState.SortBy,
			SortDir:               queryState.SortDir,
			PageOffset:            offset,
			PageLimit:             int32(perPage),
			ConfiguredSourceKinds: configuredSourceKinds,
			ConfiguredSourceNames: configuredSourceNames,
			Query:                 queryState.Q,
			IdentityType:          queryState.IdentityType,
			SourceKind:            queryState.Source.Kind,
			SourceName:            queryState.Source.Name,
		}
	}
	countParams := gen.CountIdentitiesInventoryByFiltersParams{
		ManagedState:          queryState.ManagedState,
		PrivilegedOnly:        queryState.PrivilegedOnly,
		Status:                queryState.Status,
		ActivityState:         queryState.ActivityState,
		RowState:              queryState.RowState,
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
		Query:                 queryState.Q,
		IdentityType:          queryState.IdentityType,
		SourceKind:            queryState.Source.Kind,
		SourceName:            queryState.Source.Name,
	}

	requestedPage := page
	if requestedPage < 1 {
		requestedPage = 1
	}
	requestedOffset, requestedOffsetOK := pageOffsetInt32(requestedPage, perPage)

	var rows []gen.ListIdentitiesInventoryPageByFiltersRow
	if requestedOffsetOK {
		rows, err = h.Q.ListIdentitiesInventoryPageByFilters(ctx, listParams(requestedOffset))
		if err != nil {
			return h.RenderError(c, err)
		}
	}

	totalCount := int64(0)
	switch {
	case len(rows) > 0:
		totalCount = rows[0].TotalCount
	case requestedPage > 1 || !requestedOffsetOK:
		totalCount, err = h.Q.CountIdentitiesInventoryByFilters(ctx, countParams)
		if err != nil {
			return h.RenderError(c, err)
		}
		pagination = newPaginatedListState(totalCount, page, perPage)
		clampedOffset, clampedOffsetOK := pageOffsetInt32(pagination.page, perPage)
		if totalCount > 0 && clampedOffsetOK && (!requestedOffsetOK || clampedOffset != requestedOffset) {
			rows, err = h.Q.ListIdentitiesInventoryPageByFilters(ctx, listParams(clampedOffset))
			if err != nil {
				return h.RenderError(c, err)
			}
		}
	}

	pagination = newPaginatedListState(totalCount, page, perPage)

	items := make([]viewmodels.IdentityListItem, 0, len(rows))
	for _, row := range rows {
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
			LastSeen:          calendarDateWithRelativeDisplay(row.LastSeenAt),
			FirstSeen:         calendarDateDisplay(row.FirstSeenAt),
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

func pageOffsetInt32(page, perPage int) (int32, bool) {
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 1
	}
	pageIndex := int64(page - 1)
	perPage64 := int64(perPage)
	if pageIndex > math.MaxInt32/perPage64 {
		return 0, false
	}
	offset := pageIndex * perPage64
	return int32(offset), true
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
	if okta.Configured() && okta.Enabled() {
		appendSource("okta", okta.SourceName())
	}
	entra := stateView.Entra()
	if entra.Configured() && entra.Enabled() {
		appendSource("entra", entra.SourceName())
	}
	google := stateView.GoogleWorkspace()
	if google.Configured() && google.Enabled() {
		appendSource(configstore.KindGoogleWorkspace, google.SourceName())
	}
	github := stateView.GitHub()
	if github.Configured() && github.Enabled() {
		appendSource("github", github.SourceName())
	}
	datadog := stateView.Datadog()
	if datadog.Configured() && datadog.Enabled() {
		appendSource("datadog", datadog.SourceName())
	}
	aws := stateView.AWSIdentityCenter()
	if aws.Configured() && aws.Enabled() {
		appendSource(configstore.KindAWSIdentityCenter, aws.SourceName())
	}
	vault := stateView.Vault()
	if vault.Configured() && vault.Enabled() {
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

func identityInitials(displayName, primaryEmail string) string {
	name := strings.TrimSpace(displayName)
	if name == "" {
		name = strings.TrimSpace(primaryEmail)
	}
	if name == "" {
		return "?"
	}
	words := strings.Fields(name)
	initials := make([]rune, 0, len(words))
	for _, word := range words {
		if initial, ok := firstInitialRune(word); ok {
			initials = append(initials, initial)
		}
	}
	if len(initials) == 0 {
		return "?"
	}
	if len(initials) == 1 {
		return strings.ToUpper(string(initials[0]))
	}
	return strings.ToUpper(string([]rune{initials[0], initials[len(initials)-1]}))
}

func firstInitialRune(word string) (rune, bool) {
	for _, r := range word {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r, true
		}
	}
	return 0, false
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

	switch strings.TrimSpace(summary.Kind) {
	case "service", "bot":
		return c.Redirect(http.StatusSeeOther, "/non-human-identities/identity-"+strconv.FormatInt(summary.ID, 10))
	}

	accounts, err := h.Q.ListLinkedAccountsForIdentity(ctx, id)
	if err != nil {
		return h.RenderError(c, err)
	}
	now := time.Now().UTC()

	accountByID := make(map[int64]gen.Account, len(accounts))
	accountDormantByID := make(map[int64]bool, len(accounts))
	accountLastSignInByID := make(map[int64]viewmodels.TimeDisplay, len(accounts))
	accountLastSignInUnixByID := make(map[int64]int64, len(accounts))
	for _, account := range accounts {
		accountByID[account.ID] = account
		accountDormantByID[account.ID] = isDormantAt(now, account.LastLoginAt, 60*24*time.Hour)
		accountLastSignInByID[account.ID] = relativeWithTitleDisplay(now, account.LastLoginAt, "—", "No account sign-in observed")
		accountLastSignInUnixByID[account.ID] = timestamptzUnix(account.LastLoginAt)
	}

	entitlementsByAccountID := make(map[int64]int, len(accounts))
	entitlementViews := []viewmodels.IdentityEntitlementView{}
	adminCount := 0
	adminByKind := map[string]int{}
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
			account, ok := accountByID[entitlement.AccountID]
			if !ok {
				continue
			}
			view := identityEntitlementView(account, entitlement, now)
			if view.IsAdmin {
				adminCount++
				adminByKind[view.Kind]++
			}
			entitlementViews = append(entitlementViews, view)
		}
	}

	linkedAccounts := make([]viewmodels.IdentityLinkedAccountView, 0, len(accounts))
	totalEntitlements := 0
	activeAccountCount := 0
	dormantAccountCount := 0
	distinctSourceKinds := make([]string, 0, len(accounts))
	seenSourceKinds := map[string]struct{}{}
	for _, account := range accounts {
		totalEntitlements += entitlementsByAccountID[account.ID]
		isActive := strings.EqualFold(strings.TrimSpace(account.Status), "ACTIVE")
		if isActive {
			activeAccountCount++
		}
		lastSignIn := accountLastSignInByID[account.ID]
		dormant := accountDormantByID[account.ID]
		if dormant {
			dormantAccountCount++
		}
		linkedAccounts = append(linkedAccounts, viewmodels.IdentityLinkedAccountView{
			Account:          account,
			EntitlementCount: entitlementsByAccountID[account.ID],
			DetailHref:       linkedAccountDetailHref(account),
			StatusActive:     isActive,
			LastSignIn:       lastSignIn,
			LastSignInUnix:   accountLastSignInUnixByID[account.ID],
			Dormant:          dormant,
		})

		kindKey := strings.ToLower(strings.TrimSpace(account.SourceKind))
		if kindKey == "" {
			continue
		}
		if _, ok := seenSourceKinds[kindKey]; ok {
			continue
		}
		seenSourceKinds[kindKey] = struct{}{}
		distinctSourceKinds = append(distinctSourceKinds, strings.TrimSpace(account.SourceKind))
	}

	nonHumanIdentitiesHref := ""
	if email := strings.TrimSpace(summary.PrimaryEmail); email != "" {
		nonHumanIdentitiesHref = "/non-human-identities?q=" + url.QueryEscape(email)
	}

	h.trackNonHumanIdentitiesOutboundClick(c, "identity", summary.ID)

	namePrimary := identityNamePrimary(summary.DisplayName, summary.PrimaryEmail, summary.ID)
	statusLabel, statusTone := identityStatusFromAccounts(accounts)
	groupMode := viewmodels.IdentityEntitlementGroupResource
	accountSortMode := viewmodels.ParseLinkedAccountSortMode(c.QueryParam("account_sort"))
	accountQuery := strings.TrimSpace(c.QueryParam("account_q"))
	entitlementQuery := strings.TrimSpace(c.QueryParam("entitlement_q"))
	entitlementAdminOnly := isTruthyParam(c.QueryParam("admin"))
	entitlementDormantOnly := isTruthyParam(c.QueryParam("dormant"))
	entitlementSourceFilter := normalizeSourceKindFilter(c.QueryParam("source_kind"), distinctSourceKinds)
	basePath := "/identities/" + strconv.FormatInt(summary.ID, 10)

	query := viewmodels.IdentityShowQuery{
		Group:              groupMode,
		AccountQuery:       accountQuery,
		EntitlementQuery:   entitlementQuery,
		AccountSort:        accountSortMode,
		EntitlementAdmin:   entitlementAdminOnly,
		EntitlementDormant: entitlementDormantOnly,
		EntitlementSource:  entitlementSourceFilter,
	}

	identityTypeLabel := views.HumanizeIdentityType(summary.Kind)
	breadcrumbKindLabel, breadcrumbKindHref := identityBreadcrumbKind(summary.Kind)
	profileHints := identityProfileHints(accounts)
	lastActive := relativeWithTitleDisplay(now, maxIdentityActivity(accounts), "—", "No activity observed")
	profileFacts := identityProfileFacts(summary, profileHints, lastActive)
	reviewSummary := identityReviewSummary(summary.Managed, totalEntitlements, adminCount, dormantAccountCount, lastActive)

	summaryTiles := identitydomain.BuildSummaryTiles(
		len(linkedAccounts),
		totalEntitlements,
		adminCount,
		dormantAccountCount,
		distinctSourceKinds,
		len(distinctSourceKinds),
		adminByKind,
	)
	sortLinkedAccounts(linkedAccounts, accountSortMode)
	filteredLinkedAccounts := filterLinkedAccounts(linkedAccounts, accountQuery)
	filteredEntitlements := filterIdentityEntitlements(entitlementViews, entitlementQuery, entitlementAdminOnly, entitlementDormantOnly, entitlementSourceFilter)
	adminScopeSummary := identitydomain.SummarizeAdminByKind(adminByKind)

	entitlementSourceOptions := buildEntitlementSourceOptions(distinctSourceKinds, entitlementSourceFilter)
	entitlementFilterCount := 0
	if entitlementAdminOnly {
		entitlementFilterCount++
	}
	if entitlementDormantOnly {
		entitlementFilterCount++
	}
	if entitlementSourceFilter != "" {
		entitlementFilterCount++
	}
	entitlementFilterChips := []viewmodels.IdentityFilterChip{}
	if entitlementAdminOnly {
		withoutAdmin := query
		withoutAdmin.EntitlementAdmin = false
		entitlementFilterChips = append(entitlementFilterChips, viewmodels.IdentityFilterChip{
			Label:     "Admin only",
			ClearHref: viewmodels.BuildIdentityShowHref(basePath, withoutAdmin),
		})
	}
	if entitlementDormantOnly {
		withoutDormant := query
		withoutDormant.EntitlementDormant = false
		entitlementFilterChips = append(entitlementFilterChips, viewmodels.IdentityFilterChip{
			Label:     "Dormant account grants",
			ClearHref: viewmodels.BuildIdentityShowHref(basePath, withoutDormant),
		})
	}
	if entitlementSourceFilter != "" {
		withoutSource := query
		withoutSource.EntitlementSource = ""
		entitlementFilterChips = append(entitlementFilterChips, viewmodels.IdentityFilterChip{
			Label:     "Source: " + entitlementSourceFilterLabel(entitlementSourceOptions, entitlementSourceFilter),
			ClearHref: viewmodels.BuildIdentityShowHref(basePath, withoutSource),
		})
	}

	clearSearchQuery := query
	clearSearchQuery.EntitlementQuery = ""

	clearFiltersQuery := query
	clearFiltersQuery.EntitlementAdmin = false
	clearFiltersQuery.EntitlementDormant = false
	clearFiltersQuery.EntitlementSource = ""

	clearAccountSearchQuery := query
	clearAccountSearchQuery.AccountQuery = ""

	entitlementFormHiddenInputs := []viewmodels.IdentityFormHiddenInput{}
	if accountQuery != "" {
		entitlementFormHiddenInputs = append(entitlementFormHiddenInputs, viewmodels.IdentityFormHiddenInput{Name: "account_q", Value: accountQuery})
	}
	if accountSortMode != viewmodels.IdentityLinkedAccountSortGrants {
		entitlementFormHiddenInputs = append(entitlementFormHiddenInputs, viewmodels.IdentityFormHiddenInput{Name: "account_sort", Value: string(accountSortMode)})
	}

	hasEntitlementFilter := entitlementQuery != "" || entitlementFilterCount > 0

	return h.RenderComponent(c, views.IdentityShowPage(viewmodels.IdentityShowViewData{
		Layout: layout,
		Breadcrumb: viewmodels.IdentityShowBreadcrumb{
			KindLabel: breadcrumbKindLabel,
			KindHref:  breadcrumbKindHref,
			Current:   namePrimary,
		},
		Profile: viewmodels.IdentityShowProfile{
			Identity:               summary,
			NamePrimary:            namePrimary,
			NameSecondary:          identityNameSecondary(summary.DisplayName, summary.PrimaryEmail),
			Initials:               identityInitials(summary.DisplayName, summary.PrimaryEmail),
			AvatarClass:            views.AppAvatarClass(namePrimary),
			StatusLabel:            statusLabel,
			StatusTone:             statusTone,
			IdentityTypeLabel:      identityTypeLabel,
			Tags:                   profileHints.Tags,
			AdminScopeSummary:      adminScopeSummary,
			ReviewSummary:          reviewSummary,
			Facts:                  profileFacts,
			CreatedOn:              calendarDateDisplay(summary.CreatedAt),
			UpdatedOn:              calendarDateDisplay(summary.UpdatedAt),
			NonHumanIdentitiesHref: nonHumanIdentitiesHref,
		},
		Summary: viewmodels.IdentityShowSummary{
			Tiles: summaryTiles,
		},
		LinkedAccounts: viewmodels.IdentityShowLinkedAccountsPanel{
			Total:          len(linkedAccounts),
			Active:         activeAccountCount,
			Dormant:        dormantAccountCount,
			Visible:        len(filteredLinkedAccounts),
			Items:          filteredLinkedAccounts,
			Query:          accountQuery,
			QueryClearHref: viewmodels.BuildIdentityShowHref(basePath, clearAccountSearchQuery),
			SortMode:       accountSortMode,
			HasItems:       len(filteredLinkedAccounts) > 0,
			HasFilter:      accountQuery != "",
		},
		Entitlements: viewmodels.IdentityShowEntitlementsPanel{
			Total:            totalEntitlements,
			Visible:          len(filteredEntitlements),
			Items:            filteredEntitlements,
			Query:            entitlementQuery,
			QueryClearHref:   viewmodels.BuildIdentityShowHref(basePath, clearSearchQuery),
			AdminOnly:        entitlementAdminOnly,
			DormantOnly:      entitlementDormantOnly,
			SourceFilter:     entitlementSourceFilter,
			SourceOptions:    entitlementSourceOptions,
			FilterCount:      entitlementFilterCount,
			FilterChips:      entitlementFilterChips,
			ClearFiltersHref: viewmodels.BuildIdentityShowHref(basePath, clearFiltersQuery),
			FormHiddenInputs: entitlementFormHiddenInputs,
			Groups:           identitydomain.BuildEntitlementGroups(filteredEntitlements, groupMode),
			HasItems:         len(filteredEntitlements) > 0,
			HasFilter:        hasEntitlementFilter,
		},
	}))
}

func identityStatusFromAccounts(accounts []gen.Account) (string, string) {
	hasActive := false
	hasSuspended := false
	for _, account := range accounts {
		switch strings.ToUpper(strings.TrimSpace(account.Status)) {
		case "ACTIVE":
			hasActive = true
		case "SUSPENDED", "INACTIVE", "DISABLED":
			hasSuspended = true
		}
	}
	switch {
	case hasActive:
		return "Active", "active"
	case hasSuspended:
		return "Suspended", "warn"
	default:
		return "", ""
	}
}

func identityBreadcrumbKind(kind string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "human":
		return "Human", "/identities"
	case "service":
		return "Service", "/non-human-identities"
	case "bot":
		return "Bot", "/non-human-identities"
	default:
		return views.HumanizeIdentityType(kind), ""
	}
}

type identityProfileHintSet struct {
	Manager  string
	MFA      string
	Location string
	Tags     []string
}

func identityProfileFacts(summary gen.GetIdentitySummaryByIDRow, hints identityProfileHintSet, lastActive viewmodels.TimeDisplay) []viewmodels.IdentityProfileFact {
	facts := make([]viewmodels.IdentityProfileFact, 0, 6)
	appendFact := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		facts = append(facts, viewmodels.IdentityProfileFact{Label: label, Value: value})
	}

	appendFact("Manager", hints.Manager)
	appendFact("Joined", calendarDateDisplay(summary.CreatedAt).Label)
	appendFact("Last active", lastActive.Label)
	appendFact("MFA", hints.MFA)
	appendFact("Location", hints.Location)
	appendFact("Updated", calendarDateDisplay(summary.UpdatedAt).Label)
	return facts
}

func identityReviewSummary(managed bool, totalEntitlements, adminCount, dormantAccountCount int, lastActive viewmodels.TimeDisplay) viewmodels.IdentityReviewSummary {
	lastActiveLabel := strings.TrimSpace(lastActive.Label)
	if lastActiveLabel == "" || lastActiveLabel == "—" {
		lastActiveLabel = "no activity observed"
	} else {
		lastActiveLabel = "last active " + lastActiveLabel
	}

	grantLabel := countNoun(totalEntitlements, "grant", "grants")
	switch {
	case adminCount > 0:
		return viewmodels.IdentityReviewSummary{
			Label:  "Privileged access",
			Detail: countNoun(adminCount, "admin scope", "admin scopes") + " · " + grantLabel + " · " + lastActiveLabel,
			Tone:   "danger",
		}
	case dormantAccountCount > 0:
		return viewmodels.IdentityReviewSummary{
			Label:  "Review recommended",
			Detail: countNoun(dormantAccountCount, "dormant account", "dormant accounts") + " · " + grantLabel + " · " + lastActiveLabel,
			Tone:   "warn",
		}
	case !managed:
		return viewmodels.IdentityReviewSummary{
			Label:  "Ownership check",
			Detail: "unmanaged identity · " + grantLabel + " · " + lastActiveLabel,
			Tone:   "warn",
		}
	default:
		return viewmodels.IdentityReviewSummary{
			Label:  "No review needed",
			Detail: grantLabel + " · " + lastActiveLabel,
			Tone:   "ok",
		}
	}
}

func countNoun(count int, singular, plural string) string {
	if count == 1 {
		return "1 " + singular
	}
	return strconv.Itoa(count) + " " + plural
}

func identityProfileHints(accounts []gen.Account) identityProfileHintSet {
	hints := identityProfileHintSet{}
	seenTags := map[string]struct{}{}

	addTag := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		key := strings.ToLower(value)
		if _, ok := seenTags[key]; ok {
			return
		}
		seenTags[key] = struct{}{}
		hints.Tags = append(hints.Tags, value)
	}

	for _, account := range accounts {
		raw := decodeRawJSONObject(account.RawJson)
		if len(raw) == 0 {
			continue
		}
		if hints.Manager == "" {
			hints.Manager = firstJSONText(raw,
				"manager",
				"managerName",
				"manager_name",
				"managerDisplayName",
				"manager_display_name",
				"managerEmail",
				"manager_email",
			)
		}
		if hints.MFA == "" {
			hints.MFA = firstMFAText(raw,
				"mfa",
				"mfaMethod",
				"mfa_method",
				"mfaMethods",
				"mfa_methods",
				"factors",
				"strongAuthenticationMethods",
				"strong_authentication_methods",
			)
		}
		if hints.Location == "" {
			hints.Location = firstJSONText(raw,
				"location",
				"officeLocation",
				"office_location",
				"city",
				"country",
				"usageLocation",
				"usage_location",
			)
		}

		department := firstJSONText(raw, "department", "team", "division")
		title := firstJSONText(raw, "jobTitle", "job_title", "title")
		switch {
		case department != "" && title != "":
			addTag(department + " · " + title)
		case department != "":
			addTag(department)
		case title != "":
			addTag(title)
		}
	}

	if len(hints.Tags) > 3 {
		hints.Tags = hints.Tags[:3]
	}
	return hints
}

func decodeRawJSONObject(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func firstJSONText(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key]; ok {
			if text := jsonValueText(value); text != "" {
				return text
			}
		}
	}
	return ""
}

func firstMFAText(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := raw[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case bool:
			if typed {
				return "Enabled"
			}
			return "Not enrolled"
		case []any:
			labels := make([]string, 0, 2)
			for _, item := range typed {
				if text := jsonValueText(item); text != "" {
					labels = append(labels, text)
				}
				if len(labels) == 2 {
					break
				}
			}
			if len(labels) > 0 {
				if len(typed) > len(labels) {
					return strings.Join(labels, " · ") + " · +" + strconv.Itoa(len(typed)-len(labels))
				}
				return strings.Join(labels, " · ")
			}
		default:
			if text := jsonValueText(value); text != "" {
				return text
			}
		}
	}
	return ""
}

func jsonValueText(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		if typed {
			return "Yes"
		}
		return "No"
	case map[string]any:
		return firstJSONText(typed, "displayName", "display_name", "name", "email", "mail", "userPrincipalName", "user_principal_name")
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := jsonValueText(item); text != "" {
				parts = append(parts, text)
			}
			if len(parts) == 2 {
				break
			}
		}
		if len(parts) == 0 {
			return ""
		}
		if len(typed) > len(parts) {
			return strings.Join(parts, " · ") + " · +" + strconv.Itoa(len(typed)-len(parts))
		}
		return strings.Join(parts, " · ")
	default:
		return ""
	}
}

func maxIdentityActivity(accounts []gen.Account) pgtype.Timestamptz {
	var maxValue pgtype.Timestamptz
	for _, account := range accounts {
		value := account.LastLoginAt
		if !value.Valid {
			value = account.LastObservedAt
		}
		if !value.Valid {
			continue
		}
		if !maxValue.Valid || value.Time.After(maxValue.Time) {
			maxValue = value
		}
	}
	return maxValue
}

func timestamptzUnix(value pgtype.Timestamptz) int64 {
	if !value.Valid {
		return 0
	}
	return value.Time.Unix()
}

func isDormantAt(now time.Time, value pgtype.Timestamptz, threshold time.Duration) bool {
	if !value.Valid {
		return false
	}
	return identitydomain.IsDormantAt(now, value.Time, threshold)
}

func sortLinkedAccounts(accounts []viewmodels.IdentityLinkedAccountView, mode viewmodels.IdentityLinkedAccountSortMode) {
	sort.SliceStable(accounts, func(i, j int) bool {
		left := accounts[i]
		right := accounts[j]
		switch mode {
		case viewmodels.IdentityLinkedAccountSortSource:
			if strings.ToLower(left.Account.SourceKind) != strings.ToLower(right.Account.SourceKind) {
				return strings.ToLower(left.Account.SourceKind) < strings.ToLower(right.Account.SourceKind)
			}
			if strings.ToLower(left.Account.SourceName) != strings.ToLower(right.Account.SourceName) {
				return strings.ToLower(left.Account.SourceName) < strings.ToLower(right.Account.SourceName)
			}
		case viewmodels.IdentityLinkedAccountSortActivity:
			if left.LastSignInUnix != right.LastSignInUnix {
				return left.LastSignInUnix > right.LastSignInUnix
			}
		default:
			if left.EntitlementCount != right.EntitlementCount {
				return left.EntitlementCount > right.EntitlementCount
			}
		}
		if left.Dormant != right.Dormant {
			return left.Dormant
		}
		return strings.ToLower(linkedAccountLabel(left.Account)) < strings.ToLower(linkedAccountLabel(right.Account))
	})
}

func filterLinkedAccounts(accounts []viewmodels.IdentityLinkedAccountView, query string) []viewmodels.IdentityLinkedAccountView {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return accounts
	}
	out := make([]viewmodels.IdentityLinkedAccountView, 0, len(accounts))
	for _, account := range accounts {
		haystack := strings.ToLower(strings.Join([]string{
			views.HumanizeConnectorKind(account.Account.SourceKind),
			account.Account.SourceKind,
			account.Account.SourceName,
			account.Account.ExternalID,
			account.Account.Email,
			account.Account.DisplayName,
			account.Account.Status,
		}, " "))
		if strings.Contains(haystack, query) {
			out = append(out, account)
		}
	}
	return out
}

func filterIdentityEntitlements(entitlements []viewmodels.IdentityEntitlementView, query string, adminOnly, dormantOnly bool, sourceKind string) []viewmodels.IdentityEntitlementView {
	query = strings.ToLower(strings.TrimSpace(query))
	sourceKind = strings.ToLower(strings.TrimSpace(sourceKind))
	if query == "" && !adminOnly && !dormantOnly && sourceKind == "" {
		return entitlements
	}
	out := make([]viewmodels.IdentityEntitlementView, 0, len(entitlements))
	for _, ent := range entitlements {
		if adminOnly && !ent.IsAdmin {
			continue
		}
		if dormantOnly && !ent.Dormant {
			continue
		}
		if sourceKind != "" && strings.ToLower(strings.TrimSpace(ent.AccountSourceKind)) != sourceKind {
			continue
		}
		if query != "" {
			haystack := strings.ToLower(strings.Join([]string{
				views.HumanizeConnectorKind(ent.AccountSourceKind),
				ent.AccountSourceKind,
				ent.AccountSourceName,
				ent.AccountLabel,
				ent.Kind,
				ent.ResourceKind,
				ent.ResourceID,
				ent.ResourceLabel,
				ent.Permission,
			}, " "))
			if !strings.Contains(haystack, query) {
				continue
			}
		}
		out = append(out, ent)
	}
	return out
}

func isTruthyParam(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func normalizeSourceKindFilter(raw string, allowed []string) string {
	candidate := strings.ToLower(strings.TrimSpace(raw))
	if candidate == "" {
		return ""
	}
	for _, kind := range allowed {
		if strings.ToLower(strings.TrimSpace(kind)) == candidate {
			return strings.TrimSpace(kind)
		}
	}
	return ""
}

func buildEntitlementSourceOptions(distinctSourceKinds []string, active string) []viewmodels.IdentitySourceFilterOption {
	if len(distinctSourceKinds) == 0 {
		return nil
	}
	options := make([]viewmodels.IdentitySourceFilterOption, 0, len(distinctSourceKinds))
	activeKey := strings.ToLower(strings.TrimSpace(active))
	for _, kind := range distinctSourceKinds {
		trimmed := strings.TrimSpace(kind)
		if trimmed == "" {
			continue
		}
		options = append(options, viewmodels.IdentitySourceFilterOption{
			Value:    trimmed,
			Label:    views.HumanizeConnectorKind(trimmed),
			Selected: strings.ToLower(trimmed) == activeKey,
		})
	}
	sort.SliceStable(options, func(i, j int) bool {
		return strings.ToLower(options[i].Label) < strings.ToLower(options[j].Label)
	})
	return options
}

func entitlementSourceFilterLabel(options []viewmodels.IdentitySourceFilterOption, value string) string {
	valueKey := strings.ToLower(strings.TrimSpace(value))
	for _, opt := range options {
		if strings.ToLower(strings.TrimSpace(opt.Value)) == valueKey && strings.TrimSpace(opt.Label) != "" {
			return opt.Label
		}
	}
	if label := views.HumanizeConnectorKind(value); strings.TrimSpace(label) != "" {
		return label
	}
	return strings.TrimSpace(value)
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

func identityEntitlementView(account gen.Account, ent gen.ListEntitlementsForAccountIDsRow, now time.Time) viewmodels.IdentityEntitlementView {
	resourceKind, resourceID, ok := accessgraph.ParseCanonicalResourceRef(ent.Resource)
	if !ok {
		resourceID = strings.TrimSpace(ent.Resource)
	}

	resourceHref := ""
	if ok {
		resourceHref = accessgraph.BuildResourceHref(account.SourceKind, account.SourceName, resourceKind, resourceID)
	}

	resourceLabel := strings.TrimSpace(accessgraph.DisplayResourceLabel(ent.Resource, ent.RawJson))
	if resourceLabel == "" {
		resourceLabel = resourceID
	}
	if resourceLabel == "" {
		resourceLabel = "(unknown resource)"
	}

	return viewmodels.IdentityEntitlementView{
		AccountLabel:        linkedAccountLabel(account),
		AccountHref:         linkedAccountDetailHref(account),
		AccountSourceKind:   strings.TrimSpace(account.SourceKind),
		AccountSourceName:   strings.TrimSpace(account.SourceName),
		Kind:                strings.TrimSpace(ent.Kind),
		ResourceKind:        resourceKind,
		ResourceID:          resourceID,
		ResourceLabel:       resourceLabel,
		ResourceHref:        resourceHref,
		Permission:          accessgraph.DisplayEntitlementPermission(ent.Kind, ent.Permission, ent.RawJson),
		IsAdmin:             identityEntitlementIsAdmin(ent),
		AccountLastSignIn:   relativeWithTitleDisplay(now, account.LastLoginAt, "—", "No account sign-in observed"),
		AccountActivityUnix: timestamptzUnix(account.LastLoginAt),
		Dormant:             isDormantAt(now, account.LastLoginAt, 60*24*time.Hour),
	}
}

func identityEntitlementIsAdmin(ent gen.ListEntitlementsForAccountIDsRow) bool {
	return identitydomain.IsPrivilegedEntitlement(ent.Permission, ent.RawJson)
}

func linkedAccountLabel(account gen.Account) string {
	if value := strings.TrimSpace(account.Email); value != "" {
		return value
	}
	if value := strings.TrimSpace(account.ExternalID); value != "" {
		return value
	}
	if value := strings.TrimSpace(account.DisplayName); value != "" {
		return value
	}
	return "Account " + strconv.FormatInt(account.ID, 10)
}

func listAccountHref(basePath, externalID string) string {
	externalID = strings.TrimSpace(externalID)
	if externalID == "" {
		return basePath
	}
	return basePath + "?q=" + url.QueryEscape(externalID)
}
