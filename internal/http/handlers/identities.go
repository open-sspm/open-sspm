package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleIdentities(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Identities")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	sourcePairs := availableIdentitySourcePairs(snap)
	sourceKindOptions := identitySourceKindOptions(sourcePairs)
	selectedSourceKind, selectedSourceName := normalizeIdentitySourceSelection(
		c.QueryParam("source_kind"),
		c.QueryParam("source_name"),
		sourcePairs,
	)
	sourceNameOptions := identitySourceNameOptions(selectedSourceKind, sourcePairs)

	query := strings.TrimSpace(c.QueryParam("q"))
	identityType := normalizeIdentityType(c.QueryParam("identity_type"))
	managedState := normalizeIdentityManagedState(c.QueryParam("managed_state"))
	privilegedOnly := parseIdentityBool(c.QueryParam("privileged"))
	status := normalizeIdentityStatus(c.QueryParam("status"))
	activityState := normalizeIdentityActivityState(c.QueryParam("activity_state"))
	linkQuality := normalizeIdentityLinkQuality(c.QueryParam("link_quality"))
	sortBy := normalizeIdentitySortBy(c.QueryParam("sort_by"))
	sortDir := normalizeIdentitySortDir(c.QueryParam("sort_dir"), sortBy)
	showFirstSeen := parseIdentityBool(c.QueryParam("show_first_seen"))
	showLinkQuality := parseIdentityBool(c.QueryParam("show_link_quality"))
	showLinkReason := parseIdentityBool(c.QueryParam("show_link_reason"))
	page := parsePageParam(c)

	data := viewmodels.IdentitiesViewData{
		Layout:             layout,
		Items:              nil,
		Sources:            sourceKindOptions,
		SourceNameOptions:  sourceNameOptions,
		SelectedSourceKind: selectedSourceKind,
		SelectedSourceName: selectedSourceName,
		Query:              query,
		IdentityType:       identityType,
		ManagedState:       managedState,
		PrivilegedOnly:     privilegedOnly,
		Status:             status,
		ActivityState:      activityState,
		LinkQuality:        linkQuality,
		SortBy:             sortBy,
		SortDir:            sortDir,
		ShowFirstSeen:      showFirstSeen,
		ShowLinkQuality:    showLinkQuality,
		ShowLinkReason:     showLinkReason,
		Page:               1,
		PerPage:            perPage,
		TotalPages:         1,
		EmptyStateMsg:      "No identities found yet.",
	}
	renderIdentities := func() error {
		if isHX(c) && isHXTarget(c, "identities-results") {
			return h.RenderComponent(c, views.IdentitiesPageResults(data))
		}
		return h.RenderComponent(c, views.IdentitiesPage(data))
	}

	if len(sourcePairs) == 0 {
		data.EmptyStateMsg = "Configure and enable identity connectors to populate inventory."
		return renderIdentities()
	}

	configuredSourceKinds, configuredSourceNames := identityConfiguredSourcePairs(sourcePairs)
	totalCount, err := h.Q.CountIdentitiesInventoryByFilters(ctx, gen.CountIdentitiesInventoryByFiltersParams{
		ManagedState:          managedState,
		PrivilegedOnly:        privilegedOnly,
		Status:                status,
		ActivityState:         activityState,
		LinkQuality:           linkQuality,
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
		Query:                 query,
		IdentityType:          identityType,
		SourceKind:            selectedSourceKind,
		SourceName:            selectedSourceName,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	rows, err := h.Q.ListIdentitiesInventoryPageByFilters(ctx, gen.ListIdentitiesInventoryPageByFiltersParams{
		ManagedState:          managedState,
		PrivilegedOnly:        privilegedOnly,
		Status:                status,
		ActivityState:         activityState,
		LinkQuality:           linkQuality,
		SortBy:                sortBy,
		SortDir:               sortDir,
		PageOffset:            int32(offset),
		PageLimit:             int32(perPage),
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
		Query:                 query,
		IdentityType:          identityType,
		SourceKind:            selectedSourceKind,
		SourceName:            selectedSourceName,
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
			FirstSeenOn:       identityCalendarDate(row.FirstSeenAt),
			LinkQuality:       strings.TrimSpace(row.LinkQuality),
			LinkReason:        linkReason,
			MinLinkConfidence: row.MinLinkConfidence,
			RowState:          strings.TrimSpace(row.RowState),
		})
	}

	showingCount := len(items)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	data.Items = items
	data.ShowingCount = showingCount
	data.ShowingFrom = showingFrom
	data.ShowingTo = showingTo
	data.TotalCount = totalCount
	data.Page = page
	data.TotalPages = totalPages
	data.HasIdentities = showingCount > 0
	if identitiesFilterActive(query, identityType, managedState, privilegedOnly, status, activityState, linkQuality, selectedSourceKind, selectedSourceName) {
		data.EmptyStateMsg = "No identities match the current filters."
	}

	return renderIdentities()
}

func availableIdentitySourcePairs(snap ConnectorSnapshot) []viewmodels.ProgrammaticSourceOption {
	out := make([]viewmodels.ProgrammaticSourceOption, 0, 6)
	appendSource := func(kind, sourceName string) {
		kind = NormalizeConnectorKind(kind)
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

	if snap.OktaConfigured {
		appendSource("okta", snap.Okta.Domain)
	}
	if snap.EntraConfigured {
		appendSource("entra", snap.Entra.TenantID)
	}
	if snap.GitHubConfigured {
		appendSource("github", snap.GitHub.Org)
	}
	if snap.DatadogConfigured {
		appendSource("datadog", snap.Datadog.Site)
	}
	if snap.AWSIdentityCenterConfigured {
		sourceName := strings.TrimSpace(snap.AWSIdentityCenter.Name)
		if sourceName == "" {
			sourceName = strings.TrimSpace(snap.AWSIdentityCenter.Region)
		}
		appendSource("aws", sourceName)
	}
	if snap.VaultConfigured {
		appendSource("vault", snap.Vault.SourceName())
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

func normalizeIdentitySourceSelection(rawKind, rawName string, sourcePairs []viewmodels.ProgrammaticSourceOption) (string, string) {
	selectedKind := NormalizeConnectorKind(rawKind)
	selectedName := strings.TrimSpace(rawName)
	if selectedKind != "" && !identityHasSourceKind(selectedKind, sourcePairs) {
		selectedKind = ""
	}
	if selectedKind == "" && selectedName != "" {
		matchedKinds := map[string]struct{}{}
		canonicalName := ""
		for _, source := range sourcePairs {
			if strings.EqualFold(source.SourceName, selectedName) {
				kind := NormalizeConnectorKind(source.SourceKind)
				if kind == "" {
					continue
				}
				if canonicalName == "" {
					canonicalName = strings.TrimSpace(source.SourceName)
				}
				matchedKinds[kind] = struct{}{}
			}
		}
		switch len(matchedKinds) {
		case 0:
			selectedName = ""
		case 1:
			for kind := range matchedKinds {
				selectedKind = kind
			}
			if canonicalName != "" {
				selectedName = canonicalName
			}
		default:
			if canonicalName != "" {
				selectedName = canonicalName
			}
		}
	}
	if selectedKind == "" {
		if selectedName != "" && !identityHasSourceName(selectedName, sourcePairs) {
			selectedName = ""
		}
		return "", selectedName
	}
	if selectedName != "" && !identitySourcePairExists(selectedKind, selectedName, sourcePairs) {
		selectedName = ""
	}
	return selectedKind, selectedName
}

func identityHasSourceKind(kind string, sourcePairs []viewmodels.ProgrammaticSourceOption) bool {
	kind = NormalizeConnectorKind(kind)
	if kind == "" {
		return false
	}
	for _, source := range sourcePairs {
		if NormalizeConnectorKind(source.SourceKind) == kind {
			return true
		}
	}
	return false
}

func identityHasSourceName(name string, sourcePairs []viewmodels.ProgrammaticSourceOption) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, source := range sourcePairs {
		if strings.EqualFold(strings.TrimSpace(source.SourceName), name) {
			return true
		}
	}
	return false
}

func identitySourcePairExists(kind, name string, sourcePairs []viewmodels.ProgrammaticSourceOption) bool {
	kind = NormalizeConnectorKind(kind)
	name = strings.TrimSpace(name)
	if kind == "" || name == "" {
		return false
	}
	for _, source := range sourcePairs {
		if NormalizeConnectorKind(source.SourceKind) != kind {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(source.SourceName), name) {
			return true
		}
	}
	return false
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

func normalizeIdentityType(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "human":
		return "human"
	case "service":
		return "service"
	case "bot":
		return "bot"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeIdentityManagedState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "managed":
		return "managed"
	case "unmanaged":
		return "unmanaged"
	default:
		return ""
	}
}

func normalizeIdentityStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "active":
		return "active"
	case "suspended":
		return "suspended"
	case "deleted":
		return "deleted"
	case "orphaned":
		return "orphaned"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeIdentityActivityState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "recent":
		return "recent"
	case "aging":
		return "aging"
	case "stale":
		return "stale"
	case "never_seen":
		return "never_seen"
	default:
		return ""
	}
}

func normalizeIdentityLinkQuality(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "high":
		return "high"
	case "medium":
		return "medium"
	case "low":
		return "low"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeIdentitySortBy(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "identity":
		return "identity"
	case "identity_type":
		return "identity_type"
	case "managed":
		return "managed"
	case "source_type":
		return "source_type"
	case "linked_sources":
		return "linked_sources"
	case "privileged_roles":
		return "privileged_roles"
	case "status":
		return "status"
	case "last_seen":
		return "last_seen"
	default:
		return ""
	}
}

func normalizeIdentitySortDir(raw, sortBy string) string {
	if normalizeIdentitySortBy(sortBy) == "" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "asc":
		return "asc"
	case "desc":
		return "desc"
	default:
		return "desc"
	}
}

func parseIdentityBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func identitiesFilterActive(query, identityType, managedState string, privilegedOnly bool, status, activityState, linkQuality, sourceKind, sourceName string) bool {
	return strings.TrimSpace(query) != "" ||
		identityType != "" ||
		managedState != "" ||
		privilegedOnly ||
		status != "" ||
		activityState != "" ||
		linkQuality != "" ||
		sourceKind != "" ||
		sourceName != ""
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
