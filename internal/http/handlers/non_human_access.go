package handlers

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

const nonHumanAccessPerPage = 20

type nonHumanAccessInventoryQuery struct {
	queryState            querystate.NonHumanAccessQuery
	configuredSourceKinds []string
	configuredSourceNames []string
}

func newNonHumanAccessInventoryQuery(queryState querystate.NonHumanAccessQuery, sourcePairs []viewmodels.ProgrammaticSourceOption) nonHumanAccessInventoryQuery {
	configuredSourceKinds, configuredSourceNames := identityConfiguredSourcePairs(sourcePairs)
	return nonHumanAccessInventoryQuery{
		queryState:            queryState,
		configuredSourceKinds: configuredSourceKinds,
		configuredSourceNames: configuredSourceNames,
	}
}

func (q nonHumanAccessInventoryQuery) CountParams() gen.CountNonHumanPrincipalsByFiltersParams {
	return gen.CountNonHumanPrincipalsByFiltersParams{
		Query:                 q.queryState.Q,
		SourceKind:            q.queryState.Source.Kind,
		SourceName:            q.queryState.Source.Name,
		PrincipalType:         q.queryState.PrincipalType,
		OwnerPresence:         q.queryState.OwnerPresence,
		GovernanceState:       q.queryState.GovernanceState,
		RiskLevel:             q.queryState.RiskLevel,
		ActivityState:         q.queryState.ActivityState,
		FreshnessState:        q.queryState.FreshnessState,
		ConfiguredSourceKinds: q.configuredSourceKinds,
		ConfiguredSourceNames: q.configuredSourceNames,
	}
}

func (q nonHumanAccessInventoryQuery) ListParams(offset, limit int32) gen.ListNonHumanPrincipalsPageByFiltersParams {
	return gen.ListNonHumanPrincipalsPageByFiltersParams{
		SortBy:                q.queryState.SortBy,
		SortDir:               q.queryState.SortDir,
		PageOffset:            offset,
		PageLimit:             limit,
		Query:                 q.queryState.Q,
		SourceKind:            q.queryState.Source.Kind,
		SourceName:            q.queryState.Source.Name,
		PrincipalType:         q.queryState.PrincipalType,
		OwnerPresence:         q.queryState.OwnerPresence,
		GovernanceState:       q.queryState.GovernanceState,
		RiskLevel:             q.queryState.RiskLevel,
		ActivityState:         q.queryState.ActivityState,
		FreshnessState:        q.queryState.FreshnessState,
		ConfiguredSourceKinds: q.configuredSourceKinds,
		ConfiguredSourceNames: q.configuredSourceNames,
	}
}

func (h *Handlers) HandleNonHumanAccess(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Non-Human Principals")
	if err != nil {
		return h.RenderError(c, err)
	}

	sourcePairs := availableIdentitySourcePairs(stateView)
	sourceKindOptions := identitySourceKindOptions(sourcePairs)
	queryValues := c.Request().URL.Query()
	if queryValues.Has("source_name") {
		clonedValues := make(url.Values, len(queryValues))
		for key, values := range queryValues {
			clonedValues[key] = append([]string(nil), values...)
		}
		queryValues = clonedValues
		// Non-human access no longer exposes source_name, so drop stale/manual params.
		queryValues.Del("source_name")
	}
	queryState := querystate.ParseNonHumanAccessQuery(queryValues, programmaticQuerySources(sourcePairs))
	queryParams := newNonHumanAccessInventoryQuery(queryState, sourcePairs)
	pagination := newPaginatedListState(0, queryState.Page, nonHumanAccessPerPage)

	data := viewmodels.NonHumanAccessViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No non-human principals match the current filters.", ""),
		Sources:               sourceKindOptions,
		Query:                 queryState,
	}

	render := func() error {
		if isNonHumanAccessInventoryTarget(c) {
			return h.RenderComponent(c, views.NonHumanAccessInventorySwap(data))
		}
		if isNonHumanAccessResultsTarget(c) {
			return h.RenderComponent(c, views.NonHumanAccessPageResults(data))
		}
		return h.RenderComponent(c, views.NonHumanAccessPage(data))
	}

	if len(sourcePairs) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "Configure a connector with identity or programmatic-access data to populate non-human access."
		return render()
	}

	totalCount, err := h.Q.CountNonHumanPrincipalsByFilters(ctx, queryParams.CountParams())
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination = newPaginatedListState(totalCount, queryState.Page, nonHumanAccessPerPage)
	rows, err := h.Q.ListNonHumanPrincipalsPageByFilters(ctx, queryParams.ListParams(int32(pagination.Offset()), int32(nonHumanAccessPerPage)))
	if err != nil {
		return h.RenderError(c, err)
	}

	linkResolver := newIdentityLinkResolver(h, ctx)
	items := make([]viewmodels.NonHumanAccessListItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, nonHumanAccessListItemFromRow(linkResolver, row))
	}

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No non-human principals found yet.", "")
	data.HasItems = len(items) > 0
	if queryState.HasFilters() {
		data.PaginatedListPageData.EmptyStateMsg = "No non-human principals match the current filters."
	}

	h.trackNonHumanAccessListEvents(c, queryState)

	return render()
}

func isNonHumanAccessInventoryTarget(c *echo.Context) bool {
	return isHX(c) && isHXTarget(c, "non-human-access-inventory")
}

func isNonHumanAccessResultsTarget(c *echo.Context) bool {
	return isHX(c) && isHXTarget(c, "non-human-access-results")
}

func (h *Handlers) HandleNonHumanAccessShow(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Non-Human Principal")
	if err != nil {
		return h.RenderError(c, err)
	}

	principalRef := strings.TrimSpace(c.Param("ref"))
	if principalRef == "" {
		return RenderNotFound(c)
	}

	principal, err := h.Q.GetNonHumanPrincipalByRef(ctx, principalRef)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	assetRows, err := h.Q.ListNonHumanPrincipalAssetsByRef(ctx, principalRef)
	if err != nil {
		return h.RenderError(c, err)
	}
	credentialRows, err := h.Q.ListNonHumanPrincipalCredentialsByRef(ctx, principalRef)
	if err != nil {
		return h.RenderError(c, err)
	}

	linkResolver := newIdentityLinkResolver(h, ctx)
	summary := nonHumanAccessSummaryFromRow(linkResolver, principal)

	assets := make([]viewmodels.NonHumanAccessRelatedAssetItem, 0, len(assetRows))
	for _, row := range assetRows {
		assets = append(assets, nonHumanAccessRelatedAssetItemFromRow(linkResolver, row))
	}

	credentials := make([]viewmodels.NonHumanAccessRelatedCredentialItem, 0, len(credentialRows))
	for _, row := range credentialRows {
		credentials = append(credentials, nonHumanAccessRelatedCredentialItemFromRow(linkResolver, row))
	}

	bestAvailableAttribution, bestAvailableAttributionHref := nonHumanBestAvailableAttribution(linkResolver, credentialRows)
	if bestAvailableAttribution != "" && bestAvailableAttribution != summary.AccountableOwner {
		summary.BestAvailableAttribution = bestAvailableAttribution
		summary.BestAvailableAttributionHref = bestAvailableAttributionHref
	}

	signals := nonHumanPrincipalRiskSignals(principal)
	data := viewmodels.NonHumanAccessShowViewData{
		Layout:         layout,
		Principal:      summary,
		RiskSignals:    signals,
		HasRiskSignals: len(signals) > 0,
		RelatedAssets:  assets,
		Credentials:    credentials,
		HasAssets:      len(assets) > 0,
		HasCredentials: len(credentials) > 0,
	}

	h.trackNonHumanAccessDetailOpen(c, principalRef)
	return h.RenderComponent(c, views.NonHumanAccessShowPage(data))
}

func nonHumanAccessListItemFromRow(linkResolver *identityLinkResolver, row gen.ListNonHumanPrincipalsPageByFiltersRow) viewmodels.NonHumanAccessListItem {
	sourceKind := strings.TrimSpace(row.SourceKind)
	sourceName := strings.TrimSpace(row.SourceName)
	ownerPresence := strings.TrimSpace(row.OwnerPresence)

	return viewmodels.NonHumanAccessListItem{
		PrincipalRef:           row.PrincipalRef,
		IdentityID:             row.IdentityID,
		AppAssetID:             row.AppAssetID,
		PrincipalType:          strings.TrimSpace(row.PrincipalType),
		SourceKind:             sourceKind,
		SourceName:             sourceName,
		DisplayName:            fallbackDash(strings.TrimSpace(row.DisplayName)),
		SecondaryName:          strings.TrimSpace(row.SecondaryName),
		LinkedAssetsCount:      row.LinkedAssetsCount,
		LinkedCredentialsCount: row.LinkedCredentialsCount,
		LastSeen:               calendarDateWithRelativeDisplay(row.LastSeenAt),
		ActivityState:          strings.TrimSpace(row.ActivityState),
		FreshnessState:         strings.TrimSpace(row.FreshnessState),
		GovernanceState:        strings.TrimSpace(row.GovernanceState),
		AccountableOwner:       nonHumanAccountableOwnerLabel(row.AccountableOwnerDisplayName, row.AccountableOwnerPrimaryEmail, ownerPresence),
		AccountableOwnerHref:   nonHumanOwnerHref(linkResolver, sourceKind, sourceName, row.AccountableOwnerIdentityID, row.AccountableOwnerPrimaryEmail, row.AccountableOwnerDisplayName),
		OwnerPresence:          ownerPresence,
		RiskLevel:              strings.TrimSpace(row.RiskLevel),
	}
}

func nonHumanAccessSummaryFromRow(linkResolver *identityLinkResolver, principal gen.NonHumanPrincipalReadModelsV) viewmodels.NonHumanAccessSummaryView {
	sourceKind := strings.TrimSpace(principal.SourceKind)
	sourceName := strings.TrimSpace(principal.SourceName)
	ownerPresence := strings.TrimSpace(principal.OwnerPresence)

	return viewmodels.NonHumanAccessSummaryView{
		PrincipalRef:           principal.PrincipalRef,
		IdentityID:             principal.IdentityID,
		IdentityHref:           nonHumanIdentityHref(principal.IdentityID),
		AppAssetID:             principal.AppAssetID,
		AppAssetHref:           nonHumanAppAssetHref(principal.AppAssetID),
		PrincipalType:          strings.TrimSpace(principal.PrincipalType),
		DisplayName:            fallbackDash(strings.TrimSpace(principal.DisplayName)),
		SecondaryName:          strings.TrimSpace(principal.SecondaryName),
		SourceKind:             sourceKind,
		SourceName:             sourceName,
		LinkedAssetsCount:      principal.LinkedAssetsCount,
		LinkedCredentialsCount: principal.LinkedCredentialsCount,
		LastSeen:               calendarDateWithRelativeDisplay(principal.LastSeenAt),
		ActivityState:          strings.TrimSpace(principal.ActivityState),
		FreshnessState:         strings.TrimSpace(principal.FreshnessState),
		GovernanceState:        strings.TrimSpace(principal.GovernanceState),
		AccountableOwner:       nonHumanAccountableOwnerLabel(principal.AccountableOwnerDisplayName, principal.AccountableOwnerPrimaryEmail, ownerPresence),
		AccountableOwnerHref:   nonHumanOwnerHref(linkResolver, sourceKind, sourceName, principal.AccountableOwnerIdentityID, principal.AccountableOwnerPrimaryEmail, principal.AccountableOwnerDisplayName),
		OwnerPresence:          ownerPresence,
		RiskLevel:              strings.TrimSpace(principal.RiskLevel),
		HasCriticalCredential:  principal.HasCriticalCredential,
		HasHighRiskCredential:  principal.HasHighRiskCredential,
		HasExpiredCredential:   principal.HasExpiredCredential,
		HasExpiringCredential:  principal.HasExpiringCredential,
		HasUnusedCredential:    principal.HasUnusedCredential,
		HasStaleEvidence:       principal.HasStaleEvidence,
	}
}

func nonHumanAccessRelatedAssetItemFromRow(linkResolver *identityLinkResolver, row gen.ListNonHumanPrincipalAssetsByRefRow) viewmodels.NonHumanAccessRelatedAssetItem {
	sourceKind := strings.TrimSpace(row.SourceKind)
	sourceName := strings.TrimSpace(row.SourceName)
	ownerPresence := ownerPresenceForIDOrValue(row.GovernanceOwnerIdentityID, row.GovernanceOwnerDisplayName, row.GovernanceOwnerPrimaryEmail)

	return viewmodels.NonHumanAccessRelatedAssetItem{
		ID:                  row.ID,
		Href:                "/app-assets/" + views.FormatInt64(row.ID),
		SourceKind:          sourceKind,
		SourceName:          sourceName,
		AssetKind:           strings.TrimSpace(row.AssetKind),
		DisplayName:         fallbackDash(strings.TrimSpace(row.DisplayName)),
		ExternalID:          strings.TrimSpace(row.ExternalID),
		Status:              fallbackDash(strings.TrimSpace(row.Status)),
		GovernanceState:     strings.TrimSpace(row.GovernanceState),
		GovernanceOwner:     nonHumanAccountableOwnerLabel(row.GovernanceOwnerDisplayName, row.GovernanceOwnerPrimaryEmail, ownerPresence),
		GovernanceOwnerHref: nonHumanOwnerHref(linkResolver, sourceKind, sourceName, row.GovernanceOwnerIdentityID, row.GovernanceOwnerPrimaryEmail, row.GovernanceOwnerDisplayName),
		LinkedCredentials:   row.GrantCount,
		EvidenceFreshness:   strings.TrimSpace(row.EvidenceFreshness),
		EvidenceConfidence:  strings.TrimSpace(row.EvidenceConfidence),
		EvidenceSeen:        calendarDateDisplay(row.EvidenceLastSeenAt),
	}
}

func nonHumanAccessRelatedCredentialItemFromRow(linkResolver *identityLinkResolver, row gen.ListNonHumanPrincipalCredentialsByRefRow) viewmodels.NonHumanAccessRelatedCredentialItem {
	sourceKind := strings.TrimSpace(row.SourceKind)
	sourceName := strings.TrimSpace(row.SourceName)

	return viewmodels.NonHumanAccessRelatedCredentialItem{
		ID:              row.ID,
		Href:            "/credentials/" + views.FormatInt64(row.ID),
		SourceKind:      sourceKind,
		SourceName:      sourceName,
		CredentialKind:  strings.TrimSpace(row.CredentialKind),
		DisplayName:     fallbackDash(strings.TrimSpace(row.DisplayName)),
		ExternalID:      strings.TrimSpace(row.ExternalID),
		Status:          fallbackDash(strings.TrimSpace(row.Status)),
		RiskLevel:       strings.TrimSpace(row.RiskLevel),
		ExpiresAt:       calendarDateDisplay(row.ExpiresAtSource),
		LastUsedAt:      calendarDateDisplay(row.LastUsedAtSource),
		CreatedBy:       fallbackDash(actorDisplayName(row.CreatedByDisplayName, row.CreatedByExternalID)),
		CreatedByHref:   linkResolver.Resolve(sourceKind, sourceName, row.CreatedByExternalID, "", row.CreatedByDisplayName),
		ApprovedBy:      fallbackDash(actorDisplayName(row.ApprovedByDisplayName, row.ApprovedByExternalID)),
		ApprovedByHref:  linkResolver.Resolve(sourceKind, sourceName, row.ApprovedByExternalID, "", row.ApprovedByDisplayName),
		AppAssetID:      row.AppAssetID,
		AppAssetDisplay: fallbackDash(strings.TrimSpace(row.AppAssetDisplayName)),
		AppAssetHref:    nonHumanAppAssetHref(row.AppAssetID),
	}
}

func nonHumanIdentityHref(identityID int64) string {
	if identityID <= 0 {
		return ""
	}
	return "/identities/" + views.FormatInt64(identityID)
}

func nonHumanAppAssetHref(appAssetID int64) string {
	if appAssetID <= 0 {
		return ""
	}
	return "/app-assets/" + views.FormatInt64(appAssetID)
}

func nonHumanOwnerHref(linkResolver *identityLinkResolver, sourceKind, sourceName string, ownerIdentityID int64, ownerEmail, ownerDisplayName string) string {
	if ownerIdentityID > 0 {
		return nonHumanIdentityHref(ownerIdentityID)
	}
	if linkResolver == nil {
		return ""
	}
	return linkResolver.Resolve(strings.TrimSpace(sourceKind), strings.TrimSpace(sourceName), "", ownerEmail, ownerDisplayName)
}

func ownerPresenceForIDOrValue(ownerIdentityID int64, ownerDisplayName, ownerPrimaryEmail string) string {
	if ownerIdentityID > 0 {
		return "owned"
	}
	if strings.TrimSpace(ownerDisplayName) != "" || strings.TrimSpace(ownerPrimaryEmail) != "" {
		return "owned"
	}
	return "unknown"
}

func nonHumanAccountableOwnerLabel(ownerDisplayName, ownerPrimaryEmail, ownerPresence string) string {
	if strings.TrimSpace(ownerPresence) == "unknown" {
		return "Unknown"
	}
	if ownerDisplayName = strings.TrimSpace(ownerDisplayName); ownerDisplayName != "" {
		return ownerDisplayName
	}
	if ownerPrimaryEmail = strings.TrimSpace(ownerPrimaryEmail); ownerPrimaryEmail != "" {
		return ownerPrimaryEmail
	}
	return "Unknown"
}

func nonHumanPrincipalRiskSignals(principal gen.NonHumanPrincipalReadModelsV) []viewmodels.NonHumanAccessRiskSignal {
	if len(principal.RiskSignalsJson) == 0 {
		return nil
	}
	var signals []viewmodels.NonHumanAccessRiskSignal
	if err := json.Unmarshal(principal.RiskSignalsJson, &signals); err != nil {
		slog.Warn("failed to decode non-human principal risk signals", "principal_ref", principal.PrincipalRef, "error", err)
		return nil
	}
	filtered := make([]viewmodels.NonHumanAccessRiskSignal, 0, len(signals))
	for _, signal := range signals {
		if strings.TrimSpace(signal.Title) == "" {
			continue
		}
		filtered = append(filtered, viewmodels.NonHumanAccessRiskSignal{
			Severity: strings.TrimSpace(signal.Severity),
			Title:    strings.TrimSpace(signal.Title),
			Evidence: strings.TrimSpace(signal.Evidence),
		})
	}
	return filtered
}

func nonHumanBestAvailableAttribution(linkResolver *identityLinkResolver, rows []gen.ListNonHumanPrincipalCredentialsByRefRow) (string, string) {
	resolve := func(sourceKind, sourceName, externalID, displayName string) string {
		if linkResolver == nil {
			return ""
		}
		return linkResolver.Resolve(strings.TrimSpace(sourceKind), strings.TrimSpace(sourceName), externalID, "", displayName)
	}

	for _, row := range rows {
		label := actorDisplayName(row.CreatedByDisplayName, row.CreatedByExternalID)
		if strings.TrimSpace(label) == "" {
			continue
		}
		return label, resolve(row.SourceKind, row.SourceName, row.CreatedByExternalID, row.CreatedByDisplayName)
	}

	for _, row := range rows {
		label := actorDisplayName(row.ApprovedByDisplayName, row.ApprovedByExternalID)
		if strings.TrimSpace(label) == "" {
			continue
		}
		return label, resolve(row.SourceKind, row.SourceName, row.ApprovedByExternalID, row.ApprovedByDisplayName)
	}

	return "", ""
}
