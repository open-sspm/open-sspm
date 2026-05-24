package handlers

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/normalize"
)

const nonHumanIdentitiesPerPage = 20

type nonHumanIdentitiesInventoryQuery struct {
	queryState            querystate.NonHumanIdentitiesQuery
	configuredSourceKinds []string
	configuredSourceNames []string
}

func newNonHumanIdentitiesInventoryQuery(queryState querystate.NonHumanIdentitiesQuery, sourcePairs []viewmodels.ProgrammaticSourceOption) nonHumanIdentitiesInventoryQuery {
	configuredSourceKinds, configuredSourceNames := identityConfiguredSourcePairs(sourcePairs)
	return nonHumanIdentitiesInventoryQuery{
		queryState:            queryState,
		configuredSourceKinds: configuredSourceKinds,
		configuredSourceNames: configuredSourceNames,
	}
}

func (q nonHumanIdentitiesInventoryQuery) CountParams() gen.CountNonHumanPrincipalsByFiltersParams {
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

func (q nonHumanIdentitiesInventoryQuery) ListParams(offset, limit int32) gen.ListNonHumanPrincipalsPageByFiltersParams {
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

func (h *Handlers) HandleNonHumanIdentities(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Non-Human Identities")
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
		// Non-human identities no longer expose source_name, so drop stale/manual params.
		queryValues.Del("source_name")
	}
	queryState := querystate.ParseNonHumanIdentitiesQuery(queryValues, programmaticQuerySources(sourcePairs))
	queryParams := newNonHumanIdentitiesInventoryQuery(queryState, sourcePairs)
	pagination := newPaginatedListState(0, queryState.Page, nonHumanIdentitiesPerPage)

	data := viewmodels.NonHumanIdentitiesViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No non-human identities match the current filters.", ""),
		Sources:               sourceKindOptions,
		Query:                 queryState,
	}

	render := func() error {
		if isNonHumanIdentitiesInventoryTarget(c) {
			return h.RenderComponent(c, views.NonHumanIdentitiesInventorySwap(data))
		}
		if isNonHumanIdentitiesResultsTarget(c) {
			return h.RenderComponent(c, views.NonHumanIdentitiesPageResults(data))
		}
		return h.RenderComponent(c, views.NonHumanIdentitiesPage(data))
	}

	if len(sourcePairs) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "Configure a connector with identity or programmatic-access data to populate non-human identities."
		return render()
	}

	totalCount, err := h.Q.CountNonHumanPrincipalsByFilters(ctx, queryParams.CountParams())
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination = newPaginatedListState(totalCount, queryState.Page, nonHumanIdentitiesPerPage)
	rows, err := h.Q.ListNonHumanPrincipalsPageByFilters(ctx, queryParams.ListParams(int32(pagination.Offset()), int32(nonHumanIdentitiesPerPage)))
	if err != nil {
		return h.RenderError(c, err)
	}

	linkResolver := newIdentityLinkResolver(h, ctx, stateView)
	items := make([]viewmodels.NonHumanIdentitiesListItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, nonHumanIdentitiesListItemFromRow(linkResolver, row))
	}

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No non-human identities found yet.", "")
	data.HasItems = len(items) > 0
	if queryState.HasFilters() {
		data.PaginatedListPageData.EmptyStateMsg = "No non-human identities match the current filters."
	}

	h.trackNonHumanIdentitiesListEvents(c, queryState)

	return render()
}

func isNonHumanIdentitiesInventoryTarget(c *echo.Context) bool {
	return isHX(c) && isHXTarget(c, "non-human-identities-inventory")
}

func isNonHumanIdentitiesResultsTarget(c *echo.Context) bool {
	return isHX(c) && isHXTarget(c, "non-human-identities-results")
}

func (h *Handlers) HandleNonHumanIdentityShow(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Non-Human Identity")
	if err != nil {
		return h.RenderError(c, err)
	}

	principalRef := strings.TrimSpace(c.Param("ref"))
	if principalRef == "" {
		return RenderNotFound(c)
	}
	if identityID, ok := nonHumanIdentityIDFromRef(principalRef); ok {
		if redirect, err := h.Q.GetIdentityMergeRedirect(ctx, identityID); err == nil {
			return c.Redirect(http.StatusSeeOther, "/non-human-identities/identity-"+strconv.FormatInt(redirect.TargetIdentityID, 10))
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return h.RenderError(c, err)
		}
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

	linkResolver := newIdentityLinkResolver(h, ctx, stateView)
	summary := nonHumanIdentitiesSummaryFromRow(linkResolver, principal)
	relationshipRows := []gen.ListIdentityScopedAccountRelationshipsRow(nil)
	if principal.IdentityID > 0 {
		relationshipRows, err = h.Q.ListIdentityScopedAccountRelationships(ctx, gen.ListIdentityScopedAccountRelationshipsParams{
			IdentityID:     principal.IdentityID,
			LifecycleState: "active",
		})
		if err != nil {
			return h.RenderError(c, err)
		}
	}
	relationships := make([]viewmodels.NonHumanIdentityRelationshipItem, 0, len(relationshipRows))
	for _, row := range relationshipRows {
		relationships = append(relationships, nonHumanIdentityRelationshipItemFromRow(row))
	}
	if owner := preferredNonHumanRelationshipOwner(relationships); owner != nil && summary.OwnerPresence == "unknown" {
		summary.OwnerPresence = "owned"
		summary.AccountableOwner = nonHumanRelationshipIdentityLabel(*owner)
		summary.AccountableOwnerHref = owner.IdentityHref
	}

	assets := make([]viewmodels.NonHumanIdentitiesRelatedAssetItem, 0, len(assetRows))
	for _, row := range assetRows {
		assets = append(assets, nonHumanIdentitiesRelatedAssetItemFromRow(linkResolver, row))
	}

	credentials := make([]viewmodels.NonHumanIdentitiesRelatedCredentialItem, 0, len(credentialRows))
	for _, row := range credentialRows {
		credentials = append(credentials, nonHumanIdentitiesRelatedCredentialItemFromRow(linkResolver, row))
	}

	bestAvailableAttribution, bestAvailableAttributionHref := nonHumanBestAvailableAttribution(linkResolver, credentialRows)
	if bestAvailableAttribution != "" && bestAvailableAttribution != summary.AccountableOwner {
		summary.BestAvailableAttribution = bestAvailableAttribution
		summary.BestAvailableAttributionHref = bestAvailableAttributionHref
	}

	signals := nonHumanPrincipalRiskSignals(principal)
	data := viewmodels.NonHumanIdentitiesShowViewData{
		Layout:                 layout,
		Principal:              summary,
		RiskSignals:            signals,
		HasRiskSignals:         len(signals) > 0,
		Relationships:          relationships,
		HasRelationships:       len(relationships) > 0,
		CanAssignRelationships: principal.IdentityID > 0,
		RelationshipAction:     nonHumanRelationshipActionPath(principalRef),
		RelatedAssets:          assets,
		Credentials:            credentials,
		HasAssets:              len(assets) > 0,
		HasCredentials:         len(credentials) > 0,
	}

	h.trackNonHumanIdentityDetailOpen(c, principalRef)
	return h.RenderComponent(c, views.NonHumanIdentityShowPage(data))
}

func (h *Handlers) HandleNonHumanIdentityRelationshipCreate(c *echo.Context) error {
	ctx := c.Request().Context()
	principalRef := strings.TrimSpace(c.Param("ref"))
	if principalRef == "" {
		return RenderNotFound(c)
	}
	redirectPath := nonHumanRelationshipRedirectPath(principalRef)

	principal, err := h.Q.GetNonHumanPrincipalByRef(ctx, principalRef)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if principal.IdentityID <= 0 {
		return redirectWithFlash(c, redirectPath, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Relationship not saved",
			Description: "Only identity-backed non-human principals can store account ownership relationships.",
		})
	}

	relationshipType := identityResolutionRelationshipTypeInput(c)
	if relationshipType == "" {
		relationshipType = "custodian"
	}
	identityEmail := normalize.Email(c.FormValue("identity_email"))
	if identityEmail == "" {
		return redirectWithFlash(c, redirectPath, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Identity email required",
			Description: "Enter an existing identity email for the owner or custodian.",
		})
	}

	configuredSourceKinds, configuredSourceNames, err := h.loadConfiguredIdentitySourcePairs(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	result, err := h.resolveStrictOwnerByEmail(ctx, "Identity", identityEmail, configuredSourceKinds, configuredSourceNames)
	if err != nil {
		return h.RenderError(c, err)
	}
	if result.Alert != nil {
		return redirectWithFlash(c, redirectPath, viewmodels.ToastViewData{
			Category:    "error",
			Title:       result.Alert.Title,
			Description: result.Alert.Message,
		})
	}
	ownerIdentity := result.Identity
	if nonHumanRelationshipTargetIsNonHuman(ownerIdentity) {
		return redirectWithFlash(c, redirectPath, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Choose a human identity",
			Description: "Service-account relationships should point to the human owner or custodian, not another service identity.",
		})
	}

	err = h.WithTx(ctx, func(qtx *gen.Queries) error {
		accounts, err := qtx.ListLinkedAccountsForIdentity(ctx, principal.IdentityID)
		if err != nil {
			return err
		}
		if len(accounts) == 0 {
			return pgx.ErrNoRows
		}
		for _, account := range accounts {
			if _, err := qtx.UpsertAccountIdentityRelationship(ctx, gen.UpsertAccountIdentityRelationshipParams{
				AccountID:        account.ID,
				IdentityID:       ownerIdentity.ID,
				RelationshipType: relationshipType,
				SourceKind:       pgtype.Text{String: account.SourceKind, Valid: strings.TrimSpace(account.SourceKind) != ""},
				SourceName:       pgtype.Text{String: account.SourceName, Valid: strings.TrimSpace(account.SourceName) != ""},
				Confidence:       identityResolutionRelationshipConfidence(relationshipType),
				LifecycleState:   "active",
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return redirectWithFlash(c, redirectPath, viewmodels.ToastViewData{
				Category:    "error",
				Title:       "Relationship not saved",
				Description: "No active source accounts are linked to this non-human identity.",
			})
		}
		return h.RenderError(c, err)
	}

	return redirectWithFlash(c, redirectPath, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Relationship saved",
		Description: "The non-human account now has an account-scoped owner or custodian relationship.",
	})
}

func nonHumanIdentitiesListItemFromRow(linkResolver *identityLinkResolver, row gen.ListNonHumanPrincipalsPageByFiltersRow) viewmodels.NonHumanIdentitiesListItem {
	sourceKind := strings.TrimSpace(row.SourceKind)
	sourceName := strings.TrimSpace(row.SourceName)
	ownerPresence := strings.TrimSpace(row.OwnerPresence)

	return viewmodels.NonHumanIdentitiesListItem{
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

func nonHumanIdentitiesSummaryFromRow(linkResolver *identityLinkResolver, principal gen.NonHumanPrincipalReadModelsV) viewmodels.NonHumanIdentitiesSummaryView {
	sourceKind := strings.TrimSpace(principal.SourceKind)
	sourceName := strings.TrimSpace(principal.SourceName)
	ownerPresence := strings.TrimSpace(principal.OwnerPresence)

	return viewmodels.NonHumanIdentitiesSummaryView{
		PrincipalRef:           principal.PrincipalRef,
		IdentityID:             principal.IdentityID,
		IdentityHref:           identityHrefForPrincipal(principal.PrincipalRef, principal.IdentityID),
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

func nonHumanIdentitiesRelatedAssetItemFromRow(linkResolver *identityLinkResolver, row gen.ListNonHumanPrincipalAssetsByRefRow) viewmodels.NonHumanIdentitiesRelatedAssetItem {
	sourceKind := strings.TrimSpace(row.SourceKind)
	sourceName := strings.TrimSpace(row.SourceName)
	ownerPresence := ownerPresenceForIDOrValue(row.GovernanceOwnerIdentityID, row.GovernanceOwnerDisplayName, row.GovernanceOwnerPrimaryEmail)

	return viewmodels.NonHumanIdentitiesRelatedAssetItem{
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

func nonHumanIdentitiesRelatedCredentialItemFromRow(linkResolver *identityLinkResolver, row gen.ListNonHumanPrincipalCredentialsByRefRow) viewmodels.NonHumanIdentitiesRelatedCredentialItem {
	sourceKind := strings.TrimSpace(row.SourceKind)
	sourceName := strings.TrimSpace(row.SourceName)

	return viewmodels.NonHumanIdentitiesRelatedCredentialItem{
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

func nonHumanIdentityRelationshipItemFromRow(row gen.ListIdentityScopedAccountRelationshipsRow) viewmodels.NonHumanIdentityRelationshipItem {
	return viewmodels.NonHumanIdentityRelationshipItem{
		ID:                          row.ID,
		AccountID:                   row.AccountID,
		AccountDisplayName:          fallbackNonEmpty(row.AccountDisplayName, row.AccountExternalID),
		AccountExternalID:           strings.TrimSpace(row.AccountExternalID),
		AccountSourceKind:           strings.TrimSpace(row.AccountSourceKind),
		AccountSourceName:           strings.TrimSpace(row.AccountSourceName),
		IdentityID:                  row.IdentityID,
		IdentityHref:                nonHumanIdentityHref(row.IdentityID),
		IdentityDisplayName:         fallbackNonEmpty(row.RelationshipDisplayName, row.RelationshipPrimaryEmail, views.FormatInt64(row.IdentityID)),
		IdentityPrimaryEmail:        strings.TrimSpace(row.RelationshipPrimaryEmail),
		RelationshipType:            strings.TrimSpace(row.RelationshipType),
		RelationshipIdentityKind:    strings.TrimSpace(row.RelationshipIdentityKind),
		RelationshipResolutionState: strings.TrimSpace(row.RelationshipResolutionState),
		Confidence:                  row.Confidence,
		LastSeen:                    calendarDateWithRelativeDisplay(row.LastSeenAt),
	}
}

func preferredNonHumanRelationshipOwner(items []viewmodels.NonHumanIdentityRelationshipItem) *viewmodels.NonHumanIdentityRelationshipItem {
	preferredRank := func(relationshipType string) int {
		switch strings.TrimSpace(relationshipType) {
		case "owner":
			return 0
		case "custodian":
			return 1
		case "approver":
			return 2
		case "attributed_user":
			return 3
		case "last_observed_user":
			return 4
		default:
			return 5
		}
	}
	var best *viewmodels.NonHumanIdentityRelationshipItem
	bestRank := 100
	for i := range items {
		rank := preferredRank(items[i].RelationshipType)
		if best == nil || rank < bestRank {
			best = &items[i]
			bestRank = rank
		}
	}
	return best
}

func nonHumanRelationshipIdentityLabel(item viewmodels.NonHumanIdentityRelationshipItem) string {
	if label := strings.TrimSpace(item.IdentityDisplayName); label != "" {
		return label
	}
	if email := strings.TrimSpace(item.IdentityPrimaryEmail); email != "" {
		return email
	}
	return "Identity #" + views.FormatInt64(item.IdentityID)
}

func nonHumanRelationshipActionPath(principalRef string) string {
	principalRef = strings.TrimSpace(principalRef)
	if principalRef == "" {
		return ""
	}
	return "/non-human-identities/" + url.PathEscape(principalRef) + "/relationships"
}

func nonHumanRelationshipRedirectPath(principalRef string) string {
	principalRef = strings.TrimSpace(principalRef)
	if principalRef == "" {
		return "/non-human-identities"
	}
	return "/non-human-identities/" + url.PathEscape(principalRef)
}

func nonHumanIdentityIDFromRef(principalRef string) (int64, bool) {
	raw := strings.TrimPrefix(strings.TrimSpace(principalRef), "identity-")
	if raw == strings.TrimSpace(principalRef) {
		return 0, false
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	return id, err == nil && id > 0
}

func nonHumanRelationshipTargetIsNonHuman(identity gen.Identity) bool {
	switch strings.ToLower(strings.TrimSpace(identity.Kind)) {
	case "service", "bot":
		return true
	}
	switch strings.ToLower(strings.TrimSpace(identity.IdentityKind)) {
	case "service", "shared", "application":
		return true
	default:
		return false
	}
}

func nonHumanIdentityHref(identityID int64) string {
	if identityID <= 0 {
		return ""
	}
	return "/identities/" + views.FormatInt64(identityID)
}

func identityHrefForPrincipal(principalRef string, identityID int64) string {
	if identityID > 0 && principalRef == "identity-"+views.FormatInt64(identityID) {
		return ""
	}
	return nonHumanIdentityHref(identityID)
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

func nonHumanPrincipalRiskSignals(principal gen.NonHumanPrincipalReadModelsV) []viewmodels.NonHumanIdentitiesRiskSignal {
	if len(principal.RiskSignalsJson) == 0 {
		return nil
	}
	var signals []viewmodels.NonHumanIdentitiesRiskSignal
	if err := json.Unmarshal(principal.RiskSignalsJson, &signals); err != nil {
		slog.Warn("failed to decode non-human principal risk signals", "principal_ref", principal.PrincipalRef, "error", err)
		return nil
	}
	filtered := make([]viewmodels.NonHumanIdentitiesRiskSignal, 0, len(signals))
	for _, signal := range signals {
		if strings.TrimSpace(signal.Title) == "" {
			continue
		}
		filtered = append(filtered, viewmodels.NonHumanIdentitiesRiskSignal{
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
