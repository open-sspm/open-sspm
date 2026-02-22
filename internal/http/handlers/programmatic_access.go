package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleAppAssets(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "App Assets")
	if err != nil {
		return h.RenderError(c, err)
	}

	sources := availableProgrammaticSources(snap)
	selected, hasSource := selectProgrammaticSource(c, sources)
	query := strings.TrimSpace(c.QueryParam("q"))
	assetKind := strings.TrimSpace(c.QueryParam("asset_kind"))
	page := parsePageParam(c)
	const perPage = 20

	data := viewmodels.AppAssetsViewData{
		Layout:             layout,
		Sources:            sources,
		SelectedSourceKind: selected.SourceKind,
		SelectedSourceName: selected.SourceName,
		Query:              query,
		AssetKind:          assetKind,
		Page:               1,
		PerPage:            perPage,
		TotalPages:         1,
		EmptyStateMsg:      "No app assets found for the current filters.",
	}
	renderAppAssets := func() error {
		if isHX(c) && isHXTarget(c, "app-assets-results") {
			return h.RenderComponent(c, views.AppAssetsPageResults(data))
		}
		return h.RenderComponent(c, views.AppAssetsPage(data))
	}

	if !hasSource {
		data.EmptyStateMsg = "Configure and enable GitHub, Microsoft Entra, or Vault connectors to populate app assets."
		return renderAppAssets()
	}

	activeSources := effectiveProgrammaticSources(selected, sources)
	if len(activeSources) == 0 {
		data.EmptyStateMsg = "No matching source found. Choose another source filter."
		return renderAppAssets()
	}

	var totalCount int64
	var totalPages int
	var offset int
	var assets []gen.AppAsset

	if len(activeSources) == 1 {
		source := activeSources[0]
		totalCount, err = h.Q.CountAppAssetsBySourceAndQueryAndKind(ctx, gen.CountAppAssetsBySourceAndQueryAndKindParams{
			SourceKind: source.SourceKind,
			SourceName: source.SourceName,
			AssetKind:  assetKind,
			Query:      query,
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		page, totalPages, offset = paginate(totalCount, page, perPage)
		assets, err = h.Q.ListAppAssetsPageBySourceAndQueryAndKind(ctx, gen.ListAppAssetsPageBySourceAndQueryAndKindParams{
			SourceKind: source.SourceKind,
			SourceName: source.SourceName,
			AssetKind:  assetKind,
			Query:      query,
			PageLimit:  int32(perPage),
			PageOffset: int32(offset),
		})
		if err != nil {
			return h.RenderError(c, err)
		}
	} else {
		allAssets, err := h.listAppAssetsAcrossSources(ctx, activeSources, assetKind, query)
		if err != nil {
			return h.RenderError(c, err)
		}
		sortAppAssetsForList(allAssets)
		totalCount = int64(len(allAssets))
		page, totalPages, offset = paginate(totalCount, page, perPage)
		assets = paginateAppAssets(allAssets, offset, perPage)
	}

	ownerCounts := map[int64]int{}
	credentialCounts := map[int64]int{}

	assetIDs := make([]int64, 0, len(assets))
	type sourceCredentialRefs struct {
		sourceKind     string
		sourceName     string
		refKinds       []string
		refExternalIDs []string
	}
	refGroups := map[string]*sourceCredentialRefs{}
	refToAssetID := map[string]int64{}
	for _, asset := range assets {
		assetIDs = append(assetIDs, asset.ID)
		refKind, refExternalID := appAssetCredentialRef(asset)
		if refKind == "" || refExternalID == "" {
			continue
		}

		assetSourceKind := strings.TrimSpace(asset.SourceKind)
		assetSourceName := strings.TrimSpace(asset.SourceName)
		groupKey := sourceKey(assetSourceKind, assetSourceName)
		group := refGroups[groupKey]
		if group == nil {
			group = &sourceCredentialRefs{
				sourceKind:     assetSourceKind,
				sourceName:     assetSourceName,
				refKinds:       make([]string, 0, len(assets)),
				refExternalIDs: make([]string, 0, len(assets)),
			}
			refGroups[groupKey] = group
		}
		group.refKinds = append(group.refKinds, refKind)
		group.refExternalIDs = append(group.refExternalIDs, refExternalID)
		refToAssetID[credentialSourceRefKey(assetSourceKind, assetSourceName, refKind, refExternalID)] = asset.ID
	}

	if len(assetIDs) > 0 {
		owners, err := h.Q.ListAppAssetOwnersByAssetIDs(ctx, assetIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, owner := range owners {
			ownerCounts[owner.AppAssetID]++
		}
	}

	for _, group := range refGroups {
		if len(group.refKinds) == 0 {
			continue
		}
		countRows, err := h.Q.ListCredentialArtifactCountsByAssetRef(ctx, gen.ListCredentialArtifactCountsByAssetRefParams{
			SourceKind:          group.sourceKind,
			SourceName:          group.sourceName,
			AssetRefKinds:       group.refKinds,
			AssetRefExternalIds: group.refExternalIDs,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, row := range countRows {
			assetID, ok := refToAssetID[credentialSourceRefKey(group.sourceKind, group.sourceName, row.AssetRefKind, row.AssetRefExternalID)]
			if !ok {
				continue
			}
			credentialCounts[assetID] = int(row.CredentialCount)
		}
	}

	items := make([]viewmodels.AppAssetListItem, 0, len(assets))
	for _, asset := range assets {
		displayName := strings.TrimSpace(asset.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(asset.ExternalID)
		}
		if displayName == "" {
			displayName = fmt.Sprintf("Asset %d", asset.ID)
		}
		items = append(items, viewmodels.AppAssetListItem{
			ID:               asset.ID,
			SourceKind:       strings.TrimSpace(asset.SourceKind),
			SourceName:       strings.TrimSpace(asset.SourceName),
			AssetKind:        strings.TrimSpace(asset.AssetKind),
			DisplayName:      displayName,
			ExternalID:       strings.TrimSpace(asset.ExternalID),
			Status:           fallbackDash(strings.TrimSpace(asset.Status)),
			OwnersCount:      ownerCounts[asset.ID],
			CredentialsCount: credentialCounts[asset.ID],
			LastSeenAt:       formatProgrammaticDate(asset.LastObservedAt),
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
	data.HasItems = showingCount > 0
	if query != "" || assetKind != "" {
		data.EmptyStateMsg = "No app assets match the current search filters."
	}

	return renderAppAssets()
}

func (h *Handlers) HandleAppAssetShow(c *echo.Context) error {
	assetID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	asset, err := h.Q.GetAppAssetByID(ctx, assetID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	layout, _, err := h.LayoutData(ctx, c, "App Asset")
	if err != nil {
		return h.RenderError(c, err)
	}

	owners, err := h.Q.ListAppAssetOwnersByAssetID(ctx, asset.ID)
	if err != nil {
		return h.RenderError(c, err)
	}

	now := time.Now().UTC()
	linkResolver := newIdentityLinkResolver(h, ctx)

	ownerItems := make([]viewmodels.AppAssetOwnerItem, 0, len(owners))
	for _, owner := range owners {
		displayName := strings.TrimSpace(owner.OwnerDisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(owner.OwnerEmail)
		}
		if displayName == "" {
			displayName = strings.TrimSpace(owner.OwnerExternalID)
		}
		ownerItems = append(ownerItems, viewmodels.AppAssetOwnerItem{
			OwnerKind:         fallbackDash(strings.TrimSpace(owner.OwnerKind)),
			OwnerDisplayName:  fallbackDash(displayName),
			OwnerEmail:        fallbackDash(strings.TrimSpace(owner.OwnerEmail)),
			OwnerExternalID:   fallbackDash(strings.TrimSpace(owner.OwnerExternalID)),
			OwnerIdentityHref: linkResolver.Resolve(strings.TrimSpace(asset.SourceKind), strings.TrimSpace(asset.SourceName), owner.OwnerExternalID, owner.OwnerEmail, owner.OwnerDisplayName),
		})
	}

	credentialRows, err := h.listCredentialArtifactsForAsset(ctx, asset)
	if err != nil {
		return h.RenderError(c, err)
	}

	credentialItems := make([]viewmodels.AppAssetCredentialItem, 0, len(credentialRows))
	credentialDisplayByRef := map[string]string{}
	for _, credential := range credentialRows {
		displayName := strings.TrimSpace(credential.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(credential.ExternalID)
		}
		credentialItems = append(credentialItems, viewmodels.AppAssetCredentialItem{
			ID:             credential.ID,
			Href:           "/credentials/" + strconv.FormatInt(credential.ID, 10),
			CredentialKind: fallbackDash(strings.TrimSpace(credential.CredentialKind)),
			DisplayName:    fallbackDash(displayName),
			Status:         fallbackDash(strings.TrimSpace(credential.Status)),
			RiskLevel:      credentialRiskLevel(credential, now),
			ExpiresAt:      formatProgrammaticDate(credential.ExpiresAtSource),
			LastUsedAt:     formatProgrammaticDate(credential.LastUsedAtSource),
			CreatedBy:      fallbackDash(actorDisplayName(credential.CreatedByDisplayName, credential.CreatedByExternalID)),
			CreatedByHref:  linkResolver.Resolve(strings.TrimSpace(credential.SourceKind), strings.TrimSpace(credential.SourceName), credential.CreatedByExternalID, "", credential.CreatedByDisplayName),
		})
		credentialDisplayByRef[credentialRefKey(strings.TrimSpace(credential.CredentialKind), strings.TrimSpace(credential.ExternalID))] = displayName
	}

	events, err := h.Q.ListCredentialAuditEventsForTarget(ctx, gen.ListCredentialAuditEventsForTargetParams{
		SourceKind:       strings.TrimSpace(asset.SourceKind),
		SourceName:       strings.TrimSpace(asset.SourceName),
		TargetKind:       strings.TrimSpace(asset.AssetKind),
		TargetExternalID: strings.TrimSpace(asset.ExternalID),
		LimitRows:        100,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	auditItems := make([]viewmodels.ProgrammaticAuditEventItem, 0, len(events))
	for _, event := range events {
		credentialKind := strings.TrimSpace(event.CredentialKind)
		credentialExternalID := strings.TrimSpace(event.CredentialExternalID)
		credentialName := credentialDisplayByRef[credentialRefKey(credentialKind, credentialExternalID)]
		auditItems = append(auditItems, viewmodels.ProgrammaticAuditEventItem{
			EventType:             fallbackDash(strings.TrimSpace(event.EventType)),
			EventTime:             formatProgrammaticDate(event.EventTime),
			Actor:                 fallbackDash(actorDisplayName(event.ActorDisplayName, event.ActorExternalID)),
			Target:                fallbackDash(actorDisplayName(event.TargetDisplayName, event.TargetExternalID)),
			CredentialKind:        fallbackDash(credentialKind),
			CredentialExternalID:  fallbackDash(credentialExternalID),
			CredentialDisplayName: fallbackDash(credentialName),
		})
	}

	displayName := strings.TrimSpace(asset.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(asset.ExternalID)
	}
	if displayName == "" {
		displayName = fmt.Sprintf("Asset %d", asset.ID)
	}

	data := viewmodels.AppAssetShowViewData{
		Layout: layout,
		Asset: viewmodels.AppAssetSummaryView{
			ID:               asset.ID,
			SourceKind:       strings.TrimSpace(asset.SourceKind),
			SourceName:       strings.TrimSpace(asset.SourceName),
			AssetKind:        strings.TrimSpace(asset.AssetKind),
			DisplayName:      displayName,
			ExternalID:       strings.TrimSpace(asset.ExternalID),
			ParentExternalID: fallbackDash(strings.TrimSpace(asset.ParentExternalID)),
			Status:           fallbackDash(strings.TrimSpace(asset.Status)),
			CreatedAtSource:  formatProgrammaticDate(asset.CreatedAtSource),
			UpdatedAtSource:  formatProgrammaticDate(asset.UpdatedAtSource),
			LastObservedAt:   formatProgrammaticDate(asset.LastObservedAt),
		},
		Owners:         ownerItems,
		Credentials:    credentialItems,
		AuditEvents:    auditItems,
		HasOwners:      len(ownerItems) > 0,
		HasCredentials: len(credentialItems) > 0,
		HasAuditEvents: len(auditItems) > 0,
	}

	return h.RenderComponent(c, views.AppAssetShowPage(data))
}

func (h *Handlers) HandleCredentials(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Credentials")
	if err != nil {
		return h.RenderError(c, err)
	}

	sources := availableProgrammaticSources(snap)
	selected, hasSource := selectProgrammaticSource(c, sources)
	query := strings.TrimSpace(c.QueryParam("q"))
	credentialKind := strings.TrimSpace(c.QueryParam("credential_kind"))
	status := strings.TrimSpace(c.QueryParam("status"))
	riskLevel := normalizeCredentialRiskFilter(c.QueryParam("risk_level"))
	expiryState := strings.ToLower(strings.TrimSpace(c.QueryParam("expiry_state")))
	switch expiryState {
	case "", "active", "expired":
	default:
		expiryState = ""
	}
	expiresInDays := max(parseIntParamDefault(c.QueryParam("expires_in_days"), 0), 0)
	if expiresInDays > 3650 {
		expiresInDays = 3650
	}
	page := parsePageParam(c)
	const perPage = 20

	data := viewmodels.CredentialsViewData{
		Layout:             layout,
		Sources:            sources,
		SelectedSourceKind: selected.SourceKind,
		SelectedSourceName: selected.SourceName,
		Query:              query,
		CredentialKind:     credentialKind,
		Status:             status,
		RiskLevel:          riskLevel,
		ExpiryState:        expiryState,
		ExpiresInDays:      expiresInDays,
		Page:               1,
		PerPage:            perPage,
		TotalPages:         1,
		EmptyStateMsg:      "No credentials found for the current filters.",
	}
	renderCredentials := func() error {
		if isHX(c) && isHXTarget(c, "credentials-results") {
			return h.RenderComponent(c, views.CredentialsPageResults(data))
		}
		return h.RenderComponent(c, views.CredentialsPage(data))
	}

	if !hasSource {
		data.EmptyStateMsg = "Configure and enable GitHub, Microsoft Entra, or Vault connectors to populate credential inventory."
		return renderCredentials()
	}

	activeSources := effectiveProgrammaticSources(selected, sources)
	if len(activeSources) == 0 {
		data.EmptyStateMsg = "No matching source found. Choose another source filter."
		return renderCredentials()
	}

	var totalCount int64
	var totalPages int
	var offset int
	var rows []gen.CredentialArtifact

	if len(activeSources) == 1 {
		source := activeSources[0]
		totalCount, err = h.Q.CountCredentialArtifactsBySourceAndQueryAndFilters(ctx, gen.CountCredentialArtifactsBySourceAndQueryAndFiltersParams{
			SourceKind:     source.SourceKind,
			SourceName:     source.SourceName,
			CredentialKind: credentialKind,
			Status:         status,
			RiskLevel:      riskLevel,
			ExpiryState:    expiryState,
			ExpiresInDays:  int32(expiresInDays),
			Query:          query,
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		page, totalPages, offset = paginate(totalCount, page, perPage)
		rows, err = h.Q.ListCredentialArtifactsPageBySourceAndQueryAndFilters(ctx, gen.ListCredentialArtifactsPageBySourceAndQueryAndFiltersParams{
			SourceKind:     source.SourceKind,
			SourceName:     source.SourceName,
			CredentialKind: credentialKind,
			Status:         status,
			RiskLevel:      riskLevel,
			ExpiryState:    expiryState,
			ExpiresInDays:  int32(expiresInDays),
			Query:          query,
			PageLimit:      int32(perPage),
			PageOffset:     int32(offset),
		})
		if err != nil {
			return h.RenderError(c, err)
		}
	} else {
		rows, err = h.listCredentialsAcrossSources(ctx, activeSources, credentialKind, status, riskLevel, expiryState, expiresInDays, query)
		if err != nil {
			return h.RenderError(c, err)
		}
		sortCredentialsForList(rows)
		totalCount = int64(len(rows))
		page, totalPages, offset = paginate(totalCount, page, perPage)
		rows = paginateCredentials(rows, offset, perPage)
	}

	now := time.Now().UTC()
	linkResolver := newIdentityLinkResolver(h, ctx)
	items := make([]viewmodels.CredentialArtifactListItem, 0, len(rows))
	for _, row := range rows {
		displayName := strings.TrimSpace(row.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(row.ExternalID)
		}
		assetRefKind := strings.TrimSpace(row.AssetRefKind)
		assetRefExternalID := strings.TrimSpace(row.AssetRefExternalID)
		assetRef := ""
		switch {
		case assetRefKind != "" && assetRefExternalID != "":
			assetRef = assetRefKind + ":" + assetRefExternalID
		case assetRefKind != "":
			assetRef = assetRefKind
		case assetRefExternalID != "":
			assetRef = assetRefExternalID
		}
		createdBy := fallbackDash(actorDisplayName(row.CreatedByDisplayName, row.CreatedByExternalID))
		approvedBy := fallbackDash(actorDisplayName(row.ApprovedByDisplayName, row.ApprovedByExternalID))
		items = append(items, viewmodels.CredentialArtifactListItem{
			ID:             row.ID,
			SourceKind:     strings.TrimSpace(row.SourceKind),
			SourceName:     strings.TrimSpace(row.SourceName),
			CredentialKind: fallbackDash(strings.TrimSpace(row.CredentialKind)),
			DisplayName:    fallbackDash(displayName),
			ExternalID:     fallbackDash(strings.TrimSpace(row.ExternalID)),
			AssetRef:       fallbackDash(assetRef),
			AssetRefKind:   fallbackDash(assetRefKind),
			AssetRefID:     fallbackDash(assetRefExternalID),
			Status:         fallbackDash(strings.TrimSpace(row.Status)),
			RiskLevel:      credentialRiskLevel(row, now),
			ExpiresAt:      formatProgrammaticDate(row.ExpiresAtSource),
			LastUsedAt:     formatProgrammaticDate(row.LastUsedAtSource),
			CreatedBy:      createdBy,
			CreatedByHref:  linkResolver.Resolve(strings.TrimSpace(row.SourceKind), strings.TrimSpace(row.SourceName), row.CreatedByExternalID, "", row.CreatedByDisplayName),
			ApprovedBy:     approvedBy,
			ApprovedByHref: linkResolver.Resolve(strings.TrimSpace(row.SourceKind), strings.TrimSpace(row.SourceName), row.ApprovedByExternalID, "", row.ApprovedByDisplayName),
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
	data.HasItems = showingCount > 0
	if query != "" || credentialKind != "" || status != "" || riskLevel != "" || expiryState != "" || expiresInDays > 0 {
		data.EmptyStateMsg = "No credentials match the current search filters."
	}

	return renderCredentials()
}

func (h *Handlers) HandleCredentialShow(c *echo.Context) error {
	credentialID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	credential, err := h.Q.GetCredentialArtifactByID(ctx, credentialID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	layout, _, err := h.LayoutData(ctx, c, "Credential")
	if err != nil {
		return h.RenderError(c, err)
	}

	events, err := h.Q.ListCredentialAuditEventsForCredential(ctx, gen.ListCredentialAuditEventsForCredentialParams{
		SourceKind:           strings.TrimSpace(credential.SourceKind),
		SourceName:           strings.TrimSpace(credential.SourceName),
		CredentialKind:       strings.TrimSpace(credential.CredentialKind),
		CredentialExternalID: strings.TrimSpace(credential.ExternalID),
		LimitRows:            100,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	eventItems := make([]viewmodels.ProgrammaticAuditEventItem, 0, len(events))
	for _, event := range events {
		eventItems = append(eventItems, viewmodels.ProgrammaticAuditEventItem{
			EventType:            fallbackDash(strings.TrimSpace(event.EventType)),
			EventTime:            formatProgrammaticDate(event.EventTime),
			Actor:                fallbackDash(actorDisplayName(event.ActorDisplayName, event.ActorExternalID)),
			Target:               fallbackDash(actorDisplayName(event.TargetDisplayName, event.TargetExternalID)),
			CredentialKind:       fallbackDash(strings.TrimSpace(event.CredentialKind)),
			CredentialExternalID: fallbackDash(strings.TrimSpace(event.CredentialExternalID)),
		})
	}

	assetHref := h.resolveCredentialAssetHref(ctx, credential)
	displayName := strings.TrimSpace(credential.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(credential.ExternalID)
	}
	now := time.Now().UTC()
	riskLevel := credentialRiskLevel(credential, now)
	riskReasons := credentialRiskReasons(credential, now)
	linkResolver := newIdentityLinkResolver(h, ctx)

	data := viewmodels.CredentialShowViewData{
		Layout: layout,
		Credential: viewmodels.CredentialArtifactSummaryView{
			ID:                 credential.ID,
			SourceKind:         strings.TrimSpace(credential.SourceKind),
			SourceName:         strings.TrimSpace(credential.SourceName),
			CredentialKind:     fallbackDash(strings.TrimSpace(credential.CredentialKind)),
			DisplayName:        fallbackDash(displayName),
			ExternalID:         fallbackDash(strings.TrimSpace(credential.ExternalID)),
			AssetRefKind:       fallbackDash(strings.TrimSpace(credential.AssetRefKind)),
			AssetRefExternalID: fallbackDash(strings.TrimSpace(credential.AssetRefExternalID)),
			Status:             fallbackDash(strings.TrimSpace(credential.Status)),
			RiskLevel:          riskLevel,
			CreatedAtSource:    formatProgrammaticDate(credential.CreatedAtSource),
			ExpiresAtSource:    formatProgrammaticDate(credential.ExpiresAtSource),
			LastUsedAtSource:   formatProgrammaticDate(credential.LastUsedAtSource),
			CreatedBy:          fallbackDash(actorDisplayName(credential.CreatedByDisplayName, credential.CreatedByExternalID)),
			CreatedByHref:      linkResolver.Resolve(strings.TrimSpace(credential.SourceKind), strings.TrimSpace(credential.SourceName), credential.CreatedByExternalID, "", credential.CreatedByDisplayName),
			ApprovedBy:         fallbackDash(actorDisplayName(credential.ApprovedByDisplayName, credential.ApprovedByExternalID)),
			ApprovedByHref:     linkResolver.Resolve(strings.TrimSpace(credential.SourceKind), strings.TrimSpace(credential.SourceName), credential.ApprovedByExternalID, "", credential.ApprovedByDisplayName),
			AssetHref:          assetHref,
		},
		ScopeJSON:   prettyProgrammaticJSON(credential.ScopeJson),
		AuditEvents: eventItems,
		RiskReasons: riskReasons,
		HasEvents:   len(eventItems) > 0,
	}

	return h.RenderComponent(c, views.CredentialShowPage(data))
}

func availableProgrammaticSources(snap ConnectorSnapshot) []viewmodels.ProgrammaticSourceOption {
	sources := make([]viewmodels.ProgrammaticSourceOption, 0, 4)

	if snap.EntraEnabled && snap.EntraConfigured {
		if sourceName := strings.TrimSpace(snap.Entra.TenantID); sourceName != "" {
			sources = append(sources, viewmodels.ProgrammaticSourceOption{
				SourceKind: "entra",
				SourceName: sourceName,
				Label:      sourcePrimaryLabel("entra"),
			})
		}
	}
	if snap.GoogleWorkspaceEnabled && snap.GoogleWorkspaceConfigured {
		if sourceName := strings.TrimSpace(snap.GoogleWorkspace.CustomerID); sourceName != "" {
			sources = append(sources, viewmodels.ProgrammaticSourceOption{
				SourceKind: configstore.KindGoogleWorkspace,
				SourceName: sourceName,
				Label:      sourcePrimaryLabel(configstore.KindGoogleWorkspace),
			})
		}
	}
	if snap.GitHubEnabled && snap.GitHubConfigured {
		if sourceName := strings.TrimSpace(snap.GitHub.Org); sourceName != "" {
			sources = append(sources, viewmodels.ProgrammaticSourceOption{
				SourceKind: "github",
				SourceName: sourceName,
				Label:      sourcePrimaryLabel("github"),
			})
		}
	}
	if snap.VaultEnabled && snap.VaultConfigured {
		if sourceName := strings.TrimSpace(snap.Vault.SourceName()); sourceName != "" {
			sources = append(sources, viewmodels.ProgrammaticSourceOption{
				SourceKind: "vault",
				SourceName: sourceName,
				Label:      sourcePrimaryLabel("vault"),
			})
		}
	}

	sort.SliceStable(sources, func(i, j int) bool {
		if sources[i].Label == sources[j].Label {
			return strings.ToLower(sources[i].SourceName) < strings.ToLower(sources[j].SourceName)
		}
		return sources[i].Label < sources[j].Label
	})
	return sources
}

func selectProgrammaticSource(c *echo.Context, sources []viewmodels.ProgrammaticSourceOption) (viewmodels.ProgrammaticSourceOption, bool) {
	if len(sources) == 0 {
		return viewmodels.ProgrammaticSourceOption{}, false
	}

	queryKind := strings.ToLower(strings.TrimSpace(c.QueryParam("source_kind")))
	queryName := strings.TrimSpace(c.QueryParam("source_name"))

	for _, source := range sources {
		if source.SourceKind == queryKind {
			return viewmodels.ProgrammaticSourceOption{
				SourceKind: source.SourceKind,
			}, true
		}
	}

	if queryName != "" {
		for _, source := range sources {
			if strings.EqualFold(strings.TrimSpace(source.SourceName), queryName) {
				return viewmodels.ProgrammaticSourceOption{
					SourceKind: source.SourceKind,
				}, true
			}
		}
	}

	// Unknown source filters fall back to "All configured".
	return viewmodels.ProgrammaticSourceOption{}, true
}

func effectiveProgrammaticSources(selected viewmodels.ProgrammaticSourceOption, all []viewmodels.ProgrammaticSourceOption) []viewmodels.ProgrammaticSourceOption {
	if len(all) == 0 {
		return nil
	}

	selectedKind := strings.TrimSpace(selected.SourceKind)
	selectedName := strings.TrimSpace(selected.SourceName)
	if selectedKind == "" && selectedName == "" {
		out := make([]viewmodels.ProgrammaticSourceOption, len(all))
		copy(out, all)
		return out
	}

	out := make([]viewmodels.ProgrammaticSourceOption, 0, len(all))
	for _, source := range all {
		if selectedKind != "" && source.SourceKind != selectedKind {
			continue
		}
		if selectedName != "" && source.SourceName != selectedName {
			continue
		}
		out = append(out, source)
	}
	return out
}

func (h *Handlers) listAppAssetsAcrossSources(ctx context.Context, sources []viewmodels.ProgrammaticSourceOption, assetKind, query string) ([]gen.AppAsset, error) {
	out := make([]gen.AppAsset, 0)
	for _, source := range sources {
		rows, err := h.listAppAssetsForSource(ctx, source, assetKind, query)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

func (h *Handlers) listAppAssetsForSource(ctx context.Context, source viewmodels.ProgrammaticSourceOption, assetKind, query string) ([]gen.AppAsset, error) {
	const pageSize = 1000
	out := make([]gen.AppAsset, 0)
	for offset := 0; ; offset += pageSize {
		rows, err := h.Q.ListAppAssetsPageBySourceAndQueryAndKind(ctx, gen.ListAppAssetsPageBySourceAndQueryAndKindParams{
			SourceKind: source.SourceKind,
			SourceName: source.SourceName,
			AssetKind:  assetKind,
			Query:      query,
			PageLimit:  int32(pageSize),
			PageOffset: int32(offset),
		})
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if len(rows) < pageSize {
			break
		}
	}
	return out, nil
}

func (h *Handlers) listCredentialsAcrossSources(ctx context.Context, sources []viewmodels.ProgrammaticSourceOption, credentialKind, status, riskLevel, expiryState string, expiresInDays int, query string) ([]gen.CredentialArtifact, error) {
	out := make([]gen.CredentialArtifact, 0)
	for _, source := range sources {
		rows, err := h.listCredentialsForSource(ctx, source, credentialKind, status, riskLevel, expiryState, expiresInDays, query)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

func (h *Handlers) listCredentialsForSource(ctx context.Context, source viewmodels.ProgrammaticSourceOption, credentialKind, status, riskLevel, expiryState string, expiresInDays int, query string) ([]gen.CredentialArtifact, error) {
	const pageSize = 1000
	out := make([]gen.CredentialArtifact, 0)
	for offset := 0; ; offset += pageSize {
		rows, err := h.Q.ListCredentialArtifactsPageBySourceAndQueryAndFilters(ctx, gen.ListCredentialArtifactsPageBySourceAndQueryAndFiltersParams{
			SourceKind:     source.SourceKind,
			SourceName:     source.SourceName,
			CredentialKind: credentialKind,
			Status:         status,
			RiskLevel:      riskLevel,
			ExpiryState:    expiryState,
			ExpiresInDays:  int32(expiresInDays),
			Query:          query,
			PageLimit:      int32(pageSize),
			PageOffset:     int32(offset),
		})
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if len(rows) < pageSize {
			break
		}
	}
	return out, nil
}

func sortAppAssetsForList(rows []gen.AppAsset) {
	sort.SliceStable(rows, func(i, j int) bool {
		leftName := strings.ToLower(strings.TrimSpace(rows[i].DisplayName))
		if leftName == "" {
			leftName = strings.ToLower(strings.TrimSpace(rows[i].ExternalID))
		}
		rightName := strings.ToLower(strings.TrimSpace(rows[j].DisplayName))
		if rightName == "" {
			rightName = strings.ToLower(strings.TrimSpace(rows[j].ExternalID))
		}
		if leftName != rightName {
			return leftName < rightName
		}
		if rows[i].SourceKind != rows[j].SourceKind {
			return rows[i].SourceKind < rows[j].SourceKind
		}
		if rows[i].SourceName != rows[j].SourceName {
			return rows[i].SourceName < rows[j].SourceName
		}
		return rows[i].ID < rows[j].ID
	})
}

func sortCredentialsForList(rows []gen.CredentialArtifact) {
	sort.SliceStable(rows, func(i, j int) bool {
		leftExpires := rows[i].ExpiresAtSource
		rightExpires := rows[j].ExpiresAtSource
		if leftExpires.Valid != rightExpires.Valid {
			// NULL sorts last, equivalent to COALESCE(expires_at_source, 'infinity').
			return leftExpires.Valid
		}
		if leftExpires.Valid && !leftExpires.Time.Equal(rightExpires.Time) {
			return leftExpires.Time.Before(rightExpires.Time)
		}

		leftName := strings.ToLower(strings.TrimSpace(rows[i].DisplayName))
		if leftName == "" {
			leftName = strings.ToLower(strings.TrimSpace(rows[i].ExternalID))
		}
		rightName := strings.ToLower(strings.TrimSpace(rows[j].DisplayName))
		if rightName == "" {
			rightName = strings.ToLower(strings.TrimSpace(rows[j].ExternalID))
		}
		if leftName != rightName {
			return leftName < rightName
		}
		if rows[i].SourceKind != rows[j].SourceKind {
			return rows[i].SourceKind < rows[j].SourceKind
		}
		if rows[i].SourceName != rows[j].SourceName {
			return rows[i].SourceName < rows[j].SourceName
		}
		return rows[i].ID < rows[j].ID
	})
}

func paginateAppAssets(rows []gen.AppAsset, offset, limit int) []gen.AppAsset {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || offset >= len(rows) {
		return nil
	}
	end := min(offset+limit, len(rows))
	return rows[offset:end]
}

func paginateCredentials(rows []gen.CredentialArtifact, offset, limit int) []gen.CredentialArtifact {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || offset >= len(rows) {
		return nil
	}
	end := min(offset+limit, len(rows))
	return rows[offset:end]
}

func parsePositiveInt64Param(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("missing id")
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, errors.New("invalid id")
	}
	return parsed, nil
}

func parseIntParamDefault(raw string, defaultValue int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return defaultValue
	}
	return value
}

func normalizeCredentialRiskFilter(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "critical", "high", "medium", "low":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func fallbackDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "—"
	}
	return value
}

func actorDisplayName(displayName, externalID string) string {
	displayName = strings.TrimSpace(displayName)
	if displayName != "" {
		return displayName
	}
	return strings.TrimSpace(externalID)
}

func formatProgrammaticTime(value pgtype.Timestamptz) string {
	if !value.Valid {
		return "—"
	}
	return value.Time.UTC().Format("Jan 2, 2006 15:04 UTC")
}

func formatProgrammaticDate(value pgtype.Timestamptz) string {
	return identityCalendarDate(value)
}

func credentialRiskLevel(credential gen.CredentialArtifact, now time.Time) string {
	now = now.UTC()
	status := strings.ToLower(strings.TrimSpace(credential.Status))
	credentialKind := strings.ToLower(strings.TrimSpace(credential.CredentialKind))
	createdByExternalID := strings.TrimSpace(credential.CreatedByExternalID)
	approvedByExternalID := strings.TrimSpace(credential.ApprovedByExternalID)

	if credential.ExpiresAtSource.Valid && credential.ExpiresAtSource.Time.UTC().Before(now) {
		if isCredentialStatusActiveLike(status) {
			return "critical"
		}
		return "high"
	}

	if isHighPrivilegeCredentialKind(credentialKind) && createdByExternalID == "" && approvedByExternalID == "" {
		return "critical"
	}

	if credential.ExpiresAtSource.Valid {
		expiresAt := credential.ExpiresAtSource.Time.UTC()
		if !expiresAt.Before(now) && !expiresAt.After(now.Add(7*24*time.Hour)) {
			return "high"
		}
	}

	if createdByExternalID == "" {
		return "high"
	}

	if credential.LastUsedAtSource.Valid && credential.LastUsedAtSource.Time.UTC().Before(now.Add(-90*24*time.Hour)) {
		return "high"
	}

	if credential.ExpiresAtSource.Valid {
		expiresAt := credential.ExpiresAtSource.Time.UTC()
		if !expiresAt.Before(now) && !expiresAt.After(now.Add(30*24*time.Hour)) {
			return "medium"
		}
	}

	return "low"
}

func credentialRiskReasons(credential gen.CredentialArtifact, now time.Time) []string {
	now = now.UTC()
	reasons := make([]string, 0, 4)

	status := strings.ToLower(strings.TrimSpace(credential.Status))
	credentialKind := strings.ToLower(strings.TrimSpace(credential.CredentialKind))
	createdByExternalID := strings.TrimSpace(credential.CreatedByExternalID)
	approvedByExternalID := strings.TrimSpace(credential.ApprovedByExternalID)

	if credential.ExpiresAtSource.Valid && credential.ExpiresAtSource.Time.UTC().Before(now) {
		if isCredentialStatusActiveLike(status) {
			reasons = append(reasons, "Credential has expired while still marked active.")
		} else {
			reasons = append(reasons, "Credential has expired.")
		}
	}

	if isHighPrivilegeCredentialKind(credentialKind) && createdByExternalID == "" && approvedByExternalID == "" {
		reasons = append(reasons, "High-privilege credential has no creator or approver attribution.")
	}

	if credential.ExpiresAtSource.Valid {
		expiresAt := credential.ExpiresAtSource.Time.UTC()
		if !expiresAt.Before(now) && !expiresAt.After(now.Add(7*24*time.Hour)) {
			reasons = append(reasons, "Credential expires within 7 days.")
		} else if !expiresAt.Before(now) && !expiresAt.After(now.Add(30*24*time.Hour)) {
			reasons = append(reasons, "Credential expires within 30 days.")
		}
	}

	if createdByExternalID == "" {
		reasons = append(reasons, "Creator attribution is missing.")
	}

	if credential.LastUsedAtSource.Valid && credential.LastUsedAtSource.Time.UTC().Before(now.Add(-90*24*time.Hour)) {
		reasons = append(reasons, "Credential has not been used in over 90 days.")
	}

	if len(reasons) == 0 {
		reasons = append(reasons, "Credential metadata appears healthy based on current heuristics.")
	}

	return reasons
}

func isCredentialStatusActiveLike(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "", "active", "approved", "pending_approval":
		return true
	default:
		return false
	}
}

func isHighPrivilegeCredentialKind(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "entra_client_secret", "github_deploy_key", "github_pat_request", "github_pat_fine_grained":
		return true
	default:
		return false
	}
}

type identityLinkResolver struct {
	h                    *Handlers
	ctx                  context.Context
	emailHrefByCandidate map[string]string
	actorHrefByKey       map[string]string
}

func newIdentityLinkResolver(h *Handlers, ctx context.Context) *identityLinkResolver {
	return &identityLinkResolver{
		h:                    h,
		ctx:                  ctx,
		emailHrefByCandidate: map[string]string{},
		actorHrefByKey:       map[string]string{},
	}
}

func (r *identityLinkResolver) Resolve(sourceKind, sourceName, externalID, email, displayName string) string {
	if r == nil || r.h == nil || r.h.Q == nil {
		return ""
	}

	if candidate := emailCandidate(externalID); candidate != "" {
		if href := r.resolveByEmail(candidate); href != "" {
			return href
		}
	}

	if href := r.resolveBySourceAndExternalID(sourceKind, sourceName, externalID); href != "" {
		return href
	}

	if candidate := emailCandidate(email); candidate != "" {
		if href := r.resolveByEmail(candidate); href != "" {
			return href
		}
	}

	if candidate := emailCandidate(displayName); candidate != "" {
		if href := r.resolveByEmail(candidate); href != "" {
			return href
		}
	}

	return ""
}

func (r *identityLinkResolver) resolveBySourceAndExternalID(sourceKind, sourceName, externalID string) string {
	sourceKind = strings.TrimSpace(sourceKind)
	sourceName = strings.TrimSpace(sourceName)
	externalID = strings.TrimSpace(externalID)
	if sourceKind == "" || sourceName == "" || externalID == "" {
		return ""
	}

	cacheKey := strings.ToLower(sourceKind) + "|" + strings.ToLower(sourceName) + "|" + strings.ToLower(externalID)
	if href, ok := r.actorHrefByKey[cacheKey]; ok {
		return href
	}

	identity, err := r.h.Q.GetIdentityBySourceAndExternalID(r.ctx, gen.GetIdentityBySourceAndExternalIDParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
		ExternalID: externalID,
	})
	if err != nil {
		r.actorHrefByKey[cacheKey] = ""
		return ""
	}

	href := "/identities/" + strconv.FormatInt(identity.ID, 10)
	r.actorHrefByKey[cacheKey] = href
	return href
}

func (r *identityLinkResolver) resolveByEmail(candidate string) string {
	candidate = emailCandidate(candidate)
	if candidate == "" {
		return ""
	}
	if href, ok := r.emailHrefByCandidate[candidate]; ok {
		return href
	}

	identity, err := r.h.Q.GetPreferredIdentityByPrimaryEmail(r.ctx, candidate)
	if err != nil {
		r.emailHrefByCandidate[candidate] = ""
		return ""
	}

	href := "/identities/" + strconv.FormatInt(identity.ID, 10)
	r.emailHrefByCandidate[candidate] = href
	return href
}

func emailCandidate(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	if left := strings.Index(raw, "<"); left >= 0 {
		if right := strings.Index(raw[left+1:], ">"); right >= 0 {
			raw = raw[left+1 : left+1+right]
		}
	}

	raw = strings.Trim(strings.TrimSpace(raw), "\"'<>[](){}.,;")
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" || strings.Count(raw, "@") != 1 {
		return ""
	}
	if strings.ContainsAny(raw, " \t\r\n") {
		return ""
	}
	return raw
}

func prettyProgrammaticJSON(raw []byte) string {
	if len(raw) == 0 {
		return "{}"
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return string(raw)
	}
	return out.String()
}

func credentialRefKey(kind, externalID string) string {
	return strings.TrimSpace(kind) + "|" + strings.TrimSpace(externalID)
}

func credentialSourceRefKey(sourceKind, sourceName, kind, externalID string) string {
	return sourceKey(sourceKind, sourceName) + "|" + credentialRefKey(kind, externalID)
}

func appAssetCredentialRef(asset gen.AppAsset) (string, string) {
	assetKind := strings.TrimSpace(asset.AssetKind)
	externalID := strings.TrimSpace(asset.ExternalID)
	if externalID == "" {
		return "", ""
	}

	if strings.EqualFold(strings.TrimSpace(asset.SourceKind), "entra") {
		return "app_asset", appAssetRefExternalID(assetKind, externalID)
	}
	return "app_asset", externalID
}

func appAssetCredentialRefs(asset gen.AppAsset) []gen.ListCredentialArtifactsForAssetRefParams {
	refs := make([]gen.ListCredentialArtifactsForAssetRefParams, 0, 2)
	assetKind := strings.TrimSpace(asset.AssetKind)
	externalID := strings.TrimSpace(asset.ExternalID)
	if externalID == "" {
		return refs
	}

	sourceKind := strings.TrimSpace(asset.SourceKind)
	sourceName := strings.TrimSpace(asset.SourceName)
	if sourceKind == "" || sourceName == "" {
		return refs
	}

	addRef := func(assetRefExternalID string) {
		assetRefExternalID = strings.TrimSpace(assetRefExternalID)
		if assetRefExternalID == "" {
			return
		}
		params := gen.ListCredentialArtifactsForAssetRefParams{
			SourceKind:         sourceKind,
			SourceName:         sourceName,
			AssetRefKind:       "app_asset",
			AssetRefExternalID: assetRefExternalID,
		}
		for _, existing := range refs {
			if existing.AssetRefKind == params.AssetRefKind && existing.AssetRefExternalID == params.AssetRefExternalID {
				return
			}
		}
		refs = append(refs, params)
	}

	addRef(appAssetRefExternalID(assetKind, externalID))
	addRef(externalID)
	return refs
}

func appAssetRefExternalID(assetKind, externalID string) string {
	assetKind = strings.TrimSpace(assetKind)
	externalID = strings.TrimSpace(externalID)
	if assetKind == "" {
		return externalID
	}
	if externalID == "" {
		return assetKind
	}
	return assetKind + ":" + externalID
}

func (h *Handlers) listCredentialArtifactsForAsset(ctx context.Context, asset gen.AppAsset) ([]gen.CredentialArtifact, error) {
	refs := appAssetCredentialRefs(asset)
	if len(refs) == 0 {
		return nil, nil
	}

	credentialsByID := map[int64]gen.CredentialArtifact{}
	for _, ref := range refs {
		rows, err := h.Q.ListCredentialArtifactsForAssetRef(ctx, ref)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			credentialsByID[row.ID] = row
		}
	}

	out := make([]gen.CredentialArtifact, 0, len(credentialsByID))
	for _, row := range credentialsByID {
		out = append(out, row)
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(out[i].DisplayName))
		right := strings.ToLower(strings.TrimSpace(out[j].DisplayName))
		if left == right {
			return out[i].ID < out[j].ID
		}
		return left < right
	})
	return out, nil
}

func (h *Handlers) resolveCredentialAssetHref(ctx context.Context, credential gen.CredentialArtifact) string {
	if strings.TrimSpace(credential.AssetRefKind) != "app_asset" {
		return ""
	}

	assetRefExternalID := strings.TrimSpace(credential.AssetRefExternalID)
	if assetRefExternalID == "" {
		return ""
	}

	assetKind := ""
	assetExternalID := assetRefExternalID
	if strings.Contains(assetRefExternalID, ":") {
		parts := strings.SplitN(assetRefExternalID, ":", 2)
		assetKind = strings.TrimSpace(parts[0])
		assetExternalID = strings.TrimSpace(parts[1])
	}
	if assetKind == "" || assetExternalID == "" {
		return ""
	}

	asset, err := h.Q.GetAppAssetBySourceAndKindAndExternalID(ctx, gen.GetAppAssetBySourceAndKindAndExternalIDParams{
		SourceKind: strings.TrimSpace(credential.SourceKind),
		SourceName: strings.TrimSpace(credential.SourceName),
		AssetKind:  assetKind,
		ExternalID: assetExternalID,
	})
	if err != nil {
		return ""
	}

	return "/app-assets/" + strconv.FormatInt(asset.ID, 10)
}
