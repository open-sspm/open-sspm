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
	layout, stateView, err := h.LayoutData(ctx, c, "App Assets")
	if err != nil {
		return h.RenderError(c, err)
	}

	sources := availableProgrammaticSources(stateView)
	selected, hasSource := selectProgrammaticSource(c, sources)
	query := strings.TrimSpace(c.QueryParam("q"))
	assetKind := strings.TrimSpace(c.QueryParam("asset_kind"))
	page := parsePageParam(c)
	const perPage = 20
	pagination := newPaginatedListState(0, page, perPage)

	data := viewmodels.AppAssetsViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No app assets found for the current filters.", ""),
		Sources:               sources,
		SelectedSourceKind:    selected.SourceKind,
		SelectedSourceName:    selected.SourceName,
		Query:                 query,
		AssetKind:             assetKind,
	}
	renderAppAssets := func() error {
		if isHX(c) && isHXTarget(c, "app-assets-results") {
			return h.RenderComponent(c, views.AppAssetsPageResults(data))
		}
		return h.RenderComponent(c, views.AppAssetsPage(data))
	}

	if !hasSource {
		data.PaginatedListPageData.EmptyStateMsg = "Configure and enable GitHub, Microsoft Entra, or Vault connectors to populate app assets."
		return renderAppAssets()
	}

	activeSources := effectiveProgrammaticSources(selected, sources)
	if len(activeSources) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "No matching source found. Choose another source filter."
		return renderAppAssets()
	}

	var totalCount int64
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

		pagination = newPaginatedListState(totalCount, page, perPage)
		assets, err = h.Q.ListAppAssetsPageBySourceAndQueryAndKind(ctx, gen.ListAppAssetsPageBySourceAndQueryAndKindParams{
			SourceKind: source.SourceKind,
			SourceName: source.SourceName,
			AssetKind:  assetKind,
			Query:      query,
			PageLimit:  int32(perPage),
			PageOffset: int32(pagination.Offset()),
		})
		if err != nil {
			return h.RenderError(c, err)
		}
	} else {
		sourceKinds, sourceNames := programmaticConfiguredSourcePairs(activeSources)
		totalCount, err = h.Q.CountAppAssetsBySourcesAndQueryAndKind(ctx, gen.CountAppAssetsBySourcesAndQueryAndKindParams{
			ConfiguredSourceKinds: sourceKinds,
			ConfiguredSourceNames: sourceNames,
			AssetKind:             assetKind,
			Query:                 query,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		pagination = newPaginatedListState(totalCount, page, perPage)
		assets, err = h.Q.ListAppAssetsPageBySourcesAndQueryAndKind(ctx, gen.ListAppAssetsPageBySourcesAndQueryAndKindParams{
			ConfiguredSourceKinds: sourceKinds,
			ConfiguredSourceNames: sourceNames,
			AssetKind:             assetKind,
			Query:                 query,
			PageLimit:             int32(perPage),
			PageOffset:            int32(pagination.Offset()),
		})
		if err != nil {
			return h.RenderError(c, err)
		}
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

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No app assets found for the current filters.", "")
	data.HasItems = len(items) > 0
	if query != "" || assetKind != "" {
		data.PaginatedListPageData.EmptyStateMsg = "No app assets match the current search filters."
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

	credentialRows, err := h.listCredentialArtifactsForAsset(ctx, asset, pgTimestamptz(now))
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
			RiskLevel:      strings.TrimSpace(credential.RiskLevel),
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
	layout, stateView, err := h.LayoutData(ctx, c, "Credentials")
	if err != nil {
		return h.RenderError(c, err)
	}

	sources := availableProgrammaticSources(stateView)
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
	pagination := newPaginatedListState(0, page, perPage)

	data := viewmodels.CredentialsViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No credentials found for the current filters.", ""),
		Sources:               sources,
		SelectedSourceKind:    selected.SourceKind,
		SelectedSourceName:    selected.SourceName,
		Query:                 query,
		CredentialKind:        credentialKind,
		Status:                status,
		RiskLevel:             riskLevel,
		ExpiryState:           expiryState,
		ExpiresInDays:         expiresInDays,
	}
	renderCredentials := func() error {
		if isHX(c) && isHXTarget(c, "credentials-results") {
			return h.RenderComponent(c, views.CredentialsPageResults(data))
		}
		return h.RenderComponent(c, views.CredentialsPage(data))
	}

	if !hasSource {
		data.PaginatedListPageData.EmptyStateMsg = "Configure and enable GitHub, Microsoft Entra, or Vault connectors to populate credential inventory."
		return renderCredentials()
	}

	activeSources := effectiveProgrammaticSources(selected, sources)
	if len(activeSources) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "No matching source found. Choose another source filter."
		return renderCredentials()
	}

	now := time.Now().UTC()
	evaluatedAt := pgTimestamptz(now)
	linkResolver := newIdentityLinkResolver(h, ctx)
	var totalCount int64
	var items []viewmodels.CredentialArtifactListItem

	if len(activeSources) == 1 {
		source := activeSources[0]
		totalCount, err = h.Q.CountCredentialArtifactsBySourceAndQueryAndFilters(ctx, gen.CountCredentialArtifactsBySourceAndQueryAndFiltersParams{
			EvaluatedAt:    evaluatedAt,
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

		pagination = newPaginatedListState(totalCount, page, perPage)
		rows, err := h.Q.ListCredentialArtifactsPageBySourceAndQueryAndFilters(ctx, gen.ListCredentialArtifactsPageBySourceAndQueryAndFiltersParams{
			EvaluatedAt:    evaluatedAt,
			SourceKind:     source.SourceKind,
			SourceName:     source.SourceName,
			CredentialKind: credentialKind,
			Status:         status,
			RiskLevel:      riskLevel,
			ExpiryState:    expiryState,
			ExpiresInDays:  int32(expiresInDays),
			Query:          query,
			PageLimit:      int32(perPage),
			PageOffset:     int32(pagination.Offset()),
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		items = make([]viewmodels.CredentialArtifactListItem, 0, len(rows))
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
				RiskLevel:      strings.TrimSpace(row.RiskLevel),
				ExpiresAt:      formatProgrammaticDate(row.ExpiresAtSource),
				LastUsedAt:     formatProgrammaticDate(row.LastUsedAtSource),
				CreatedBy:      createdBy,
				CreatedByHref:  linkResolver.Resolve(strings.TrimSpace(row.SourceKind), strings.TrimSpace(row.SourceName), row.CreatedByExternalID, "", row.CreatedByDisplayName),
				ApprovedBy:     approvedBy,
				ApprovedByHref: linkResolver.Resolve(strings.TrimSpace(row.SourceKind), strings.TrimSpace(row.SourceName), row.ApprovedByExternalID, "", row.ApprovedByDisplayName),
			})
		}
	} else {
		sourceKinds, sourceNames := programmaticConfiguredSourcePairs(activeSources)
		totalCount, err = h.Q.CountCredentialArtifactsBySourcesAndQueryAndFilters(ctx, gen.CountCredentialArtifactsBySourcesAndQueryAndFiltersParams{
			EvaluatedAt:           evaluatedAt,
			ConfiguredSourceKinds: sourceKinds,
			ConfiguredSourceNames: sourceNames,
			CredentialKind:        credentialKind,
			Status:                status,
			RiskLevel:             riskLevel,
			ExpiryState:           expiryState,
			ExpiresInDays:         int32(expiresInDays),
			Query:                 query,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		pagination = newPaginatedListState(totalCount, page, perPage)
		rows, err := h.Q.ListCredentialArtifactsPageBySourcesAndQueryAndFilters(ctx, gen.ListCredentialArtifactsPageBySourcesAndQueryAndFiltersParams{
			EvaluatedAt:           evaluatedAt,
			ConfiguredSourceKinds: sourceKinds,
			ConfiguredSourceNames: sourceNames,
			CredentialKind:        credentialKind,
			Status:                status,
			RiskLevel:             riskLevel,
			ExpiryState:           expiryState,
			ExpiresInDays:         int32(expiresInDays),
			Query:                 query,
			PageLimit:             int32(perPage),
			PageOffset:            int32(pagination.Offset()),
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		items = make([]viewmodels.CredentialArtifactListItem, 0, len(rows))
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
				RiskLevel:      strings.TrimSpace(row.RiskLevel),
				ExpiresAt:      formatProgrammaticDate(row.ExpiresAtSource),
				LastUsedAt:     formatProgrammaticDate(row.LastUsedAtSource),
				CreatedBy:      createdBy,
				CreatedByHref:  linkResolver.Resolve(strings.TrimSpace(row.SourceKind), strings.TrimSpace(row.SourceName), row.CreatedByExternalID, "", row.CreatedByDisplayName),
				ApprovedBy:     approvedBy,
				ApprovedByHref: linkResolver.Resolve(strings.TrimSpace(row.SourceKind), strings.TrimSpace(row.SourceName), row.ApprovedByExternalID, "", row.ApprovedByDisplayName),
			})
		}
	}

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No credentials found for the current filters.", "")
	data.HasItems = len(items) > 0
	if query != "" || credentialKind != "" || status != "" || riskLevel != "" || expiryState != "" || expiresInDays > 0 {
		data.PaginatedListPageData.EmptyStateMsg = "No credentials match the current search filters."
	}

	return renderCredentials()
}

func (h *Handlers) HandleCredentialShow(c *echo.Context) error {
	credentialID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	now := time.Now().UTC()
	credential, err := h.Q.GetCredentialArtifactByID(ctx, gen.GetCredentialArtifactByIDParams{
		EvaluatedAt: pgTimestamptz(now),
		ID:          credentialID,
	})
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

	assetHref := h.resolveCredentialAssetHref(ctx, credential.SourceKind, credential.SourceName, credential.AssetRefKind, credential.AssetRefExternalID)
	displayName := strings.TrimSpace(credential.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(credential.ExternalID)
	}
	riskLevel := strings.TrimSpace(credential.RiskLevel)
	riskReasons := credentialRiskReasonsFor(credential.Status, credential.CredentialKind, credential.CreatedByExternalID, credential.ApprovedByExternalID, credential.ExpiresAtSource, credential.LastUsedAtSource, now)
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

func availableProgrammaticSources(stateView connectorStateView) []viewmodels.ProgrammaticSourceOption {
	return programmaticSourcesByView(stateView, true)
}

func configuredProgrammaticSources(stateView connectorStateView) []viewmodels.ProgrammaticSourceOption {
	return programmaticSourcesByView(stateView, false)
}

func programmaticConfiguredSourcePairs(sourcePairs []viewmodels.ProgrammaticSourceOption) ([]string, []string) {
	kinds := make([]string, 0, len(sourcePairs))
	names := make([]string, 0, len(sourcePairs))
	for _, source := range sourcePairs {
		kind := strings.TrimSpace(source.SourceKind)
		name := strings.TrimSpace(source.SourceName)
		if kind == "" || name == "" {
			continue
		}
		kinds = append(kinds, kind)
		names = append(names, name)
	}
	return kinds, names
}

func programmaticSourcesByView(stateView connectorStateView, requireEnabled bool) []viewmodels.ProgrammaticSourceOption {
	sources := make([]viewmodels.ProgrammaticSourceOption, 0, 4)

	entra := stateView.Entra()
	if entra.Configured() && (!requireEnabled || entra.Enabled()) {
		if sourceName := entra.SourceName(); sourceName != "" {
			sources = append(sources, viewmodels.ProgrammaticSourceOption{
				SourceKind: querySourceKind("entra"),
				SourceName: sourceName,
				Label:      sourcePrimaryLabel("entra"),
			})
		}
	}
	google := stateView.GoogleWorkspace()
	if google.Configured() && (!requireEnabled || google.Enabled()) {
		if sourceName := google.SourceName(); sourceName != "" {
			sources = append(sources, viewmodels.ProgrammaticSourceOption{
				SourceKind: configstore.KindGoogleWorkspace,
				SourceName: sourceName,
				Label:      sourcePrimaryLabel(configstore.KindGoogleWorkspace),
			})
		}
	}
	github := stateView.GitHub()
	if github.Configured() && (!requireEnabled || github.Enabled()) {
		if sourceName := github.SourceName(); sourceName != "" {
			sources = append(sources, viewmodels.ProgrammaticSourceOption{
				SourceKind: querySourceKind("github"),
				SourceName: sourceName,
				Label:      sourcePrimaryLabel("github"),
			})
		}
	}
	vault := stateView.Vault()
	if vault.Configured() && (!requireEnabled || vault.Enabled()) {
		if sourceName := vault.SourceName(); sourceName != "" {
			sources = append(sources, viewmodels.ProgrammaticSourceOption{
				SourceKind: querySourceKind("vault"),
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

func pgTimestamptz(ts time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ts.UTC(), Valid: true}
}

func credentialRiskReasons(credential gen.CredentialArtifact, now time.Time) []string {
	return credentialRiskReasonsFor(
		credential.Status,
		credential.CredentialKind,
		credential.CreatedByExternalID,
		credential.ApprovedByExternalID,
		credential.ExpiresAtSource,
		credential.LastUsedAtSource,
		now,
	)
}

func credentialRiskReasonsFor(statusValue, credentialKindValue, createdByValue, approvedByValue string, expiresAt, lastUsedAt pgtype.Timestamptz, now time.Time) []string {
	now = now.UTC()
	reasons := make([]string, 0, 4)

	status := strings.ToLower(strings.TrimSpace(statusValue))
	credentialKind := strings.ToLower(strings.TrimSpace(credentialKindValue))
	createdByExternalID := strings.TrimSpace(createdByValue)
	approvedByExternalID := strings.TrimSpace(approvedByValue)

	if expiresAt.Valid && expiresAt.Time.UTC().Before(now) {
		if isCredentialStatusActiveLike(status) {
			reasons = append(reasons, "Credential has expired while still marked active.")
		} else {
			reasons = append(reasons, "Credential has expired.")
		}
	}

	if isHighPrivilegeCredentialKind(credentialKind) && createdByExternalID == "" && approvedByExternalID == "" {
		reasons = append(reasons, "High-privilege credential has no creator or approver attribution.")
	}

	if expiresAt.Valid {
		expiresAtTime := expiresAt.Time.UTC()
		if !expiresAtTime.Before(now) && !expiresAtTime.After(now.Add(7*24*time.Hour)) {
			reasons = append(reasons, "Credential expires within 7 days.")
		} else if !expiresAtTime.Before(now) && !expiresAtTime.After(now.Add(30*24*time.Hour)) {
			reasons = append(reasons, "Credential expires within 30 days.")
		}
	}

	if createdByExternalID == "" {
		reasons = append(reasons, "Creator attribution is missing.")
	}

	if lastUsedAt.Valid && lastUsedAt.Time.UTC().Before(now.Add(-90*24*time.Hour)) {
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

	switch NormalizeConnectorKind(asset.SourceKind) {
	case configstore.KindEntra:
		return "app_asset", appAssetRefExternalID(assetKind, externalID)
	case configstore.KindGoogleWorkspace:
		refKind := assetKind
		if refKind == "" {
			refKind = "app_asset"
		}
		return refKind, appAssetRefExternalID(assetKind, externalID)
	default:
		return "app_asset", externalID
	}
}

func appAssetCredentialRefs(asset gen.AppAsset) []gen.ListCredentialArtifactsForAssetRefParams {
	refs := make([]gen.ListCredentialArtifactsForAssetRefParams, 0, 3)
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

	addRef := func(assetRefKind, assetRefExternalID string) {
		assetRefKind = strings.TrimSpace(assetRefKind)
		assetRefExternalID = strings.TrimSpace(assetRefExternalID)
		if assetRefKind == "" || assetRefExternalID == "" {
			return
		}
		params := gen.ListCredentialArtifactsForAssetRefParams{
			SourceKind:         sourceKind,
			SourceName:         sourceName,
			AssetRefKind:       assetRefKind,
			AssetRefExternalID: assetRefExternalID,
		}
		for _, existing := range refs {
			if existing.AssetRefKind == params.AssetRefKind && existing.AssetRefExternalID == params.AssetRefExternalID {
				return
			}
		}
		refs = append(refs, params)
	}

	switch NormalizeConnectorKind(sourceKind) {
	case configstore.KindGoogleWorkspace:
		if assetKind != "" {
			addRef(assetKind, appAssetRefExternalID(assetKind, externalID))
		}
		addRef("app_asset", appAssetRefExternalID(assetKind, externalID))
		addRef("app_asset", externalID)
	default:
		addRef("app_asset", appAssetRefExternalID(assetKind, externalID))
		addRef("app_asset", externalID)
	}
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

func (h *Handlers) listCredentialArtifactsForAsset(ctx context.Context, asset gen.AppAsset, evaluatedAt pgtype.Timestamptz) ([]gen.ListCredentialArtifactsForAssetRefRow, error) {
	refs := appAssetCredentialRefs(asset)
	if len(refs) == 0 {
		return nil, nil
	}

	credentialsByID := map[int64]gen.ListCredentialArtifactsForAssetRefRow{}
	for _, ref := range refs {
		ref.EvaluatedAt = evaluatedAt
		rows, err := h.Q.ListCredentialArtifactsForAssetRef(ctx, ref)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			credentialsByID[row.ID] = row
		}
	}

	out := make([]gen.ListCredentialArtifactsForAssetRefRow, 0, len(credentialsByID))
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

func credentialAssetLookupKey(credential gen.CredentialArtifact) (string, string, bool) {
	return credentialAssetLookupKeyValues(credential.AssetRefKind, credential.AssetRefExternalID)
}

func credentialAssetLookupKeyValues(assetRefKind, assetRefExternalID string) (string, string, bool) {
	assetRefKind = strings.TrimSpace(assetRefKind)
	assetRefExternalID = strings.TrimSpace(assetRefExternalID)
	if assetRefExternalID == "" {
		return "", "", false
	}

	assetKind := ""
	assetExternalID := assetRefExternalID
	if strings.Contains(assetRefExternalID, ":") {
		parts := strings.SplitN(assetRefExternalID, ":", 2)
		assetKind = strings.TrimSpace(parts[0])
		assetExternalID = strings.TrimSpace(parts[1])
	}
	if assetKind == "" && assetRefKind != "" && assetRefKind != "app_asset" {
		assetKind = assetRefKind
	}
	if assetKind == "" || assetExternalID == "" {
		return "", "", false
	}

	return assetKind, assetExternalID, true
}

func (h *Handlers) resolveCredentialAssetHref(ctx context.Context, sourceKind, sourceName, assetRefKind, assetRefExternalID string) string {
	assetKind, assetExternalID, ok := credentialAssetLookupKeyValues(assetRefKind, assetRefExternalID)
	if !ok {
		return ""
	}

	asset, err := h.Q.GetAppAssetBySourceAndKindAndExternalID(ctx, gen.GetAppAssetBySourceAndKindAndExternalIDParams{
		SourceKind: strings.TrimSpace(sourceKind),
		SourceName: strings.TrimSpace(sourceName),
		AssetKind:  assetKind,
		ExternalID: assetExternalID,
	})
	if err != nil {
		return ""
	}

	return "/app-assets/" + strconv.FormatInt(asset.ID, 10)
}
