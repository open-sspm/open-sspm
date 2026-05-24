package handlers

import (
	"bytes"
	"context"
	"encoding/csv"
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
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/riskpolicy"
)

func (h *Handlers) HandleAppAssets(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "App Assets")
	if err != nil {
		return h.RenderError(c, err)
	}

	sources := availableProgrammaticSources(stateView)
	queryState := querystate.ParseAppAssetsQuery(c.Request().URL.Query(), programmaticQuerySources(sources))
	connectedAppsQuery := querystate.ParseConnectedAppsQuery(c.Request().URL.Query())
	page := queryState.Page
	const perPage = 20
	pagination := newPaginatedListState(0, page, perPage)

	data := viewmodels.AppAssetsViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No app assets found for the current filters.", ""),
		Sources:               sources,
		Query:                 queryState,
	}
	renderAppAssets := func() error {
		if isHX(c) && isHXTarget(c, "app-assets-results") {
			return h.RenderComponent(c, views.AppAssetsPageResults(data))
		}
		return h.RenderComponent(c, views.AppAssetsPage(data))
	}

	if queryState.IsConnectedAppsSlice() {
		oauthData, err := h.buildConnectedAppsViewData(ctx, layout, stateView, connectedAppsQuery)
		if err != nil {
			return h.RenderError(c, err)
		}
		data.PaginatedListPageData = oauthData.PaginatedListPageData
		data.GoogleOAuthView = &oauthData
		data.HasItems = oauthData.HasItems
		return renderAppAssets()
	}

	if len(sources) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "Configure and enable GitHub, Google Workspace, Microsoft Entra, or Vault connectors to populate app assets."
		return renderAppAssets()
	}

	activeSources := effectiveProgrammaticSources(queryState.Source, sources)
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
			AssetKind:  queryState.AssetKind,
			Query:      queryState.Q,
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		pagination = newPaginatedListState(totalCount, page, perPage)
		assets, err = h.Q.ListAppAssetsPageBySourceAndQueryAndKind(ctx, gen.ListAppAssetsPageBySourceAndQueryAndKindParams{
			SourceKind: source.SourceKind,
			SourceName: source.SourceName,
			AssetKind:  queryState.AssetKind,
			Query:      queryState.Q,
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
			AssetKind:             queryState.AssetKind,
			Query:                 queryState.Q,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		pagination = newPaginatedListState(totalCount, page, perPage)
		assets, err = h.Q.ListAppAssetsPageBySourcesAndQueryAndKind(ctx, gen.ListAppAssetsPageBySourcesAndQueryAndKindParams{
			ConfiguredSourceKinds: sourceKinds,
			ConfiguredSourceNames: sourceNames,
			AssetKind:             queryState.AssetKind,
			Query:                 queryState.Q,
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
			LastSeen:         calendarDateDisplay(asset.LastObservedAt),
		})
	}

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No app assets found for the current filters.", "")
	data.HasItems = len(items) > 0
	if queryState.HasFilters() {
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
	if isGoogleConnectedApp(asset.SourceKind, asset.AssetKind) {
		return h.renderAppAssetShow(c, assetID, connectedAppShowOptions{})
	}

	layout, stateView, err := h.LayoutData(ctx, c, "App Asset")
	if err != nil {
		return h.RenderError(c, err)
	}

	owners, err := h.Q.ListAppAssetOwnersByAssetID(ctx, asset.ID)
	if err != nil {
		return h.RenderError(c, err)
	}

	linkResolver := newIdentityLinkResolver(h, ctx, stateView)

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
			RiskLevel:      strings.TrimSpace(credential.RiskLevel),
			ExpiresAt:      calendarDateDisplay(credential.ExpiresAtSource),
			LastUsedAt:     calendarDateDisplay(credential.LastUsedAtSource),
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
			EventTime:             calendarDateDisplay(event.EventTime),
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
			CreatedAtSource:  calendarDateDisplay(asset.CreatedAtSource),
			UpdatedAtSource:  calendarDateDisplay(asset.UpdatedAtSource),
			LastObservedAt:   calendarDateDisplay(asset.LastObservedAt),
		},
		Owners:         ownerItems,
		Credentials:    credentialItems,
		AuditEvents:    auditItems,
		HasOwners:      len(ownerItems) > 0,
		HasCredentials: len(credentialItems) > 0,
		HasAuditEvents: len(auditItems) > 0,
	}

	h.trackNonHumanIdentitiesOutboundClick(c, "app_asset", asset.ID)

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
	queryState := querystate.ParseCredentialsQuery(c.Request().URL.Query(), programmaticQuerySources(sources))
	page := queryState.Page
	const perPage = 20
	pagination := newPaginatedListState(0, page, perPage)

	data := viewmodels.CredentialsViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No credentials found for the current filters.", ""),
		Sources:               sources,
		Query:                 queryState,
	}
	renderCredentials := func() error {
		if isHX(c) && isHXTarget(c, "credentials-results") {
			return h.RenderComponent(c, views.CredentialsPageResults(data))
		}
		return h.RenderComponent(c, views.CredentialsPage(data))
	}

	if len(sources) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "Configure and enable GitHub, Microsoft Entra, or Vault connectors to populate credential inventory."
		return renderCredentials()
	}

	activeSources := effectiveProgrammaticSources(queryState.Source, sources)
	if len(activeSources) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "No matching source found. Choose another source filter."
		return renderCredentials()
	}

	now := time.Now().UTC()
	evaluatedAt := pgTimestamptz(now)
	ownerFilter := credentialOwnerFilter(queryState.Owner, layout.UserEmail)
	linkResolver := newIdentityLinkResolver(h, ctx, stateView)
	var totalCount int64
	var items []viewmodels.CredentialArtifactListItem

	if len(activeSources) == 1 {
		source := activeSources[0]
		totalCount, err = h.Q.CountCredentialArtifactsBySourceAndQueryAndFilters(ctx, gen.CountCredentialArtifactsBySourceAndQueryAndFiltersParams{
			EvaluatedAt:    evaluatedAt,
			SourceKind:     source.SourceKind,
			SourceName:     source.SourceName,
			CredentialKind: queryState.CredentialKind,
			Status:         queryState.Status,
			RiskLevel:      queryState.RiskLevel,
			ExpiryState:    queryState.ExpiryState,
			ExpiresInDays:  int32(queryState.ExpiresInDays),
			Owner:          ownerFilter,
			Asset:          queryState.Asset,
			NewerDays:      int32(queryState.NewerThanDays),
			Query:          queryState.Q,
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		pagination = newPaginatedListState(totalCount, page, perPage)
		rows, err := h.Q.ListCredentialArtifactsPageBySourceAndQueryAndFilters(ctx, gen.ListCredentialArtifactsPageBySourceAndQueryAndFiltersParams{
			EvaluatedAt:    evaluatedAt,
			SourceKind:     source.SourceKind,
			SourceName:     source.SourceName,
			CredentialKind: queryState.CredentialKind,
			Status:         queryState.Status,
			RiskLevel:      queryState.RiskLevel,
			ExpiryState:    queryState.ExpiryState,
			ExpiresInDays:  int32(queryState.ExpiresInDays),
			Owner:          ownerFilter,
			Asset:          queryState.Asset,
			NewerDays:      int32(queryState.NewerThanDays),
			SortBy:         queryState.SortBy,
			Query:          queryState.Q,
			PageLimit:      int32(perPage),
			PageOffset:     int32(pagination.Offset()),
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		items = make([]viewmodels.CredentialArtifactListItem, 0, len(rows))
		for _, row := range rows {
			items = append(items, buildCredentialListItem(now, credentialListRowFromBySource(row), linkResolver))
		}

		summary, err := h.Q.SummarizeCredentialsBySourceAndQuery(ctx, gen.SummarizeCredentialsBySourceAndQueryParams{
			EvaluatedAt:    evaluatedAt,
			SourceKind:     source.SourceKind,
			SourceName:     source.SourceName,
			CredentialKind: queryState.CredentialKind,
			Owner:          ownerFilter,
			Asset:          queryState.Asset,
			NewerDays:      int32(queryState.NewerThanDays),
			Query:          queryState.Q,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		data.Summary = viewmodels.CredentialsSummary{
			Total:           summary.Total,
			Active:          summary.Active,
			Expired:         summary.Expired,
			ExpiringSoon:    summary.ExpiringSoon,
			Critical:        summary.Critical,
			High:            summary.High,
			Warning:         summary.Warning,
			PendingApproval: summary.PendingApproval,
			Revoked:         summary.Revoked,
			AssetCount:      summary.AssetCount,
		}
	} else {
		sourceKinds, sourceNames := programmaticConfiguredSourcePairs(activeSources)
		totalCount, err = h.Q.CountCredentialArtifactsBySourcesAndQueryAndFilters(ctx, gen.CountCredentialArtifactsBySourcesAndQueryAndFiltersParams{
			EvaluatedAt:           evaluatedAt,
			ConfiguredSourceKinds: sourceKinds,
			ConfiguredSourceNames: sourceNames,
			CredentialKind:        queryState.CredentialKind,
			Status:                queryState.Status,
			RiskLevel:             queryState.RiskLevel,
			ExpiryState:           queryState.ExpiryState,
			ExpiresInDays:         int32(queryState.ExpiresInDays),
			Owner:                 ownerFilter,
			Asset:                 queryState.Asset,
			NewerDays:             int32(queryState.NewerThanDays),
			Query:                 queryState.Q,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		pagination = newPaginatedListState(totalCount, page, perPage)
		rows, err := h.Q.ListCredentialArtifactsPageBySourcesAndQueryAndFilters(ctx, gen.ListCredentialArtifactsPageBySourcesAndQueryAndFiltersParams{
			EvaluatedAt:           evaluatedAt,
			ConfiguredSourceKinds: sourceKinds,
			ConfiguredSourceNames: sourceNames,
			CredentialKind:        queryState.CredentialKind,
			Status:                queryState.Status,
			RiskLevel:             queryState.RiskLevel,
			ExpiryState:           queryState.ExpiryState,
			ExpiresInDays:         int32(queryState.ExpiresInDays),
			Owner:                 ownerFilter,
			Asset:                 queryState.Asset,
			NewerDays:             int32(queryState.NewerThanDays),
			SortBy:                queryState.SortBy,
			Query:                 queryState.Q,
			PageLimit:             int32(perPage),
			PageOffset:            int32(pagination.Offset()),
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		items = make([]viewmodels.CredentialArtifactListItem, 0, len(rows))
		for _, row := range rows {
			items = append(items, buildCredentialListItem(now, credentialListRowFromBySources(row), linkResolver))
		}

		summary, err := h.Q.SummarizeCredentialsBySourcesAndQuery(ctx, gen.SummarizeCredentialsBySourcesAndQueryParams{
			EvaluatedAt:           evaluatedAt,
			ConfiguredSourceKinds: sourceKinds,
			ConfiguredSourceNames: sourceNames,
			CredentialKind:        queryState.CredentialKind,
			Owner:                 ownerFilter,
			Asset:                 queryState.Asset,
			NewerDays:             int32(queryState.NewerThanDays),
			Query:                 queryState.Q,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		data.Summary = viewmodels.CredentialsSummary{
			Total:           summary.Total,
			Active:          summary.Active,
			Expired:         summary.Expired,
			ExpiringSoon:    summary.ExpiringSoon,
			Critical:        summary.Critical,
			High:            summary.High,
			Warning:         summary.Warning,
			PendingApproval: summary.PendingApproval,
			Revoked:         summary.Revoked,
			AssetCount:      summary.AssetCount,
		}
	}

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No credentials found for the current filters.", "")
	data.HasItems = len(items) > 0
	data.HasLastUsedData = views.CredentialsHasLastUsedVariance(items)
	if queryState.HasFilters() {
		data.PaginatedListPageData.EmptyStateMsg = "No credentials match the current search filters."
	}

	return renderCredentials()
}

type credentialExportRecord struct {
	SourceKind     string
	SourceName     string
	CredentialKind string
	DisplayName    string
	ExternalID     string
	AssetName      string
	AssetRefKind   string
	AssetRefID     string
	Status         string
	RiskLevel      string
	ExpiresAt      string
	LastUsedAt     string
	CreatedBy      string
	ApprovedBy     string
}

// credentialListRow is the subset of sqlc row fields the credentials list and
// export handlers consume. Both ListCredentialArtifactsPageBySource* row types
// have the same shape; this struct lets buildCredentialListItem and the export
// builder take a single value instead of long positional argument lists.
type credentialListRow struct {
	ID                    int64
	SourceKind            string
	SourceName            string
	CredentialKind        string
	DisplayName           string
	ExternalID            string
	AssetRefKind          string
	AssetRefExternalID    string
	AssetName             string
	Status                string
	RiskLevel             string
	ExpiresAtSource       pgtype.Timestamptz
	LastUsedAtSource      pgtype.Timestamptz
	CreatedByDisplayName  string
	CreatedByExternalID   string
	ApprovedByDisplayName string
	ApprovedByExternalID  string
	VersionCount          int64
}

func credentialListRowFromBySource(r gen.ListCredentialArtifactsPageBySourceAndQueryAndFiltersRow) credentialListRow {
	return credentialListRow{
		ID:                    r.ID,
		SourceKind:            r.SourceKind,
		SourceName:            r.SourceName,
		CredentialKind:        r.CredentialKind,
		DisplayName:           r.DisplayName,
		ExternalID:            r.ExternalID,
		AssetRefKind:          r.AssetRefKind,
		AssetRefExternalID:    r.AssetRefExternalID,
		AssetName:             r.AssetName,
		Status:                r.Status,
		RiskLevel:             r.RiskLevel,
		ExpiresAtSource:       r.ExpiresAtSource,
		LastUsedAtSource:      r.LastUsedAtSource,
		CreatedByDisplayName:  r.CreatedByDisplayName,
		CreatedByExternalID:   r.CreatedByExternalID,
		ApprovedByDisplayName: r.ApprovedByDisplayName,
		ApprovedByExternalID:  r.ApprovedByExternalID,
		VersionCount:          r.VersionCount,
	}
}

func credentialListRowFromBySources(r gen.ListCredentialArtifactsPageBySourcesAndQueryAndFiltersRow) credentialListRow {
	return credentialListRow{
		ID:                    r.ID,
		SourceKind:            r.SourceKind,
		SourceName:            r.SourceName,
		CredentialKind:        r.CredentialKind,
		DisplayName:           r.DisplayName,
		ExternalID:            r.ExternalID,
		AssetRefKind:          r.AssetRefKind,
		AssetRefExternalID:    r.AssetRefExternalID,
		AssetName:             r.AssetName,
		Status:                r.Status,
		RiskLevel:             r.RiskLevel,
		ExpiresAtSource:       r.ExpiresAtSource,
		LastUsedAtSource:      r.LastUsedAtSource,
		CreatedByDisplayName:  r.CreatedByDisplayName,
		CreatedByExternalID:   r.CreatedByExternalID,
		ApprovedByDisplayName: r.ApprovedByDisplayName,
		ApprovedByExternalID:  r.ApprovedByExternalID,
		VersionCount:          r.VersionCount,
	}
}

func credentialListRowFromForExport(r gen.ListCredentialArtifactsForExportBySourcesAndQueryAndFiltersRow) credentialListRow {
	return credentialListRow{
		ID:                    r.ID,
		SourceKind:            r.SourceKind,
		SourceName:            r.SourceName,
		CredentialKind:        r.CredentialKind,
		DisplayName:           r.DisplayName,
		ExternalID:            r.ExternalID,
		AssetRefKind:          r.AssetRefKind,
		AssetRefExternalID:    r.AssetRefExternalID,
		AssetName:             r.AssetName,
		Status:                r.Status,
		RiskLevel:             r.RiskLevel,
		ExpiresAtSource:       r.ExpiresAtSource,
		LastUsedAtSource:      r.LastUsedAtSource,
		CreatedByDisplayName:  r.CreatedByDisplayName,
		CreatedByExternalID:   r.CreatedByExternalID,
		ApprovedByDisplayName: r.ApprovedByDisplayName,
		ApprovedByExternalID:  r.ApprovedByExternalID,
		VersionCount:          r.VersionCount,
	}
}

func (h *Handlers) HandleCredentialsExport(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Credentials Export")
	if err != nil {
		return h.RenderError(c, err)
	}

	sources := availableProgrammaticSources(stateView)
	queryState := querystate.ParseCredentialsQuery(c.Request().URL.Query(), programmaticQuerySources(sources))
	activeSources := effectiveProgrammaticSources(queryState.Source, sources)
	if len(activeSources) == 0 {
		return h.RenderError(c, errors.New("no credential sources configured for export"))
	}
	now := time.Now().UTC()
	evaluatedAt := pgTimestamptz(now)
	ownerFilter := credentialOwnerFilter(queryState.Owner, layout.UserEmail)
	const exportLimit = 5000

	sourceKinds, sourceNames := programmaticConfiguredSourcePairs(activeSources)
	rows, err := h.Q.ListCredentialArtifactsForExportBySourcesAndQueryAndFilters(ctx, gen.ListCredentialArtifactsForExportBySourcesAndQueryAndFiltersParams{
		EvaluatedAt:           evaluatedAt,
		ConfiguredSourceKinds: sourceKinds,
		ConfiguredSourceNames: sourceNames,
		CredentialKind:        queryState.CredentialKind,
		Status:                queryState.Status,
		RiskLevel:             queryState.RiskLevel,
		ExpiryState:           queryState.ExpiryState,
		ExpiresInDays:         int32(queryState.ExpiresInDays),
		Owner:                 ownerFilter,
		Asset:                 queryState.Asset,
		NewerDays:             int32(queryState.NewerThanDays),
		SortBy:                queryState.SortBy,
		Query:                 queryState.Q,
		PageLimit:             exportLimit,
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	records := make([]credentialExportRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, credentialExportRecordFromRow(credentialListRowFromForExport(row)))
	}

	if len(records) == exportLimit {
		c.Response().Header().Set("X-Open-SSPM-Export-Truncated", "true")
	}
	c.Response().Header().Set(echo.HeaderContentType, "text/csv; charset=utf-8")
	c.Response().Header().Set(echo.HeaderContentDisposition, `attachment; filename="credentials-export.csv"`)
	writer := csv.NewWriter(c.Response())
	if err := writer.Write([]string{"source_kind", "source_name", "credential_kind", "display_name", "external_id", "asset_name", "asset_ref_kind", "asset_ref_id", "status", "risk_level", "expires_at", "last_used_at", "created_by", "approved_by"}); err != nil {
		return err
	}
	for _, record := range records {
		if err := writer.Write([]string{record.SourceKind, record.SourceName, record.CredentialKind, record.DisplayName, record.ExternalID, record.AssetName, record.AssetRefKind, record.AssetRefID, record.Status, record.RiskLevel, record.ExpiresAt, record.LastUsedAt, record.CreatedBy, record.ApprovedBy}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func credentialExportRecordFromRow(row credentialListRow) credentialExportRecord {
	return credentialExportRecord{
		SourceKind:     defangCSVCell(strings.TrimSpace(row.SourceKind)),
		SourceName:     defangCSVCell(strings.TrimSpace(row.SourceName)),
		CredentialKind: defangCSVCell(strings.TrimSpace(row.CredentialKind)),
		DisplayName:    defangCSVCell(strings.TrimSpace(row.DisplayName)),
		ExternalID:     defangCSVCell(strings.TrimSpace(row.ExternalID)),
		AssetName:      defangCSVCell(strings.TrimSpace(row.AssetName)),
		AssetRefKind:   defangCSVCell(strings.TrimSpace(row.AssetRefKind)),
		AssetRefID:     defangCSVCell(strings.TrimSpace(row.AssetRefExternalID)),
		Status:         defangCSVCell(strings.TrimSpace(row.Status)),
		RiskLevel:      defangCSVCell(strings.TrimSpace(row.RiskLevel)),
		ExpiresAt:      credentialExportTime(row.ExpiresAtSource),
		LastUsedAt:     credentialExportTime(row.LastUsedAtSource),
		CreatedBy:      defangCSVCell(actorDisplayName(row.CreatedByDisplayName, row.CreatedByExternalID)),
		ApprovedBy:     defangCSVCell(actorDisplayName(row.ApprovedByDisplayName, row.ApprovedByExternalID)),
	}
}

func credentialExportTime(value pgtype.Timestamptz) string {
	if !value.Valid {
		return ""
	}
	return value.Time.UTC().Format(time.RFC3339)
}

// credentialOwnerFilter resolves the synthetic "me" owner token to the
// authenticated user's email. The SQL owner filter matches that string against
// created_by_external_id, created_by_display_name, approved_by_external_id,
// and approved_by_display_name via ILIKE — so "Owned by me" only works for
// sources where one of those fields contains the user's email (Entra/Google
// typically do, raw GitHub usernames do not).
func credentialOwnerFilter(owner, userEmail string) string {
	owner = strings.TrimSpace(owner)
	if strings.EqualFold(owner, "me") {
		return strings.TrimSpace(userEmail)
	}
	return owner
}

func buildCredentialListItem(now time.Time, row credentialListRow, linkResolver *identityLinkResolver) viewmodels.CredentialArtifactListItem {
	rawDisplay := strings.TrimSpace(row.DisplayName)
	if rawDisplay == "" {
		rawDisplay = strings.TrimSpace(row.ExternalID)
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
	cleanAssetName := strings.TrimSpace(row.AssetName)
	namePrimary, nameSecondary := views.CredentialDisplayName(rawDisplay, cleanAssetName)

	rowState := views.ComputeCredentialRowState(now, row.Status, row.RiskLevel, expiresAtTime(row.ExpiresAtSource), row.ExpiresAtSource.Valid)
	expiryTone := views.CredentialExpiryTextClass(now, expiresAtTime(row.ExpiresAtSource), row.ExpiresAtSource.Valid)

	createdBy := fallbackDash(actorDisplayName(row.CreatedByDisplayName, row.CreatedByExternalID))
	approvedBy := fallbackDash(actorDisplayName(row.ApprovedByDisplayName, row.ApprovedByExternalID))

	sourceKind := strings.TrimSpace(row.SourceKind)
	sourceName := strings.TrimSpace(row.SourceName)

	return viewmodels.CredentialArtifactListItem{
		ID:             row.ID,
		SourceKind:     sourceKind,
		SourceName:     sourceName,
		CredentialKind: fallbackDash(strings.TrimSpace(row.CredentialKind)),
		DisplayName:    fallbackDash(rawDisplay),
		NamePrimary:    namePrimary,
		NameSecondary:  nameSecondary,
		ExternalID:     fallbackDash(strings.TrimSpace(row.ExternalID)),
		AssetRef:       fallbackDash(assetRef),
		AssetRefKind:   fallbackDash(assetRefKind),
		AssetRefID:     fallbackDash(assetRefExternalID),
		AssetName:      cleanAssetName,
		Status:         fallbackDash(strings.TrimSpace(row.Status)),
		RiskLevel:      strings.TrimSpace(row.RiskLevel),
		RowStateTone:   rowState.Tone,
		RowStateLabel:  rowState.Label,
		ExpiresAt:      calendarDateWithRelativeDisplay(row.ExpiresAtSource),
		ExpiresTone:    expiryTone,
		LastUsedAt:     calendarDateWithRelativeDisplay(row.LastUsedAtSource),
		CreatedBy:      createdBy,
		CreatedByHref:  linkResolver.Resolve(sourceKind, sourceName, row.CreatedByExternalID, "", row.CreatedByDisplayName),
		ApprovedBy:     approvedBy,
		ApprovedByHref: linkResolver.Resolve(sourceKind, sourceName, row.ApprovedByExternalID, "", row.ApprovedByDisplayName),
		VersionCount:   row.VersionCount,
	}
}

func expiresAtTime(value pgtype.Timestamptz) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return value.Time.UTC()
}

func (h *Handlers) HandleCredentialShow(c *echo.Context) error {
	credentialID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	now := time.Now().UTC()
	credential, err := h.Q.GetCredentialArtifactByID(ctx, credentialID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	layout, stateView, err := h.LayoutData(ctx, c, "Credential")
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
			EventTime:            calendarDateDisplay(event.EventTime),
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
	riskFindings := credentialRiskFindingsFromSignals(credentialRiskSignalsFromStoredJSON(credential.RiskSignalsJson), credential.ExpiresAtSource, credential.LastUsedAtSource, now)
	linkResolver := newIdentityLinkResolver(h, ctx, stateView)

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
			CreatedAtSource:    calendarDateWithRelativeDisplay(credential.CreatedAtSource),
			ExpiresAtSource:    calendarDateWithRelativeDisplay(credential.ExpiresAtSource),
			LastUsedAtSource:   calendarDateWithRelativeDisplay(credential.LastUsedAtSource),
			CreatedBy:          fallbackDash(actorDisplayName(credential.CreatedByDisplayName, credential.CreatedByExternalID)),
			CreatedByHref:      linkResolver.Resolve(strings.TrimSpace(credential.SourceKind), strings.TrimSpace(credential.SourceName), credential.CreatedByExternalID, "", credential.CreatedByDisplayName),
			ApprovedBy:         fallbackDash(actorDisplayName(credential.ApprovedByDisplayName, credential.ApprovedByExternalID)),
			ApprovedByHref:     linkResolver.Resolve(strings.TrimSpace(credential.SourceKind), strings.TrimSpace(credential.SourceName), credential.ApprovedByExternalID, "", credential.ApprovedByDisplayName),
			AssetHref:          assetHref,
		},
		ScopeJSON:    prettyProgrammaticJSON(credential.ScopeJson),
		AuditEvents:  eventItems,
		RiskFindings: riskFindings,
		HasEvents:    len(eventItems) > 0,
		HasFindings:  len(riskFindings) > 0,
	}

	h.trackNonHumanIdentitiesOutboundClick(c, "credential", credential.ID)

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

func effectiveProgrammaticSources(selected querystate.SourceSelection, all []viewmodels.ProgrammaticSourceOption) []viewmodels.ProgrammaticSourceOption {
	if len(all) == 0 {
		return nil
	}

	selectedKind := strings.TrimSpace(selected.Kind)
	selectedName := strings.TrimSpace(selected.Name)
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

func pgTimestamptz(ts time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ts.UTC(), Valid: true}
}

func credentialRiskFindingsFromSignals(signals []riskpolicy.RiskSignal, expiresAt, lastUsedAt pgtype.Timestamptz, now time.Time) []viewmodels.CredentialRiskFinding {
	findings := make([]viewmodels.CredentialRiskFinding, 0, len(signals))
	for _, signal := range signals {
		findings = append(findings, viewmodels.CredentialRiskFinding{
			Severity: signal.Severity,
			Title:    signal.Title,
			Evidence: credentialSignalEvidence(signal, expiresAt, lastUsedAt, now),
		})
	}
	return findings
}

func credentialRiskSignalsFromStoredJSON(raw []byte) []riskpolicy.RiskSignal {
	if len(raw) == 0 {
		return nil
	}
	var signals []riskpolicy.RiskSignal
	if err := json.Unmarshal(raw, &signals); err != nil {
		return nil
	}
	out := make([]riskpolicy.RiskSignal, 0, len(signals))
	for _, signal := range signals {
		signal.Severity = strings.TrimSpace(signal.Severity)
		signal.Title = strings.TrimSpace(signal.Title)
		signal.Evidence = strings.TrimSpace(signal.Evidence)
		if signal.Severity == "" || signal.Title == "" {
			continue
		}
		out = append(out, signal)
	}
	return out
}

func credentialSignalEvidence(signal riskpolicy.RiskSignal, expiresAt, lastUsedAt pgtype.Timestamptz, now time.Time) string {
	switch signal.ID {
	case "expired_active", "expired_inactive":
		if expiresAt.Valid {
			return "Expired " + relativeDateLabelAt(now.UTC(), expiresAt.Time.UTC())
		}
	case "expiring_within_7_days", "expiring_within_30_days":
		if expiresAt.Valid {
			return "Expires " + relativeDateLabelAt(now.UTC(), expiresAt.Time.UTC())
		}
	case "unused_over_90_days":
		if lastUsedAt.Valid {
			return "Last used " + relativeDateLabelAt(now.UTC(), lastUsedAt.Time.UTC())
		}
	}
	return strings.TrimSpace(signal.Evidence)
}

type identityLinkResolver struct {
	h                    *Handlers
	ctx                  context.Context
	configuredKinds      []string
	configuredNames      []string
	emailHrefByCandidate map[string]string
	actorHrefByKey       map[string]string
}

func newIdentityLinkResolver(h *Handlers, ctx context.Context, stateView connectorStateView) *identityLinkResolver {
	configuredKinds, configuredNames := configuredIdentitySourcePairsFromView(stateView)
	return &identityLinkResolver{
		h:                    h,
		ctx:                  ctx,
		configuredKinds:      configuredKinds,
		configuredNames:      configuredNames,
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

	// FindUnambiguous returns ErrNoRows for both "nobody owns this email" and
	// "two identities tie at the top tier". Either case is a non-link for
	// rendering purposes — the badge collapses to a non-clickable label rather
	// than risk routing the operator to an arbitrary identity.
	identity, err := r.h.Q.FindUnambiguousIdentityByPrimaryEmail(r.ctx, gen.FindUnambiguousIdentityByPrimaryEmailParams{
		ConfiguredSourceKinds: r.configuredKinds,
		ConfiguredSourceNames: r.configuredNames,
		PrimaryEmail:          candidate,
	})
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

func (h *Handlers) listCredentialArtifactsForAsset(ctx context.Context, asset gen.AppAsset) ([]gen.ListCredentialArtifactsForAssetRefRow, error) {
	refs := appAssetCredentialRefs(asset)
	if len(refs) == 0 {
		return nil, nil
	}

	credentialsByID := map[int64]gen.ListCredentialArtifactsForAssetRefRow{}
	for _, ref := range refs {
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
