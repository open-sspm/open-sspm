package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

const (
	discoveryAppsPerPage   = 20
	discoveryHotspotsLimit = 200
)

func (h *Handlers) HandleDiscoveryApps(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "SaaS Discovery")
	if err != nil {
		return h.RenderError(c, err)
	}

	sourceOptions := discoverySourceOptions(stateView)
	queryState := querystate.ParseDiscoveryAppsQuery(c.Request().URL.Query(), discoveryQuerySources(sourceOptions))
	sourceNameOptions := discoverySourceNameOptions(queryState.Source.Kind, sourceOptions)
	page := queryState.Page

	totalCount, err := h.Q.CountSaaSAppsByFilters(ctx, gen.CountSaaSAppsByFiltersParams{
		ManagedState: queryState.ManagedState,
		RiskLevel:    queryState.RiskLevel,
		SourceKind:   queryState.Source.Kind,
		SourceName:   queryState.Source.Name,
		Query:        queryState.Q,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination := newPaginatedListState(totalCount, page, discoveryAppsPerPage)
	rows, err := h.Q.ListSaaSAppsPageByFilters(ctx, gen.ListSaaSAppsPageByFiltersParams{
		ManagedState: queryState.ManagedState,
		RiskLevel:    queryState.RiskLevel,
		PageOffset:   int32(pagination.Offset()),
		PageLimit:    int32(discoveryAppsPerPage),
		SourceKind:   queryState.Source.Kind,
		SourceName:   queryState.Source.Name,
		Query:        queryState.Q,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.DiscoveryAppListItem, 0, len(rows))
	for _, row := range rows {
		displayName := strings.TrimSpace(row.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(row.CanonicalKey)
		}
		domainLabel, vendorLabel := discoveryAppSecondaryLabels(displayName, row.PrimaryDomain, row.VendorName)
		ownerLabel := discoveryOwnerLabel(row.OwnerDisplayName, row.OwnerPrimaryEmail)

		items = append(items, viewmodels.DiscoveryAppListItem{
			ID:            row.ID,
			DisplayName:   displayName,
			Domain:        domainLabel,
			VendorName:    vendorLabel,
			ManagedState:  strings.TrimSpace(row.ManagedState),
			ManagedReason: strings.TrimSpace(row.ManagedReason),
			RiskScore:     row.RiskScore,
			RiskLevel:     strings.TrimSpace(row.RiskLevel),
			Owner:         ownerLabel,
			Actors30d:     row.Actors30d,
			LastSeenAt:    formatProgrammaticDate(row.LastSeenAt),
		})
	}

	data := viewmodels.DiscoveryAppsViewData{
		PaginatedListPageData: pagination.PageData(layout, len(items), "No discovered SaaS apps match the current filters.", ""),
		SourceOptions:         sourceKindOptions(sourceOptions),
		SourceNameOptions:     sourceNameOptions,
		Query:                 queryState,
		Items:                 items,
		HasItems:              len(items) > 0,
	}
	if totalCount == 0 && len(sourceOptions) == 0 {
		data.PaginatedListPageData.EmptyStateMsg = "Enable Okta, Microsoft Entra, or Google Workspace discovery in connector settings, then run sync."
	}

	if isHX(c) && isHXTarget(c, "discovery-apps-results") {
		return h.RenderComponent(c, views.DiscoveryAppsPageResults(data))
	}
	return h.RenderComponent(c, views.DiscoveryAppsPage(data))
}

func (h *Handlers) HandleDiscoveryHotspots(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Discovery Hotspots")
	if err != nil {
		return h.RenderError(c, err)
	}

	sourceOptions := discoverySourceOptions(stateView)
	queryState := querystate.ParseDiscoveryHotspotsQuery(c.Request().URL.Query(), discoveryQuerySources(sourceOptions))
	sourceNameOptions := discoverySourceNameOptions(queryState.Source.Kind, sourceOptions)

	rows, err := h.Q.ListSaaSAppHotspots(ctx, gen.ListSaaSAppHotspotsParams{
		LimitRows:  discoveryHotspotsLimit,
		SourceKind: queryState.Source.Kind,
		SourceName: queryState.Source.Name,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.DiscoveryHotspotItem, 0, len(rows))
	for _, row := range rows {
		displayName := strings.TrimSpace(row.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(row.CanonicalKey)
		}
		domainLabel, _ := discoveryAppSecondaryLabels(displayName, row.PrimaryDomain, row.VendorName)
		items = append(items, viewmodels.DiscoveryHotspotItem{
			ID:           row.ID,
			DisplayName:  displayName,
			Domain:       domainLabel,
			ManagedState: strings.TrimSpace(row.ManagedState),
			RiskScore:    row.RiskScore,
			RiskLevel:    strings.TrimSpace(row.RiskLevel),
			Owner:        discoveryOwnerLabel(row.OwnerDisplayName, row.OwnerPrimaryEmail),
			Actors30d:    row.Actors30d,
		})
	}

	data := viewmodels.DiscoveryHotspotsViewData{
		Layout:            layout,
		SourceOptions:     sourceKindOptions(sourceOptions),
		SourceNameOptions: sourceNameOptions,
		Query:             queryState,
		Items:             items,
		HasItems:          len(items) > 0,
		EmptyStateMsg:     "No discovery hotspots are currently above the high-risk threshold.",
	}

	if isHX(c) && isHXTarget(c, "discovery-hotspots-results") {
		return h.RenderComponent(c, views.DiscoveryHotspotsPageResults(data))
	}
	return h.RenderComponent(c, views.DiscoveryHotspotsPage(data))
}

func (h *Handlers) HandleDiscoveryAppShow(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	app, err := h.Q.GetSaaSAppByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	displayName := strings.TrimSpace(app.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(app.CanonicalKey)
	}

	layout, _, err := h.LayoutData(ctx, c, displayName)
	if err != nil {
		return h.RenderError(c, err)
	}

	sources, err := h.Q.ListSaaSAppSourcesBySaaSAppID(ctx, appID)
	if err != nil {
		return h.RenderError(c, err)
	}
	sourceItems := make([]viewmodels.DiscoverySourceEvidenceItem, 0, len(sources))
	for _, source := range sources {
		sourceItems = append(sourceItems, viewmodels.DiscoverySourceEvidenceItem{
			SourceKind:      strings.TrimSpace(source.SourceKind),
			SourceName:      strings.TrimSpace(source.SourceName),
			SourceAppID:     fallbackDash(strings.TrimSpace(source.SourceAppID)),
			SourceAppName:   fallbackDash(strings.TrimSpace(source.SourceAppName)),
			SourceAppDomain: fallbackDash(strings.TrimSpace(source.SourceAppDomain)),
			LastObservedAt:  formatProgrammaticDate(source.LastObservedAt),
		})
	}

	events, err := h.Q.ListSaaSAppEventsBySaaSAppID(ctx, gen.ListSaaSAppEventsBySaaSAppIDParams{
		SaasAppID: appID,
		LimitRows: 100,
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	eventItems := make([]viewmodels.DiscoveryEventItem, 0, len(events))
	for _, event := range events {
		actor := strings.TrimSpace(event.ActorDisplayName)
		if actor == "" {
			actor = strings.TrimSpace(event.ActorEmail)
		}
		if actor == "" {
			actor = strings.TrimSpace(event.ActorExternalID)
		}
		sourceApp := strings.TrimSpace(event.SourceAppName)
		if sourceApp == "" {
			sourceApp = strings.TrimSpace(event.SourceAppDomain)
		}
		if sourceApp == "" {
			sourceApp = strings.TrimSpace(event.SourceAppID)
		}
		eventItems = append(eventItems, viewmodels.DiscoveryEventItem{
			SignalKind:    strings.TrimSpace(event.SignalKind),
			ObservedAt:    formatProgrammaticDate(event.ObservedAt),
			Actor:         fallbackDash(actor),
			SourceApp:     fallbackDash(sourceApp),
			ScopesSummary: summarizeDiscoveryScopes(event.ScopesJson),
		})
	}

	actors, err := h.Q.ListTopActorsForSaaSAppByID(ctx, gen.ListTopActorsForSaaSAppByIDParams{
		SaasAppID: appID,
		LimitRows: 25,
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	actorItems := make([]viewmodels.DiscoveryActorItem, 0, len(actors))
	for _, actor := range actors {
		actorItems = append(actorItems, viewmodels.DiscoveryActorItem{
			ActorLabel:      fallbackDash(strings.TrimSpace(actor.ActorLabel)),
			ActorEmail:      fallbackDash(strings.TrimSpace(actor.ActorEmail)),
			ActorExternalID: fallbackDash(strings.TrimSpace(actor.ActorExternalID)),
			EventCount:      actor.EventCount,
			LastObservedAt:  formatProgrammaticDate(actor.LastObservedAt),
		})
	}

	domainLabel, vendorLabel := discoveryAppSecondaryLabels(displayName, app.PrimaryDomain, app.VendorName)

	data := viewmodels.DiscoveryAppShowViewData{
		Layout: layout,
		App: viewmodels.DiscoveryAppSummaryView{
			ID:                           app.ID,
			DisplayName:                  displayName,
			CanonicalKey:                 strings.TrimSpace(app.CanonicalKey),
			PrimaryDomain:                domainLabel,
			VendorName:                   vendorLabel,
			ManagedState:                 strings.TrimSpace(app.ManagedState),
			ManagedReason:                strings.TrimSpace(app.ManagedReason),
			RiskScore:                    app.RiskScore,
			RiskLevel:                    strings.TrimSpace(app.RiskLevel),
			SuggestedBusinessCriticality: strings.TrimSpace(app.SuggestedBusinessCriticality),
			SuggestedDataClassification:  strings.TrimSpace(app.SuggestedDataClassification),
			FirstSeenAt:                  formatProgrammaticDate(app.FirstSeenAt),
			LastSeenAt:                   formatProgrammaticDate(app.LastSeenAt),
		},
		Sources:      sourceItems,
		TopActors:    actorItems,
		Events:       eventItems,
		HasSources:   len(sourceItems) > 0,
		HasTopActors: len(actorItems) > 0,
		HasEvents:    len(eventItems) > 0,
	}

	return h.RenderComponent(c, views.DiscoveryAppShowPage(data))
}

func discoverySourceOptions(stateView connectorStateView) []viewmodels.DiscoverySourceOption {
	options := make([]viewmodels.DiscoverySourceOption, 0, 3)
	okta := stateView.Okta()
	if okta.Configured() && okta.Config().DiscoveryEnabled && okta.SourceName() != "" {
		options = append(options, viewmodels.DiscoverySourceOption{
			SourceKind: querySourceKind("okta"),
			SourceName: okta.SourceName(),
			Label:      sourcePrimaryLabel("okta"),
		})
	}
	entra := stateView.Entra()
	if entra.Configured() && entra.Config().DiscoveryEnabled && entra.SourceName() != "" {
		options = append(options, viewmodels.DiscoverySourceOption{
			SourceKind: querySourceKind("entra"),
			SourceName: entra.SourceName(),
			Label:      sourcePrimaryLabel("entra"),
		})
	}
	google := stateView.GoogleWorkspace()
	if google.Configured() && google.Config().DiscoveryEnabled && google.SourceName() != "" {
		options = append(options, viewmodels.DiscoverySourceOption{
			SourceKind: configstore.KindGoogleWorkspace,
			SourceName: google.SourceName(),
			Label:      sourcePrimaryLabel(configstore.KindGoogleWorkspace),
		})
	}
	return options
}

func sourceKindOptions(sourceOptions []viewmodels.DiscoverySourceOption) []viewmodels.DiscoverySourceOption {
	seen := map[string]struct{}{}
	out := make([]viewmodels.DiscoverySourceOption, 0, len(sourceOptions))
	for _, option := range sourceOptions {
		kind := normalizeDiscoverySourceKind(option.SourceKind)
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		out = append(out, viewmodels.DiscoverySourceOption{
			SourceKind: kind,
			Label:      sourcePrimaryLabel(kind),
		})
	}
	return out
}

func discoverySourceNameOptions(selectedSourceKind string, sourceOptions []viewmodels.DiscoverySourceOption) []viewmodels.DiscoverySourceOption {
	out := make([]viewmodels.DiscoverySourceOption, 0, len(sourceOptions))
	for _, option := range sourceOptions {
		if selectedSourceKind != "" && option.SourceKind != selectedSourceKind {
			continue
		}
		out = append(out, option)
	}
	return out
}

func normalizeDiscoverySourceKind(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "okta":
		return "okta"
	case "entra":
		return "entra"
	case configstore.KindGoogleWorkspace:
		return configstore.KindGoogleWorkspace
	default:
		return ""
	}
}

func normalizeDiscoveryBusinessCriticality(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "low":
		return "low"
	case "medium":
		return "medium"
	case "high":
		return "high"
	case "critical":
		return "critical"
	default:
		return "unknown"
	}
}

func normalizeDiscoveryDataClassification(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "public":
		return "public"
	case "internal":
		return "internal"
	case "confidential":
		return "confidential"
	case "restricted":
		return "restricted"
	default:
		return "unknown"
	}
}

func discoveryAppSecondaryLabels(displayName, domain, vendor string) (string, string) {
	displayName = strings.TrimSpace(displayName)
	domain = strings.TrimSpace(domain)
	vendor = strings.TrimSpace(vendor)
	if vendor == "" {
		return domain, ""
	}
	if strings.EqualFold(vendor, displayName) {
		return domain, ""
	}
	if domainVendor := discovery.VendorLabelFromDomain(domain); domainVendor != "" && strings.EqualFold(vendor, domainVendor) {
		return domain, ""
	}
	return domain, vendor
}

func discoveryOwnerLabel(displayName, email string) string {
	displayName = strings.TrimSpace(displayName)
	email = strings.TrimSpace(email)
	switch {
	case displayName != "" && email != "":
		return displayName + " (" + email + ")"
	case displayName != "":
		return displayName
	case email != "":
		return email
	default:
		return "—"
	}
}

func summarizeDiscoveryScopes(raw []byte) string {
	scopes := make([]string, 0, 8)
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &scopes)
	}
	scopes = discovery.NormalizeScopes(scopes)
	if len(scopes) == 0 {
		return "—"
	}
	if len(scopes) <= 3 {
		return strings.Join(scopes, ", ")
	}
	return strings.Join(scopes[:3], ", ") + fmt.Sprintf(" +%d", len(scopes)-3)
}

func discoveryAppHref(appID int64) string {
	return "/discovery/apps/" + strconv.FormatInt(appID, 10)
}
