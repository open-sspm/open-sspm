package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

const (
	discoveryAppsPerPage   = 20
	discoveryHotspotsLimit = 200
)

type discoveryConnectorRuntime struct {
	SourceName string
	Configured bool
	Enabled    bool
}

func (h *Handlers) HandleDiscoveryApps(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	if err := h.recomputeDiscoveryPosture(ctx); err != nil {
		return h.RenderError(c, err)
	}

	layout, snap, err := h.LayoutData(ctx, c, "SaaS Discovery")
	if err != nil {
		return h.RenderError(c, err)
	}

	sourceOptions := discoverySourceOptions(snap)
	selectedSourceKind, selectedSourceName := normalizeDiscoverySourceSelection(
		c.QueryParam("source_kind"),
		c.QueryParam("source_name"),
		sourceOptions,
	)

	sourceNameOptions := discoverySourceNameOptions(selectedSourceKind, sourceOptions)
	configuredSourceKinds, configuredSourceNames := discoveryConfiguredSourcePairs(sourceOptions)

	query := strings.TrimSpace(c.QueryParam("q"))
	managedState := normalizeDiscoveryManagedState(c.QueryParam("managed_state"))
	riskLevel := normalizeDiscoveryRiskLevel(c.QueryParam("risk_level"))
	page := parsePageParam(c)

	totalCount, err := h.Q.CountSaaSAppsByFilters(ctx, gen.CountSaaSAppsByFiltersParams{
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
		SourceKind:            selectedSourceKind,
		SourceName:            selectedSourceName,
		ManagedState:          managedState,
		RiskLevel:             riskLevel,
		Query:                 query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, discoveryAppsPerPage)
	rows, err := h.Q.ListSaaSAppsPageByFilters(ctx, gen.ListSaaSAppsPageByFiltersParams{
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
		SourceKind:            selectedSourceKind,
		SourceName:            selectedSourceName,
		ManagedState:          managedState,
		RiskLevel:             riskLevel,
		Query:                 query,
		PageOffset:            int32(offset),
		PageLimit:             int32(discoveryAppsPerPage),
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

	showingCount := len(items)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)
	data := viewmodels.DiscoveryAppsViewData{
		Layout:             layout,
		SourceOptions:      sourceKindOptions(sourceOptions),
		SourceNameOptions:  sourceNameOptions,
		SelectedSourceKind: selectedSourceKind,
		SelectedSourceName: selectedSourceName,
		Query:              query,
		ManagedState:       managedState,
		RiskLevel:          riskLevel,
		Items:              items,
		ShowingCount:       showingCount,
		ShowingFrom:        showingFrom,
		ShowingTo:          showingTo,
		TotalCount:         totalCount,
		Page:               page,
		PerPage:            discoveryAppsPerPage,
		TotalPages:         totalPages,
		HasItems:           showingCount > 0,
		EmptyStateMsg:      "No discovered SaaS apps match the current filters.",
	}
	if totalCount == 0 && len(sourceOptions) == 0 {
		data.EmptyStateMsg = "Enable Okta or Microsoft Entra discovery in connector settings, then run sync."
	}

	if isHX(c) && isHXTarget(c, "discovery-apps-results") {
		return h.RenderComponent(c, views.DiscoveryAppsPageResults(data))
	}
	return h.RenderComponent(c, views.DiscoveryAppsPage(data))
}

func (h *Handlers) HandleDiscoveryHotspots(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	if err := h.recomputeDiscoveryPosture(ctx); err != nil {
		return h.RenderError(c, err)
	}

	layout, snap, err := h.LayoutData(ctx, c, "Discovery Hotspots")
	if err != nil {
		return h.RenderError(c, err)
	}

	sourceOptions := discoverySourceOptions(snap)
	selectedSourceKind, selectedSourceName := normalizeDiscoverySourceSelection(
		c.QueryParam("source_kind"),
		c.QueryParam("source_name"),
		sourceOptions,
	)
	sourceNameOptions := discoverySourceNameOptions(selectedSourceKind, sourceOptions)
	configuredSourceKinds, configuredSourceNames := discoveryConfiguredSourcePairs(sourceOptions)

	rows, err := h.Q.ListSaaSAppHotspots(ctx, gen.ListSaaSAppHotspotsParams{
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
		SourceKind:            selectedSourceKind,
		SourceName:            selectedSourceName,
		LimitRows:             discoveryHotspotsLimit,
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
		Layout:             layout,
		SourceOptions:      sourceKindOptions(sourceOptions),
		SourceNameOptions:  sourceNameOptions,
		SelectedSourceKind: selectedSourceKind,
		SelectedSourceName: selectedSourceName,
		Items:              items,
		HasItems:           len(items) > 0,
		EmptyStateMsg:      "No discovery hotspots are currently above the high-risk threshold.",
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
	if err := h.recomputeDiscoveryPosture(ctx); err != nil {
		return h.RenderError(c, err)
	}

	app, err := h.Q.GetSaaSAppByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	layout, _, err := h.LayoutData(ctx, c, "Discovered SaaS App")
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

	displayName := strings.TrimSpace(app.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(app.CanonicalKey)
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

func (h *Handlers) recomputeDiscoveryPosture(ctx context.Context) error {
	rows, err := h.Q.ListSaaSAppPostureInputs(ctx)
	if err != nil {
		return fmt.Errorf("list discovery posture inputs: %w", err)
	}

	runtimes, err := h.discoveryConnectorRuntimes(ctx)
	if err != nil {
		return err
	}
	configuredSourceKinds, configuredSourceNames := discoveryConfiguredSourcePairsFromRuntimes(runtimes)

	seenSources := map[string]struct{}{}
	sourceKinds := make([]string, 0, len(rows))
	sourceNames := make([]string, 0, len(rows))
	for _, row := range rows {
		connectorKind := NormalizeConnectorKind(row.BindingConnectorKind)
		connectorSource := strings.TrimSpace(row.BindingConnectorSourceName)
		if connectorKind == "" || connectorSource == "" {
			continue
		}
		key := connectorKind + "\x00" + connectorSource
		if _, ok := seenSources[key]; ok {
			continue
		}
		seenSources[key] = struct{}{}
		sourceKinds = append(sourceKinds, connectorKind)
		sourceNames = append(sourceNames, connectorSource)
	}

	lastSuccessBySource := map[string]time.Time{}
	if len(sourceKinds) > 0 {
		latestRows, err := h.Q.ListLatestSuccessfulSyncFinishedAtForSources(ctx, gen.ListLatestSuccessfulSyncFinishedAtForSourcesParams{
			SourceKinds: sourceKinds,
			SourceNames: sourceNames,
		})
		if err != nil {
			return fmt.Errorf("list latest successful sync timestamps: %w", err)
		}
		for _, row := range latestRows {
			if !row.LastSuccessAt.Valid {
				continue
			}
			key := NormalizeConnectorKind(row.SourceKind) + "\x00" + strings.TrimSpace(row.SourceName)
			lastSuccessBySource[key] = row.LastSuccessAt.Time.UTC()
		}
	}

	now := time.Now().UTC()
	for _, row := range rows {
		connectorKind := NormalizeConnectorKind(row.BindingConnectorKind)
		connectorSource := strings.TrimSpace(row.BindingConnectorSourceName)
		hasPrimaryBinding := connectorKind != "" && connectorSource != ""

		connectorConfigured := false
		connectorEnabled := false
		if hasPrimaryBinding {
			if runtime, ok := runtimes[connectorKind]; ok && strings.EqualFold(strings.TrimSpace(runtime.SourceName), connectorSource) {
				connectorConfigured = runtime.Configured
				connectorEnabled = runtime.Enabled
			}
		}

		lastSuccessAt, hasLastSuccess := lastSuccessBySource[connectorKind+"\x00"+connectorSource]
		managedState, managedReason := discovery.ManagedStateAndReason(discovery.ManagedStateInput{
			HasPrimaryBinding:     hasPrimaryBinding,
			ConnectorEnabled:      connectorEnabled,
			ConnectorConfigured:   connectorConfigured,
			LastSuccessfulSyncAt:  lastSuccessAt,
			HasLastSuccessfulSync: hasLastSuccess,
			FreshnessWindow:       h.discoveryFreshnessWindow(connectorKind),
			Now:                   now,
		})

		suggestedBusinessCriticality := discovery.SuggestedBusinessCriticality(row.Actors30d, row.HasPrivilegedScope)
		suggestedDataClassification := discovery.SuggestedDataClassification(row.HasPrivilegedScope, row.HasConfidentialScope)

		effectiveBusinessCriticality := normalizeDiscoveryBusinessCriticality(row.BusinessCriticality)
		if effectiveBusinessCriticality == "unknown" {
			effectiveBusinessCriticality = suggestedBusinessCriticality
		}
		effectiveDataClassification := normalizeDiscoveryDataClassification(row.DataClassification)
		if effectiveDataClassification == "unknown" {
			effectiveDataClassification = suggestedDataClassification
		}

		riskScore, riskLevel := discovery.RiskScoreAndLevel(discovery.RiskInput{
			ManagedState:          managedState,
			HasPrivilegedScopes:   row.HasPrivilegedScope,
			HasConfidentialScopes: row.HasConfidentialScope,
			HasOwner:              row.OwnerIdentityID > 0,
			Actors30d:             row.Actors30d,
			BusinessCriticality:   effectiveBusinessCriticality,
			DataClassification:    effectiveDataClassification,
		})

		if err := h.Q.UpdateSaaSAppPosture(ctx, gen.UpdateSaaSAppPostureParams{
			ManagedState:                 managedState,
			ManagedReason:                managedReason,
			BoundConnectorKind:           connectorKind,
			BoundConnectorSourceName:     connectorSource,
			RiskScore:                    riskScore,
			RiskLevel:                    riskLevel,
			SuggestedBusinessCriticality: suggestedBusinessCriticality,
			SuggestedDataClassification:  suggestedDataClassification,
			ID:                           row.ID,
		}); err != nil {
			return fmt.Errorf("update saas app posture for app %d: %w", row.ID, err)
		}
	}

	if err := h.refreshDiscoveryMetrics(ctx, configuredSourceKinds, configuredSourceNames); err != nil {
		return err
	}
	return nil
}

func (h *Handlers) refreshDiscoveryMetrics(ctx context.Context, configuredSourceKinds, configuredSourceNames []string) error {
	for _, state := range []string{"managed", "unmanaged"} {
		metrics.DiscoveryAppsTotal.WithLabelValues(state).Set(0)
	}
	for _, level := range []string{"low", "medium", "high", "critical"} {
		metrics.DiscoveryHotspotsTotal.WithLabelValues(level).Set(0)
	}

	managedCounts, err := h.Q.CountSaaSAppsGroupedByManagedState(ctx, gen.CountSaaSAppsGroupedByManagedStateParams{
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
	})
	if err != nil {
		return fmt.Errorf("count discovery apps by managed state: %w", err)
	}
	for _, row := range managedCounts {
		state := strings.ToLower(strings.TrimSpace(row.ManagedState))
		if state != "managed" && state != "unmanaged" {
			continue
		}
		metrics.DiscoveryAppsTotal.WithLabelValues(state).Set(float64(row.AppCount))
	}

	riskCounts, err := h.Q.CountSaaSAppsGroupedByRiskLevel(ctx, gen.CountSaaSAppsGroupedByRiskLevelParams{
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
	})
	if err != nil {
		return fmt.Errorf("count discovery apps by risk level: %w", err)
	}
	for _, row := range riskCounts {
		level := strings.ToLower(strings.TrimSpace(row.RiskLevel))
		switch level {
		case "high", "critical":
			metrics.DiscoveryHotspotsTotal.WithLabelValues(level).Set(float64(row.AppCount))
		}
	}
	return nil
}

func discoveryConfiguredSourcePairsFromRuntimes(runtimes map[string]discoveryConnectorRuntime) ([]string, []string) {
	type sourcePair struct {
		kind string
		name string
	}

	pairs := make([]sourcePair, 0, 2)
	for _, kind := range []string{configstore.KindOkta, configstore.KindEntra} {
		runtime, ok := runtimes[kind]
		if !ok || !runtime.Configured {
			continue
		}
		sourceName := strings.TrimSpace(runtime.SourceName)
		if sourceName == "" {
			continue
		}
		pairs = append(pairs, sourcePair{kind: kind, name: sourceName})
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].kind == pairs[j].kind {
			return strings.ToLower(pairs[i].name) < strings.ToLower(pairs[j].name)
		}
		return pairs[i].kind < pairs[j].kind
	})

	sourceKinds := make([]string, 0, len(pairs))
	sourceNames := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		sourceKinds = append(sourceKinds, pair.kind)
		sourceNames = append(sourceNames, pair.name)
	}
	return sourceKinds, sourceNames
}

func (h *Handlers) discoveryConnectorRuntimes(ctx context.Context) (map[string]discoveryConnectorRuntime, error) {
	out := map[string]discoveryConnectorRuntime{}
	if h.Registry == nil || h.Q == nil {
		return out, nil
	}
	states, err := h.Registry.LoadStates(ctx, h.Q)
	if err != nil {
		return nil, fmt.Errorf("load connector states for discovery posture: %w", err)
	}
	for _, state := range states {
		kind := NormalizeConnectorKind(state.Definition.Kind())
		if kind == "" {
			continue
		}
		out[kind] = discoveryConnectorRuntime{
			SourceName: strings.TrimSpace(state.SourceName),
			Configured: state.Configured,
			Enabled:    state.Enabled,
		}
	}
	return out, nil
}

func (h *Handlers) discoveryFreshnessWindow(kind string) time.Duration {
	interval := h.Cfg.SyncInterval
	switch NormalizeConnectorKind(kind) {
	case configstore.KindOkta:
		if h.Cfg.SyncOktaInterval > 0 {
			interval = h.Cfg.SyncOktaInterval
		}
	case configstore.KindEntra:
		if h.Cfg.SyncEntraInterval > 0 {
			interval = h.Cfg.SyncEntraInterval
		}
	case configstore.KindGitHub:
		if h.Cfg.SyncGitHubInterval > 0 {
			interval = h.Cfg.SyncGitHubInterval
		}
	case configstore.KindDatadog:
		if h.Cfg.SyncDatadogInterval > 0 {
			interval = h.Cfg.SyncDatadogInterval
		}
	case configstore.KindAWSIdentityCenter:
		if h.Cfg.SyncAWSInterval > 0 {
			interval = h.Cfg.SyncAWSInterval
		}
	}
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	window := max(interval*2, 30*time.Minute)
	return window
}

func discoverySourceOptions(snap ConnectorSnapshot) []viewmodels.DiscoverySourceOption {
	options := make([]viewmodels.DiscoverySourceOption, 0, 2)
	oktaSource := strings.TrimSpace(snap.Okta.Domain)
	if snap.OktaConfigured && oktaSource != "" {
		options = append(options, viewmodels.DiscoverySourceOption{
			SourceKind: "okta",
			SourceName: oktaSource,
			Label:      sourcePrimaryLabel("okta"),
		})
	}
	entraSource := strings.TrimSpace(snap.Entra.TenantID)
	if snap.EntraConfigured && entraSource != "" {
		options = append(options, viewmodels.DiscoverySourceOption{
			SourceKind: "entra",
			SourceName: entraSource,
			Label:      sourcePrimaryLabel("entra"),
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

func normalizeDiscoverySourceSelection(rawKind, rawName string, sourceOptions []viewmodels.DiscoverySourceOption) (string, string) {
	selectedKind := normalizeDiscoverySourceKind(rawKind)
	selectedName := strings.TrimSpace(rawName)
	hasKind := selectedKind != ""
	hasName := selectedName != ""

	if !hasKind && !hasName {
		return "", ""
	}

	if hasKind && hasName {
		for _, option := range sourceOptions {
			optionKind := normalizeDiscoverySourceKind(option.SourceKind)
			optionName := strings.TrimSpace(option.SourceName)
			if optionKind == selectedKind && strings.EqualFold(optionName, selectedName) {
				return optionKind, optionName
			}
		}
		// Invalid kind/name pair falls back to "All configured".
		return "", ""
	}

	if hasKind {
		if discoveryHasSourceKind(selectedKind, sourceOptions) {
			return selectedKind, ""
		}
		return "", ""
	}

	matchCount := 0
	matchKind := ""
	matchName := ""
	for _, option := range sourceOptions {
		optionKind := normalizeDiscoverySourceKind(option.SourceKind)
		optionName := strings.TrimSpace(option.SourceName)
		if optionKind == "" || optionName == "" {
			continue
		}
		if !strings.EqualFold(optionName, selectedName) {
			continue
		}
		matchCount++
		if matchCount == 1 {
			matchKind = optionKind
			matchName = optionName
		}
	}
	if matchCount == 1 {
		return matchKind, matchName
	}

	// Unknown or ambiguous name-only filters fall back to "All configured".
	return "", ""
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

func discoveryConfiguredSourcePairs(sourceOptions []viewmodels.DiscoverySourceOption) ([]string, []string) {
	type sourcePair struct {
		kind string
		name string
	}

	pairs := make([]sourcePair, 0, len(sourceOptions))
	seen := make(map[string]struct{}, len(sourceOptions))
	for _, option := range sourceOptions {
		kind := normalizeDiscoverySourceKind(option.SourceKind)
		name := strings.TrimSpace(option.SourceName)
		if kind == "" || name == "" {
			continue
		}
		key := kind + "\x00" + strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		pairs = append(pairs, sourcePair{kind: kind, name: name})
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].kind == pairs[j].kind {
			return strings.ToLower(pairs[i].name) < strings.ToLower(pairs[j].name)
		}
		return pairs[i].kind < pairs[j].kind
	})

	kinds := make([]string, 0, len(pairs))
	names := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		kinds = append(kinds, pair.kind)
		names = append(names, pair.name)
	}
	return kinds, names
}

func discoveryHasSourceKind(kind string, sourceOptions []viewmodels.DiscoverySourceOption) bool {
	for _, option := range sourceOptions {
		if normalizeDiscoverySourceKind(option.SourceKind) == kind {
			return true
		}
	}
	return false
}

func discoveryHasSourceName(selectedKind, sourceName string, options []viewmodels.DiscoverySourceOption) bool {
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" {
		return true
	}
	for _, option := range options {
		if selectedKind != "" && option.SourceKind != selectedKind {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(option.SourceName), sourceName) {
			return true
		}
	}
	return false
}

func normalizeDiscoverySourceKind(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "okta":
		return "okta"
	case "entra":
		return "entra"
	default:
		return ""
	}
}

func normalizeDiscoveryManagedState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case discovery.ManagedStateManaged:
		return discovery.ManagedStateManaged
	case discovery.ManagedStateUnmanaged:
		return discovery.ManagedStateUnmanaged
	default:
		return ""
	}
}

func normalizeDiscoveryRiskLevel(raw string) string {
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
