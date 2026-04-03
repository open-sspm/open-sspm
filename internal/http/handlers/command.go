package handlers

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"golang.org/x/sync/errgroup"
)

const (
	commandSearchMinQueryRunes = 2
	commandSearchLimitRows     = 5
	commandSearchPlaceholder   = "Search identities and apps… (⌘K / Ctrl+K)"
	commandSearchAriaLabel     = "Search identities and apps"
)

func (h *Handlers) HandleCommandSearch(c *echo.Context) error {
	query := strings.TrimSpace(c.QueryParam("q"))

	ctx := c.Request().Context()
	snap, err := h.LoadConnectorSnapshot(ctx)
	if err != nil {
		return h.RenderComponent(c, views.CommandSearch(commandSearchErrorData(query, ConnectorSnapshot{})))
	}

	data := commandSearchShellData(snap, query)
	queryLen := utf8.RuneCountInString(query)
	if queryLen == 0 {
		return h.RenderComponent(c, views.CommandSearch(data))
	}

	if queryLen < commandSearchMinQueryRunes {
		data.Notices = append(data.Notices, commandNoticeItem(
			"cmd-notice-min-query",
			"Type at least 2 characters for direct matches.",
		))
		if actionSection := commandActionSection(snap, query); len(actionSection.Items) > 0 {
			data.Sections = append(data.Sections, actionSection)
		}
		return h.RenderComponent(c, views.CommandSearch(data))
	}

	identityKinds, identityNames := identityConfiguredSourcePairs(availableIdentitySourcePairs(snap))
	programmaticSources := availableProgrammaticSources(snap)
	discoveryKinds, discoveryNames := discoveryConfiguredSourcePairs(discoverySourceOptions(snap))
	connectedSourceName, hasConnectedApps := commandConnectedAppsSourceName(snap)
	cutoffs := h.discoveryPostureCutoffs(time.Now().UTC())

	var (
		identityRows  []gen.SearchIdentitiesForCommandRow
		connectedRows []gen.SearchConnectedAppsForCommandRow
		appAssetRows  []gen.SearchAppAssetsForCommandRow
		discoveryRows []gen.SearchDiscoveryAppsForCommandRow
		oktaAppRows   []gen.SearchOktaAppsForCommandRow
	)

	var group errgroup.Group
	if len(identityKinds) > 0 {
		group.Go(func() error {
			rows, err := h.Q.SearchIdentitiesForCommand(ctx, gen.SearchIdentitiesForCommandParams{
				LimitRows:             commandSearchLimitRows,
				ConfiguredSourceKinds: identityKinds,
				ConfiguredSourceNames: identityNames,
				Query:                 query,
			})
			if err != nil {
				return err
			}
			identityRows = rows
			return nil
		})
	}
	if hasConnectedApps {
		group.Go(func() error {
			rows, err := h.Q.SearchConnectedAppsForCommand(ctx, gen.SearchConnectedAppsForCommandParams{
				SourceName: connectedSourceName,
				Query:      query,
				LimitRows:  commandSearchLimitRows,
			})
			if err != nil {
				return err
			}
			connectedRows = rows
			return nil
		})
	}
	if len(programmaticSources) > 0 {
		programmaticKinds := make([]string, 0, len(programmaticSources))
		programmaticNames := make([]string, 0, len(programmaticSources))
		for _, source := range programmaticSources {
			programmaticKinds = append(programmaticKinds, source.SourceKind)
			programmaticNames = append(programmaticNames, source.SourceName)
		}
		group.Go(func() error {
			rows, err := h.Q.SearchAppAssetsForCommand(ctx, gen.SearchAppAssetsForCommandParams{
				Query:                             query,
				ExcludeGoogleWorkspaceOauthClient: hasConnectedApps,
				LimitRows:                         commandSearchLimitRows,
				ConfiguredSourceKinds:             programmaticKinds,
				ConfiguredSourceNames:             programmaticNames,
			})
			if err != nil {
				return err
			}
			appAssetRows = rows
			return nil
		})
	}
	if len(discoveryKinds) > 0 {
		group.Go(func() error {
			rows, err := h.Q.SearchDiscoveryAppsForCommand(ctx, gen.SearchDiscoveryAppsForCommandParams{
				Query:                     query,
				LimitRows:                 commandSearchLimitRows,
				ConfiguredSourceKinds:     discoveryKinds,
				ConfiguredSourceNames:     discoveryNames,
				OktaFreshAfter:            cutoffs.OktaFreshAfter,
				EntraFreshAfter:           cutoffs.EntraFreshAfter,
				GoogleWorkspaceFreshAfter: cutoffs.GoogleWorkspaceFreshAfter,
				GithubFreshAfter:          cutoffs.GithubFreshAfter,
				DatadogFreshAfter:         cutoffs.DatadogFreshAfter,
				AwsFreshAfter:             cutoffs.AwsFreshAfter,
				DefaultFreshAfter:         cutoffs.DefaultFreshAfter,
			})
			if err != nil {
				return err
			}
			discoveryRows = rows
			return nil
		})
	}
	if commandOktaAppsAvailable(snap) {
		group.Go(func() error {
			rows, err := h.Q.SearchOktaAppsForCommand(ctx, gen.SearchOktaAppsForCommandParams{
				Query:     query,
				LimitRows: commandSearchLimitRows,
			})
			if err != nil {
				return err
			}
			oktaAppRows = rows
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return h.RenderComponent(c, views.CommandSearch(commandSearchErrorData(query, snap)))
	}

	if len(identityRows) > 0 {
		data.Sections = append(data.Sections, viewmodels.CommandSectionView{
			Key:   "identities",
			Title: "Identities",
			Items: commandIdentityItems(identityRows),
		})
	}
	if len(connectedRows) > 0 {
		data.Sections = append(data.Sections, viewmodels.CommandSectionView{
			Key:   "connected-apps",
			Title: "OAuth Apps",
			Items: commandConnectedAppItems(connectedRows),
		})
	}
	if len(appAssetRows) > 0 {
		data.Sections = append(data.Sections, viewmodels.CommandSectionView{
			Key:   "app-assets",
			Title: "App Assets",
			Items: commandAppAssetItems(appAssetRows),
		})
	}
	if len(discoveryRows) > 0 {
		data.Sections = append(data.Sections, viewmodels.CommandSectionView{
			Key:   "discovery-apps",
			Title: "Discovery Apps",
			Items: commandDiscoveryAppItems(discoveryRows),
		})
	}
	if len(oktaAppRows) > 0 {
		data.Sections = append(data.Sections, viewmodels.CommandSectionView{
			Key:   "okta-apps",
			Title: "Assigned Apps",
			Items: commandOktaAppItems(oktaAppRows),
		})
	}

	if len(data.Sections) == 0 {
		data.Notices = append(data.Notices, commandNoticeItem("cmd-notice-no-matches", "No direct matches."))
	}
	if actionSection := commandActionSection(snap, query); len(actionSection.Items) > 0 {
		data.Sections = append(data.Sections, actionSection)
	}

	return h.RenderComponent(c, views.CommandSearch(data))
}

func commandSearchShellData(_ ConnectorSnapshot, query string) viewmodels.CommandSearchViewData {
	query = strings.TrimSpace(query)
	return viewmodels.CommandSearchViewData{
		Query:       query,
		Placeholder: commandSearchPlaceholder,
		AriaLabel:   commandSearchAriaLabel,
	}
}

func commandSearchErrorData(query string, snap ConnectorSnapshot) viewmodels.CommandSearchViewData {
	data := commandSearchShellData(snap, query)
	data.Notices = append(data.Notices, commandNoticeItem(
		"cmd-notice-search-error",
		"Search unavailable. Open an inventory below.",
	))
	if strings.TrimSpace(query) != "" {
		if actionSection := commandActionSection(snap, query); len(actionSection.Items) > 0 {
			data.Sections = append(data.Sections, actionSection)
		}
	}
	return data
}

func commandNoticeItem(id, message string) viewmodels.CommandItemView {
	return viewmodels.CommandItemView{
		ID:           id,
		Kind:         "notice",
		Primary:      strings.TrimSpace(message),
		FilterText:   strings.TrimSpace(message),
		Disabled:     true,
		ForceVisible: true,
	}
}

func commandIdentityItems(rows []gen.SearchIdentitiesForCommandRow) []viewmodels.CommandItemView {
	items := make([]viewmodels.CommandItemView, 0, len(rows))
	for _, row := range rows {
		name := identityNamePrimary(row.DisplayName, row.PrimaryEmail, row.ID)
		secondary := identityNameSecondary(row.DisplayName, row.PrimaryEmail)
		if secondary == "" {
			secondary = sourcePrimaryLabel(row.SourceKind)
		}
		items = append(items, viewmodels.CommandItemView{
			ID:         "cmd-identity-" + strconv.FormatInt(row.ID, 10),
			Kind:       "identity",
			Href:       "/identities/" + strconv.FormatInt(row.ID, 10),
			Primary:    name,
			Secondary:  secondary,
			FilterText: strings.TrimSpace(name + " " + row.PrimaryEmail + " " + row.SourceKind + " " + row.SourceName),
			Keywords:   row.PrimaryEmail + " " + row.SourceKind + " " + row.SourceName,
			Badges: []viewmodels.CommandBadgeView{
				{
					Label: views.HumanizeIdentityType(row.IdentityType),
					Class: "badge-outline shrink-0",
				},
			},
		})
	}
	return items
}

func commandConnectedAppItems(rows []gen.SearchConnectedAppsForCommandRow) []viewmodels.CommandItemView {
	items := make([]viewmodels.CommandItemView, 0, len(rows))
	for _, row := range rows {
		displayName := fallbackDash(row.DisplayName)
		status := fallbackDash(row.Status)
		freshness := connectedAppFreshness(row.EvidenceLastSeenAt)
		items = append(items, viewmodels.CommandItemView{
			ID:         "cmd-connected-app-" + strconv.FormatInt(row.ID, 10),
			Kind:       "connected_app",
			Href:       "/oauth-apps/" + strconv.FormatInt(row.ID, 10),
			Primary:    displayName,
			Secondary:  fallbackDash(strings.TrimSpace(row.ExternalID)),
			FilterText: strings.TrimSpace(displayName + " " + row.ExternalID + " " + status + " " + row.ReviewState),
			Keywords:   row.ExternalID,
			Badges: []viewmodels.CommandBadgeView{
				{
					Label: views.HumanizeConnectedAppReviewState(row.ReviewState),
					Class: views.ConnectedAppReviewStateBadgeClass(row.ReviewState) + " shrink-0",
				},
				{
					Label: views.HumanizeConnectedAppFreshness(freshness),
					Class: views.ConnectedAppFreshnessBadgeClass(freshness) + " shrink-0",
				},
			},
		})
	}
	return items
}

func commandAppAssetItems(rows []gen.SearchAppAssetsForCommandRow) []viewmodels.CommandItemView {
	items := make([]viewmodels.CommandItemView, 0, len(rows))
	for _, row := range rows {
		displayName := fallbackDash(row.DisplayName)
		items = append(items, viewmodels.CommandItemView{
			ID:         "cmd-app-asset-" + strconv.FormatInt(row.ID, 10),
			Kind:       "app_asset",
			Href:       "/app-assets/" + strconv.FormatInt(row.ID, 10),
			Primary:    displayName,
			Secondary:  fallbackDash(strings.TrimSpace(row.ExternalID)),
			FilterText: strings.TrimSpace(displayName + " " + row.ExternalID + " " + row.SourceKind + " " + row.AssetKind),
			Keywords:   row.ExternalID + " " + row.SourceKind + " " + row.AssetKind,
			Badges: []viewmodels.CommandBadgeView{
				{
					Label: views.HumanizeProgrammaticKind(row.SourceKind),
					Class: "badge-outline shrink-0",
				},
				{
					Label: views.HumanizeProgrammaticKind(row.AssetKind),
					Class: "badge-outline shrink-0",
				},
			},
		})
	}
	return items
}

func commandDiscoveryAppItems(rows []gen.SearchDiscoveryAppsForCommandRow) []viewmodels.CommandItemView {
	items := make([]viewmodels.CommandItemView, 0, len(rows))
	for _, row := range rows {
		displayName := fallbackDash(row.DisplayName)
		domainLabel, vendorLabel := discoveryAppSecondaryLabels(displayName, row.PrimaryDomain, row.VendorName)
		secondaryParts := make([]string, 0, 2)
		if domainLabel != "" {
			secondaryParts = append(secondaryParts, domainLabel)
		}
		if vendorLabel != "" {
			secondaryParts = append(secondaryParts, vendorLabel)
		}
		items = append(items, viewmodels.CommandItemView{
			ID:         "cmd-discovery-app-" + strconv.FormatInt(row.ID, 10),
			Kind:       "discovery_app",
			Href:       "/discovery/apps/" + strconv.FormatInt(row.ID, 10),
			Primary:    displayName,
			Secondary:  strings.Join(secondaryParts, " • "),
			FilterText: strings.TrimSpace(displayName + " " + row.PrimaryDomain + " " + row.VendorName),
			Keywords:   row.PrimaryDomain + " " + row.VendorName,
			Badges: []viewmodels.CommandBadgeView{
				{
					Label: views.HumanizeDiscoveryManagedState(row.ManagedState),
					Class: views.DiscoveryManagedBadgeClass(row.ManagedState) + " shrink-0",
				},
				{
					Label: views.HumanizeCredentialRisk(row.RiskLevel),
					Class: views.CredentialRiskBadgeClass(row.RiskLevel) + " shrink-0",
				},
			},
		})
	}
	return items
}

func commandOktaAppItems(rows []gen.SearchOktaAppsForCommandRow) []viewmodels.CommandItemView {
	items := make([]viewmodels.CommandItemView, 0, len(rows))
	for _, row := range rows {
		displayName := fallbackDash(row.Label)
		secondary := strings.TrimSpace(row.Name)
		if secondary == "" {
			secondary = strings.TrimSpace(row.ExternalID)
		}
		items = append(items, viewmodels.CommandItemView{
			ID:         "cmd-okta-app-" + strings.TrimSpace(row.ExternalID),
			Kind:       "okta_app",
			Href:       views.AppDetailURL(IntegratedAppHref(row.IntegrationKind), row.ExternalID),
			Primary:    displayName,
			Secondary:  fallbackDash(secondary),
			FilterText: strings.TrimSpace(displayName + " " + row.Name + " " + row.ExternalID),
			Keywords:   row.ExternalID,
			Badges: []viewmodels.CommandBadgeView{
				{
					Label: fallbackDash(strings.TrimSpace(row.Status)),
					Class: views.StatusBadgeClass(row.Status) + " shrink-0",
				},
			},
		})
	}
	return items
}

func commandActionSection(snap ConnectorSnapshot, query string) viewmodels.CommandSectionView {
	query = strings.TrimSpace(query)
	items := make([]viewmodels.CommandItemView, 0, 5)
	if commandHasIdentitySurface(snap) {
		items = append(items, commandActionItem(
			"cmd-action-identities",
			fmt.Sprintf("Search Identities for “%s”", query),
			commandQueryURL("/identities", query),
		))
	}
	if _, ok := commandConnectedAppsSourceName(snap); ok {
		items = append(items, commandActionItem(
			"cmd-action-connected-apps",
			fmt.Sprintf("Search OAuth Apps for “%s”", query),
			commandQueryURL("/oauth-apps", query),
		))
	}
	if commandHasAppAssetsSurface(snap) {
		items = append(items, commandActionItem(
			"cmd-action-app-assets",
			fmt.Sprintf("Search App Assets for “%s”", query),
			commandQueryURL("/app-assets", query),
		))
	}
	if commandHasDiscoverySurface(snap) {
		items = append(items, commandActionItem(
			"cmd-action-discovery-apps",
			fmt.Sprintf("Search Discovery Apps for “%s”", query),
			commandQueryURL("/discovery/apps", query),
		))
	}
	if commandOktaAppsAvailable(snap) {
		items = append(items, commandActionItem(
			"cmd-action-okta-apps",
			fmt.Sprintf("Search Assigned Apps for “%s”", query),
			commandQueryURL("/assigned-apps", query),
		))
	}
	return viewmodels.CommandSectionView{
		Key:   "actions",
		Title: "Actions",
		Items: items,
	}
}

func commandActionItem(id, label, href string) viewmodels.CommandItemView {
	return viewmodels.CommandItemView{
		ID:           id,
		Kind:         "action",
		Href:         href,
		Primary:      strings.TrimSpace(label),
		FilterText:   strings.TrimSpace(label),
		Disabled:     false,
		ForceVisible: true,
	}
}

func commandConnectedAppsSourceName(snap ConnectorSnapshot) (string, bool) {
	sourceName := strings.TrimSpace(snap.GoogleWorkspace.CustomerID)
	if !snap.GoogleWorkspaceConfigured || !snap.GoogleWorkspaceEnabled || sourceName == "" {
		return "", false
	}
	return sourceName, true
}

func commandHasIdentitySurface(snap ConnectorSnapshot) bool {
	return len(availableIdentitySourcePairs(snap)) > 0
}

func commandHasAppAssetsSurface(snap ConnectorSnapshot) bool {
	return len(availableProgrammaticSources(snap)) > 0
}

func commandHasDiscoverySurface(snap ConnectorSnapshot) bool {
	return len(discoverySourceOptions(snap)) > 0
}

func commandOktaAppsAvailable(snap ConnectorSnapshot) bool {
	return snap.OktaConfigured && strings.TrimSpace(snap.Okta.Domain) != ""
}

func commandQueryURL(path, query string) string {
	values := url.Values{}
	if query = strings.TrimSpace(query); query != "" {
		values.Set("q", query)
	}
	if len(values) == 0 {
		return path
	}
	return path + "?" + values.Encode()
}
