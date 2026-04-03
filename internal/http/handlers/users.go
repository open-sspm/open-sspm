package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/accessgraph"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleOktaAccounts renders the Okta accounts list page.
func (h *Handlers) HandleOktaAccounts(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Okta Accounts")
	if err != nil {
		return h.RenderError(c, err)
	}
	const perPage = 20

	query := strings.TrimSpace(c.QueryParam("q"))
	state := strings.ToLower(strings.TrimSpace(c.QueryParam("state")))
	if state == "" {
		state = strings.ToLower(strings.TrimSpace(c.QueryParam("status")))
	}
	switch state {
	case "active", "inactive":
	default:
		state = ""
	}
	page := parsePageParam(c)

	totalCount, err := h.Q.CountOktaAccountsByQueryAndState(ctx, gen.CountOktaAccountsByQueryAndStateParams{
		Query: query,
		State: state,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination := newPaginatedListState(totalCount, page, perPage)
	users, err := h.Q.ListOktaAccountsPageByQueryAndState(ctx, gen.ListOktaAccountsPageByQueryAndStateParams{
		Query:      query,
		State:      state,
		PageLimit:  int32(perPage),
		PageOffset: int32(pagination.Offset()),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	emptyState := "No Okta accounts synced yet."
	if query != "" || state != "" {
		emptyState = "No Okta accounts match the current search."
	}

	data := viewmodels.OktaAccountsViewData{
		PaginatedListPageData: pagination.PageData(layout, len(users), emptyState, ""),
		Users:                 users,
		Query:                 query,
		State:                 state,
		HasUsers:              len(users) > 0,
	}

	return h.RenderComponent(c, views.OktaAccountsPage(data))
}

// HandleOktaAccountShow renders the Okta account detail page.
func (h *Handlers) HandleOktaAccountShow(c *echo.Context) error {
	idStr := strings.TrimSpace(c.Param("id"))
	if idStr == "" {
		idStr = strings.Trim(c.Param("*"), "/")
	}
	if idStr == "" {
		return RenderNotFound(c)
	}
	id, err := strconv.ParseInt(path.Clean("/" + idStr)[1:], 10, 64)
	if err != nil {
		return RenderNotFound(c)
	}
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Okta Account")
	if err != nil {
		return h.RenderError(c, err)
	}
	user, err := h.Q.GetOktaAccount(ctx, id)
	if err != nil {
		return RenderNotFound(c)
	}
	_, linked, err := h.linkedAccountsForOktaAccount(ctx, user.ID)
	if err != nil {
		return h.RenderError(c, err)
	}
	linkedIDs := make([]int64, 0, len(linked))
	for _, app := range linked {
		linkedIDs = append(linkedIDs, app.ID)
	}
	entitlementsByAccountID := make(map[int64][]gen.ListEntitlementsForAccountIDsRow, len(linked))
	if len(linkedIDs) > 0 {
		ents, err := h.Q.ListEntitlementsForAccountIDs(ctx, linkedIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, ent := range ents {
			entitlementsByAccountID[ent.AccountID] = append(entitlementsByAccountID[ent.AccountID], ent)
		}
	}
	var linkedAccounts []viewmodels.LinkedAccountView
	for _, account := range linked {
		entitlementViews := make([]viewmodels.LinkedEntitlementView, 0, len(entitlementsByAccountID[account.ID]))
		for _, ent := range entitlementsByAccountID[account.ID] {
			resourceKind, resourceID, ok := accessgraph.ParseCanonicalResourceRef(ent.Resource)
			if !ok {
				resourceID = strings.TrimSpace(ent.Resource)
			}
			resourceHref := ""
			if ok {
				resourceHref = accessgraph.BuildResourceHref(account.SourceKind, account.SourceName, resourceKind, resourceID)
			}
			resourceLabel := accessgraph.DisplayResourceLabel(ent.Resource, ent.RawJson)
			if strings.TrimSpace(resourceLabel) == "" {
				if ok {
					resourceLabel = resourceID
				} else {
					resourceLabel = strings.TrimSpace(ent.Resource)
				}
			}
			entitlementViews = append(entitlementViews, viewmodels.LinkedEntitlementView{
				Kind:          strings.TrimSpace(ent.Kind),
				ResourceKind:  resourceKind,
				ResourceID:    resourceID,
				ResourceLabel: resourceLabel,
				ResourceHref:  resourceHref,
				Permission:    strings.TrimSpace(ent.Permission),
			})
		}
		linkedAccounts = append(linkedAccounts, viewmodels.LinkedAccountView{Account: account, Entitlements: entitlementViews})
	}

	assignments, err := h.Q.ListOktaAppAssignmentsForOktaAccount(ctx, user.ID)
	if err != nil {
		return h.RenderError(c, err)
	}
	userGroups, err := h.Q.ListOktaGroupsForOktaAccount(ctx, user.ID)
	if err != nil {
		return h.RenderError(c, err)
	}

	groupNames := make(map[int64]string)
	for _, group := range userGroups {
		name := group.Name
		if name == "" {
			name = group.ExternalID
		}
		groupNames[group.ID] = name
	}

	appIDSet := make(map[int64]struct{})
	for _, assignment := range assignments {
		appIDSet[assignment.OktaAppID] = struct{}{}
	}
	appIDs := make([]int64, 0, len(appIDSet))
	for appID := range appIDSet {
		appIDs = append(appIDs, appID)
	}

	appGroupAssignments := []gen.ListOktaAppGroupAssignmentsByAppIDsRow{}
	if len(appIDs) > 0 {
		appGroupAssignments, err = h.Q.ListOktaAppGroupAssignmentsByAppIDs(ctx, appIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
	}

	appGroupIDs := make(map[int64][]int64)
	for _, row := range appGroupAssignments {
		appGroupIDs[row.OktaAppID] = append(appGroupIDs[row.OktaAppID], row.OktaGroupID)
	}

	var oktaAssignments []viewmodels.OktaAssignmentView
	for _, assignment := range assignments {
		appLabel := assignment.AppLabel
		if appLabel == "" {
			appLabel = assignment.OktaAppExternalID
		}
		assignedVia := "Unknown"
		scope := strings.ToUpper(strings.TrimSpace(assignment.Scope))
		if scope == "USER" {
			assignedVia = "Direct"
		} else if scope == "GROUP" {
			assignedVia = "Group"
		}
		var groups []string
		if scope == "GROUP" {
			for _, groupID := range appGroupIDs[assignment.OktaAppID] {
				if name, ok := groupNames[groupID]; ok && name != "" {
					groups = append(groups, name)
				}
			}
			sort.Strings(groups)
			if len(groups) == 0 {
				groups = []string{"(unknown)"}
			}
		}
		oktaAssignments = append(oktaAssignments, viewmodels.OktaAssignmentView{
			AppLabel: appLabel,
			AppName:  assignment.AppName,
			AppHref: func() string {
				href := IntegratedAppHref(assignment.IntegrationKind)
				if href == "" {
					if externalID := strings.TrimSpace(assignment.OktaAppExternalID); externalID != "" {
						href = "/assigned-apps/" + externalID
					}
				}
				return href
			}(),
			AssignedVia: assignedVia,
			Groups:      groups,
			Permissions: SummarizeProfilePermissions(assignment.ProfileJson),
		})
	}

	data := viewmodels.OktaAccountShowViewData{
		Layout:              layout,
		User:                user,
		OktaAssignments:     oktaAssignments,
		OktaAppCount:        len(oktaAssignments),
		LinkedAccounts:      linkedAccounts,
		LinkedAccountsCount: len(linkedAccounts),
		HasLinkedAccounts:   len(linkedAccounts) > 0,
	}

	return h.RenderComponent(c, views.OktaAccountShowPage(data))
}

// HandleGitHubUsers renders the GitHub users page.
func (h *Handlers) HandleGitHubUsers(c *echo.Context) error {
	inventory, err := h.buildSourceAccountInventoryPage(c, sourceAccountInventoryOptions{
		Title:              "GitHub Users",
		ConnectorName:      "GitHub",
		EmptyStateHref:     "/settings/connectors?open=github",
		SyncedEmptyState:   "No GitHub users synced yet.",
		FilteredEmptyState: "No GitHub users match the current search.",
		IsConfigured: func(snap ConnectorSnapshot) bool {
			return snap.GitHubConfigured
		},
		IsEnabled: func(snap ConnectorSnapshot) bool {
			return snap.GitHubEnabled
		},
		Count: func(ctx context.Context, snap ConnectorSnapshot, query string) (int64, error) {
			return h.Q.CountGitHubUsersBySourceAndQuery(ctx, gen.CountGitHubUsersBySourceAndQueryParams{
				SourceKind: "github",
				SourceName: snap.GitHub.Org,
				Query:      query,
			})
		},
		List: func(ctx context.Context, snap ConnectorSnapshot, query string, offset, limit int) ([]sourceAccountInventoryAccount, error) {
			users, err := h.Q.ListGitHubUsersPageBySourceAndQuery(ctx, gen.ListGitHubUsersPageBySourceAndQueryParams{
				SourceKind: "github",
				SourceName: snap.GitHub.Org,
				Query:      query,
				PageLimit:  int32(limit),
				PageOffset: int32(offset),
			})
			if err != nil {
				return nil, err
			}

			accounts := make([]sourceAccountInventoryAccount, 0, len(users))
			for _, user := range users {
				accounts = append(accounts, sourceAccountInventoryAccount{
					ID:          user.ID,
					ExternalID:  strings.TrimSpace(user.ExternalID),
					DisplayName: strings.TrimSpace(user.DisplayName),
					IdentityID:  user.IdentityID,
				})
			}
			return accounts, nil
		},
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.GitHubUserListItem, 0, len(inventory.Accounts))
	for _, user := range inventory.Accounts {
		items = append(items, viewmodels.GitHubUserListItem{
			ID:          user.ID,
			ExternalID:  user.ExternalID,
			DisplayName: user.DisplayName,
			IdentityID:  user.IdentityID,
		})
	}

	data := viewmodels.GitHubUsersViewData{
		SourceAccountInventoryPageData: inventory.PageData,
		Users:                          items,
		HasUsers:                       inventory.PageData.HasAccounts,
	}

	return h.RenderComponent(c, views.GitHubUsersPage(data))
}

// HandleDatadogUsers renders the Datadog users page.
func (h *Handlers) HandleDatadogUsers(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Datadog Users")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	state := strings.ToLower(strings.TrimSpace(c.QueryParam("state")))
	switch state {
	case "active", "inactive":
	default:
		state = ""
	}
	page := parsePageParam(c)
	unavailablePagination := newPaginatedListState(0, page, perPage)

	if !snap.DatadogConfigured || !snap.DatadogEnabled {
		message := connectorUnavailableMessage("Datadog", snap.DatadogConfigured, snap.DatadogEnabled)
		data := viewmodels.DatadogUsersViewData{
			PaginatedListPageData: unavailablePagination.PageData(layout, 0, message, "/settings/connectors?open=datadog"),
			Query:                 query,
			State:                 state,
			HasUsers:              false,
		}
		return h.RenderComponent(c, views.DatadogUsersPage(data))
	}

	totalCount, err := h.Q.CountSourceAccountsBySourceAndQueryAndState(ctx, gen.CountSourceAccountsBySourceAndQueryAndStateParams{
		SourceKind:     "datadog",
		SourceName:     snap.Datadog.Site,
		EntityCategory: registry.EntityCategoryUser,
		Query:          query,
		State:          state,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination := newPaginatedListState(totalCount, page, perPage)
	users, err := h.Q.ListSourceAccountsPageBySourceAndQueryAndState(ctx, gen.ListSourceAccountsPageBySourceAndQueryAndStateParams{
		SourceKind:     "datadog",
		SourceName:     snap.Datadog.Site,
		EntityCategory: registry.EntityCategoryUser,
		Query:          query,
		State:          state,
		PageLimit:      int32(perPage),
		PageOffset:     int32(pagination.Offset()),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	accountIDs := make([]int64, 0, len(users))
	for _, user := range users {
		accountIDs = append(accountIDs, user.ID)
	}

	rolesByAccountID := make(map[int64][]string)
	if len(accountIDs) > 0 {
		ents, err := h.Q.ListEntitlementsForAccountIDs(ctx, accountIDs)
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, ent := range ents {
			if strings.TrimSpace(ent.Kind) != "datadog_role" {
				continue
			}
			label := strings.TrimSpace(accessgraph.DisplayResourceLabel(ent.Resource, ent.RawJson))
			if label == "" {
				continue
			}
			rolesByAccountID[ent.AccountID] = append(rolesByAccountID[ent.AccountID], label)
		}
	}

	items := make([]viewmodels.DatadogUserListItem, 0, len(users))
	for _, user := range users {
		userName := strings.TrimSpace(user.DisplayName)
		if userName == "" {
			userName = strings.TrimSpace(user.Email)
		}
		if userName == "" {
			userName = strings.TrimSpace(user.ExternalID)
		}
		status := datadogUserStatus(user.ID, user.ExternalID, user.RawJson)

		roles := rolesByAccountID[user.ID]
		sort.Strings(roles)
		roles = DedupeStrings(roles)
		rolesDisplay := strings.Join(roles, ", ")

		items = append(items, viewmodels.DatadogUserListItem{
			UserName:     userName,
			Status:       status,
			RolesDisplay: rolesDisplay,
		})
	}

	emptyState := "No Datadog users synced yet."
	if query != "" || state != "" {
		emptyState = "No Datadog users match the current search."
	}

	data := viewmodels.DatadogUsersViewData{
		PaginatedListPageData: pagination.PageData(layout, len(items), emptyState, "/settings/connectors?open=datadog"),
		Users:                 items,
		Query:                 query,
		State:                 state,
		HasUsers:              len(items) > 0,
	}

	return h.RenderComponent(c, views.DatadogUsersPage(data))
}

// HandleUnmatchedGitHub renders the unlinked GitHub accounts page.
func (h *Handlers) HandleUnmatchedGitHub(c *echo.Context) error {
	unmatched, err := h.buildUnmatchedSourceAccountsPage(c, unmatchedSourceAccountOptions{
		Title:              "Unlinked GitHub Accounts",
		ConnectorName:      "GitHub",
		SourceKind:         "github",
		EmptyStateHref:     "/settings/connectors?open=github",
		SyncedEmptyState:   "No unlinked GitHub accounts.",
		FilteredEmptyState: "No unlinked GitHub accounts match the current search.",
		IsConfigured: func(snap ConnectorSnapshot) bool {
			return snap.GitHubConfigured
		},
		IsEnabled: func(snap ConnectorSnapshot) bool {
			return snap.GitHubEnabled
		},
		ResolveSourceName: func(c *echo.Context, snap ConnectorSnapshot) (string, error) {
			org := routeParamOrWildcard(c, "org")
			if org == "" {
				return "", errUnmatchedSourceAccountNotFound
			}
			if org != snap.GitHub.Org {
				return "", unmatchedSourceNameError("unknown org")
			}
			return org, nil
		},
	})
	if err != nil {
		return h.renderUnmatchedSourceAccountsError(c, err)
	}

	data := viewmodels.UnmatchedGitHubViewData{
		UnmatchedSourceAccountsPageData: unmatched.PageData,
	}

	return h.RenderComponent(c, views.UnmatchedGitHubPage(data))
}

// HandleUnmatchedDatadog renders the unlinked Datadog accounts page.
func (h *Handlers) HandleUnmatchedDatadog(c *echo.Context) error {
	unmatched, err := h.buildUnmatchedSourceAccountsPage(c, unmatchedSourceAccountOptions{
		Title:              "Unlinked Datadog Accounts",
		ConnectorName:      "Datadog",
		SourceKind:         "datadog",
		EmptyStateHref:     "/settings/connectors?open=datadog",
		SyncedEmptyState:   "No unlinked Datadog accounts.",
		FilteredEmptyState: "No unlinked Datadog accounts match the current search.",
		IsConfigured: func(snap ConnectorSnapshot) bool {
			return snap.DatadogConfigured
		},
		IsEnabled: func(snap ConnectorSnapshot) bool {
			return snap.DatadogEnabled
		},
		ResolveSourceName: func(c *echo.Context, snap ConnectorSnapshot) (string, error) {
			site := routeParamOrWildcard(c, "site")
			if site == "" {
				return "", errUnmatchedSourceAccountNotFound
			}
			if site != snap.Datadog.Site {
				return "", unmatchedSourceNameError("unknown site")
			}
			return site, nil
		},
	})
	if err != nil {
		return h.renderUnmatchedSourceAccountsError(c, err)
	}

	data := viewmodels.UnmatchedDatadogViewData{
		UnmatchedSourceAccountsPageData: unmatched.PageData,
	}

	return h.RenderComponent(c, views.UnmatchedDatadogPage(data))
}

func connectorUnavailableMessage(connectorName string, configured, enabled bool) string {
	connectorName = strings.TrimSpace(connectorName)
	if connectorName == "" {
		connectorName = "Connector"
	}
	if configured && !enabled {
		return connectorName + " sync is disabled. Enable it in Connectors."
	}
	return connectorName + " is not configured yet. Add credentials in Connectors."
}

// HandleCreateLink creates an identity link between an identity and source account.
func (h *Handlers) HandleCreateLink(c *echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	identityID, accountID, reason, err := parseCreateLinkForm(c)
	if err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}
	_, err = h.Q.UpsertIdentityAccountLink(c.Request().Context(), gen.UpsertIdentityAccountLinkParams{
		IdentityID: identityID,
		AccountID:  accountID,
		LinkReason: reason,
		Confidence: 1.0,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	redirect := c.Request().Header.Get("Referer")
	if redirect == "" {
		snap, err := h.LoadConnectorSnapshot(c.Request().Context())
		if err == nil && snap.GitHub.Org != "" {
			redirect = fmt.Sprintf("/accounts/unlinked/github/%s", snap.GitHub.Org)
		} else {
			redirect = "/settings/connectors?open=github"
		}
	}
	return c.Redirect(http.StatusSeeOther, redirect)
}

func parseCreateLinkForm(c *echo.Context) (identityID int64, accountID int64, reason string, err error) {
	identityRaw := strings.TrimSpace(c.FormValue("identity_id"))
	identityID, err = strconv.ParseInt(identityRaw, 10, 64)
	if err != nil {
		return 0, 0, "", errors.New("invalid identity_id")
	}

	accountRaw := strings.TrimSpace(c.FormValue("account_id"))
	accountID, err = strconv.ParseInt(accountRaw, 10, 64)
	if err != nil {
		return 0, 0, "", errors.New("invalid account_id")
	}

	reason = strings.TrimSpace(c.FormValue("reason"))
	if reason == "" {
		reason = "manual"
	}

	return identityID, accountID, reason, nil
}

// HandleOktaAccountAccessTree handles the access tree API endpoint.
func (h *Handlers) HandleOktaAccountAccessTree(c *echo.Context) error {
	addVary(c, "HX-Request")

	renderAccessTreeError := func(status int, message string) error {
		if isHX(c) {
			return h.RenderComponent(c, views.AccessGraphError(message))
		}
		return c.String(status, message)
	}

	id, err := strconv.ParseInt(strings.TrimSpace(c.Param("id")), 10, 64)
	if err != nil || id <= 0 {
		return renderAccessTreeError(http.StatusBadRequest, "invalid okta account id")
	}

	ctx := c.Request().Context()
	_, err = h.Q.GetOktaAccount(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return renderAccessTreeError(http.StatusNotFound, "okta account not found")
		}
		return renderAccessTreeError(http.StatusInternalServerError, "internal error")
	}
	currentIdentityID, linkedAccounts, err := h.linkedAccountsForOktaAccount(ctx, id)
	if err != nil {
		return renderAccessTreeError(http.StatusInternalServerError, "internal error")
	}

	nodeID := strings.TrimSpace(c.QueryParam("node"))
	if nodeID == "" {
		nodeID = "root"
	}

	encodeNodePart := func(v string) string {
		return url.PathEscape(v)
	}
	decodeNodePart := func(v string) (string, error) {
		return url.PathUnescape(v)
	}

	var nodes []viewmodels.AccessTreeNode

	switch {
	case nodeID == "root":
		nodes = []viewmodels.AccessTreeNode{
			{ID: "connector:okta", Label: "Okta", HasChildren: true},
			{ID: "connector:github", Label: "GitHub", HasChildren: true},
			{ID: "connector:datadog", Label: "Datadog", HasChildren: true},
			{ID: "connector:aws", Label: "AWS Identity Center", HasChildren: true},
		}
	case nodeID == "connector:okta":
		nodes = []viewmodels.AccessTreeNode{{ID: "okta_apps", Label: "Apps", HasChildren: true}}
	case nodeID == "okta_apps":
		assignments, err := h.Q.ListOktaAppAssignmentsForOktaAccount(ctx, id)
		if err != nil {
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}
		for _, assignment := range assignments {
			externalID := strings.TrimSpace(assignment.OktaAppExternalID)
			if externalID == "" {
				continue
			}
			label := strings.TrimSpace(assignment.AppLabel)
			if label == "" {
				label = externalID
			}
			subLabel := strings.TrimSpace(assignment.AppName)

			var badges []string
			if status := strings.TrimSpace(assignment.AppStatus); status != "" {
				badges = append(badges, status)
			}
			if mode := strings.TrimSpace(assignment.AppSignOnMode); mode != "" {
				badges = append(badges, mode)
			}

			scope := strings.ToUpper(strings.TrimSpace(assignment.Scope))
			switch scope {
			case "USER":
				badges = append(badges, "Direct")
			case "GROUP":
				badges = append(badges, "Group")
			case "":
			default:
				badges = append(badges, "Unknown")
			}

			href := IntegratedAppHref(assignment.IntegrationKind)
			if href == "" {
				href = "/assigned-apps/" + externalID
			}

			nodes = append(nodes, viewmodels.AccessTreeNode{
				ID:          "app:" + externalID,
				Label:       label,
				SubLabel:    subLabel,
				Badges:      badges,
				HasChildren: true,
				Href:        href,
			})
		}
		if len(nodes) == 0 {
			nodes = []viewmodels.AccessTreeNode{{
				ID:          "apps-empty",
				Label:       "No assigned apps found.",
				HasChildren: false,
			}}
		}
	case nodeID == "connector:github" || nodeID == "connector:datadog" || nodeID == "connector:aws":
		sourceKind := strings.TrimSpace(strings.TrimPrefix(nodeID, "connector:"))
		if sourceKind == "" {
			return renderAccessTreeError(http.StatusBadRequest, "invalid connector node")
		}

		sourceCounts := make(map[string]int)
		for _, app := range linkedAccounts {
			if strings.EqualFold(strings.TrimSpace(app.SourceKind), sourceKind) {
				sourceCounts[app.SourceName]++
			}
		}

		sourceNames := make([]string, 0, len(sourceCounts))
		for name := range sourceCounts {
			sourceNames = append(sourceNames, name)
		}
		sort.Strings(sourceNames)

		for _, name := range sourceNames {
			count := sourceCounts[name]
			badge := "1 account"
			if count != 1 {
				badge = fmt.Sprintf("%d accounts", count)
			}
			label := strings.TrimSpace(name)
			if label == "" {
				label = "(unknown)"
			}
			nodes = append(nodes, viewmodels.AccessTreeNode{
				ID:          "inst:" + strings.ToLower(sourceKind) + ":" + encodeNodePart(name),
				Label:       label,
				Badges:      []string{badge},
				HasChildren: true,
			})
		}
		if len(nodes) == 0 {
			nodes = []viewmodels.AccessTreeNode{{
				ID:          sourceKind + "-empty",
				Label:       "No linked accounts found.",
				HasChildren: false,
			}}
		}
	case strings.HasPrefix(nodeID, "inst:"):
		raw := strings.TrimSpace(strings.TrimPrefix(nodeID, "inst:"))
		parts := strings.SplitN(raw, ":", 2)
		if len(parts) != 2 {
			return renderAccessTreeError(http.StatusBadRequest, "invalid instance node")
		}
		sourceKind := strings.TrimSpace(parts[0])
		sourceName, err := decodeNodePart(parts[1])
		if err != nil {
			return renderAccessTreeError(http.StatusBadRequest, "invalid instance node")
		}
		if sourceKind == "" || sourceName == "" {
			return renderAccessTreeError(http.StatusBadRequest, "invalid instance node")
		}

		for _, app := range linkedAccounts {
			if !strings.EqualFold(strings.TrimSpace(app.SourceKind), sourceKind) {
				continue
			}
			if app.SourceName != sourceName {
				continue
			}
			label := strings.TrimSpace(app.DisplayName)
			if label == "" {
				label = strings.TrimSpace(app.ExternalID)
			}
			if label == "" {
				label = "(unknown)"
			}

			subLabel := strings.TrimSpace(app.Email)
			if subLabel == "" {
				subLabel = strings.TrimSpace(app.ExternalID)
			}

			nodes = append(nodes, viewmodels.AccessTreeNode{
				ID:          "account:" + strconv.FormatInt(app.ID, 10),
				Label:       label,
				SubLabel:    subLabel,
				HasChildren: true,
			})
		}
		if len(nodes) == 0 {
			nodes = []viewmodels.AccessTreeNode{{ID: "inst-empty:" + raw, Label: "No linked accounts found.", HasChildren: false}}
		}
	case strings.HasPrefix(nodeID, "account:"):
		rawID := strings.TrimSpace(strings.TrimPrefix(nodeID, "account:"))
		accountID, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil || accountID <= 0 {
			return renderAccessTreeError(http.StatusBadRequest, "invalid account node")
		}

		link, err := h.Q.GetIdentityAccountLinkByAccountID(ctx, accountID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return renderAccessTreeError(http.StatusNotFound, "account not linked")
			}
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}
		if currentIdentityID == 0 || link.IdentityID != currentIdentityID {
			return renderAccessTreeError(http.StatusNotFound, "account not linked")
		}

		account, err := h.Q.GetSourceAccount(ctx, accountID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return renderAccessTreeError(http.StatusNotFound, "account not found")
			}
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}

		ents, err := h.Q.ListEntitlementsForAccount(ctx, accountID)
		if err != nil {
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}

		type resourceGroup struct {
			resourceKind string
			externalID   string
			label        string
			count        int
		}

		resourceGroups := make(map[string]*resourceGroup)
		var unmapped []viewmodels.AccessTreeNode

		for _, ent := range ents {
			resourceKind, externalID, ok := accessgraph.ParseCanonicalResourceRef(ent.Resource)
			if !ok {
				label := strings.TrimSpace(ent.Resource)
				if label == "" {
					label = strings.TrimSpace(ent.Kind)
				}
				if label == "" {
					label = "(unknown)"
				}
				var badges []string
				if kind := strings.TrimSpace(ent.Kind); kind != "" {
					badges = append(badges, kind)
				}
				if perm := strings.TrimSpace(ent.Permission); perm != "" {
					badges = append(badges, perm)
				}
				unmapped = append(unmapped, viewmodels.AccessTreeNode{
					ID:          "ent:" + strconv.FormatInt(ent.ID, 10),
					Label:       label,
					Badges:      badges,
					HasChildren: false,
				})
				continue
			}

			key := resourceKind + "\x00" + externalID
			if existing := resourceGroups[key]; existing != nil {
				existing.count++
				continue
			}

			label := accessgraph.DisplayResourceLabel(ent.Resource, ent.RawJson)
			if strings.TrimSpace(label) == "" {
				label = externalID
			}
			resourceGroups[key] = &resourceGroup{
				resourceKind: resourceKind,
				externalID:   externalID,
				label:        label,
				count:        1,
			}
		}

		keys := make([]string, 0, len(resourceGroups))
		for key := range resourceGroups {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			left := resourceGroups[keys[i]]
			right := resourceGroups[keys[j]]
			if left.resourceKind == right.resourceKind {
				return strings.ToLower(left.label) < strings.ToLower(right.label)
			}
			return left.resourceKind < right.resourceKind
		})

		for _, key := range keys {
			group := resourceGroups[key]
			if group == nil {
				continue
			}
			var badges []string
			if kindLabel := humanizeResourceKind(group.resourceKind); kindLabel != "" {
				badges = append(badges, kindLabel)
			}
			if group.count > 1 {
				badges = append(badges, fmt.Sprintf("%d entitlements", group.count))
			}

			subLabel := ""
			if group.label != group.externalID {
				subLabel = group.externalID
			}

			nodes = append(nodes, viewmodels.AccessTreeNode{
				ID:          "res:" + strconv.FormatInt(accountID, 10) + ":" + group.resourceKind + ":" + encodeNodePart(group.externalID),
				Label:       group.label,
				SubLabel:    subLabel,
				Badges:      badges,
				HasChildren: true,
				Href:        accessgraph.BuildResourceHref(account.SourceKind, account.SourceName, group.resourceKind, group.externalID),
			})
		}

		if len(unmapped) > 0 {
			sort.Slice(unmapped, func(i, j int) bool {
				return strings.ToLower(unmapped[i].Label) < strings.ToLower(unmapped[j].Label)
			})
			nodes = append(nodes, unmapped...)
		}

		if len(nodes) == 0 {
			nodes = []viewmodels.AccessTreeNode{{ID: "entitlements-empty:" + rawID, Label: "No entitlements found.", HasChildren: false}}
		}
	case strings.HasPrefix(nodeID, "res:"):
		raw := strings.TrimSpace(strings.TrimPrefix(nodeID, "res:"))
		parts := strings.SplitN(raw, ":", 3)
		if len(parts) != 3 {
			return renderAccessTreeError(http.StatusBadRequest, "invalid resource node")
		}
		accountID, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || accountID <= 0 {
			return renderAccessTreeError(http.StatusBadRequest, "invalid resource node")
		}
		resourceKind := strings.TrimSpace(parts[1])
		externalID, err := decodeNodePart(parts[2])
		if err != nil || resourceKind == "" || externalID == "" {
			return renderAccessTreeError(http.StatusBadRequest, "invalid resource node")
		}

		link, err := h.Q.GetIdentityAccountLinkByAccountID(ctx, accountID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return renderAccessTreeError(http.StatusNotFound, "account not linked")
			}
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}
		if currentIdentityID == 0 || link.IdentityID != currentIdentityID {
			return renderAccessTreeError(http.StatusNotFound, "account not linked")
		}

		ents, err := h.Q.ListEntitlementsForAccount(ctx, accountID)
		if err != nil {
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}

		for _, ent := range ents {
			kind, eid, ok := accessgraph.ParseCanonicalResourceRef(ent.Resource)
			if !ok {
				continue
			}
			if kind != resourceKind || eid != externalID {
				continue
			}
			label := strings.TrimSpace(ent.Permission)
			if label == "" {
				label = "(no permission)"
			}
			subLabel := strings.TrimSpace(ent.Kind)
			if subLabel == "" {
				subLabel = strings.TrimSpace(ent.Resource)
			}
			nodes = append(nodes, viewmodels.AccessTreeNode{
				ID:          "ent:" + strconv.FormatInt(ent.ID, 10),
				Label:       label,
				SubLabel:    subLabel,
				HasChildren: false,
			})
		}

		if len(nodes) == 0 {
			nodes = []viewmodels.AccessTreeNode{{ID: "perms-empty:" + raw, Label: "No permissions found.", HasChildren: false}}
		} else {
			sort.Slice(nodes, func(i, j int) bool {
				return strings.ToLower(nodes[i].Label) < strings.ToLower(nodes[j].Label)
			})
		}
	case strings.HasPrefix(nodeID, "app:"):
		externalID := strings.TrimPrefix(nodeID, "app:")
		externalID = strings.TrimSpace(externalID)
		if externalID == "" {
			return renderAccessTreeError(http.StatusBadRequest, "invalid app node")
		}
		_, err := h.Q.GetOktaAppAssignmentForOktaAccountByOktaAppExternalID(ctx, gen.GetOktaAppAssignmentForOktaAccountByOktaAppExternalIDParams{
			OktaAccountID:     id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return renderAccessTreeError(http.StatusNotFound, "app assignment not found")
			}
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}
		nodes = []viewmodels.AccessTreeNode{
			{ID: "appattrs:" + externalID, Label: "Attributes", HasChildren: true},
			{ID: "appgroups:" + externalID, Label: "Groups", HasChildren: true},
			{ID: "appperms:" + externalID, Label: "Permissions", HasChildren: true},
		}
	case strings.HasPrefix(nodeID, "appattrs:"):
		externalID := strings.TrimSpace(strings.TrimPrefix(nodeID, "appattrs:"))
		if externalID == "" {
			return renderAccessTreeError(http.StatusBadRequest, "invalid app attributes node")
		}
		assignment, err := h.Q.GetOktaAppAssignmentForOktaAccountByOktaAppExternalID(ctx, gen.GetOktaAppAssignmentForOktaAccountByOktaAppExternalIDParams{
			OktaAccountID:     id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return renderAccessTreeError(http.StatusNotFound, "app assignment not found")
			}
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}

		assignedVia := "Unknown"
		scope := strings.ToUpper(strings.TrimSpace(assignment.Scope))
		switch scope {
		case "USER":
			assignedVia = "Direct"
		case "GROUP":
			assignedVia = "Group"
		}

		nodes = []viewmodels.AccessTreeNode{
			{ID: "attr:status:" + externalID, Label: "Status", SubLabel: strings.TrimSpace(assignment.AppStatus), HasChildren: false},
			{ID: "attr:signon:" + externalID, Label: "Sign-on", SubLabel: strings.TrimSpace(assignment.AppSignOnMode), HasChildren: false},
			{ID: "attr:assignedvia:" + externalID, Label: "Assigned via", SubLabel: assignedVia, HasChildren: false},
			{ID: "attr:externalid:" + externalID, Label: "Okta app external id", SubLabel: externalID, HasChildren: false},
		}

		if kind := strings.TrimSpace(assignment.IntegrationKind); kind != "" {
			label := strings.TrimSpace(ConnectorDisplayName(kind))
			if label == "" {
				label = kind
			}
			nodes = append(nodes, viewmodels.AccessTreeNode{
				ID:          "attr:integration:" + externalID,
				Label:       "Integration",
				SubLabel:    label,
				HasChildren: false,
				Href:        IntegratedAppHref(kind),
			})
		}
	case strings.HasPrefix(nodeID, "appgroups:"):
		externalID := strings.TrimSpace(strings.TrimPrefix(nodeID, "appgroups:"))
		if externalID == "" {
			return renderAccessTreeError(http.StatusBadRequest, "invalid app groups node")
		}
		_, err := h.Q.GetOktaAppAssignmentForOktaAccountByOktaAppExternalID(ctx, gen.GetOktaAppAssignmentForOktaAccountByOktaAppExternalIDParams{
			OktaAccountID:     id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return renderAccessTreeError(http.StatusNotFound, "app assignment not found")
			}
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}

		groups, err := h.Q.ListOktaAppGrantingGroupsForOktaAccountByOktaAppExternalID(ctx, gen.ListOktaAppGrantingGroupsForOktaAccountByOktaAppExternalIDParams{
			OktaAccountID:     id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}
		if len(groups) == 0 {
			nodes = []viewmodels.AccessTreeNode{{ID: "groups-empty:" + externalID, Label: "No granting groups found.", HasChildren: false}}
		} else {
			for _, group := range groups {
				name := strings.TrimSpace(group.OktaGroupName)
				ext := strings.TrimSpace(group.OktaGroupExternalID)
				if name == "" {
					name = ext
				}
				nodes = append(nodes, viewmodels.AccessTreeNode{
					ID:          "group:" + ext,
					Label:       name,
					SubLabel:    ext,
					HasChildren: false,
				})
			}
		}
	case strings.HasPrefix(nodeID, "appperms:"):
		externalID := strings.TrimSpace(strings.TrimPrefix(nodeID, "appperms:"))
		if externalID == "" {
			return renderAccessTreeError(http.StatusBadRequest, "invalid app permissions node")
		}
		assignment, err := h.Q.GetOktaAppAssignmentForOktaAccountByOktaAppExternalID(ctx, gen.GetOktaAppAssignmentForOktaAccountByOktaAppExternalIDParams{
			OktaAccountID:     id,
			OktaAppExternalID: externalID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return renderAccessTreeError(http.StatusNotFound, "app assignment not found")
			}
			return renderAccessTreeError(http.StatusInternalServerError, "internal error")
		}

		perms := SummarizeProfilePermissions(assignment.ProfileJson)
		if len(perms) == 0 {
			nodes = []viewmodels.AccessTreeNode{{ID: "perms-empty:" + externalID, Label: "No permissions found.", HasChildren: false}}
		} else {
			for i, perm := range perms {
				text := strings.TrimSpace(perm.Text)
				if text == "" {
					continue
				}
				nodes = append(nodes, viewmodels.AccessTreeNode{
					ID:          "perm:" + externalID + ":" + strconv.Itoa(i),
					Label:       text,
					HasChildren: false,
				})
			}
			if len(nodes) == 0 {
				nodes = []viewmodels.AccessTreeNode{{ID: "perms-empty:" + externalID, Label: "No permissions found.", HasChildren: false}}
			}
		}
	default:
		return renderAccessTreeError(http.StatusBadRequest, "unknown node")
	}

	return h.RenderComponent(c, views.AccessGraphChildren(id, nodes))
}

func datadogUserStatus(accountID int64, externalID string, rawJSON []byte) string {
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rawJSON, &payload); err != nil {
		slog.Warn("datadog user raw_json parse failed", "account_id", accountID, "external_id", strings.TrimSpace(externalID), "err", err)
		return ""
	}
	return strings.TrimSpace(payload.Status)
}

func (h *Handlers) linkedAccountsForOktaAccount(ctx context.Context, oktaAccountID int64) (int64, []gen.Account, error) {
	link, err := h.Q.GetIdentityAccountLinkByAccountID(ctx, oktaAccountID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil, nil
		}
		return 0, nil, err
	}

	identityID := link.IdentityID
	linked, err := h.Q.ListLinkedAccountsForIdentity(ctx, identityID)
	if err != nil {
		return 0, nil, err
	}

	filtered := make([]gen.Account, 0, len(linked))
	for _, account := range linked {
		if account.ID == oktaAccountID {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(account.SourceKind), "okta") {
			continue
		}
		filtered = append(filtered, account)
	}

	return identityID, filtered, nil
}
