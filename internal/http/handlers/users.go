package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/accessgraph"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
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

	queryState := querystate.ParseBasicListQuery("/accounts/okta", c.Request().URL.Query(), querystate.BasicListOptions{
		StateAliases:   []string{"status"},
		NormalizeState: normalizeActiveInactiveState,
	})
	page := queryState.Page

	totalCount, err := h.Q.CountOktaAccountsByQueryAndState(ctx, gen.CountOktaAccountsByQueryAndStateParams{
		Query: queryState.Q,
		State: queryState.State,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination := newPaginatedListState(totalCount, page, perPage)
	users, err := h.Q.ListOktaAccountsPageByQueryAndState(ctx, gen.ListOktaAccountsPageByQueryAndStateParams{
		Query:      queryState.Q,
		State:      queryState.State,
		PageLimit:  int32(perPage),
		PageOffset: int32(pagination.Offset()),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	emptyState := "No Okta accounts synced yet."
	if queryState.HasFilters() {
		emptyState = "No Okta accounts match the current search."
	}

	data := viewmodels.OktaAccountsViewData{
		PaginatedListPageData: pagination.PageData(layout, len(users), emptyState, ""),
		Users:                 users,
		Query:                 queryState,
		HasUsers:              len(users) > 0,
	}

	return h.renderListWithHX(c, "okta-accounts-results", views.OktaAccountsPageResults(data), views.OktaAccountsPage(data))
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
	layout, stateView, err := h.LayoutData(ctx, c, "Okta Account")
	if err != nil {
		return h.RenderError(c, err)
	}
	user, err := h.Q.GetOktaAccount(ctx, id)
	if err != nil {
		return RenderNotFound(c)
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
	groupBadges := make([]viewmodels.OktaGroupBadge, 0, len(userGroups))
	for _, group := range userGroups {
		name := strings.TrimSpace(group.Name)
		if name == "" {
			name = strings.TrimSpace(group.ExternalID)
		}
		if name == "" {
			continue
		}
		groupNames[group.ID] = name
		groupBadges = append(groupBadges, viewmodels.OktaGroupBadge{
			Name:       name,
			ExternalID: strings.TrimSpace(group.ExternalID),
		})
	}
	sort.Slice(groupBadges, func(i, j int) bool {
		return strings.ToLower(groupBadges[i].Name) < strings.ToLower(groupBadges[j].Name)
	})

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

	oktaAssignments := make([]viewmodels.OktaAssignmentView, 0, len(assignments))
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
			Attributes:  SummarizeProfileAttributes(assignment.ProfileJson),
		})
	}

	configuredSourceKinds, configuredSourceNames := configuredIdentitySourcePairsFromView(stateView)
	identityHref, identityName := h.identityBacklinkForAccount(ctx, user.ID, configuredSourceKinds, configuredSourceNames)

	data := viewmodels.SourceAccountShowViewData{
		Layout:         layout,
		Account:        user,
		LastLoginAt:    calendarDateWithRelativeDisplay(user.LastLoginAt),
		LastObservedAt: calendarDateWithRelativeDisplay(user.LastObservedAt),
		IdentityHref:   identityHref,
		IdentityName:   identityName,
		OktaSection: &viewmodels.OktaInspectorSection{
			Groups:          groupBadges,
			Assignments:     oktaAssignments,
			AssignmentCount: len(oktaAssignments),
		},
	}

	return h.RenderComponent(c, views.OktaAccountShowPage(data))
}

// identityBacklinkForAccount resolves the identity that owns the given source
// account so the inspector page can offer a "Part of identity X" upward link.
// Returns empty strings if the account is not linked or the lookup fails — the
// caller renders nothing in that case rather than surfacing an error.
func (h *Handlers) identityBacklinkForAccount(ctx context.Context, accountID int64, configuredSourceKinds, configuredSourceNames []string) (string, string) {
	link, err := h.Q.GetIdentityAccountLinkByAccountID(ctx, accountID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("identity backlink lookup failed", "account_id", accountID, "err", err)
		}
		return "", ""
	}
	summary, err := h.Q.GetIdentitySummaryByID(ctx, gen.GetIdentitySummaryByIDParams{
		ID:                    link.IdentityID,
		ConfiguredSourceKinds: configuredSourceKinds,
		ConfiguredSourceNames: configuredSourceNames,
	})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("identity backlink summary lookup failed", "account_id", accountID, "identity_id", link.IdentityID, "err", err)
		}
		return "/identities/" + strconv.FormatInt(link.IdentityID, 10), ""
	}
	return "/identities/" + strconv.FormatInt(summary.ID, 10),
		identityNamePrimary(summary.DisplayName, summary.PrimaryEmail, summary.ID)
}

// HandleGitHubUsers renders the GitHub users page.
func (h *Handlers) HandleGitHubUsers(c *echo.Context) error {
	inventory, err := h.buildSourceAccountInventoryPage(c, sourceAccountInventoryOptions{
		Title:              "GitHub Users",
		BasePath:           "/accounts/github",
		ConnectorName:      "GitHub",
		ConnectorKind:      "github",
		EmptyStateHref:     "/settings/connectors?open=github",
		SyncedEmptyState:   "No GitHub users synced yet.",
		FilteredEmptyState: "No GitHub users match the current search.",
		Count: func(ctx context.Context, sourceName, query string) (int64, error) {
			return h.Q.CountGitHubUsersBySourceAndQuery(ctx, gen.CountGitHubUsersBySourceAndQueryParams{
				SourceKind: "github",
				SourceName: sourceName,
				Query:      query,
			})
		},
		List: func(ctx context.Context, sourceName, query string, offset, limit int) ([]sourceAccountInventoryAccount, error) {
			users, err := h.Q.ListGitHubUsersPageBySourceAndQuery(ctx, gen.ListGitHubUsersPageBySourceAndQueryParams{
				SourceKind: "github",
				SourceName: sourceName,
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

	return h.renderListWithHX(c, "github-users-results", views.GitHubUsersPageResults(data), views.GitHubUsersPage(data))
}

// HandleDatadogUsers renders the Datadog users page.
func (h *Handlers) HandleDatadogUsers(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Datadog Users")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	queryState := querystate.ParseBasicListQuery("/accounts/datadog", c.Request().URL.Query(), querystate.BasicListOptions{
		NormalizeState: normalizeActiveInactiveState,
	})
	page := queryState.Page
	unavailablePagination := newPaginatedListState(0, page, perPage)
	datadog := stateView.Datadog()
	sourceName := datadog.SourceName()

	if !datadog.Configured() || !datadog.Enabled() || sourceName == "" {
		message := connectorUnavailableMessage("Datadog", datadog.Configured(), datadog.Enabled())
		data := viewmodels.DatadogUsersViewData{
			PaginatedListPageData: unavailablePagination.PageData(layout, 0, message, "/settings/connectors?open=datadog"),
			Query:                 queryState,
			HasUsers:              false,
		}
		return h.renderListWithHX(c, "datadog-users-results", views.DatadogUsersPageResults(data), views.DatadogUsersPage(data))
	}

	totalCount, err := h.Q.CountSourceAccountsBySourceAndQueryAndState(ctx, gen.CountSourceAccountsBySourceAndQueryAndStateParams{
		SourceKind:     "datadog",
		SourceName:     sourceName,
		EntityCategory: registry.EntityCategoryUser,
		Query:          queryState.Q,
		State:          queryState.State,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	pagination := newPaginatedListState(totalCount, page, perPage)
	users, err := h.Q.ListSourceAccountsPageBySourceAndQueryAndState(ctx, gen.ListSourceAccountsPageBySourceAndQueryAndStateParams{
		SourceKind:     "datadog",
		SourceName:     sourceName,
		EntityCategory: registry.EntityCategoryUser,
		Query:          queryState.Q,
		State:          queryState.State,
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
	if queryState.HasFilters() {
		emptyState = "No Datadog users match the current search."
	}

	data := viewmodels.DatadogUsersViewData{
		PaginatedListPageData: pagination.PageData(layout, len(items), emptyState, "/settings/connectors?open=datadog"),
		Users:                 items,
		Query:                 queryState,
		HasUsers:              len(items) > 0,
	}
	return h.renderListWithHX(c, "datadog-users-results", views.DatadogUsersPageResults(data), views.DatadogUsersPage(data))
}

// HandleGitHubAccountsNeedingAnchor renders the GitHub accounts that still need an authoritative identity anchor.
func (h *Handlers) HandleGitHubAccountsNeedingAnchor(c *echo.Context) error {
	needsAnchor, err := h.buildSourceAccountsNeedingAnchorPage(c, sourceAccountsNeedingAnchorOptions{
		Title:              "GitHub Accounts Needing Anchor",
		BasePath:           "/accounts/needs-anchor/github/" + routeParamOrWildcard(c, "org"),
		ConnectorName:      "GitHub",
		ConnectorKind:      "github",
		SourceKind:         "github",
		EntityCategory:     registry.EntityCategoryUser,
		EmptyStateHref:     "/settings/connectors?open=github",
		SyncedEmptyState:   "No GitHub accounts need an anchor.",
		FilteredEmptyState: "No GitHub accounts needing an anchor match the current search.",
		ResolveSourceName: func(c *echo.Context, configuredSourceName string) (string, error) {
			org := routeParamOrWildcard(c, "org")
			if org == "" {
				return "", errSourceAccountNeedsAnchorNotFound
			}
			if org != configuredSourceName {
				return "", sourceAccountNeedsAnchorNameError("unknown org")
			}
			return org, nil
		},
	})
	if err != nil {
		return h.renderSourceAccountsNeedingAnchorError(c, err)
	}

	data := viewmodels.GitHubAccountsNeedingAnchorViewData{
		SourceAccountsNeedingAnchorPageData: needsAnchor.PageData,
	}

	return h.renderListWithHX(c, "github-needs-anchor-results", views.GitHubAccountsNeedingAnchorPageResults(data), views.GitHubAccountsNeedingAnchorPage(data))
}

// HandleDatadogAccountsNeedingAnchor renders the Datadog accounts that still need an authoritative identity anchor.
func (h *Handlers) HandleDatadogAccountsNeedingAnchor(c *echo.Context) error {
	needsAnchor, err := h.buildSourceAccountsNeedingAnchorPage(c, sourceAccountsNeedingAnchorOptions{
		Title:              "Datadog Accounts Needing Anchor",
		BasePath:           "/accounts/needs-anchor/datadog/" + routeParamOrWildcard(c, "site"),
		ConnectorName:      "Datadog",
		ConnectorKind:      "datadog",
		SourceKind:         "datadog",
		EntityCategory:     registry.EntityCategoryUser,
		EmptyStateHref:     "/settings/connectors?open=datadog",
		SyncedEmptyState:   "No Datadog accounts need an anchor.",
		FilteredEmptyState: "No Datadog accounts needing an anchor match the current search.",
		ResolveSourceName: func(c *echo.Context, configuredSourceName string) (string, error) {
			site := routeParamOrWildcard(c, "site")
			if site == "" {
				return "", errSourceAccountNeedsAnchorNotFound
			}
			if site != configuredSourceName {
				return "", sourceAccountNeedsAnchorNameError("unknown site")
			}
			return site, nil
		},
	})
	if err != nil {
		return h.renderSourceAccountsNeedingAnchorError(c, err)
	}

	data := viewmodels.DatadogAccountsNeedingAnchorViewData{
		SourceAccountsNeedingAnchorPageData: needsAnchor.PageData,
	}

	return h.renderListWithHX(c, "datadog-needs-anchor-results", views.DatadogAccountsNeedingAnchorPageResults(data), views.DatadogAccountsNeedingAnchorPage(data))
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
		stateView, err := h.LoadConnectorStateView(c.Request().Context())
		if err == nil && stateView.SourceName("github") != "" {
			redirect = fmt.Sprintf("/accounts/needs-anchor/github/%s", stateView.SourceName("github"))
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
