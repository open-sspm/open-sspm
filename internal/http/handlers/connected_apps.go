package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/googleworkspace"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

const (
	connectedAppAssetKindGoogle = "google_oauth_client"
	connectedAppsPerPage        = 20
)

type connectedAppShowOptions struct {
	alert            *viewmodels.ConnectedAppsAlert
	ownerEmailInput  string
	reviewStateInput string
	ticketRefInput   string
	notesInput       string
}

type googleGrantRaw struct {
	UserKey  string `json:"user_key"`
	ClientID string `json:"client_id"`
}

func (h *Handlers) HandleConnectedApps(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "OAuth Apps")
	if err != nil {
		return h.RenderError(c, err)
	}

	query := strings.TrimSpace(c.QueryParam("q"))
	reviewState := normalizeConnectedAppReviewState(c.QueryParam("review_state"), true)
	page := parsePageParam(c)
	pagination := newPaginatedListState(0, page, connectedAppsPerPage)

	data := viewmodels.ConnectedAppsViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No OAuth apps match the current filters.", ""),
		Query:                 query,
		ReviewState:           reviewState,
	}

	render := func() error {
		if isHX(c) && isHXTarget(c, "connected-apps-results") {
			return h.RenderComponent(c, views.ConnectedAppsPageResults(data))
		}
		return h.RenderComponent(c, views.ConnectedAppsPage(data))
	}

	google := stateView.GoogleWorkspace()
	sourceName := google.SourceName()
	if !google.Configured() || !google.Enabled() || sourceName == "" {
		data.PaginatedListPageData.EmptyStateMsg = connectorUnavailableMessage("Google Workspace", google.Configured(), google.Enabled())
		data.ReviewCounts = buildConnectedAppReviewCounts(nil, query, reviewState)
		return render()
	}

	totalCount, err := h.Q.CountConnectedAppsBySourceAndQueryAndReviewState(ctx, gen.CountConnectedAppsBySourceAndQueryAndReviewStateParams{
		SourceKind:  configstore.KindGoogleWorkspace,
		SourceName:  sourceName,
		AssetKind:   connectedAppAssetKindGoogle,
		ReviewState: reviewState,
		Query:       query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	countRows, err := h.Q.CountConnectedAppsGroupedByReviewState(ctx, gen.CountConnectedAppsGroupedByReviewStateParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		AssetKind:  connectedAppAssetKindGoogle,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	data.ReviewCounts = buildConnectedAppReviewCounts(countRows, query, reviewState)

	pagination = newPaginatedListState(totalCount, page, connectedAppsPerPage)
	rows, err := h.Q.ListConnectedAppsPageBySourceAndQueryAndReviewState(ctx, gen.ListConnectedAppsPageBySourceAndQueryAndReviewStateParams{
		SourceKind:  configstore.KindGoogleWorkspace,
		SourceName:  sourceName,
		AssetKind:   connectedAppAssetKindGoogle,
		ReviewState: reviewState,
		Query:       query,
		PageLimit:   int32(connectedAppsPerPage),
		PageOffset:  int32(pagination.Offset()),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.ConnectedAppListItem, 0, len(rows))
	for _, row := range rows {
		displayName := strings.TrimSpace(row.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(row.ExternalID)
		}
		reviewOwner := strings.TrimSpace(row.ReviewOwnerDisplayName)
		reviewOwnerEmail := strings.TrimSpace(row.ReviewOwnerPrimaryEmail)
		if reviewOwner == "" {
			reviewOwner = reviewOwnerEmail
		}
		if reviewOwner == "" {
			reviewOwner = "—"
		}
		confidence, confidenceReason := connectedAppConfidence(row.OwnerCount, row.GrantCount, row.DiscoverySourceCount, row.ReviewOwnerIdentityID > 0)
		items = append(items, viewmodels.ConnectedAppListItem{
			ID:                     row.ID,
			DisplayName:            displayName,
			ExternalID:             strings.TrimSpace(row.ExternalID),
			Status:                 fallbackDash(strings.TrimSpace(row.Status)),
			ReviewState:            strings.TrimSpace(row.ReviewState),
			ReviewOwner:            reviewOwner,
			ReviewOwnerEmail:       reviewOwnerEmail,
			LikelyOwnerCount:       int(row.OwnerCount),
			GrantCount:             int(row.GrantCount),
			ActorCount:             row.ActorCount,
			DiscoveryEventCount30d: row.DiscoveryEventCount30d,
			Freshness:              connectedAppFreshness(row.EvidenceLastSeenAt),
			Confidence:             confidence,
			ConfidenceReason:       confidenceReason,
			LastSeenAt:             formatProgrammaticDate(row.EvidenceLastSeenAt),
			TicketRef:              strings.TrimSpace(row.TicketRef),
		})
	}

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No OAuth apps match the current filters.", "")
	data.HasItems = len(items) > 0
	if query == "" && reviewState == "" {
		data.PaginatedListPageData.EmptyStateMsg = "No OAuth apps have been synced from Google Workspace yet."
	}

	return render()
}

func (h *Handlers) HandleConnectedAppShow(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}
	return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{})
}

func (h *Handlers) HandleConnectedAppReviewUpdate(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	summary, err := h.Q.GetConnectedAppSummaryByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return RenderNotFound(c)
	}

	reviewState := normalizeConnectedAppReviewState(c.FormValue("review_state"), false)
	if reviewState == "" {
		return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.ConnectedAppsAlert{
				Title:       "Invalid review state",
				Message:     "Choose a valid disposition before saving the review.",
				Destructive: true,
			},
			ownerEmailInput:  strings.TrimSpace(c.FormValue("owner_email")),
			reviewStateInput: strings.TrimSpace(c.FormValue("review_state")),
			ticketRefInput:   strings.TrimSpace(c.FormValue("ticket_ref")),
			notesInput:       strings.TrimSpace(c.FormValue("notes")),
		})
	}

	ownerEmailInput := auth.NormalizeEmail(c.FormValue("owner_email"))
	var ownerIdentityID pgtype.Int8
	if ownerEmailInput != "" {
		ownerIdentity, err := h.Q.GetPreferredIdentityByPrimaryEmail(ctx, ownerEmailInput)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
					alert: &viewmodels.ConnectedAppsAlert{
						Title:       "Owner not found",
						Message:     "Assign an owner using an existing identity email address.",
						Destructive: true,
					},
					ownerEmailInput:  ownerEmailInput,
					reviewStateInput: reviewState,
					ticketRefInput:   strings.TrimSpace(c.FormValue("ticket_ref")),
					notesInput:       strings.TrimSpace(c.FormValue("notes")),
				})
			}
			return h.RenderError(c, err)
		}
		ownerIdentityID = pgtype.Int8{Int64: ownerIdentity.ID, Valid: true}
	}

	ticketRef := strings.TrimSpace(c.FormValue("ticket_ref"))
	if reviewState == "ticketed" && ticketRef == "" {
		return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.ConnectedAppsAlert{
				Title:       "Ticket reference required",
				Message:     "Enter a ticket reference before marking this app as ticketed.",
				Destructive: true,
			},
			ownerEmailInput:  ownerEmailInput,
			reviewStateInput: reviewState,
			ticketRefInput:   ticketRef,
			notesInput:       strings.TrimSpace(c.FormValue("notes")),
		})
	}

	notes := strings.TrimSpace(c.FormValue("notes"))
	if len(notes) > 4000 {
		return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.ConnectedAppsAlert{
				Title:       "Notes too long",
				Message:     "Keep review notes under 4000 characters.",
				Destructive: true,
			},
			ownerEmailInput:  ownerEmailInput,
			reviewStateInput: reviewState,
			ticketRefInput:   ticketRef,
			notesInput:       notes,
		})
	}

	principal, ok := authn.PrincipalFromContext(c)
	if !ok {
		return c.NoContent(http.StatusForbidden)
	}

	if _, err := h.Q.UpsertConnectedAppGovernance(ctx, gen.UpsertConnectedAppGovernanceParams{
		AppAssetID:          appID,
		ReviewState:         reviewState,
		OwnerIdentityID:     ownerIdentityID,
		TicketRef:           ticketRef,
		Notes:               notes,
		UpdatedByAuthUserID: pgtype.Int8{Int64: principal.UserID, Valid: principal.UserID > 0},
	}); err != nil {
		return h.RenderError(c, err)
	}

	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "OAuth app review saved",
		Description: "Owner assignment and disposition updated.",
	})
	if isHX(c) {
		return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.ConnectedAppsAlert{
				Title:       "OAuth app review saved",
				Message:     "Owner assignment and disposition updated.",
				Destructive: false,
			},
			ownerEmailInput: ownerEmailInput,
		})
	}

	return c.Redirect(http.StatusSeeOther, "/oauth-apps/"+strconv.FormatInt(appID, 10))
}

func (h *Handlers) HandleConnectedAppExport(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	summary, err := h.Q.GetConnectedAppSummaryByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return RenderNotFound(c)
	}

	owners, err := h.Q.ListAppAssetOwnersByAssetID(ctx, appID)
	if err != nil {
		return h.RenderError(c, err)
	}

	now := time.Now().UTC()
	grants, err := h.Q.ListCredentialArtifactsForAssetRef(ctx, gen.ListCredentialArtifactsForAssetRefParams{
		EvaluatedAt:        pgTimestamptz(now),
		SourceKind:         strings.TrimSpace(summary.SourceKind),
		SourceName:         strings.TrimSpace(summary.SourceName),
		AssetRefKind:       strings.TrimSpace(summary.AssetKind),
		AssetRefExternalID: strings.TrimSpace(summary.AssetKind) + ":" + strings.TrimSpace(summary.ExternalID),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	cutoffs := h.discoveryPostureCutoffs(now)
	discoverySources, err := h.Q.ListConnectedAppDiscoverySourcesBySourceAppID(ctx, gen.ListConnectedAppDiscoverySourcesBySourceAppIDParams{
		SourceKind:                strings.TrimSpace(summary.SourceKind),
		SourceName:                strings.TrimSpace(summary.SourceName),
		SourceAppID:               strings.TrimSpace(summary.ExternalID),
		OktaFreshAfter:            cutoffs.OktaFreshAfter,
		EntraFreshAfter:           cutoffs.EntraFreshAfter,
		GoogleWorkspaceFreshAfter: cutoffs.GoogleWorkspaceFreshAfter,
		GithubFreshAfter:          cutoffs.GithubFreshAfter,
		DatadogFreshAfter:         cutoffs.DatadogFreshAfter,
		AwsFreshAfter:             cutoffs.AwsFreshAfter,
		DefaultFreshAfter:         cutoffs.DefaultFreshAfter,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	discoveryEvents, err := h.Q.ListConnectedAppDiscoveryEventsBySourceAppID(ctx, gen.ListConnectedAppDiscoveryEventsBySourceAppIDParams{
		SourceKind:  strings.TrimSpace(summary.SourceKind),
		SourceName:  strings.TrimSpace(summary.SourceName),
		SourceAppID: strings.TrimSpace(summary.ExternalID),
		LimitRows:   100,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	confidence, confidenceReason := connectedAppConfidence(summary.OwnerCount, summary.GrantCount, summary.DiscoverySourceCount, summary.ReviewOwnerIdentityID > 0)
	payload := map[string]any{
		"connected_app": map[string]any{
			"id":                        summary.ID,
			"display_name":              strings.TrimSpace(summary.DisplayName),
			"external_id":               strings.TrimSpace(summary.ExternalID),
			"source_kind":               strings.TrimSpace(summary.SourceKind),
			"source_name":               strings.TrimSpace(summary.SourceName),
			"status":                    strings.TrimSpace(summary.Status),
			"review_state":              strings.TrimSpace(summary.ReviewState),
			"review_owner_display_name": strings.TrimSpace(summary.ReviewOwnerDisplayName),
			"review_owner_email":        strings.TrimSpace(summary.ReviewOwnerPrimaryEmail),
			"review_owner_kind":         strings.TrimSpace(summary.ReviewOwnerKind),
			"ticket_ref":                strings.TrimSpace(summary.TicketRef),
			"notes":                     strings.TrimSpace(summary.Notes),
			"likely_owner_count":        summary.OwnerCount,
			"grant_count":               summary.GrantCount,
			"actor_count":               summary.ActorCount,
			"discovery_source_count":    summary.DiscoverySourceCount,
			"discovery_event_count_30d": summary.DiscoveryEventCount30d,
			"freshness":                 connectedAppFreshness(summary.EvidenceLastSeenAt),
			"confidence":                confidence,
			"confidence_reason":         confidenceReason,
			"last_seen_at":              formatProgrammaticDate(summary.EvidenceLastSeenAt),
		},
		"likely_owners":     owners,
		"grant_inventory":   connectedAppGrantExport(grants),
		"discovery_sources": discoverySources,
		"recent_events":     discoveryEvents,
	}

	fileName := connectedAppExportFilename(strings.TrimSpace(summary.DisplayName), summary.ID)
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
	c.Response().Header().Set(echo.HeaderContentDisposition, fmt.Sprintf("attachment; filename=%q", fileName))
	return c.JSONPretty(http.StatusOK, payload, "  ")
}

func (h *Handlers) HandleConnectedAppGrantRevoke(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}
	credentialID, err := parsePositiveInt64Param(c.Param("credentialID"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	summary, err := h.Q.GetConnectedAppSummaryByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return RenderNotFound(c)
	}

	credential, err := h.Q.GetCredentialArtifactByID(ctx, gen.GetCredentialArtifactByIDParams{
		EvaluatedAt: pgTimestamptz(time.Now().UTC()),
		ID:          credentialID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if strings.TrimSpace(credential.SourceKind) != strings.TrimSpace(summary.SourceKind) ||
		strings.TrimSpace(credential.SourceName) != strings.TrimSpace(summary.SourceName) ||
		strings.TrimSpace(credential.AssetRefKind) != strings.TrimSpace(summary.AssetKind) ||
		strings.TrimSpace(credential.AssetRefExternalID) != strings.TrimSpace(summary.AssetKind)+":"+strings.TrimSpace(summary.ExternalID) ||
		!strings.EqualFold(strings.TrimSpace(credential.CredentialKind), "google_oauth_grant") {
		return RenderNotFound(c)
	}

	var raw googleGrantRaw
	if len(credential.RawJson) > 0 {
		_ = json.Unmarshal(credential.RawJson, &raw)
	}
	raw.UserKey = strings.TrimSpace(raw.UserKey)
	raw.ClientID = strings.TrimSpace(raw.ClientID)
	if raw.UserKey == "" || raw.ClientID == "" {
		if isHX(c) {
			return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
				alert: &viewmodels.ConnectedAppsAlert{
					Title:       "Unable to revoke grant",
					Message:     "The synced grant record is missing the Google user or client identifier.",
					Destructive: true,
				},
			})
		}
		setFlashToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Unable to revoke grant",
			Description: "The synced grant record is missing the Google user or client identifier.",
		})
		return c.Redirect(http.StatusSeeOther, "/oauth-apps/"+strconv.FormatInt(appID, 10))
	}

	stateView, err := h.LoadConnectorStateView(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	google := stateView.GoogleWorkspace()
	if !google.Configured() || !google.Enabled() {
		if isHX(c) {
			return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
				alert: &viewmodels.ConnectedAppsAlert{
					Title:       "Google Workspace unavailable",
					Message:     "Enable the Google Workspace connector before revoking grants.",
					Destructive: true,
				},
			})
		}
		setFlashToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Google Workspace unavailable",
			Description: "Enable the Google Workspace connector before revoking grants.",
		})
		return c.Redirect(http.StatusSeeOther, "/oauth-apps/"+strconv.FormatInt(appID, 10))
	}

	client, err := googleworkspace.NewClient(google.Config())
	if err != nil {
		return h.RenderError(c, err)
	}
	if err := client.DeleteOAuthTokenGrant(ctx, raw.UserKey, raw.ClientID); err != nil {
		if isHX(c) {
			return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
				alert: &viewmodels.ConnectedAppsAlert{
					Title:       "Grant revoke failed",
					Message:     err.Error(),
					Destructive: true,
				},
			})
		}
		setFlashToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Grant revoke failed",
			Description: err.Error(),
		})
		return c.Redirect(http.StatusSeeOther, "/oauth-apps/"+strconv.FormatInt(appID, 10))
	}

	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Grant revoked",
		Description: "The Google Workspace token grant was revoked. Run sync to refresh inventory state.",
	})
	if isHX(c) {
		return h.renderConnectedAppShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.ConnectedAppsAlert{
				Title:       "Grant revoked",
				Message:     "The Google Workspace token grant was revoked. Run sync to refresh inventory state.",
				Destructive: false,
			},
		})
	}

	return c.Redirect(http.StatusSeeOther, "/oauth-apps/"+strconv.FormatInt(appID, 10))
}

func (h *Handlers) renderConnectedAppShow(c *echo.Context, appID int64, opts connectedAppShowOptions) error {
	addVary(c, "HX-Request", "HX-Target")
	ctx := c.Request().Context()
	summary, err := h.Q.GetConnectedAppSummaryByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return RenderNotFound(c)
	}

	layout, _, err := h.LayoutData(ctx, c, "OAuth App")
	if err != nil {
		return h.RenderError(c, err)
	}

	linkResolver := newIdentityLinkResolver(h, ctx)

	owners, err := h.Q.ListAppAssetOwnersByAssetID(ctx, appID)
	if err != nil {
		return h.RenderError(c, err)
	}
	likelyOwners := make([]viewmodels.AppAssetOwnerItem, 0, len(owners))
	for _, owner := range owners {
		displayName := strings.TrimSpace(owner.OwnerDisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(owner.OwnerEmail)
		}
		if displayName == "" {
			displayName = strings.TrimSpace(owner.OwnerExternalID)
		}
		likelyOwners = append(likelyOwners, viewmodels.AppAssetOwnerItem{
			OwnerKind:         fallbackDash(strings.TrimSpace(owner.OwnerKind)),
			OwnerDisplayName:  fallbackDash(displayName),
			OwnerEmail:        fallbackDash(strings.TrimSpace(owner.OwnerEmail)),
			OwnerExternalID:   fallbackDash(strings.TrimSpace(owner.OwnerExternalID)),
			OwnerIdentityHref: linkResolver.Resolve(strings.TrimSpace(summary.SourceKind), strings.TrimSpace(summary.SourceName), owner.OwnerExternalID, owner.OwnerEmail, owner.OwnerDisplayName),
		})
	}

	now := time.Now().UTC()
	grantRows, err := h.Q.ListCredentialArtifactsForAssetRef(ctx, gen.ListCredentialArtifactsForAssetRefParams{
		EvaluatedAt:        pgTimestamptz(now),
		SourceKind:         strings.TrimSpace(summary.SourceKind),
		SourceName:         strings.TrimSpace(summary.SourceName),
		AssetRefKind:       strings.TrimSpace(summary.AssetKind),
		AssetRefExternalID: strings.TrimSpace(summary.AssetKind) + ":" + strings.TrimSpace(summary.ExternalID),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	grants := make([]viewmodels.ConnectedAppGrantItem, 0, len(grantRows))
	for _, row := range grantRows {
		displayName := strings.TrimSpace(row.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(row.ExternalID)
		}
		actor := fallbackDash(actorDisplayName(row.CreatedByDisplayName, row.CreatedByExternalID))
		email := auth.NormalizeEmail(strings.TrimSpace(row.CreatedByDisplayName))
		if strings.Contains(email, " ") || !strings.Contains(email, "@") {
			email = ""
		}

		var raw googleGrantRaw
		if len(row.RawJson) > 0 {
			_ = json.Unmarshal(row.RawJson, &raw)
		}
		grants = append(grants, viewmodels.ConnectedAppGrantItem{
			CredentialID:   row.ID,
			DisplayName:    fallbackDash(displayName),
			UserLabel:      actor,
			UserEmail:      fallbackDash(email),
			UserExternalID: fallbackDash(strings.TrimSpace(row.CreatedByExternalID)),
			UserHref:       linkResolver.Resolve(strings.TrimSpace(row.SourceKind), strings.TrimSpace(row.SourceName), row.CreatedByExternalID, email, row.CreatedByDisplayName),
			Status:         fallbackDash(strings.TrimSpace(row.Status)),
			RiskLevel:      strings.TrimSpace(row.RiskLevel),
			ScopeSummary:   summarizeDiscoveryScopes(row.ScopeJson),
			ScopeCount:     connectedAppScopeCount(row.ScopeJson),
			LastUsedAt:     formatProgrammaticDate(maxTimestamp(row.LastUsedAtSource, row.LastObservedAt)),
			CanRevoke:      layout.IsAdmin && strings.TrimSpace(raw.UserKey) != "" && strings.TrimSpace(raw.ClientID) != "",
		})
	}

	cutoffs := h.discoveryPostureCutoffs(now)
	discoverySources, err := h.Q.ListConnectedAppDiscoverySourcesBySourceAppID(ctx, gen.ListConnectedAppDiscoverySourcesBySourceAppIDParams{
		SourceKind:                strings.TrimSpace(summary.SourceKind),
		SourceName:                strings.TrimSpace(summary.SourceName),
		SourceAppID:               strings.TrimSpace(summary.ExternalID),
		OktaFreshAfter:            cutoffs.OktaFreshAfter,
		EntraFreshAfter:           cutoffs.EntraFreshAfter,
		GoogleWorkspaceFreshAfter: cutoffs.GoogleWorkspaceFreshAfter,
		GithubFreshAfter:          cutoffs.GithubFreshAfter,
		DatadogFreshAfter:         cutoffs.DatadogFreshAfter,
		AwsFreshAfter:             cutoffs.AwsFreshAfter,
		DefaultFreshAfter:         cutoffs.DefaultFreshAfter,
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	sourceItems := make([]viewmodels.ConnectedAppDiscoverySourceItem, 0, len(discoverySources))
	for _, source := range discoverySources {
		sourceItems = append(sourceItems, viewmodels.ConnectedAppDiscoverySourceItem{
			DiscoveryDisplayName: fallbackDash(strings.TrimSpace(source.DiscoveryDisplayName)),
			CanonicalKey:         fallbackDash(strings.TrimSpace(source.CanonicalKey)),
			Domain:               fallbackDash(strings.TrimSpace(source.DiscoveryPrimaryDomain)),
			VendorName:           fallbackDash(strings.TrimSpace(source.DiscoveryVendorName)),
			ManagedState:         strings.TrimSpace(source.DiscoveryManagedState),
			RiskLevel:            strings.TrimSpace(source.DiscoveryRiskLevel),
			SourceName:           fallbackDash(strings.TrimSpace(source.SourceName)),
			LastObservedAt:       formatProgrammaticDate(source.LastObservedAt),
		})
	}

	eventRows, err := h.Q.ListConnectedAppDiscoveryEventsBySourceAppID(ctx, gen.ListConnectedAppDiscoveryEventsBySourceAppIDParams{
		SourceKind:  strings.TrimSpace(summary.SourceKind),
		SourceName:  strings.TrimSpace(summary.SourceName),
		SourceAppID: strings.TrimSpace(summary.ExternalID),
		LimitRows:   50,
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	eventItems := make([]viewmodels.ConnectedAppDiscoveryEventItem, 0, len(eventRows))
	for _, event := range eventRows {
		actor := strings.TrimSpace(event.ActorDisplayName)
		if actor == "" {
			actor = strings.TrimSpace(event.ActorEmail)
		}
		if actor == "" {
			actor = strings.TrimSpace(event.ActorExternalID)
		}
		eventItems = append(eventItems, viewmodels.ConnectedAppDiscoveryEventItem{
			SignalKind:    strings.TrimSpace(event.SignalKind),
			ObservedAt:    formatProgrammaticDate(event.ObservedAt),
			Actor:         fallbackDash(actor),
			ScopesSummary: summarizeDiscoveryScopes(event.ScopesJson),
		})
	}

	displayName := strings.TrimSpace(summary.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(summary.ExternalID)
	}
	confidence, confidenceReason := connectedAppConfidence(summary.OwnerCount, summary.GrantCount, summary.DiscoverySourceCount, summary.ReviewOwnerIdentityID > 0)
	ownerEmailInput := auth.NormalizeEmail(opts.ownerEmailInput)
	if ownerEmailInput == "" {
		ownerEmailInput = auth.NormalizeEmail(strings.TrimSpace(summary.ReviewOwnerPrimaryEmail))
	}
	hasFormInput := strings.TrimSpace(opts.reviewStateInput) != "" ||
		strings.TrimSpace(opts.ownerEmailInput) != "" ||
		strings.TrimSpace(opts.ticketRefInput) != "" ||
		strings.TrimSpace(opts.notesInput) != ""
	reviewStateInput := strings.TrimSpace(summary.ReviewState)
	ticketRefInput := strings.TrimSpace(summary.TicketRef)
	notesInput := strings.TrimSpace(summary.Notes)
	if hasFormInput {
		if normalized := normalizeConnectedAppReviewState(opts.reviewStateInput, false); normalized != "" {
			reviewStateInput = normalized
		}
		ticketRefInput = strings.TrimSpace(opts.ticketRefInput)
		notesInput = strings.TrimSpace(opts.notesInput)
	}

	data := viewmodels.ConnectedAppShowViewData{
		Layout: layout,
		App: viewmodels.ConnectedAppSummaryView{
			ID:                     summary.ID,
			DisplayName:            displayName,
			ExternalID:             strings.TrimSpace(summary.ExternalID),
			SourceKind:             strings.TrimSpace(summary.SourceKind),
			SourceName:             strings.TrimSpace(summary.SourceName),
			Status:                 fallbackDash(strings.TrimSpace(summary.Status)),
			ReviewState:            strings.TrimSpace(summary.ReviewState),
			ReviewOwner:            fallbackDash(strings.TrimSpace(summary.ReviewOwnerDisplayName)),
			ReviewOwnerEmail:       fallbackDash(strings.TrimSpace(summary.ReviewOwnerPrimaryEmail)),
			ReviewOwnerKind:        fallbackDash(strings.TrimSpace(summary.ReviewOwnerKind)),
			TicketRef:              strings.TrimSpace(summary.TicketRef),
			Notes:                  strings.TrimSpace(summary.Notes),
			LikelyOwnerCount:       int(summary.OwnerCount),
			GrantCount:             int(summary.GrantCount),
			ActorCount:             summary.ActorCount,
			DiscoverySourceCount:   summary.DiscoverySourceCount,
			DiscoveryEventCount30d: summary.DiscoveryEventCount30d,
			Freshness:              connectedAppFreshness(summary.EvidenceLastSeenAt),
			Confidence:             confidence,
			ConfidenceReason:       confidenceReason,
			LastSeenAt:             formatProgrammaticDate(summary.EvidenceLastSeenAt),
			ExportHref:             "/oauth-apps/" + strconv.FormatInt(summary.ID, 10) + "/export",
		},
		LikelyOwners:     likelyOwners,
		Grants:           grants,
		DiscoverySources: sourceItems,
		Events:           eventItems,
		Alert:            opts.alert,
		OwnerEmailInput:  ownerEmailInput,
		ReviewStateInput: reviewStateInput,
		TicketRefInput:   ticketRefInput,
		NotesInput:       notesInput,
		HasLikelyOwners:  len(likelyOwners) > 0,
		HasGrants:        len(grants) > 0,
		HasEvidence:      len(sourceItems) > 0,
		HasEvents:        len(eventItems) > 0,
	}

	if isHX(c) && isHXTarget(c, "connected-app-show-shell") {
		return h.RenderComponent(c, views.ConnectedAppShowBody(data))
	}
	return h.RenderComponent(c, views.ConnectedAppShowPage(data))
}

func buildConnectedAppReviewCounts(rows []gen.CountConnectedAppsGroupedByReviewStateRow, query, activeState string) []viewmodels.ConnectedAppsReviewCount {
	countsByState := map[string]int64{}
	for _, row := range rows {
		countsByState[strings.TrimSpace(row.ReviewState)] = row.AppCount
	}

	states := []string{"", "needs_revocation", "under_review", "unreviewed", "ticketed", "sanctioned"}
	out := make([]viewmodels.ConnectedAppsReviewCount, 0, len(states))
	for _, state := range states {
		label := "All states"
		if state != "" {
			label = humanizeConnectedAppReviewState(state)
		}
		count := int64(0)
		if state == "" {
			for _, c := range countsByState {
				count += c
			}
		} else {
			count = countsByState[state]
		}
		out = append(out, viewmodels.ConnectedAppsReviewCount{
			ReviewState: state,
			Label:       label,
			Count:       count,
			Href:        connectedAppsListURL(query, state, 1),
			IsActive:    state == activeState,
		})
	}
	return out
}

func normalizeConnectedAppReviewState(raw string, allowBlank bool) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		if allowBlank {
			return ""
		}
		return ""
	case "unreviewed":
		return "unreviewed"
	case "under_review":
		return "under_review"
	case "sanctioned":
		return "sanctioned"
	case "needs_revocation":
		return "needs_revocation"
	case "ticketed":
		return "ticketed"
	default:
		return ""
	}
}

func connectedAppConfidence(ownerCount, grantCount, discoverySourceCount int64, hasReviewOwner bool) (string, string) {
	signals := 0
	if grantCount > 0 {
		signals++
	}
	if ownerCount > 0 || hasReviewOwner {
		signals++
	}
	if discoverySourceCount > 0 {
		signals++
	}

	switch signals {
	case 3:
		return "high", "Inventory, ownership, and discovery evidence all line up."
	case 2:
		return "medium", "Multiple evidence paths are available, but attribution is still partial."
	default:
		return "low", "This record currently relies on a single evidence path."
	}
}

func connectedAppFreshness(lastSeen pgtype.Timestamptz) string {
	if !lastSeen.Valid {
		return "unknown"
	}
	age := time.Since(lastSeen.Time.UTC())
	switch {
	case age <= 7*24*time.Hour:
		return "fresh"
	case age <= 30*24*time.Hour:
		return "aging"
	default:
		return "stale"
	}
}

func connectedAppOwnerLabel(displayName, email string) string {
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

func connectedAppsListURL(query, reviewState string, page int) string {
	values := url.Values{}
	if query = strings.TrimSpace(query); query != "" {
		values.Set("q", query)
	}
	if reviewState = strings.TrimSpace(reviewState); reviewState != "" {
		values.Set("review_state", reviewState)
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	if len(values) == 0 {
		return "/oauth-apps"
	}
	return "/oauth-apps?" + values.Encode()
}

func connectedAppScopeCount(raw []byte) int {
	scopes := make([]string, 0, 8)
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &scopes)
	}
	return len(scopes)
}

func connectedAppGrantExport(rows []gen.ListCredentialArtifactsForAssetRefRow) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"id":                      row.ID,
			"display_name":            strings.TrimSpace(row.DisplayName),
			"external_id":             strings.TrimSpace(row.ExternalID),
			"status":                  strings.TrimSpace(row.Status),
			"created_by_kind":         strings.TrimSpace(row.CreatedByKind),
			"created_by_external_id":  strings.TrimSpace(row.CreatedByExternalID),
			"created_by_display_name": strings.TrimSpace(row.CreatedByDisplayName),
			"scope_summary":           summarizeDiscoveryScopes(row.ScopeJson),
			"scope_json":              json.RawMessage(row.ScopeJson),
			"raw_json":                json.RawMessage(row.RawJson),
			"last_used_at":            formatProgrammaticDate(maxTimestamp(row.LastUsedAtSource, row.LastObservedAt)),
		})
	}
	return out
}

func connectedAppExportFilename(displayName string, id int64) string {
	displayName = strings.ToLower(strings.TrimSpace(displayName))
	if displayName == "" {
		return fmt.Sprintf("connected-app-%d.json", id)
	}

	var b strings.Builder
	lastDash := false
	for _, r := range displayName {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if lastDash {
				continue
			}
			b.WriteRune('-')
			lastDash = true
		}
	}

	name := strings.Trim(b.String(), "-")
	if name == "" {
		name = fmt.Sprintf("connected-app-%d", id)
	}
	return name + ".json"
}

func humanizeConnectedAppReviewState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "unreviewed":
		return "Unreviewed"
	case "under_review":
		return "Under Review"
	case "sanctioned":
		return "Sanctioned"
	case "needs_revocation":
		return "Needs Revocation"
	case "ticketed":
		return "Ticketed"
	default:
		state = strings.TrimSpace(strings.ReplaceAll(state, "_", " "))
		if state == "" {
			return "—"
		}
		runes := []rune(strings.ToLower(state))
		if len(runes) > 0 {
			runes[0] = []rune(strings.ToUpper(string(runes[0])))[0]
		}
		return string(runes)
	}
}

func isGoogleConnectedApp(sourceKind, assetKind string) bool {
	return strings.TrimSpace(sourceKind) == configstore.KindGoogleWorkspace &&
		strings.TrimSpace(assetKind) == connectedAppAssetKindGoogle
}

func maxTimestamp(primary, fallback pgtype.Timestamptz) pgtype.Timestamptz {
	if primary.Valid {
		return primary
	}
	return fallback
}
