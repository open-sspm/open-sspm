package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

const (
	connectedAppAssetKindGoogle = "google_oauth_client"
	connectedAppsPerPage        = 20
)

type connectedAppShowOptions struct {
	alert                *viewmodels.AlertViewData
	ownerEmailInput      string
	governanceStateInput string
	ticketRefInput       string
	notesInput           string
}

type googleGrantRaw struct {
	UserKey  string `json:"user_key"`
	ClientID string `json:"client_id"`
}

func (h *Handlers) HandleConnectedApps(c *echo.Context) error {
	query := querystate.ParseConnectedAppsQuery(c.Request().URL.Query())
	return c.Redirect(http.StatusSeeOther, query.Href())
}

func (h *Handlers) HandleConnectedAppShow(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	summary, err := h.Q.GetAppAssetPostureByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return RenderNotFound(c)
	}

	return c.Redirect(http.StatusSeeOther, canonicalAppAssetDetailURL(appID))
}

func (h *Handlers) HandleAppAssetGovernanceUpdate(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	summary, err := h.Q.GetAppAssetPostureByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return RenderNotFound(c)
	}

	governanceState := querystate.NormalizeConnectedAppGovernanceState(c.FormValue("governance_state"), false)
	if governanceState == "" {
		return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.AlertViewData{
				Title:       "Invalid governance state",
				Message:     "Choose a valid state before saving governance.",
				Destructive: true,
			},
			ownerEmailInput:      strings.TrimSpace(c.FormValue("owner_email")),
			governanceStateInput: strings.TrimSpace(c.FormValue("governance_state")),
			ticketRefInput:       strings.TrimSpace(c.FormValue("ticket_ref")),
			notesInput:           strings.TrimSpace(c.FormValue("notes")),
		})
	}

	ownerEmailInput := auth.NormalizeEmail(c.FormValue("owner_email"))
	var ownerIdentityID pgtype.Int8
	if ownerEmailInput != "" {
		ownerIdentity, err := h.Q.GetPreferredIdentityByPrimaryEmail(ctx, ownerEmailInput)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
					alert: &viewmodels.AlertViewData{
						Title:       "Owner not found",
						Message:     "Assign an owner using an existing identity email address.",
						Destructive: true,
					},
					ownerEmailInput:      ownerEmailInput,
					governanceStateInput: governanceState,
					ticketRefInput:       strings.TrimSpace(c.FormValue("ticket_ref")),
					notesInput:           strings.TrimSpace(c.FormValue("notes")),
				})
			}
			return h.RenderError(c, err)
		}
		ownerIdentityID = pgtype.Int8{Int64: ownerIdentity.ID, Valid: true}
	}

	ticketRef := strings.TrimSpace(c.FormValue("ticket_ref"))
	if governanceState == "ticketed" && ticketRef == "" {
		return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.AlertViewData{
				Title:       "Ticket reference required",
				Message:     "Enter a ticket reference before marking this app as ticketed.",
				Destructive: true,
			},
			ownerEmailInput:      ownerEmailInput,
			governanceStateInput: governanceState,
			ticketRefInput:       ticketRef,
			notesInput:           strings.TrimSpace(c.FormValue("notes")),
		})
	}

	notes := strings.TrimSpace(c.FormValue("notes"))
	if len(notes) > 4000 {
		return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.AlertViewData{
				Title:       "Notes too long",
				Message:     "Keep governance notes under 4000 characters.",
				Destructive: true,
			},
			ownerEmailInput:      ownerEmailInput,
			governanceStateInput: governanceState,
			ticketRefInput:       ticketRef,
			notesInput:           notes,
		})
	}

	principal, ok := authn.PrincipalFromContext(c)
	if !ok {
		return c.NoContent(http.StatusForbidden)
	}

	if _, err := h.Q.UpsertAppAssetGovernance(ctx, gen.UpsertAppAssetGovernanceParams{
		AppAssetID:          appID,
		GovernanceState:     governanceState,
		OwnerIdentityID:     ownerIdentityID,
		TicketRef:           ticketRef,
		Notes:               notes,
		UpdatedByAuthUserID: pgtype.Int8{Int64: principal.UserID, Valid: principal.UserID > 0},
	}); err != nil {
		return h.RenderError(c, err)
	}

	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "OAuth app governance saved",
		Description: "Owner assignment and governance state updated.",
	})
	if isHX(c) {
		return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.AlertViewData{
				Title:       "OAuth app governance saved",
				Message:     "Owner assignment and governance state updated.",
				Destructive: false,
			},
			ownerEmailInput: ownerEmailInput,
		})
	}

	return c.Redirect(http.StatusSeeOther, canonicalAppAssetDetailURL(appID))
}

func (h *Handlers) HandleAppAssetExport(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	now := time.Now().UTC()
	summary, err := h.Q.GetAppAssetPostureByID(ctx, appID)
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

	discoverySources, err := h.Q.ListAppAssetDiscoverySourcesBySourceAppID(ctx, gen.ListAppAssetDiscoverySourcesBySourceAppIDParams{
		SourceKind:  strings.TrimSpace(summary.SourceKind),
		SourceName:  strings.TrimSpace(summary.SourceName),
		SourceAppID: strings.TrimSpace(summary.ExternalID),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	discoveryEvents, err := h.Q.ListAppAssetDiscoveryEventsBySourceAppID(ctx, gen.ListAppAssetDiscoveryEventsBySourceAppIDParams{
		SourceKind:  strings.TrimSpace(summary.SourceKind),
		SourceName:  strings.TrimSpace(summary.SourceName),
		SourceAppID: strings.TrimSpace(summary.ExternalID),
		LimitRows:   100,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	payload := map[string]any{
		"app_asset": map[string]any{
			"id":                            summary.ID,
			"display_name":                  strings.TrimSpace(summary.DisplayName),
			"external_id":                   strings.TrimSpace(summary.ExternalID),
			"source_kind":                   strings.TrimSpace(summary.SourceKind),
			"source_name":                   strings.TrimSpace(summary.SourceName),
			"status":                        strings.TrimSpace(summary.Status),
			"governance_state":              strings.TrimSpace(summary.GovernanceState),
			"governance_owner_display_name": strings.TrimSpace(summary.GovernanceOwnerDisplayName),
			"governance_owner_email":        strings.TrimSpace(summary.GovernanceOwnerPrimaryEmail),
			"governance_owner_kind":         strings.TrimSpace(summary.GovernanceOwnerKind),
			"ticket_ref":                    strings.TrimSpace(summary.TicketRef),
			"notes":                         strings.TrimSpace(summary.Notes),
			"likely_owner_count":            summary.OwnerCount,
			"grant_count":                   summary.GrantCount,
			"actor_count":                   summary.ActorCount,
			"discovery_source_count":        summary.DiscoverySourceCount,
			"discovery_event_count_30d":     summary.DiscoveryEventCount30d,
			"evidence_freshness":            strings.TrimSpace(summary.EvidenceFreshness),
			"evidence_confidence":           strings.TrimSpace(summary.EvidenceConfidence),
			"evidence_confidence_reason":    strings.TrimSpace(summary.EvidenceConfidenceReason),
			"last_seen_at":                  formatProgrammaticDate(summary.EvidenceLastSeenAt),
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

func (h *Handlers) HandleConnectedAppExport(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	summary, err := h.Q.GetAppAssetPostureByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return RenderNotFound(c)
	}

	return c.Redirect(http.StatusSeeOther, canonicalAppAssetDetailURL(appID)+"/export")
}

func (h *Handlers) HandleAppAssetGrantRevoke(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}
	credentialID, err := parsePositiveInt64Param(c.Param("credentialID"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	now := time.Now().UTC()
	summary, err := h.Q.GetAppAssetPostureByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return RenderNotFound(c)
	}

	credential, err := h.Q.GetCredentialArtifactByID(ctx, gen.GetCredentialArtifactByIDParams{EvaluatedAt: pgTimestamptz(now), ID: credentialID})
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
			return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
				alert: &viewmodels.AlertViewData{
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
		return c.Redirect(http.StatusSeeOther, canonicalAppAssetDetailURL(appID))
	}

	stateView, err := h.LoadConnectorStateView(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	google := stateView.GoogleWorkspace()
	if !google.Configured() || !google.Enabled() {
		if isHX(c) {
			return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
				alert: &viewmodels.AlertViewData{
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
		return c.Redirect(http.StatusSeeOther, canonicalAppAssetDetailURL(appID))
	}

	client, err := googleworkspace.NewClient(google.Config())
	if err != nil {
		return h.RenderError(c, err)
	}
	if err := client.DeleteOAuthTokenGrant(ctx, raw.UserKey, raw.ClientID); err != nil {
		if isHX(c) {
			return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
				alert: &viewmodels.AlertViewData{
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
		return c.Redirect(http.StatusSeeOther, canonicalAppAssetDetailURL(appID))
	}

	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Grant revoked",
		Description: "The Google Workspace token grant was revoked. Run sync to refresh inventory state.",
	})
	if isHX(c) {
		return h.renderAppAssetShow(c, appID, connectedAppShowOptions{
			alert: &viewmodels.AlertViewData{
				Title:       "Grant revoked",
				Message:     "The Google Workspace token grant was revoked. Run sync to refresh inventory state.",
				Destructive: false,
			},
		})
	}

	return c.Redirect(http.StatusSeeOther, canonicalAppAssetDetailURL(appID))
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

	c.Request().URL.Path = canonicalAppAssetDetailURL(appID) + "/grants/" + strconv.FormatInt(credentialID, 10) + "/revoke"
	return h.HandleAppAssetGrantRevoke(c)
}

func (h *Handlers) buildConnectedAppsViewData(ctx context.Context, layout viewmodels.LayoutData, stateView connectorStateView, queryState querystate.ConnectedAppsQuery) (viewmodels.ConnectedAppsViewData, error) {
	pagination := newPaginatedListState(0, queryState.Page, connectedAppsPerPage)
	data := viewmodels.ConnectedAppsViewData{
		PaginatedListPageData: pagination.PageData(layout, 0, "No OAuth apps match the current filters.", ""),
		Query:                 queryState,
	}

	google := stateView.GoogleWorkspace()
	sourceName := google.SourceName()
	if !google.Configured() || !google.Enabled() || sourceName == "" {
		data.PaginatedListPageData.EmptyStateMsg = connectorUnavailableMessage("Google Workspace", google.Configured(), google.Enabled())
		data.GovernanceCounts = buildConnectedAppGovernanceCounts(nil, queryState)
		return data, nil
	}

	totalCount, err := h.Q.CountAppAssetGovernanceBySourceAndQueryAndState(ctx, gen.CountAppAssetGovernanceBySourceAndQueryAndStateParams{
		SourceKind:      configstore.KindGoogleWorkspace,
		SourceName:      sourceName,
		AssetKind:       connectedAppAssetKindGoogle,
		GovernanceState: queryState.GovernanceState,
		Query:           queryState.Q,
	})
	if err != nil {
		return data, err
	}

	countRows, err := h.Q.CountAppAssetGovernanceGroupedByState(ctx, gen.CountAppAssetGovernanceGroupedByStateParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
		AssetKind:  connectedAppAssetKindGoogle,
		Query:      queryState.Q,
	})
	if err != nil {
		return data, err
	}
	data.GovernanceCounts = buildConnectedAppGovernanceCounts(countRows, queryState)

	pagination = newPaginatedListState(totalCount, queryState.Page, connectedAppsPerPage)
	rows, err := h.Q.ListAppAssetGovernancePageBySourceAndQueryAndState(ctx, gen.ListAppAssetGovernancePageBySourceAndQueryAndStateParams{
		SourceKind:      configstore.KindGoogleWorkspace,
		SourceName:      sourceName,
		AssetKind:       connectedAppAssetKindGoogle,
		GovernanceState: queryState.GovernanceState,
		Query:           queryState.Q,
		PageLimit:       int32(connectedAppsPerPage),
		PageOffset:      int32(pagination.Offset()),
	})
	if err != nil {
		return data, err
	}

	items := make([]viewmodels.ConnectedAppListItem, 0, len(rows))
	for _, row := range rows {
		displayName := strings.TrimSpace(row.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(row.ExternalID)
		}
		governanceOwner := strings.TrimSpace(row.GovernanceOwnerDisplayName)
		governanceOwnerEmail := strings.TrimSpace(row.GovernanceOwnerPrimaryEmail)
		if governanceOwner == "" {
			governanceOwner = governanceOwnerEmail
		}
		if governanceOwner == "" {
			governanceOwner = "—"
		}
		items = append(items, viewmodels.ConnectedAppListItem{
			ID:                       row.ID,
			DisplayName:              displayName,
			ExternalID:               strings.TrimSpace(row.ExternalID),
			Status:                   fallbackDash(strings.TrimSpace(row.Status)),
			GovernanceState:          strings.TrimSpace(row.GovernanceState),
			GovernanceOwner:          governanceOwner,
			GovernanceOwnerEmail:     governanceOwnerEmail,
			LikelyOwnerCount:         int(row.OwnerCount),
			GrantCount:               int(row.GrantCount),
			ActorCount:               row.ActorCount,
			DiscoveryEventCount30d:   row.DiscoveryEventCount30d,
			EvidenceFreshness:        strings.TrimSpace(row.EvidenceFreshness),
			EvidenceConfidence:       strings.TrimSpace(row.EvidenceConfidence),
			EvidenceConfidenceReason: strings.TrimSpace(row.EvidenceConfidenceReason),
			LastSeenAt:               formatProgrammaticDate(row.EvidenceLastSeenAt),
			TicketRef:                strings.TrimSpace(row.TicketRef),
		})
	}

	data.Items = items
	data.PaginatedListPageData = pagination.PageData(layout, len(items), "No OAuth apps match the current filters.", "")
	data.HasItems = len(items) > 0
	if !queryState.HasFilters() {
		data.PaginatedListPageData.EmptyStateMsg = "No OAuth apps have been synced from Google Workspace yet."
	}
	return data, nil
}

func (h *Handlers) buildConnectedAppShowViewData(ctx context.Context, layout viewmodels.LayoutData, appID int64, opts connectedAppShowOptions) (viewmodels.ConnectedAppShowViewData, error) {
	data := viewmodels.ConnectedAppShowViewData{}

	now := time.Now().UTC()
	summary, err := h.Q.GetAppAssetPostureByID(ctx, appID)
	if err != nil {
		return data, err
	}
	if !isGoogleConnectedApp(summary.SourceKind, summary.AssetKind) {
		return data, pgx.ErrNoRows
	}

	linkResolver := newIdentityLinkResolver(h, ctx)

	owners, err := h.Q.ListAppAssetOwnersByAssetID(ctx, appID)
	if err != nil {
		return data, err
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

	grantRows, err := h.Q.ListCredentialArtifactsForAssetRef(ctx, gen.ListCredentialArtifactsForAssetRefParams{
		EvaluatedAt:        pgTimestamptz(now),
		SourceKind:         strings.TrimSpace(summary.SourceKind),
		SourceName:         strings.TrimSpace(summary.SourceName),
		AssetRefKind:       strings.TrimSpace(summary.AssetKind),
		AssetRefExternalID: strings.TrimSpace(summary.AssetKind) + ":" + strings.TrimSpace(summary.ExternalID),
	})
	if err != nil {
		return data, err
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

	discoverySources, err := h.Q.ListAppAssetDiscoverySourcesBySourceAppID(ctx, gen.ListAppAssetDiscoverySourcesBySourceAppIDParams{
		SourceKind:  strings.TrimSpace(summary.SourceKind),
		SourceName:  strings.TrimSpace(summary.SourceName),
		SourceAppID: strings.TrimSpace(summary.ExternalID),
	})
	if err != nil {
		return data, err
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

	eventRows, err := h.Q.ListAppAssetDiscoveryEventsBySourceAppID(ctx, gen.ListAppAssetDiscoveryEventsBySourceAppIDParams{
		SourceKind:  strings.TrimSpace(summary.SourceKind),
		SourceName:  strings.TrimSpace(summary.SourceName),
		SourceAppID: strings.TrimSpace(summary.ExternalID),
		LimitRows:   50,
	})
	if err != nil {
		return data, err
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
	governanceOwnerEmail := strings.TrimSpace(summary.GovernanceOwnerPrimaryEmail)
	governanceOwner := strings.TrimSpace(summary.GovernanceOwnerDisplayName)
	if governanceOwner == "" {
		governanceOwner = governanceOwnerEmail
	}
	if governanceOwner == "" {
		governanceOwner = "—"
	}
	ownerEmailInput := auth.NormalizeEmail(opts.ownerEmailInput)
	if ownerEmailInput == "" {
		ownerEmailInput = auth.NormalizeEmail(governanceOwnerEmail)
	}
	hasFormInput := strings.TrimSpace(opts.governanceStateInput) != "" ||
		strings.TrimSpace(opts.ownerEmailInput) != "" ||
		strings.TrimSpace(opts.ticketRefInput) != "" ||
		strings.TrimSpace(opts.notesInput) != ""
	governanceStateInput := strings.TrimSpace(summary.GovernanceState)
	ticketRefInput := strings.TrimSpace(summary.TicketRef)
	notesInput := strings.TrimSpace(summary.Notes)
	if hasFormInput {
		if normalized := querystate.NormalizeConnectedAppGovernanceState(opts.governanceStateInput, false); normalized != "" {
			governanceStateInput = normalized
		}
		ticketRefInput = strings.TrimSpace(opts.ticketRefInput)
		notesInput = strings.TrimSpace(opts.notesInput)
	}

	data = viewmodels.ConnectedAppShowViewData{
		Layout: layout,
		App: viewmodels.ConnectedAppSummaryView{
			ID:                       summary.ID,
			DisplayName:              displayName,
			ExternalID:               strings.TrimSpace(summary.ExternalID),
			SourceKind:               strings.TrimSpace(summary.SourceKind),
			SourceName:               strings.TrimSpace(summary.SourceName),
			Status:                   fallbackDash(strings.TrimSpace(summary.Status)),
			GovernanceState:          strings.TrimSpace(summary.GovernanceState),
			GovernanceOwner:          governanceOwner,
			GovernanceOwnerEmail:     fallbackDash(governanceOwnerEmail),
			GovernanceOwnerKind:      fallbackDash(strings.TrimSpace(summary.GovernanceOwnerKind)),
			TicketRef:                strings.TrimSpace(summary.TicketRef),
			Notes:                    strings.TrimSpace(summary.Notes),
			LikelyOwnerCount:         int(summary.OwnerCount),
			GrantCount:               int(summary.GrantCount),
			ActorCount:               summary.ActorCount,
			DiscoverySourceCount:     summary.DiscoverySourceCount,
			DiscoveryEventCount30d:   summary.DiscoveryEventCount30d,
			EvidenceFreshness:        strings.TrimSpace(summary.EvidenceFreshness),
			EvidenceConfidence:       strings.TrimSpace(summary.EvidenceConfidence),
			EvidenceConfidenceReason: strings.TrimSpace(summary.EvidenceConfidenceReason),
			LastSeenAt:               formatProgrammaticDate(summary.EvidenceLastSeenAt),
			ExportHref:               canonicalAppAssetDetailURL(summary.ID) + "/export",
		},
		LikelyOwners:         likelyOwners,
		Grants:               grants,
		DiscoverySources:     sourceItems,
		Events:               eventItems,
		Alert:                opts.alert,
		OwnerEmailInput:      ownerEmailInput,
		GovernanceStateInput: governanceStateInput,
		TicketRefInput:       ticketRefInput,
		NotesInput:           notesInput,
		HasLikelyOwners:      len(likelyOwners) > 0,
		HasGrants:            len(grants) > 0,
		HasEvidence:          len(sourceItems) > 0,
		HasEvents:            len(eventItems) > 0,
	}
	return data, nil
}

func (h *Handlers) renderAppAssetShow(c *echo.Context, appID int64, opts connectedAppShowOptions) error {
	addVary(c, "HX-Request", "HX-Target")
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "App Asset")
	if err != nil {
		return h.RenderError(c, err)
	}

	oauthData, err := h.buildConnectedAppShowViewData(ctx, layout, appID, opts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	data := viewmodels.AppAssetShowViewData{
		Layout: layout,
		Asset: viewmodels.AppAssetSummaryView{
			ID:               oauthData.App.ID,
			SourceKind:       oauthData.App.SourceKind,
			SourceName:       oauthData.App.SourceName,
			AssetKind:        connectedAppAssetKindGoogle,
			DisplayName:      oauthData.App.DisplayName,
			ExternalID:       oauthData.App.ExternalID,
			ParentExternalID: "—",
			Status:           oauthData.App.Status,
			LastObservedAt:   oauthData.App.LastSeenAt,
		},
		GoogleOAuthView: &oauthData,
	}

	if isHX(c) && isHXTarget(c, "connected-app-show-shell") {
		return h.RenderComponent(c, views.ConnectedAppShowBody(oauthData))
	}
	return h.RenderComponent(c, views.AppAssetShowPage(data))
}

func buildConnectedAppGovernanceCounts(rows []gen.CountAppAssetGovernanceGroupedByStateRow, activeQuery querystate.ConnectedAppsQuery) []viewmodels.ConnectedAppsGovernanceCount {
	countsByState := map[string]int64{}
	for _, row := range rows {
		countsByState[strings.TrimSpace(row.GovernanceState)] = row.AppCount
	}

	states := []string{"", "action_required", "in_review", "unreviewed", "ticketed", "approved"}
	out := make([]viewmodels.ConnectedAppsGovernanceCount, 0, len(states))
	for _, state := range states {
		label := "All states"
		if state != "" {
			label = views.HumanizeAppAssetGovernanceState(state)
		}
		count := int64(0)
		if state == "" {
			for _, c := range countsByState {
				count += c
			}
		} else {
			count = countsByState[state]
		}
		out = append(out, viewmodels.ConnectedAppsGovernanceCount{
			GovernanceState: state,
			Label:           label,
			Count:           count,
			Href:            activeQuery.WithGovernanceState(state).WithPage(1).Href(),
			IsActive:        state == activeQuery.GovernanceState,
		})
	}
	return out
}

func canonicalAppAssetDetailURL(appID int64) string {
	return "/app-assets/" + strconv.FormatInt(appID, 10)
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
		return fmt.Sprintf("app-asset-%d.json", id)
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
		name = fmt.Sprintf("app-asset-%d", id)
	}
	return name + ".json"
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
