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
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/readmodels"
)

const (
	discoveryAppsPerPage               = 20
	discoveryHotspotsLimit             = 200
	discoveryReplacementCandidateLimit = 8
)

type discoveryAppShowOptions struct {
	alert                *viewmodels.AlertViewData
	governanceForm       *discoveryGovernanceFormInput
	openGovernanceDialog bool
}

type discoveryGovernanceFormInput struct {
	accountableOwnerEmailInput string
	reviewOwnerEmailInput      string
	reviewDispositionInput     string
	followUpDueDateInput       string
	ticketRefInput             string
	notesInput                 string
	replacementQueryInput      string
	replacementSaaSAppIDInput  int64
}

type discoveryGovernanceIdentityRefs struct {
	ownerIdentityID       pgtype.Int8
	reviewOwnerIdentityID pgtype.Int8
}

type discoveryGovernanceValidatedInput struct {
	followUpDueDate pgtype.Date
	replacementRef  pgtype.Int8
}

func (h *Handlers) HandleDiscoveryApps(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, stateView, err := h.LayoutData(ctx, c, "Apps & Discovery")
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
			ID:                     row.ID,
			DisplayName:            displayName,
			Domain:                 domainLabel,
			VendorName:             vendorLabel,
			ManagedState:           strings.TrimSpace(row.ManagedState),
			ManagedReason:          strings.TrimSpace(row.ManagedReason),
			RiskScore:              row.RiskScore,
			RiskLevel:              strings.TrimSpace(row.RiskLevel),
			Owner:                  ownerLabel,
			ReviewOwner:            discoveryOwnerLabel(row.ReviewOwnerDisplayName, row.ReviewOwnerPrimaryEmail),
			ReviewDisposition:      normalizeDiscoveryReviewDisposition(row.ReviewDisposition),
			FollowUpDueDate:        dateDisplay(row.FollowUpDueDate),
			IsFollowUpOverdue:      row.IsFollowUpOverdue,
			ReplacementDisplayName: strings.TrimSpace(row.ReplacementDisplayName),
			TicketRef:              strings.TrimSpace(row.TicketRef),
			Actors30d:              row.Actors30d,
			LastSeen:               calendarDateDisplay(row.LastSeenAt),
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
			ID:                     row.ID,
			DisplayName:            displayName,
			Domain:                 domainLabel,
			ManagedState:           strings.TrimSpace(row.ManagedState),
			RiskScore:              row.RiskScore,
			RiskLevel:              strings.TrimSpace(row.RiskLevel),
			Owner:                  discoveryOwnerLabel(row.OwnerDisplayName, row.OwnerPrimaryEmail),
			ReviewOwner:            discoveryOwnerLabel(row.ReviewOwnerDisplayName, row.ReviewOwnerPrimaryEmail),
			ReviewDisposition:      normalizeDiscoveryReviewDisposition(row.ReviewDisposition),
			FollowUpDueDate:        dateDisplay(row.FollowUpDueDate),
			IsFollowUpOverdue:      row.IsFollowUpOverdue,
			ReplacementDisplayName: strings.TrimSpace(row.ReplacementDisplayName),
			TicketRef:              strings.TrimSpace(row.TicketRef),
			Actors30d:              row.Actors30d,
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

	return h.renderDiscoveryAppShow(c, appID, discoveryAppShowOptions{})
}

func (h *Handlers) HandleDiscoveryAppGovernanceUpdate(c *echo.Context) error {
	appID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	summary, err := h.Q.GetSaaSAppByID(ctx, appID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	form := parseDiscoveryGovernanceForm(c)
	reviewDisposition := normalizeDiscoveryReviewDisposition(form.reviewDispositionInput)
	if reviewDisposition == "" {
		return h.renderDiscoveryGovernanceValidationError(c, appID, form, "Invalid disposition", "Choose a valid discovery disposition before saving governance.")
	}
	form.reviewDispositionInput = reviewDisposition
	if form.reviewDispositionInput != "replace" {
		form.replacementSaaSAppIDInput = 0
	}
	form.accountableOwnerEmailInput = auth.NormalizeEmail(form.accountableOwnerEmailInput)
	form.reviewOwnerEmailInput = auth.NormalizeEmail(form.reviewOwnerEmailInput)

	identityRefs, alert, err := h.resolveDiscoveryGovernanceIdentities(ctx, form)
	if err != nil {
		return h.RenderError(c, err)
	}
	if alert != nil {
		return h.renderDiscoveryGovernanceAlert(c, appID, form, alert)
	}

	validated, alert, err := h.validateDiscoveryGovernanceUpdate(ctx, summary, form, identityRefs)
	if err != nil {
		return h.RenderError(c, err)
	}
	if alert != nil {
		return h.renderDiscoveryGovernanceAlert(c, appID, form, alert)
	}

	principal, ok := authn.PrincipalFromContext(c)
	if !ok {
		return c.NoContent(http.StatusForbidden)
	}

	if err := h.persistDiscoveryGovernanceUpdate(ctx, appID, form, identityRefs, validated, principal); err != nil {
		return h.RenderError(c, err)
	}

	return h.renderDiscoveryGovernanceSuccess(c, appID)
}

func parseDiscoveryGovernanceForm(c *echo.Context) discoveryGovernanceFormInput {
	return discoveryGovernanceFormInput{
		accountableOwnerEmailInput: strings.TrimSpace(c.FormValue("owner_email")),
		reviewOwnerEmailInput:      strings.TrimSpace(c.FormValue("review_owner_email")),
		reviewDispositionInput:     strings.TrimSpace(c.FormValue("review_disposition")),
		followUpDueDateInput:       strings.TrimSpace(c.FormValue("follow_up_due_date")),
		ticketRefInput:             strings.TrimSpace(c.FormValue("ticket_ref")),
		notesInput:                 strings.TrimSpace(c.FormValue("notes")),
		replacementQueryInput:      strings.TrimSpace(c.FormValue("replacement_query")),
		replacementSaaSAppIDInput:  parseOptionalPositiveInt64(c.FormValue("replacement_saas_app_id")),
	}
}

func discoveryAppShowOptionsForGovernance(form discoveryGovernanceFormInput, alert *viewmodels.AlertViewData) discoveryAppShowOptions {
	formCopy := form
	return discoveryAppShowOptions{
		alert:                alert,
		governanceForm:       &formCopy,
		openGovernanceDialog: true,
	}
}

func (h *Handlers) renderDiscoveryGovernanceAlert(c *echo.Context, appID int64, form discoveryGovernanceFormInput, alert *viewmodels.AlertViewData) error {
	return h.renderDiscoveryAppShow(c, appID, discoveryAppShowOptionsForGovernance(form, alert))
}

func (h *Handlers) renderDiscoveryGovernanceValidationError(c *echo.Context, appID int64, form discoveryGovernanceFormInput, title, message string) error {
	return h.renderDiscoveryGovernanceAlert(c, appID, form, &viewmodels.AlertViewData{
		Title:       title,
		Message:     message,
		Destructive: true,
	})
}

func (h *Handlers) resolveDiscoveryGovernanceIdentities(ctx context.Context, form discoveryGovernanceFormInput) (discoveryGovernanceIdentityRefs, *viewmodels.AlertViewData, error) {
	refs := discoveryGovernanceIdentityRefs{}

	if form.accountableOwnerEmailInput != "" {
		ownerIdentity, err := h.Q.FindUnambiguousIdentityByPrimaryEmail(ctx, form.accountableOwnerEmailInput)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return refs, nil, err
			}
			// No strict winner — either no identity owns this email, or two or
			// more identities tie at the top tier. Distinguish the two so the
			// operator gets an actionable message.
			count, countErr := h.Q.CountIdentitiesByPrimaryEmail(ctx, form.accountableOwnerEmailInput)
			if countErr != nil {
				return refs, nil, countErr
			}
			if count > 1 {
				return refs, &viewmodels.AlertViewData{
					Title:       "Accountable owner email is ambiguous",
					Message:     "More than one identity claims this email. Resolve the conflict before assigning this owner.",
					Destructive: true,
				}, nil
			}
			return refs, &viewmodels.AlertViewData{
				Title:       "Owner not found",
				Message:     "Assign an accountable owner using an existing identity email address.",
				Destructive: true,
			}, nil
		}
		refs.ownerIdentityID = pgtype.Int8{Int64: ownerIdentity.ID, Valid: true}
	}

	if form.reviewOwnerEmailInput != "" {
		reviewOwner, err := h.Q.FindUnambiguousIdentityByPrimaryEmail(ctx, form.reviewOwnerEmailInput)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return refs, nil, err
			}
			count, countErr := h.Q.CountIdentitiesByPrimaryEmail(ctx, form.reviewOwnerEmailInput)
			if countErr != nil {
				return refs, nil, countErr
			}
			if count > 1 {
				return refs, &viewmodels.AlertViewData{
					Title:       "Review owner email is ambiguous",
					Message:     "More than one identity claims this email. Resolve the conflict before assigning this owner.",
					Destructive: true,
				}, nil
			}
			return refs, &viewmodels.AlertViewData{
				Title:       "Review owner not found",
				Message:     "Assign a review owner using an existing identity email address.",
				Destructive: true,
			}, nil
		}
		refs.reviewOwnerIdentityID = pgtype.Int8{Int64: reviewOwner.ID, Valid: true}
	}

	return refs, nil, nil
}

func (h *Handlers) validateDiscoveryGovernanceUpdate(ctx context.Context, summary gen.GetSaaSAppByIDRow, form discoveryGovernanceFormInput, identityRefs discoveryGovernanceIdentityRefs) (discoveryGovernanceValidatedInput, *viewmodels.AlertViewData, error) {
	validated := discoveryGovernanceValidatedInput{}

	if discoveryDispositionRequiresOwner(form.reviewDispositionInput) && !identityRefs.ownerIdentityID.Valid {
		return validated, &viewmodels.AlertViewData{
			Title:       "Owner required",
			Message:     "Assign an accountable owner before saving this disposition.",
			Destructive: true,
		}, nil
	}

	followUpDueDate, err := parseDateInput(form.followUpDueDateInput)
	if err != nil {
		return validated, &viewmodels.AlertViewData{
			Title:       "Invalid due date",
			Message:     "Enter a valid follow-up date.",
			Destructive: true,
		}, nil
	}
	if isDateOverdue(followUpDueDate) {
		return validated, &viewmodels.AlertViewData{
			Title:       "Invalid due date",
			Message:     "Follow-up date must be today or in the future.",
			Destructive: true,
		}, nil
	}
	validated.followUpDueDate = followUpDueDate

	if form.reviewDispositionInput == "replace" {
		if form.replacementSaaSAppIDInput <= 0 {
			return validated, &viewmodels.AlertViewData{
				Title:       "Replacement required",
				Message:     "Choose a managed replacement app before saving a replace decision.",
				Destructive: true,
			}, nil
		}
		if form.replacementSaaSAppIDInput == summary.ID {
			return validated, &viewmodels.AlertViewData{
				Title:       "Invalid replacement",
				Message:     "Choose a different managed app as the replacement target.",
				Destructive: true,
			}, nil
		}

		replacement, err := h.Q.GetSaaSAppReplacementCandidateByID(ctx, form.replacementSaaSAppIDInput)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return validated, &viewmodels.AlertViewData{
					Title:       "Replacement not found",
					Message:     "Choose a managed discovered app as the replacement target.",
					Destructive: true,
				}, nil
			}
			return validated, nil, err
		}
		if !strings.EqualFold(strings.TrimSpace(replacement.ManagedState), "managed") {
			return validated, &viewmodels.AlertViewData{
				Title:       "Replacement must be managed",
				Message:     "Choose a replacement target that is currently managed.",
				Destructive: true,
			}, nil
		}

		validated.replacementRef = pgtype.Int8{Int64: form.replacementSaaSAppIDInput, Valid: true}
	}

	if len(form.notesInput) > 4000 {
		return validated, &viewmodels.AlertViewData{
			Title:       "Notes too long",
			Message:     "Keep governance notes under 4000 characters.",
			Destructive: true,
		}, nil
	}

	return validated, nil, nil
}

func (h *Handlers) persistDiscoveryGovernanceUpdate(ctx context.Context, appID int64, form discoveryGovernanceFormInput, identityRefs discoveryGovernanceIdentityRefs, validated discoveryGovernanceValidatedInput, principal auth.Principal) error {
	authUserID := pgtype.Int8{Int64: principal.UserID, Valid: principal.UserID > 0}
	return h.WithTx(ctx, func(qtx *gen.Queries) error {
		if _, err := qtx.UpsertSaaSAppReviewGovernance(ctx, gen.UpsertSaaSAppReviewGovernanceParams{
			SaasAppID:             appID,
			OwnerIdentityID:       identityRefs.ownerIdentityID,
			TicketRef:             form.ticketRefInput,
			Notes:                 form.notesInput,
			ReviewDisposition:     form.reviewDispositionInput,
			ReviewOwnerIdentityID: identityRefs.reviewOwnerIdentityID,
			FollowUpDueDate:       validated.followUpDueDate,
			ReplacementSaasAppID:  validated.replacementRef,
			UpdatedByAuthUserID:   authUserID,
		}); err != nil {
			return err
		}

		if err := qtx.InsertSaaSAppReviewDecision(ctx, gen.InsertSaaSAppReviewDecisionParams{
			SaasAppID:             appID,
			OwnerIdentityID:       identityRefs.ownerIdentityID,
			ReviewOwnerIdentityID: identityRefs.reviewOwnerIdentityID,
			ReviewDisposition:     form.reviewDispositionInput,
			TicketRef:             form.ticketRefInput,
			Notes:                 form.notesInput,
			FollowUpDueDate:       validated.followUpDueDate,
			ReplacementSaasAppID:  validated.replacementRef,
			ChangedByAuthUserID:   authUserID,
		}); err != nil {
			return err
		}
		return readmodels.NewProjector(nil, qtx, readmodels.RefreshConfigFromConfig(h.Cfg)).RefreshSaaSAppRiskReadModelByID(ctx, appID)
	})
}

func (h *Handlers) renderDiscoveryGovernanceSuccess(c *echo.Context, appID int64) error {
	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Discovery governance saved",
		Description: "The discovery disposition and review details were updated.",
	})
	if isHX(c) {
		return h.renderDiscoveryAppShow(c, appID, discoveryAppShowOptions{
			alert: &viewmodels.AlertViewData{
				Title:       "Discovery governance saved",
				Message:     "The discovery disposition and review details were updated.",
				Destructive: false,
			},
		})
	}
	return c.Redirect(http.StatusSeeOther, discoveryAppHref(appID))
}

func (h *Handlers) HandleDiscoveryReplacementCandidates(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	query := strings.TrimSpace(c.QueryParam("replacement_query"))
	if query == "" {
		query = strings.TrimSpace(c.QueryParam("q"))
	}
	excludeID := parseOptionalPositiveInt64(c.QueryParam("exclude_id"))
	selectedReplacementID := parseOptionalPositiveInt64(c.QueryParam("replacement_saas_app_id"))

	data, err := h.buildDiscoveryReplacementCandidatesViewData(ctx, excludeID, query, selectedReplacementID)
	if err != nil {
		return h.RenderError(c, err)
	}
	return h.RenderComponent(c, views.DiscoveryReplacementCandidateResults(data))
}

func (h *Handlers) renderDiscoveryAppShow(c *echo.Context, appID int64, opts discoveryAppShowOptions) error {
	addVary(c, "HX-Request", "HX-Target")

	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Discovery App")
	if err != nil {
		return h.RenderError(c, err)
	}

	data, err := h.buildDiscoveryAppShowViewData(ctx, layout, appID, opts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	if isHX(c) && isHXTarget(c, "discovery-app-show-shell") {
		return h.RenderComponent(c, views.DiscoveryAppShowBody(data))
	}
	return h.RenderComponent(c, views.DiscoveryAppShowPage(data))
}

func (h *Handlers) buildDiscoveryAppShowViewData(ctx context.Context, layout viewmodels.LayoutData, appID int64, opts discoveryAppShowOptions) (viewmodels.DiscoveryAppShowViewData, error) {
	data := viewmodels.DiscoveryAppShowViewData{}

	app, err := h.Q.GetSaaSAppByID(ctx, appID)
	if err != nil {
		return data, err
	}

	displayName := strings.TrimSpace(app.DisplayName)
	if displayName == "" {
		displayName = strings.TrimSpace(app.CanonicalKey)
	}
	domainLabel, vendorLabel := discoveryAppSecondaryLabels(displayName, app.PrimaryDomain, app.VendorName)

	sources, err := h.Q.ListSaaSAppSourcesBySaaSAppID(ctx, appID)
	if err != nil {
		return data, err
	}
	sourceItems := make([]viewmodels.DiscoverySourceEvidenceItem, 0, len(sources))
	for _, source := range sources {
		sourceItems = append(sourceItems, viewmodels.DiscoverySourceEvidenceItem{
			SourceKind:      strings.TrimSpace(source.SourceKind),
			SourceName:      strings.TrimSpace(source.SourceName),
			SourceAppID:     fallbackDash(strings.TrimSpace(source.SourceAppID)),
			SourceAppName:   fallbackDash(strings.TrimSpace(source.SourceAppName)),
			SourceAppDomain: fallbackDash(strings.TrimSpace(source.SourceAppDomain)),
			LastObservedAt:  calendarDateDisplay(source.LastObservedAt),
		})
	}

	events, err := h.Q.ListSaaSAppEventsBySaaSAppID(ctx, gen.ListSaaSAppEventsBySaaSAppIDParams{
		SaasAppID: appID,
		LimitRows: 100,
	})
	if err != nil {
		return data, err
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
			ObservedAt:    calendarDateDisplay(event.ObservedAt),
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
		return data, err
	}
	actorItems := make([]viewmodels.DiscoveryActorItem, 0, len(actors))
	for _, actor := range actors {
		actorItems = append(actorItems, viewmodels.DiscoveryActorItem{
			ActorLabel:      fallbackDash(strings.TrimSpace(actor.ActorLabel)),
			ActorEmail:      fallbackDash(strings.TrimSpace(actor.ActorEmail)),
			ActorExternalID: fallbackDash(strings.TrimSpace(actor.ActorExternalID)),
			EventCount:      actor.EventCount,
			LastObservedAt:  calendarDateDisplay(actor.LastObservedAt),
		})
	}

	historyRows, err := h.Q.ListSaaSAppReviewDecisionsBySaaSAppID(ctx, gen.ListSaaSAppReviewDecisionsBySaaSAppIDParams{
		SaasAppID: appID,
		LimitRows: 25,
	})
	if err != nil {
		return data, err
	}
	historyItems := make([]viewmodels.DiscoveryReviewDecisionItem, 0, len(historyRows))
	for _, row := range historyRows {
		historyItems = append(historyItems, viewmodels.DiscoveryReviewDecisionItem{
			ChangedAt:                calendarDateDisplay(row.ChangedAt),
			ChangedBy:                fallbackDash(strings.TrimSpace(row.ChangedByAuthUserEmail)),
			Owner:                    discoveryOwnerLabel(row.OwnerDisplayName, row.OwnerPrimaryEmail),
			ReviewOwner:              discoveryOwnerLabel(row.ReviewOwnerDisplayName, row.ReviewOwnerPrimaryEmail),
			ReviewDisposition:        normalizeDiscoveryReviewDisposition(row.ReviewDisposition),
			FollowUpDueDate:          dateDisplay(row.FollowUpDueDate),
			IsFollowUpOverdue:        false,
			TicketRef:                strings.TrimSpace(row.TicketRef),
			Notes:                    strings.TrimSpace(row.Notes),
			ReplacementDisplayName:   strings.TrimSpace(row.ReplacementDisplayName),
			ReplacementPrimaryDomain: strings.TrimSpace(row.ReplacementPrimaryDomain),
		})
	}

	accountableOwnerEmailInput := auth.NormalizeEmail(app.OwnerPrimaryEmail)
	reviewOwnerEmailInput := auth.NormalizeEmail(app.ReviewOwnerPrimaryEmail)
	reviewDispositionInput := normalizeDiscoveryReviewDisposition(app.ReviewDisposition)
	if reviewDispositionInput == "" {
		reviewDispositionInput = "unreviewed"
	}
	followUpDueDateInput := dateDisplay(app.FollowUpDueDate).Label
	ticketRefInput := strings.TrimSpace(app.TicketRef)
	notesInput := strings.TrimSpace(app.Notes)
	replacementQueryInput := ""
	replacementSaaSAppIDInput := app.ReplacementSaasAppID
	if opts.governanceForm != nil {
		accountableOwnerEmailInput = auth.NormalizeEmail(opts.governanceForm.accountableOwnerEmailInput)
		reviewOwnerEmailInput = auth.NormalizeEmail(opts.governanceForm.reviewOwnerEmailInput)
		if normalized := normalizeDiscoveryReviewDisposition(opts.governanceForm.reviewDispositionInput); normalized != "" {
			reviewDispositionInput = normalized
		}
		followUpDueDateInput = strings.TrimSpace(opts.governanceForm.followUpDueDateInput)
		ticketRefInput = strings.TrimSpace(opts.governanceForm.ticketRefInput)
		notesInput = strings.TrimSpace(opts.governanceForm.notesInput)
		replacementQueryInput = strings.TrimSpace(opts.governanceForm.replacementQueryInput)
		replacementSaaSAppIDInput = opts.governanceForm.replacementSaaSAppIDInput
	}

	var replacementPicker viewmodels.DiscoveryReplacementCandidatesViewData
	if layout.IsAdmin {
		replacementPicker, err = h.buildDiscoveryReplacementCandidatesViewData(ctx, appID, replacementQueryInput, replacementSaaSAppIDInput)
		if err != nil {
			return data, err
		}
	}

	data = viewmodels.DiscoveryAppShowViewData{
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
			Owner:                        discoveryOwnerLabel(app.OwnerDisplayName, app.OwnerPrimaryEmail),
			ReviewOwner:                  discoveryOwnerLabel(app.ReviewOwnerDisplayName, app.ReviewOwnerPrimaryEmail),
			ReviewDisposition:            normalizeDiscoveryReviewDisposition(app.ReviewDisposition),
			FollowUpDueDate:              dateDisplay(app.FollowUpDueDate),
			IsFollowUpOverdue:            app.IsFollowUpOverdue,
			TicketRef:                    strings.TrimSpace(app.TicketRef),
			Notes:                        strings.TrimSpace(app.Notes),
			ReplacementDisplayName:       strings.TrimSpace(app.ReplacementDisplayName),
			ReplacementPrimaryDomain:     strings.TrimSpace(app.ReplacementPrimaryDomain),
			FirstSeen:                    calendarDateDisplay(app.FirstSeenAt),
			LastSeen:                     calendarDateDisplay(app.LastSeenAt),
		},
		Sources:                    sourceItems,
		TopActors:                  actorItems,
		Events:                     eventItems,
		DecisionHistory:            historyItems,
		Alert:                      opts.alert,
		AccountableOwnerEmailInput: accountableOwnerEmailInput,
		ReviewOwnerEmailInput:      reviewOwnerEmailInput,
		ReviewDispositionInput:     reviewDispositionInput,
		FollowUpDueDateInput:       followUpDueDateInput,
		TicketRefInput:             ticketRefInput,
		NotesInput:                 notesInput,
		ReplacementQueryInput:      replacementQueryInput,
		ReplacementPicker:          replacementPicker,
		OpenGovernanceDialog:       opts.openGovernanceDialog,
		HasSources:                 len(sourceItems) > 0,
		HasTopActors:               len(actorItems) > 0,
		HasEvents:                  len(eventItems) > 0,
		HasDecisionHistory:         len(historyItems) > 0,
	}
	return data, nil
}

func (h *Handlers) buildDiscoveryReplacementCandidatesViewData(ctx context.Context, excludeID int64, query string, selectedReplacementID int64) (viewmodels.DiscoveryReplacementCandidatesViewData, error) {
	data := viewmodels.DiscoveryReplacementCandidatesViewData{}

	if selectedReplacementID > 0 {
		selected, err := h.Q.GetSaaSAppReplacementCandidateByID(ctx, selectedReplacementID)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return data, err
			}
		} else if selected.ID != excludeID {
			item := discoveryReplacementCandidateItem(selected.ID, selected.DisplayName, selected.PrimaryDomain, selected.VendorName, selected.ManagedState, selected.RiskLevel, true)
			data.SelectedCandidate = &item
		}
	}

	query = strings.TrimSpace(query)
	switch {
	case query == "":
		data.EmptyStateMsg = "Type at least 2 characters to search managed discovery apps."
		return data, nil
	case len([]rune(query)) < 2:
		data.EmptyStateMsg = "Type at least 2 characters to search managed discovery apps."
		return data, nil
	}

	rows, err := h.Q.SearchManagedReplacementSaaSApps(ctx, gen.SearchManagedReplacementSaaSAppsParams{
		ExcludeID: excludeID,
		Query:     query,
		LimitRows: discoveryReplacementCandidateLimit,
	})
	if err != nil {
		return data, err
	}

	candidates := make([]viewmodels.DiscoveryReplacementCandidateItem, 0, len(rows))
	for _, row := range rows {
		if data.SelectedCandidate != nil && row.ID == data.SelectedCandidate.ID {
			data.SelectedCandidate.IsSelected = true
		}
		candidates = append(candidates, discoveryReplacementCandidateItem(
			row.ID,
			row.DisplayName,
			row.PrimaryDomain,
			row.VendorName,
			row.ManagedState,
			row.RiskLevel,
			data.SelectedCandidate != nil && row.ID == data.SelectedCandidate.ID,
		))
	}
	data.Candidates = candidates
	data.HasCandidates = len(candidates) > 0
	if !data.HasCandidates {
		data.EmptyStateMsg = "No managed discovery apps match the current search."
	}
	return data, nil
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

func discoveryDispositionRequiresOwner(disposition string) bool {
	switch normalizeDiscoveryReviewDisposition(disposition) {
	case "sanctioned", "tolerated", "replace":
		return true
	default:
		return false
	}
}

func normalizeDiscoveryReviewDisposition(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return ""
	case "unreviewed":
		return "unreviewed"
	case "under_review", "under review":
		return "under_review"
	case "sanctioned":
		return "sanctioned"
	case "tolerated":
		return "tolerated"
	case "replace":
		return "replace"
	default:
		return ""
	}
}

func parseOptionalPositiveInt64(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func parseDateInput(value string) (pgtype.Date, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return pgtype.Date{}, nil
	}
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return pgtype.Date{}, err
	}
	return pgtype.Date{Time: parsed.UTC(), Valid: true}, nil
}

func isDateOverdue(value pgtype.Date) bool {
	if !value.Valid {
		return false
	}
	now := time.Now().UTC()
	current := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	due := time.Date(value.Time.UTC().Year(), value.Time.UTC().Month(), value.Time.UTC().Day(), 0, 0, 0, 0, time.UTC)
	return due.Before(current)
}

func discoveryReplacementCandidateItem(id int64, displayName, domain, vendorName, managedState, riskLevel string, selected bool) viewmodels.DiscoveryReplacementCandidateItem {
	return viewmodels.DiscoveryReplacementCandidateItem{
		ID:           id,
		DisplayName:  strings.TrimSpace(displayName),
		Domain:       strings.TrimSpace(domain),
		VendorName:   strings.TrimSpace(vendorName),
		ManagedState: strings.TrimSpace(managedState),
		RiskLevel:    strings.TrimSpace(riskLevel),
		IsSelected:   selected,
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
