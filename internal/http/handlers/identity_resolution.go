package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/events"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

const identityResolutionCandidatesPerPage = 50

type identityResolutionCandidateFilters struct {
	Status         string
	Group          string
	ConfidenceBand string
	MatchReason    string
}

func (h *Handlers) HandleIdentityResolutionReview(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Identity Resolution")
	if err != nil {
		return h.RenderError(c, err)
	}

	page := 1
	if raw := strings.TrimSpace(c.QueryParam("page")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err == nil && parsed > 0 {
			page = parsed
		}
	}

	filters := identityResolutionFiltersFromRequest(c)
	totalCount, err := h.Q.CountIdentityMatchCandidatesByFilters(ctx, gen.CountIdentityMatchCandidatesByFiltersParams{
		Status:         filters.Status,
		ConfidenceBand: filters.ConfidenceBand,
		MatchReason:    filters.MatchReason,
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	pagination := newPaginatedListState(totalCount, page, identityResolutionCandidatesPerPage)
	rows, err := h.Q.ListIdentityMatchCandidateDetailsByFilters(ctx, gen.ListIdentityMatchCandidateDetailsByFiltersParams{
		Status:         filters.Status,
		ConfidenceBand: filters.ConfidenceBand,
		MatchReason:    filters.MatchReason,
		PageLimit:      int32(identityResolutionCandidatesPerPage),
		PageOffset:     int32(pagination.Offset()),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	evidenceByCandidateID, err := h.identityResolutionEvidenceByCandidateID(ctx, rows)
	if err != nil {
		return h.RenderError(c, err)
	}

	items := make([]viewmodels.IdentityResolutionCandidateItem, 0, len(rows))
	now := time.Now().UTC()
	for _, row := range rows {
		evidence := evidenceByCandidateID[row.ID]
		items = append(items, identityResolutionCandidateItem(now, row, evidence))
	}

	data := viewmodels.IdentityResolutionViewData{
		PaginatedListPageData: pagination.PageData(layout, len(items), "No pending identity resolution candidates.", ""),
		Items:                 items,
		HasItems:              len(items) > 0,
		Status:                filters.Status,
		StatusLabel:           identityResolutionStatusLabel(filters.Status),
		Group:                 filters.Group,
		StatusTabs:            identityResolutionStatusTabs(filters),
		GroupFilters:          identityResolutionGroupFilters(filters),
	}
	return h.renderListWithHX(c, "identity-resolution-results", views.IdentityResolutionPageResults(data), views.IdentityResolutionPage(data))
}

func (h *Handlers) identityResolutionEvidenceByCandidateID(ctx context.Context, rows []gen.ListIdentityMatchCandidateDetailsByFiltersRow) (map[int64][]viewmodels.IdentityResolutionEvidenceItem, error) {
	out := make(map[int64][]viewmodels.IdentityResolutionEvidenceItem, len(rows))
	if len(rows) == 0 {
		return out, nil
	}
	candidateIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		candidateIDs = append(candidateIDs, row.ID)
	}
	evidenceRows, err := h.Q.ListIdentityLinkEvidenceForCandidateIDs(ctx, candidateIDs)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	for _, row := range evidenceRows {
		if !row.CandidateID.Valid {
			continue
		}
		candidateID := row.CandidateID.Int64
		out[candidateID] = append(out[candidateID], identityResolutionEvidenceItem(now, row))
	}
	return out, nil
}

func (h *Handlers) HandleIdentityResolutionCandidateAccept(c *echo.Context) error {
	candidateID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return h.RenderPageNotFound(c)
	}
	ctx := c.Request().Context()
	reviewedBy := identityResolutionReviewedBy(c)
	reviewNote := identityResolutionReviewNote(c)
	mergeProvisional := identityResolutionBoolInput(c, "merge_provisional")

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	defer tx.Rollback(ctx)
	qtx := h.Q.WithTx(tx)

	candidate, err := qtx.GetIdentityMatchCandidateForUpdate(ctx, candidateID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return h.RenderPageNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if candidate.Status != "pending" {
		return h.identityResolutionConflict(c, "candidate is not pending")
	}

	account, err := qtx.GetAccountForIdentityResolutionUpdate(ctx, candidate.AccountID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return h.RenderPageNotFound(c)
		}
		return h.RenderError(c, err)
	}
	targetIdentity, err := qtx.GetIdentityForIdentityResolutionUpdate(ctx, candidate.CandidateIdentityID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return h.RenderPageNotFound(c)
		}
		return h.RenderError(c, err)
	}
	switch strings.TrimSpace(targetIdentity.ResolutionState) {
	case "merged", "disabled":
		return h.identityResolutionConflict(c, "candidate target identity is not active")
	}
	if _, err := qtx.GetIdentityAccountLinkByAccountIDForUpdate(ctx, candidate.AccountID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return h.RenderError(c, err)
	}

	if _, err := qtx.UpsertIdentityAccountLink(ctx, gen.UpsertIdentityAccountLinkParams{
		IdentityID: candidate.CandidateIdentityID,
		AccountID:  candidate.AccountID,
		LinkReason: "manual",
		Confidence: 1,
	}); err != nil {
		return h.RenderError(c, err)
	}
	if email := strings.ToLower(strings.TrimSpace(account.Email)); email != "" {
		if _, err := qtx.UpsertIdentityEmail(ctx, gen.UpsertIdentityEmailParams{
			IdentityID:        candidate.CandidateIdentityID,
			Email:             strings.TrimSpace(account.Email),
			NormalizedEmail:   email,
			EmailKind:         "login",
			VerificationState: "manual",
			LifecycleState:    "active",
			IsPrimary:         false,
			SourceKind:        pgtype.Text{String: strings.TrimSpace(account.SourceKind), Valid: strings.TrimSpace(account.SourceKind) != ""},
			SourceName:        pgtype.Text{String: strings.TrimSpace(account.SourceName), Valid: strings.TrimSpace(account.SourceName) != ""},
			SourceAccountID:   pgtype.Int8{Int64: account.ID, Valid: account.ID > 0},
		}); err != nil {
			return h.RenderError(c, err)
		}
	}

	if _, err := qtx.UpsertCandidateLinkEvidence(ctx, gen.UpsertCandidateLinkEvidenceParams{
		AccountID:     candidate.AccountID,
		IdentityID:    pgtype.Int8{Int64: candidate.CandidateIdentityID, Valid: true},
		CandidateID:   candidate.ID,
		EvidenceType:  "manual_override",
		EvidenceKey:   "review_decision",
		IdentityValue: pgtype.Text{String: strconv.FormatInt(candidate.CandidateIdentityID, 10), Valid: true},
		Strength:      100,
		IsPositive:    true,
		Metadata:      []byte(`{"action":"accept"}`),
	}); err != nil {
		return h.RenderError(c, err)
	}
	if err := qtx.AcceptIdentityMatchCandidate(ctx, gen.AcceptIdentityMatchCandidateParams{
		ID:         candidate.ID,
		ReviewedBy: nullableText(reviewedBy),
		ReviewNote: nullableText(reviewNote),
	}); err != nil {
		return h.RenderError(c, err)
	}
	if err := qtx.SupersedeCompetingIdentityMatchCandidates(ctx, gen.SupersedeCompetingIdentityMatchCandidatesParams{
		AccountID:           candidate.AccountID,
		AcceptedCandidateID: candidate.ID,
	}); err != nil {
		return h.RenderError(c, err)
	}
	if mergeProvisional {
		if !candidate.ProvisionalIdentityID.Valid {
			return h.identityResolutionConflict(c, "candidate has no provisional identity to merge")
		}
		mergedIdentityID := candidate.ProvisionalIdentityID.Int64
		if err := h.applyProvisionalIdentityMerge(ctx, qtx, mergedIdentityID, candidate.CandidateIdentityID, candidate.ID, reviewedBy, reviewNote); err != nil {
			return h.RenderError(c, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return h.RenderError(c, err)
	}

	return h.identityResolutionHTMLMutationSuccess(c, "accepted", viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Candidate accepted",
		Description: "The source account is now linked to the selected identity.",
	})
}

func (h *Handlers) applyProvisionalIdentityMerge(ctx context.Context, qtx *gen.Queries, sourceIdentityID, targetIdentityID, candidateID int64, reviewedBy, reviewNote string) error {
	if sourceIdentityID <= 0 || targetIdentityID <= 0 || sourceIdentityID == targetIdentityID {
		return errors.New("invalid provisional merge target")
	}
	sourceIdentity, err := qtx.GetIdentityForIdentityResolutionUpdate(ctx, sourceIdentityID)
	if err != nil {
		return err
	}
	targetIdentity, err := qtx.GetIdentityForIdentityResolutionUpdate(ctx, targetIdentityID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(sourceIdentity.ResolutionState) != "provisional" && strings.TrimSpace(sourceIdentity.ResolutionState) != "needs_review" {
		return errors.New("source identity is not provisional")
	}
	if strings.TrimSpace(targetIdentity.ResolutionState) == "merged" || strings.TrimSpace(targetIdentity.ResolutionState) == "disabled" {
		return errors.New("target identity is not active")
	}
	wouldCreateCycle, err := qtx.IdentityMergeWouldCreateCycle(ctx, gen.IdentityMergeWouldCreateCycleParams{
		SourceIdentityID: sourceIdentityID,
		TargetIdentityID: targetIdentityID,
	})
	if err != nil {
		return err
	}
	if wouldCreateCycle {
		return errors.New("identity merge would create a redirect cycle")
	}

	metadata := identityResolutionJSONMetadata(map[string]any{
		"action":       "accept_and_merge_provisional",
		"candidate_id": candidateID,
		"review_note":  reviewNote,
	})
	mergeEvent, err := qtx.CreateIdentityMergeEvent(ctx, gen.CreateIdentityMergeEventParams{
		SourceIdentityID: sourceIdentityID,
		TargetIdentityID: targetIdentityID,
		Status:           "pending",
		Reason:           "manual_review",
		ReviewedBy:       nullableText(reviewedBy),
		Metadata:         metadata,
	})
	if err != nil {
		return err
	}
	if err := qtx.MoveIdentityAccountsToIdentity(ctx, gen.MoveIdentityAccountsToIdentityParams{
		TargetIdentityID: targetIdentityID,
		LinkReason:       "manual_merge",
		ReviewedBy:       nullableText(reviewedBy),
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return err
	}
	if err := qtx.MoveIdentityEmailsToIdentity(ctx, gen.MoveIdentityEmailsToIdentityParams{
		TargetIdentityID: targetIdentityID,
		ReviewedBy:       nullableText(reviewedBy),
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return err
	}
	if err := qtx.MoveIdentityAnchorsToIdentity(ctx, gen.MoveIdentityAnchorsToIdentityParams{
		TargetIdentityID: targetIdentityID,
		ReviewedBy:       nullableText(reviewedBy),
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return err
	}
	if err := qtx.MoveAccountIdentityRelationshipsToIdentity(ctx, gen.MoveAccountIdentityRelationshipsToIdentityParams{
		TargetIdentityID: targetIdentityID,
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return err
	}
	if err := qtx.RetireRemainingAccountIdentityRelationships(ctx, gen.RetireRemainingAccountIdentityRelationshipsParams{
		TargetIdentityID: targetIdentityID,
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return err
	}
	if err := qtx.MarkIdentityMerged(ctx, sourceIdentityID); err != nil {
		return err
	}
	if _, err := qtx.UpsertIdentityMergeRedirect(ctx, gen.UpsertIdentityMergeRedirectParams{
		SourceIdentityID: sourceIdentityID,
		TargetIdentityID: targetIdentityID,
		MergeEventID:     mergeEvent.ID,
	}); err != nil {
		return err
	}
	if _, err := qtx.MarkIdentityMergeEventApplied(ctx, mergeEvent.ID); err != nil {
		return err
	}
	return nil
}

func (h *Handlers) HandleIdentityResolutionCandidateReject(c *echo.Context) error {
	candidateID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return h.RenderPageNotFound(c)
	}

	ctx := c.Request().Context()
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	defer tx.Rollback(ctx)
	qtx := h.Q.WithTx(tx)

	candidate, err := qtx.GetIdentityMatchCandidateForUpdate(ctx, candidateID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return h.RenderPageNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if candidate.Status != "pending" {
		return h.identityResolutionConflict(c, "candidate is not pending")
	}

	reviewedBy := identityResolutionReviewedBy(c)
	reviewNote := identityResolutionReviewNote(c)
	if err := qtx.RejectIdentityMatchCandidate(ctx, gen.RejectIdentityMatchCandidateParams{
		ID:         candidateID,
		ReviewedBy: nullableText(reviewedBy),
		ReviewNote: nullableText(reviewNote),
	}); err != nil {
		return h.RenderError(c, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return h.RenderError(c, err)
	}
	return h.identityResolutionHTMLMutationSuccess(c, "rejected", viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Candidate rejected",
		Description: "The candidate will stay suppressed until its evidence changes.",
	})
}

func (h *Handlers) HandleIdentityResolutionCandidateMarkService(c *echo.Context) error {
	return h.handleIdentityResolutionCandidateMarkServiceLike(c, "service")
}

func (h *Handlers) HandleIdentityResolutionCandidateMarkShared(c *echo.Context) error {
	return h.handleIdentityResolutionCandidateMarkServiceLike(c, "shared")
}

func (h *Handlers) handleIdentityResolutionCandidateMarkServiceLike(c *echo.Context, mode string) error {
	candidateID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return h.RenderPageNotFound(c)
	}
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "service" && mode != "shared" {
		return h.identityResolutionConflict(c, "invalid account classification")
	}
	relationshipType := identityResolutionRelationshipTypeInput(c)

	ctx := c.Request().Context()
	reviewedBy := identityResolutionReviewedBy(c)
	reviewNote := identityResolutionReviewNote(c)

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	defer tx.Rollback(ctx)
	qtx := h.Q.WithTx(tx)

	candidate, err := qtx.GetIdentityMatchCandidateForUpdate(ctx, candidateID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return h.RenderPageNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if candidate.Status != "pending" {
		return h.identityResolutionConflict(c, "candidate is not pending")
	}
	account, err := qtx.GetAccountForIdentityResolutionUpdate(ctx, candidate.AccountID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return h.RenderPageNotFound(c)
		}
		return h.RenderError(c, err)
	}
	link, err := qtx.GetIdentityAccountLinkByAccountIDForUpdate(ctx, candidate.AccountID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return h.identityResolutionConflict(c, "account has no current identity rollup")
		}
		return h.RenderError(c, err)
	}

	entityCategory := "service_account"
	if mode == "shared" {
		entityCategory = "entity"
	}
	if _, err := qtx.UpdateAccountIdentityResolutionClassification(ctx, gen.UpdateAccountIdentityResolutionClassificationParams{
		AccountKind:    "service",
		EntityCategory: entityCategory,
		AccountID:      candidate.AccountID,
	}); err != nil {
		return h.RenderError(c, err)
	}
	if _, err := qtx.UpdateIdentityResolutionClassification(ctx, gen.UpdateIdentityResolutionClassificationParams{
		Kind:            "service",
		IdentityKind:    mode,
		ResolutionState: "confirmed",
		IdentityID:      link.IdentityID,
	}); err != nil {
		return h.RenderError(c, err)
	}
	linkReason := "manual_service"
	evidenceType := "negative_service_account"
	if mode == "shared" {
		linkReason = "manual_shared"
		evidenceType = "negative_account_kind"
	}
	if _, err := qtx.UpsertIdentityAccountLink(ctx, gen.UpsertIdentityAccountLinkParams{
		IdentityID: link.IdentityID,
		AccountID:  candidate.AccountID,
		LinkReason: linkReason,
		Confidence: 1,
	}); err != nil {
		return h.RenderError(c, err)
	}
	if _, err := qtx.UpsertCandidateLinkEvidence(ctx, gen.UpsertCandidateLinkEvidenceParams{
		AccountID:     candidate.AccountID,
		IdentityID:    pgtype.Int8{Int64: link.IdentityID, Valid: true},
		CandidateID:   candidate.ID,
		EvidenceType:  evidenceType,
		EvidenceKey:   "review_decision",
		AccountValue:  pgtype.Text{String: account.Email, Valid: strings.TrimSpace(account.Email) != ""},
		IdentityValue: pgtype.Text{String: mode, Valid: true},
		SourceKind:    pgtype.Text{String: account.SourceKind, Valid: strings.TrimSpace(account.SourceKind) != ""},
		SourceName:    pgtype.Text{String: account.SourceName, Valid: strings.TrimSpace(account.SourceName) != ""},
		Strength:      -100,
		IsPositive:    false,
		Metadata: identityResolutionJSONMetadata(map[string]any{
			"action":      "mark_" + mode,
			"review_note": reviewNote,
		}),
	}); err != nil {
		return h.RenderError(c, err)
	}
	if relationshipType != "" {
		if _, err := qtx.UpsertAccountIdentityRelationship(ctx, gen.UpsertAccountIdentityRelationshipParams{
			AccountID:        candidate.AccountID,
			IdentityID:       candidate.CandidateIdentityID,
			RelationshipType: relationshipType,
			SourceKind:       pgtype.Text{String: account.SourceKind, Valid: strings.TrimSpace(account.SourceKind) != ""},
			SourceName:       pgtype.Text{String: account.SourceName, Valid: strings.TrimSpace(account.SourceName) != ""},
			Confidence:       identityResolutionRelationshipConfidence(relationshipType),
			LifecycleState:   "active",
		}); err != nil {
			return h.RenderError(c, err)
		}
	}
	if err := qtx.RejectPendingIdentityMatchCandidatesForAccount(ctx, gen.RejectPendingIdentityMatchCandidatesForAccountParams{
		AccountID:  candidate.AccountID,
		ReviewedBy: nullableText(reviewedBy),
		ReviewNote: nullableText(reviewNote),
	}); err != nil {
		return h.RenderError(c, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return h.RenderError(c, err)
	}

	return h.identityResolutionHTMLMutationSuccess(c, "classified", viewmodels.ToastViewData{
		Category:    "success",
		Title:       "Account classified",
		Description: "The source account is now excluded from human identity matching.",
	})
}

func identityResolutionCandidateItem(now time.Time, row gen.ListIdentityMatchCandidateDetailsByFiltersRow, evidence []viewmodels.IdentityResolutionEvidenceItem) viewmodels.IdentityResolutionCandidateItem {
	item := viewmodels.IdentityResolutionCandidateItem{
		ID:                          row.ID,
		AccountID:                   row.AccountID,
		AccountSourceKind:           strings.TrimSpace(row.AccountSourceKind),
		AccountSourceName:           strings.TrimSpace(row.AccountSourceName),
		AccountExternalID:           strings.TrimSpace(row.AccountExternalID),
		AccountEmail:                strings.TrimSpace(row.AccountEmail),
		AccountDisplayName:          fallbackNonEmpty(row.AccountDisplayName, row.AccountEmail, row.AccountExternalID),
		AccountKind:                 strings.TrimSpace(row.AccountKind),
		EntityCategory:              strings.TrimSpace(row.EntityCategory),
		CandidateIdentityID:         row.CandidateIdentityID,
		CandidateDisplayName:        fallbackNonEmpty(row.CandidateDisplayName, row.CandidatePrimaryEmail, strconv.FormatInt(row.CandidateIdentityID, 10)),
		CandidatePrimaryEmail:       strings.TrimSpace(row.CandidatePrimaryEmail),
		CandidateKind:               strings.TrimSpace(row.CandidateKind),
		CandidateResolutionState:    strings.TrimSpace(row.CandidateResolutionState),
		CandidateIdentityKind:       strings.TrimSpace(row.CandidateIdentityKind),
		CandidateHref:               "/identities/" + strconv.FormatInt(row.CandidateIdentityID, 10),
		Status:                      strings.TrimSpace(row.Status),
		ConfidenceBand:              strings.TrimSpace(row.ConfidenceBand),
		Score:                       row.Score,
		MatchReason:                 strings.TrimSpace(row.MatchReason),
		AmbiguityKey:                strings.TrimSpace(row.AmbiguityKey.String),
		ResolverVersion:             strings.TrimSpace(row.ResolverVersion),
		ResolverFingerprint:         strings.TrimSpace(row.ResolverFingerprint),
		CreatedAt:                   relativeWithTitleDisplay(now, row.CreatedAt, "—", ""),
		Evidence:                    evidence,
		HasEvidence:                 len(evidence) > 0,
		AcceptHref:                  "/identity-resolution/candidates/" + strconv.FormatInt(row.ID, 10) + "/accept",
		AcceptMergeHref:             "/identity-resolution/candidates/" + strconv.FormatInt(row.ID, 10) + "/accept",
		RejectHref:                  "/identity-resolution/candidates/" + strconv.FormatInt(row.ID, 10) + "/reject",
		MarkServiceHref:             "/identity-resolution/candidates/" + strconv.FormatInt(row.ID, 10) + "/mark-service",
		MarkServiceCustodianHref:    "/identity-resolution/candidates/" + strconv.FormatInt(row.ID, 10) + "/mark-service",
		MarkSharedHref:              "/identity-resolution/candidates/" + strconv.FormatInt(row.ID, 10) + "/mark-shared",
		CanAccept:                   strings.TrimSpace(row.Status) == "pending",
		CanReject:                   strings.TrimSpace(row.Status) == "pending",
		CanMarkService:              strings.TrimSpace(row.Status) == "pending",
		CanMarkServiceCustodian:     strings.TrimSpace(row.Status) == "pending",
		CanMarkShared:               strings.TrimSpace(row.Status) == "pending",
		CanMergeProvisional:         strings.TrimSpace(row.Status) == "pending" && row.ProvisionalIdentityID.Valid && row.ProvisionalIdentityID.Int64 != row.CandidateIdentityID && canMergeResolutionState(row.ProvisionalResolutionState.String),
		RelationshipCount:           row.RelationshipCount,
		CurrentIdentityDisplayName:  fallbackNonEmpty(row.CurrentIdentityDisplayName.String, row.CurrentIdentityPrimaryEmail.String),
		CurrentIdentityPrimaryEmail: strings.TrimSpace(row.CurrentIdentityPrimaryEmail.String),
		CurrentLinkState:            strings.TrimSpace(row.CurrentLinkState.String),
		CurrentLinkReason:           strings.TrimSpace(row.CurrentLinkReason.String),
		ProvisionalDisplayName:      fallbackNonEmpty(row.ProvisionalDisplayName.String, row.ProvisionalPrimaryEmail.String),
		ProvisionalPrimaryEmail:     strings.TrimSpace(row.ProvisionalPrimaryEmail.String),
	}
	if row.CurrentIdentityID.Valid {
		item.CurrentIdentityID = row.CurrentIdentityID.Int64
		item.CurrentIdentityHref = "/identities/" + strconv.FormatInt(row.CurrentIdentityID.Int64, 10)
	}
	if row.ProvisionalIdentityID.Valid {
		item.ProvisionalIdentityID = row.ProvisionalIdentityID.Int64
		item.ProvisionalHref = "/identities/" + strconv.FormatInt(row.ProvisionalIdentityID.Int64, 10)
	}
	return item
}

func canMergeResolutionState(state string) bool {
	switch strings.TrimSpace(state) {
	case "provisional", "needs_review":
		return true
	default:
		return false
	}
}

func identityResolutionEvidenceItem(now time.Time, row gen.IdentityLinkEvidence) viewmodels.IdentityResolutionEvidenceItem {
	return viewmodels.IdentityResolutionEvidenceItem{
		ID:            row.ID,
		EvidenceType:  strings.TrimSpace(row.EvidenceType),
		EvidenceKey:   strings.TrimSpace(row.EvidenceKey),
		AccountValue:  strings.TrimSpace(row.AccountValue.String),
		IdentityValue: strings.TrimSpace(row.IdentityValue.String),
		SourceKind:    strings.TrimSpace(row.SourceKind.String),
		SourceName:    strings.TrimSpace(row.SourceName.String),
		Strength:      row.Strength,
		IsPositive:    row.IsPositive,
		ObservedAt:    relativeWithTitleDisplay(now, row.ObservedAt, "—", ""),
		Metadata:      strings.TrimSpace(string(row.Metadata)),
	}
}

func identityResolutionReviewedBy(c *echo.Context) string {
	if principal, ok := authn.PrincipalFromContext(c); ok {
		return strings.TrimSpace(principal.Email)
	}
	return ""
}

func identityResolutionFiltersFromRequest(c *echo.Context) identityResolutionCandidateFilters {
	status := strings.ToLower(strings.TrimSpace(c.QueryParam("status")))
	switch status {
	case "pending", "accepted", "rejected", "superseded", "expired":
	default:
		status = "pending"
	}

	group := strings.ToLower(strings.TrimSpace(c.QueryParam("group")))
	filters := identityResolutionCandidateFilters{Status: status, Group: group}
	switch group {
	case "ambiguous_email":
		filters.MatchReason = "ambiguous_primary_email"
	case "anchor_conflict":
		filters.MatchReason = "conflicting_anchors"
	case "service_shared":
		filters.MatchReason = "service_shared_warning"
	case "exact":
		filters.ConfidenceBand = "exact"
	case "high", "medium", "low", "conflict":
		filters.ConfidenceBand = group
	default:
		filters.Group = ""
	}
	return filters
}

func identityResolutionStatusTabs(filters identityResolutionCandidateFilters) []viewmodels.IdentityResolutionFilterOption {
	return []viewmodels.IdentityResolutionFilterOption{
		identityResolutionFilterOption("Pending", "pending", filters.Status, filters.Group),
		identityResolutionFilterOption("Accepted", "accepted", filters.Status, filters.Group),
		identityResolutionFilterOption("Rejected", "rejected", filters.Status, filters.Group),
		identityResolutionFilterOption("Superseded", "superseded", filters.Status, filters.Group),
	}
}

func identityResolutionStatusLabel(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "accepted":
		return "Accepted Candidates"
	case "rejected":
		return "Rejected Candidates"
	case "superseded":
		return "Superseded Candidates"
	case "expired":
		return "Expired Candidates"
	default:
		return "Pending Candidates"
	}
}

func identityResolutionGroupFilters(filters identityResolutionCandidateFilters) []viewmodels.IdentityResolutionFilterOption {
	groups := []struct {
		label string
		value string
	}{
		{label: "All", value: ""},
		{label: "Ambiguous email", value: "ambiguous_email"},
		{label: "Anchor conflict", value: "anchor_conflict"},
		{label: "Service/shared", value: "service_shared"},
		{label: "Exact", value: "exact"},
		{label: "High", value: "high"},
		{label: "Medium", value: "medium"},
		{label: "Low", value: "low"},
	}
	out := make([]viewmodels.IdentityResolutionFilterOption, 0, len(groups))
	for _, group := range groups {
		out = append(out, viewmodels.IdentityResolutionFilterOption{
			Label:    group.label,
			Value:    group.value,
			Href:     identityResolutionFilterHref(filters.Status, group.value, 1),
			Selected: filters.Group == group.value,
		})
	}
	return out
}

func identityResolutionFilterOption(label, value, selectedStatus, selectedGroup string) viewmodels.IdentityResolutionFilterOption {
	return viewmodels.IdentityResolutionFilterOption{
		Label:    label,
		Value:    value,
		Href:     identityResolutionFilterHref(value, selectedGroup, 1),
		Selected: selectedStatus == value,
	}
}

func identityResolutionFilterHref(status, group string, page int) string {
	values := url.Values{}
	if status != "" && status != "pending" {
		values.Set("status", status)
	}
	if group != "" {
		values.Set("group", group)
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	encoded := values.Encode()
	if encoded == "" {
		return "/identity-resolution"
	}
	return "/identity-resolution?" + encoded
}

func identityResolutionReviewNote(c *echo.Context) string {
	if c == nil || c.Request() == nil {
		return ""
	}
	if note := strings.TrimSpace(c.FormValue("review_note")); note != "" {
		return note
	}
	return strings.TrimSpace(c.Request().Header.Get("HX-Prompt"))
}

func (h *Handlers) identityResolutionHTMLMutationSuccess(c *echo.Context, status string, toast viewmodels.ToastViewData) error {
	setResponseToast(c, toast)
	if isHX(c) {
		addHXTrigger(c, events.IdentityResolutionChanged, map[string]string{"status": strings.TrimSpace(status)})
		return c.NoContent(http.StatusOK)
	}
	return c.Redirect(http.StatusSeeOther, "/identity-resolution")
}

func identityResolutionBoolInput(c *echo.Context, key string) bool {
	if c == nil || c.Request() == nil {
		return false
	}
	value := strings.ToLower(strings.TrimSpace(c.FormValue(key)))
	if value == "" && c.Request().URL != nil {
		value = strings.ToLower(strings.TrimSpace(c.QueryParam(key)))
	}
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func identityResolutionRelationshipTypeInput(c *echo.Context) string {
	if c == nil || c.Request() == nil {
		return ""
	}
	value := strings.ToLower(strings.TrimSpace(c.FormValue("relationship_type")))
	if value == "" && c.Request().URL != nil {
		value = strings.ToLower(strings.TrimSpace(c.QueryParam("relationship_type")))
	}
	switch value {
	case "owner", "custodian", "approver", "attributed_user", "last_observed_user":
		return value
	default:
		return ""
	}
}

func identityResolutionRelationshipConfidence(relationshipType string) int32 {
	switch relationshipType {
	case "owner", "custodian":
		return 70
	case "approver":
		return 60
	default:
		return 50
	}
}

func identityResolutionJSONMetadata(values map[string]any) []byte {
	if len(values) == 0 {
		return []byte(`{}`)
	}
	data, err := json.Marshal(values)
	if err != nil {
		return []byte(`{}`)
	}
	return data
}

func (h *Handlers) identityResolutionConflict(c *echo.Context, message string) error {
	toast := viewmodels.ToastViewData{
		Category:    "error",
		Title:       "Candidate changed",
		Description: "Refresh the queue before reviewing this candidate.",
	}
	setResponseToast(c, toast)
	if isHX(c) {
		addHXTrigger(c, events.IdentityResolutionChanged, map[string]string{"status": "conflict"})
		return c.NoContent(http.StatusConflict)
	}
	return c.Redirect(http.StatusSeeOther, "/identity-resolution")
}

func fallbackNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func nullableText(value string) pgtype.Text {
	value = strings.TrimSpace(value)
	return pgtype.Text{String: value, Valid: value != ""}
}
