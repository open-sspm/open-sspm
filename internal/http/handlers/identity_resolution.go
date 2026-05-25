package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	"github.com/open-sspm/open-sspm/internal/normalize"
)

const identityResolutionCandidatesPerPage = 50

type identityResolutionCandidateResponse struct {
	ID                    int64  `json:"id"`
	AccountID             int64  `json:"account_id"`
	CandidateIdentityID   int64  `json:"candidate_identity_id"`
	ProvisionalIdentityID *int64 `json:"provisional_identity_id,omitempty"`
	Status                string `json:"status"`
	ConfidenceBand        string `json:"confidence_band"`
	Score                 int32  `json:"score"`
	MatchReason           string `json:"match_reason"`
	AmbiguityKey          string `json:"ambiguity_key,omitempty"`
	ResolverVersion       string `json:"resolver_version"`
	ResolverFingerprint   string `json:"resolver_fingerprint"`
}

type identityResolutionCandidateDetailResponse struct {
	identityResolutionCandidateResponse
	Account             identityResolutionAccountResponse        `json:"account"`
	CandidateIdentity   identityResolutionIdentityResponse       `json:"candidate_identity"`
	CurrentIdentity     *identityResolutionIdentityResponse      `json:"current_identity,omitempty"`
	CurrentLinkState    string                                   `json:"current_link_state,omitempty"`
	CurrentLinkReason   string                                   `json:"current_link_reason,omitempty"`
	ProvisionalIdentity *identityResolutionIdentityResponse      `json:"provisional_identity,omitempty"`
	Evidence            []identityResolutionEvidenceResponse     `json:"evidence"`
	Relationships       []identityResolutionRelationshipResponse `json:"relationships,omitempty"`
}

type identityResolutionAccountResponse struct {
	ID             int64  `json:"id"`
	SourceKind     string `json:"source_kind"`
	SourceName     string `json:"source_name"`
	ExternalID     string `json:"external_id"`
	Email          string `json:"email"`
	DisplayName    string `json:"display_name"`
	AccountKind    string `json:"account_kind"`
	EntityCategory string `json:"entity_category"`
}

type identityResolutionIdentityResponse struct {
	ID              int64  `json:"id"`
	DisplayName     string `json:"display_name"`
	PrimaryEmail    string `json:"primary_email"`
	Kind            string `json:"kind,omitempty"`
	ResolutionState string `json:"resolution_state,omitempty"`
	IdentityKind    string `json:"identity_kind,omitempty"`
}

type identityResolutionEvidenceResponse struct {
	ID            int64  `json:"id"`
	EvidenceType  string `json:"evidence_type"`
	EvidenceKey   string `json:"evidence_key"`
	AccountValue  string `json:"account_value,omitempty"`
	IdentityValue string `json:"identity_value,omitempty"`
	SourceKind    string `json:"source_kind,omitempty"`
	SourceName    string `json:"source_name,omitempty"`
	Strength      int32  `json:"strength"`
	IsPositive    bool   `json:"is_positive"`
	Metadata      string `json:"metadata,omitempty"`
}

type identityResolutionRelationshipResponse struct {
	ID               int64  `json:"id"`
	AccountID        int64  `json:"account_id"`
	IdentityID       int64  `json:"identity_id"`
	RelationshipType string `json:"relationship_type"`
	SourceKind       string `json:"source_kind,omitempty"`
	SourceName       string `json:"source_name,omitempty"`
	Confidence       int32  `json:"confidence"`
	LifecycleState   string `json:"lifecycle_state"`
}

type identityEmailResponse struct {
	ID                int64  `json:"id"`
	IdentityID        int64  `json:"identity_id"`
	Email             string `json:"email"`
	NormalizedEmail   string `json:"normalized_email"`
	EmailKind         string `json:"email_kind"`
	VerificationState string `json:"verification_state"`
	LifecycleState    string `json:"lifecycle_state"`
	IsPrimary         bool   `json:"is_primary"`
	SourceKind        string `json:"source_kind,omitempty"`
	SourceName        string `json:"source_name,omitempty"`
	SourceAccountID   *int64 `json:"source_account_id,omitempty"`
}

type identityAnchorResponse struct {
	ID                    int64  `json:"id"`
	IdentityID            int64  `json:"identity_id"`
	AnchorKind            string `json:"anchor_kind"`
	Issuer                string `json:"issuer"`
	AnchorValue           string `json:"anchor_value"`
	NormalizedAnchorValue string `json:"normalized_anchor_value"`
	TrustLevel            string `json:"trust_level"`
	LifecycleState        string `json:"lifecycle_state"`
	SourceKind            string `json:"source_kind,omitempty"`
	SourceName            string `json:"source_name,omitempty"`
	SourceAccountID       *int64 `json:"source_account_id,omitempty"`
}

type identityEmailUpsertRequest struct {
	Email             string `json:"email"`
	EmailKind         string `json:"email_kind"`
	VerificationState string `json:"verification_state"`
	LifecycleState    string `json:"lifecycle_state"`
	IsPrimary         bool   `json:"is_primary"`
	SourceKind        string `json:"source_kind"`
	SourceName        string `json:"source_name"`
	SourceAccountID   int64  `json:"source_account_id"`
}

type identityAnchorUpsertRequest struct {
	AnchorKind            string `json:"anchor_kind"`
	Issuer                string `json:"issuer"`
	AnchorValue           string `json:"anchor_value"`
	NormalizedAnchorValue string `json:"normalized_anchor_value"`
	TrustLevel            string `json:"trust_level"`
	LifecycleState        string `json:"lifecycle_state"`
	SourceKind            string `json:"source_kind"`
	SourceName            string `json:"source_name"`
	SourceAccountID       int64  `json:"source_account_id"`
}

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

func (h *Handlers) HandleIdentityResolutionCandidates(c *echo.Context) error {
	limit := identityResolutionCandidatesPerPage
	if raw := strings.TrimSpace(c.QueryParam("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid limit"})
		}
		if parsed < limit {
			limit = parsed
		}
	}

	offset := 0
	if raw := strings.TrimSpace(c.QueryParam("offset")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid offset"})
		}
		offset = parsed
	}

	filters := identityResolutionFiltersFromRequest(c)
	rows, err := h.Q.ListIdentityMatchCandidateDetailsByFilters(c.Request().Context(), gen.ListIdentityMatchCandidateDetailsByFiltersParams{
		Status:         filters.Status,
		ConfidenceBand: filters.ConfidenceBand,
		MatchReason:    filters.MatchReason,
		PageLimit:      int32(limit),
		PageOffset:     int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	out := make([]identityResolutionCandidateResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, identityResolutionCandidateResponseFromDetailRow(row))
	}
	return c.JSON(http.StatusOK, map[string]any{
		"candidates": out,
		"limit":      limit,
		"offset":     offset,
		"status":     filters.Status,
		"group":      filters.Group,
	})
}

func (h *Handlers) HandleIdentityResolutionCandidateDetail(c *echo.Context) error {
	candidateID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}
	ctx := c.Request().Context()
	detail, err := h.Q.GetIdentityMatchCandidateDetailByID(ctx, candidateID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	evidenceRows, err := h.Q.ListIdentityLinkEvidenceForCandidate(ctx, pgtype.Int8{Int64: candidateID, Valid: true})
	if err != nil {
		return h.RenderError(c, err)
	}
	evidence := make([]identityResolutionEvidenceResponse, 0, len(evidenceRows))
	for _, row := range evidenceRows {
		evidence = append(evidence, identityResolutionEvidenceResponseFromRow(row))
	}
	relationshipRows, err := h.Q.ListAccountIdentityRelationships(ctx, gen.ListAccountIdentityRelationshipsParams{
		AccountID:      detail.AccountID,
		LifecycleState: "active",
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	relationships := make([]identityResolutionRelationshipResponse, 0, len(relationshipRows))
	for _, row := range relationshipRows {
		relationships = append(relationships, identityResolutionRelationshipResponseFromRow(row))
	}
	return c.JSON(http.StatusOK, identityResolutionCandidateDetailResponseFromRow(detail, evidence, relationships))
}

func (h *Handlers) HandleIdentityEmails(c *echo.Context) error {
	identityID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}
	ctx := c.Request().Context()
	if _, err := h.Q.GetIdentityForIdentityResolution(ctx, identityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	rows, err := h.Q.ListIdentityEmails(ctx, identityID)
	if err != nil {
		return h.RenderError(c, err)
	}
	out := make([]identityEmailResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, identityEmailResponseFromRow(row))
	}
	return c.JSON(http.StatusOK, map[string]any{
		"identity_id": identityID,
		"emails":      out,
	})
}

func (h *Handlers) HandleIdentityEmailUpsert(c *echo.Context) error {
	identityID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}
	ctx := c.Request().Context()
	if _, err := h.Q.GetIdentityForIdentityResolution(ctx, identityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	input, err := identityEmailUpsertRequestFromContext(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid email payload"})
	}
	normalizedEmail := normalize.Email(input.Email)
	if normalizedEmail == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "email is required"})
	}
	row, err := h.Q.UpsertIdentityEmail(ctx, gen.UpsertIdentityEmailParams{
		IdentityID:        identityID,
		Email:             strings.TrimSpace(input.Email),
		NormalizedEmail:   normalizedEmail,
		EmailKind:         fallbackNonEmpty(input.EmailKind, "alias"),
		VerificationState: fallbackNonEmpty(input.VerificationState, "manual"),
		LifecycleState:    fallbackNonEmpty(input.LifecycleState, "active"),
		IsPrimary:         input.IsPrimary,
		SourceKind:        pgtype.Text{String: strings.TrimSpace(input.SourceKind), Valid: strings.TrimSpace(input.SourceKind) != ""},
		SourceName:        pgtype.Text{String: strings.TrimSpace(input.SourceName), Valid: strings.TrimSpace(input.SourceName) != ""},
		SourceAccountID:   pgtype.Int8{Int64: input.SourceAccountID, Valid: input.SourceAccountID > 0},
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	return c.JSON(http.StatusOK, identityEmailResponseFromRow(row))
}

func (h *Handlers) HandleIdentityAnchors(c *echo.Context) error {
	identityID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}
	ctx := c.Request().Context()
	if _, err := h.Q.GetIdentityForIdentityResolution(ctx, identityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	rows, err := h.Q.ListIdentityAnchors(ctx, identityID)
	if err != nil {
		return h.RenderError(c, err)
	}
	out := make([]identityAnchorResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, identityAnchorResponseFromRow(row))
	}
	return c.JSON(http.StatusOK, map[string]any{
		"identity_id": identityID,
		"anchors":     out,
	})
}

func (h *Handlers) HandleIdentityAnchorUpsert(c *echo.Context) error {
	identityID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
	}
	ctx := c.Request().Context()
	if _, err := h.Q.GetIdentityForIdentityResolution(ctx, identityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	input, err := identityAnchorUpsertRequestFromContext(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid anchor payload"})
	}
	anchorKind := strings.TrimSpace(input.AnchorKind)
	issuer := strings.TrimSpace(input.Issuer)
	anchorValue := strings.TrimSpace(input.AnchorValue)
	if anchorKind == "" || issuer == "" || anchorValue == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "anchor_kind, issuer, and anchor_value are required"})
	}
	normalizedAnchorValue := normalize.Lower(fallbackNonEmpty(input.NormalizedAnchorValue, anchorValue))
	row, err := h.Q.UpsertIdentityAnchor(ctx, gen.UpsertIdentityAnchorParams{
		IdentityID:            identityID,
		AnchorKind:            anchorKind,
		Issuer:                issuer,
		AnchorValue:           anchorValue,
		NormalizedAnchorValue: normalizedAnchorValue,
		SourceKind:            pgtype.Text{String: strings.TrimSpace(input.SourceKind), Valid: strings.TrimSpace(input.SourceKind) != ""},
		SourceName:            pgtype.Text{String: strings.TrimSpace(input.SourceName), Valid: strings.TrimSpace(input.SourceName) != ""},
		SourceAccountID:       pgtype.Int8{Int64: input.SourceAccountID, Valid: input.SourceAccountID > 0},
		TrustLevel:            fallbackNonEmpty(input.TrustLevel, "manual"),
		LifecycleState:        fallbackNonEmpty(input.LifecycleState, "active"),
	})
	if err != nil {
		return h.RenderError(c, err)
	}
	if row.IdentityID != identityID {
		return c.JSON(http.StatusConflict, map[string]string{"error": "anchor is already assigned to another identity"})
	}
	return c.JSON(http.StatusOK, identityAnchorResponseFromRow(row))
}

func (h *Handlers) HandleIdentityResolutionCandidateAccept(c *echo.Context) error {
	candidateID, err := parsePositiveInt64Param(c.Param("id"))
	if err != nil {
		return RenderNotFound(c)
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
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if candidate.Status != "pending" {
		return h.identityResolutionConflict(c, "candidate is not pending")
	}

	account, err := qtx.GetAccountForIdentityResolutionUpdate(ctx, candidate.AccountID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	targetIdentity, err := qtx.GetIdentityForIdentityResolutionUpdate(ctx, candidate.CandidateIdentityID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
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
	if email := normalize.Email(account.Email); email != "" {
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
	mergedIdentityID := int64(0)
	mergedAutomatically := false
	if mergeProvisional {
		if !candidate.ProvisionalIdentityID.Valid {
			return h.identityResolutionConflict(c, "candidate has no provisional identity to merge")
		}
		mergedIdentityID = candidate.ProvisionalIdentityID.Int64
		if err := h.applyProvisionalIdentityMerge(ctx, qtx, mergedIdentityID, candidate.CandidateIdentityID, candidate.ID, reviewedBy, reviewNote); err != nil {
			return h.RenderError(c, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return h.RenderError(c, err)
	}

	if !identityResolutionWantsJSON(c) {
		return h.identityResolutionHTMLMutationSuccess(c, "accepted", viewmodels.ToastViewData{
			Category:    "success",
			Title:       "Candidate accepted",
			Description: "The source account is now linked to the selected identity.",
		})
	}
	return c.JSON(http.StatusOK, map[string]any{
		"status":               "accepted",
		"candidate":            identityResolutionCandidateResponseFromRow(candidate),
		"identity_id":          candidate.CandidateIdentityID,
		"account_id":           candidate.AccountID,
		"merged_identity_id":   mergedIdentityID,
		"merged_automatically": mergedAutomatically,
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
		return RenderNotFound(c)
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
			return RenderNotFound(c)
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
	if !identityResolutionWantsJSON(c) {
		return h.identityResolutionHTMLMutationSuccess(c, "rejected", viewmodels.ToastViewData{
			Category:    "success",
			Title:       "Candidate rejected",
			Description: "The candidate will stay suppressed until its evidence changes.",
		})
	}
	return c.JSON(http.StatusOK, map[string]any{
		"status":       "rejected",
		"candidate_id": candidateID,
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
		return RenderNotFound(c)
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
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}
	if candidate.Status != "pending" {
		return h.identityResolutionConflict(c, "candidate is not pending")
	}
	account, err := qtx.GetAccountForIdentityResolutionUpdate(ctx, candidate.AccountID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
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

	if !identityResolutionWantsJSON(c) {
		return h.identityResolutionHTMLMutationSuccess(c, "classified", viewmodels.ToastViewData{
			Category:    "success",
			Title:       "Account classified",
			Description: "The source account is now excluded from human identity matching.",
		})
	}
	return c.JSON(http.StatusOK, map[string]any{
		"status":            "classified",
		"mode":              mode,
		"relationship_type": relationshipType,
		"candidate_id":      candidateID,
		"account_id":        candidate.AccountID,
		"identity_id":       link.IdentityID,
	})
}

func identityResolutionCandidateResponseFromRow(row gen.IdentityMatchCandidate) identityResolutionCandidateResponse {
	var provisionalID *int64
	if row.ProvisionalIdentityID.Valid {
		value := row.ProvisionalIdentityID.Int64
		provisionalID = &value
	}
	return identityResolutionCandidateResponse{
		ID:                    row.ID,
		AccountID:             row.AccountID,
		CandidateIdentityID:   row.CandidateIdentityID,
		ProvisionalIdentityID: provisionalID,
		Status:                row.Status,
		ConfidenceBand:        row.ConfidenceBand,
		Score:                 row.Score,
		MatchReason:           row.MatchReason,
		AmbiguityKey:          strings.TrimSpace(row.AmbiguityKey.String),
		ResolverVersion:       row.ResolverVersion,
		ResolverFingerprint:   row.ResolverFingerprint,
	}
}

func identityResolutionCandidateResponseFromDetailRow(row gen.ListIdentityMatchCandidateDetailsByFiltersRow) identityResolutionCandidateResponse {
	var provisionalID *int64
	if row.ProvisionalIdentityID.Valid {
		value := row.ProvisionalIdentityID.Int64
		provisionalID = &value
	}
	return identityResolutionCandidateResponse{
		ID:                    row.ID,
		AccountID:             row.AccountID,
		CandidateIdentityID:   row.CandidateIdentityID,
		ProvisionalIdentityID: provisionalID,
		Status:                row.Status,
		ConfidenceBand:        row.ConfidenceBand,
		Score:                 row.Score,
		MatchReason:           row.MatchReason,
		AmbiguityKey:          strings.TrimSpace(row.AmbiguityKey.String),
		ResolverVersion:       row.ResolverVersion,
		ResolverFingerprint:   row.ResolverFingerprint,
	}
}

func identityResolutionCandidateDetailResponseFromRow(row gen.GetIdentityMatchCandidateDetailByIDRow, evidence []identityResolutionEvidenceResponse, relationships []identityResolutionRelationshipResponse) identityResolutionCandidateDetailResponse {
	out := identityResolutionCandidateDetailResponse{
		identityResolutionCandidateResponse: identityResolutionCandidateResponse{
			ID:                  row.ID,
			AccountID:           row.AccountID,
			CandidateIdentityID: row.CandidateIdentityID,
			Status:              row.Status,
			ConfidenceBand:      row.ConfidenceBand,
			Score:               row.Score,
			MatchReason:         row.MatchReason,
			AmbiguityKey:        strings.TrimSpace(row.AmbiguityKey.String),
			ResolverVersion:     row.ResolverVersion,
			ResolverFingerprint: row.ResolverFingerprint,
		},
		Account: identityResolutionAccountResponse{
			ID:             row.AccountID,
			SourceKind:     strings.TrimSpace(row.AccountSourceKind),
			SourceName:     strings.TrimSpace(row.AccountSourceName),
			ExternalID:     strings.TrimSpace(row.AccountExternalID),
			Email:          strings.TrimSpace(row.AccountEmail),
			DisplayName:    strings.TrimSpace(row.AccountDisplayName),
			AccountKind:    strings.TrimSpace(row.AccountKind),
			EntityCategory: strings.TrimSpace(row.EntityCategory),
		},
		CandidateIdentity: identityResolutionIdentityResponse{
			ID:              row.CandidateIdentityID,
			DisplayName:     strings.TrimSpace(row.CandidateDisplayName),
			PrimaryEmail:    strings.TrimSpace(row.CandidatePrimaryEmail),
			Kind:            strings.TrimSpace(row.CandidateKind),
			ResolutionState: strings.TrimSpace(row.CandidateResolutionState),
			IdentityKind:    strings.TrimSpace(row.CandidateIdentityKind),
		},
		CurrentLinkState:  strings.TrimSpace(row.CurrentLinkState.String),
		CurrentLinkReason: strings.TrimSpace(row.CurrentLinkReason.String),
		Evidence:          evidence,
		Relationships:     relationships,
	}
	if row.ProvisionalIdentityID.Valid {
		value := row.ProvisionalIdentityID.Int64
		out.ProvisionalIdentityID = &value
		out.ProvisionalIdentity = &identityResolutionIdentityResponse{
			ID:           value,
			DisplayName:  strings.TrimSpace(row.ProvisionalDisplayName.String),
			PrimaryEmail: strings.TrimSpace(row.ProvisionalPrimaryEmail.String),
		}
	}
	if row.CurrentIdentityID.Valid {
		out.CurrentIdentity = &identityResolutionIdentityResponse{
			ID:           row.CurrentIdentityID.Int64,
			DisplayName:  strings.TrimSpace(row.CurrentIdentityDisplayName.String),
			PrimaryEmail: strings.TrimSpace(row.CurrentIdentityPrimaryEmail.String),
		}
	}
	return out
}

func identityResolutionRelationshipResponseFromRow(row gen.AccountIdentityRelationship) identityResolutionRelationshipResponse {
	return identityResolutionRelationshipResponse{
		ID:               row.ID,
		AccountID:        row.AccountID,
		IdentityID:       row.IdentityID,
		RelationshipType: strings.TrimSpace(row.RelationshipType),
		SourceKind:       strings.TrimSpace(row.SourceKind.String),
		SourceName:       strings.TrimSpace(row.SourceName.String),
		Confidence:       row.Confidence,
		LifecycleState:   strings.TrimSpace(row.LifecycleState),
	}
}

func identityEmailResponseFromRow(row gen.IdentityEmail) identityEmailResponse {
	var sourceAccountID *int64
	if row.SourceAccountID.Valid {
		value := row.SourceAccountID.Int64
		sourceAccountID = &value
	}
	return identityEmailResponse{
		ID:                row.ID,
		IdentityID:        row.IdentityID,
		Email:             strings.TrimSpace(row.Email),
		NormalizedEmail:   strings.TrimSpace(row.NormalizedEmail),
		EmailKind:         strings.TrimSpace(row.EmailKind),
		VerificationState: strings.TrimSpace(row.VerificationState),
		LifecycleState:    strings.TrimSpace(row.LifecycleState),
		IsPrimary:         row.IsPrimary,
		SourceKind:        strings.TrimSpace(row.SourceKind.String),
		SourceName:        strings.TrimSpace(row.SourceName.String),
		SourceAccountID:   sourceAccountID,
	}
}

func identityAnchorResponseFromRow(row gen.IdentityAnchor) identityAnchorResponse {
	var sourceAccountID *int64
	if row.SourceAccountID.Valid {
		value := row.SourceAccountID.Int64
		sourceAccountID = &value
	}
	return identityAnchorResponse{
		ID:                    row.ID,
		IdentityID:            row.IdentityID,
		AnchorKind:            strings.TrimSpace(row.AnchorKind),
		Issuer:                strings.TrimSpace(row.Issuer),
		AnchorValue:           strings.TrimSpace(row.AnchorValue),
		NormalizedAnchorValue: strings.TrimSpace(row.NormalizedAnchorValue),
		TrustLevel:            strings.TrimSpace(row.TrustLevel),
		LifecycleState:        strings.TrimSpace(row.LifecycleState),
		SourceKind:            strings.TrimSpace(row.SourceKind.String),
		SourceName:            strings.TrimSpace(row.SourceName.String),
		SourceAccountID:       sourceAccountID,
	}
}

func identityResolutionEvidenceResponseFromRow(row gen.IdentityLinkEvidence) identityResolutionEvidenceResponse {
	return identityResolutionEvidenceResponse{
		ID:            row.ID,
		EvidenceType:  strings.TrimSpace(row.EvidenceType),
		EvidenceKey:   strings.TrimSpace(row.EvidenceKey),
		AccountValue:  strings.TrimSpace(row.AccountValue.String),
		IdentityValue: strings.TrimSpace(row.IdentityValue.String),
		SourceKind:    strings.TrimSpace(row.SourceKind.String),
		SourceName:    strings.TrimSpace(row.SourceName.String),
		Strength:      row.Strength,
		IsPositive:    row.IsPositive,
		Metadata:      strings.TrimSpace(string(row.Metadata)),
	}
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

func identityResolutionWantsJSON(c *echo.Context) bool {
	if c == nil || c.Request() == nil || c.Request().URL == nil {
		return false
	}
	return strings.HasPrefix(c.Request().URL.Path, "/api/")
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

func identityEmailUpsertRequestFromContext(c *echo.Context) (identityEmailUpsertRequest, error) {
	var input identityEmailUpsertRequest
	if err := decodeIdentityResolutionJSONRequest(c, &input); err != nil {
		return input, err
	}
	input.Email = fallbackNonEmpty(input.Email, c.FormValue("email"), c.QueryParam("email"))
	input.EmailKind = fallbackNonEmpty(input.EmailKind, c.FormValue("email_kind"), c.QueryParam("email_kind"))
	input.VerificationState = fallbackNonEmpty(input.VerificationState, c.FormValue("verification_state"), c.QueryParam("verification_state"))
	input.LifecycleState = fallbackNonEmpty(input.LifecycleState, c.FormValue("lifecycle_state"), c.QueryParam("lifecycle_state"))
	input.SourceKind = fallbackNonEmpty(input.SourceKind, c.FormValue("source_kind"), c.QueryParam("source_kind"))
	input.SourceName = fallbackNonEmpty(input.SourceName, c.FormValue("source_name"), c.QueryParam("source_name"))
	if input.SourceAccountID == 0 {
		input.SourceAccountID = parseOptionalInt64(fallbackNonEmpty(c.FormValue("source_account_id"), c.QueryParam("source_account_id")))
	}
	input.IsPrimary = input.IsPrimary || identityResolutionBoolInput(c, "is_primary")
	return input, nil
}

func identityAnchorUpsertRequestFromContext(c *echo.Context) (identityAnchorUpsertRequest, error) {
	var input identityAnchorUpsertRequest
	if err := decodeIdentityResolutionJSONRequest(c, &input); err != nil {
		return input, err
	}
	input.AnchorKind = fallbackNonEmpty(input.AnchorKind, c.FormValue("anchor_kind"), c.QueryParam("anchor_kind"))
	input.Issuer = fallbackNonEmpty(input.Issuer, c.FormValue("issuer"), c.QueryParam("issuer"))
	input.AnchorValue = fallbackNonEmpty(input.AnchorValue, c.FormValue("anchor_value"), c.FormValue("value"), c.QueryParam("anchor_value"), c.QueryParam("value"))
	input.NormalizedAnchorValue = fallbackNonEmpty(input.NormalizedAnchorValue, c.FormValue("normalized_anchor_value"), c.QueryParam("normalized_anchor_value"))
	input.TrustLevel = fallbackNonEmpty(input.TrustLevel, c.FormValue("trust_level"), c.QueryParam("trust_level"))
	input.LifecycleState = fallbackNonEmpty(input.LifecycleState, c.FormValue("lifecycle_state"), c.QueryParam("lifecycle_state"))
	input.SourceKind = fallbackNonEmpty(input.SourceKind, c.FormValue("source_kind"), c.QueryParam("source_kind"))
	input.SourceName = fallbackNonEmpty(input.SourceName, c.FormValue("source_name"), c.QueryParam("source_name"))
	if input.SourceAccountID == 0 {
		input.SourceAccountID = parseOptionalInt64(fallbackNonEmpty(c.FormValue("source_account_id"), c.QueryParam("source_account_id")))
	}
	return input, nil
}

func decodeIdentityResolutionJSONRequest(c *echo.Context, dest any) error {
	if c == nil || c.Request() == nil || c.Request().Body == nil {
		return nil
	}
	contentType := strings.ToLower(c.Request().Header.Get(echo.HeaderContentType))
	if !strings.Contains(contentType, echo.MIMEApplicationJSON) {
		return nil
	}
	err := json.NewDecoder(c.Request().Body).Decode(dest)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func parseOptionalInt64(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
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
	if identityResolutionWantsJSON(c) {
		return c.JSON(http.StatusConflict, map[string]string{"error": message})
	}
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
