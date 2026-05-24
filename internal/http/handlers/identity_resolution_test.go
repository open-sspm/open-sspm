package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
)

func TestHandleIdentityResolutionReviewRendersPendingCandidateEvidenceAndActions(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ambiguous-user",
			Email:          "ambiguous@example.com",
			DisplayName:    "Ambiguous User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		candidateIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Candidate User")
		provisionalIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Provisional User")
		if _, err := pool.Exec(ctx, `UPDATE identities SET resolution_state = 'provisional' WHERE id = $1`, provisionalIdentity); err != nil {
			t.Fatalf("mark provisional identity: %v", err)
		}
		insertCommandSearchIdentityAccountLink(t, ctx, pool, provisionalIdentity, accountID)

		candidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:             accountID,
			CandidateIdentityID:   candidateIdentity,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisionalIdentity, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 40,
			MatchReason:           "ambiguous_primary_email",
			AmbiguityKey:          pgtype.Text{String: "email:ambiguous@example.com", Valid: true},
			ResolverVersion:       "test",
			ResolverFingerprint:   "candidate",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(): %v", err)
		}
		if _, err := q.UpsertCandidateLinkEvidence(ctx, gen.UpsertCandidateLinkEvidenceParams{
			AccountID:     accountID,
			CandidateID:   candidate.ID,
			EvidenceType:  "negative_ambiguous_email",
			EvidenceKey:   "primary_email",
			AccountValue:  pgtype.Text{String: "ambiguous@example.com", Valid: true},
			IdentityValue: pgtype.Text{String: "ambiguous@example.com", Valid: true},
			SourceKind:    pgtype.Text{String: configstore.KindGitHub, Valid: true},
			SourceName:    pgtype.Text{String: "acme", Valid: true},
			Strength:      -50,
			IsPositive:    false,
		}); err != nil {
			t.Fatalf("UpsertCandidateLinkEvidence(): %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/identity-resolution")
		(*c).SetPath("/identity-resolution")
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: 1, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleIdentityResolutionReview(c); err != nil {
			t.Fatalf("HandleIdentityResolutionReview(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := rec.Body.String()
		for _, want := range []string{
			"Pending Candidates",
			"Ambiguous User",
			"Candidate User",
			"Provisional User",
			"Evidence",
			"Negative Ambiguous Email",
			"/identity-resolution/candidates/" + strconv.FormatInt(candidate.ID, 10) + "/accept",
			"/identity-resolution/candidates/" + strconv.FormatInt(candidate.ID, 10) + "/reject",
			"/identity-resolution/candidates/" + strconv.FormatInt(candidate.ID, 10) + "/mark-service",
			"/identity-resolution/candidates/" + strconv.FormatInt(candidate.ID, 10) + "/mark-shared",
			"Accept + merge",
		} {
			assertContains(t, body, want)
		}
	})
}

func TestHandleIdentityResolutionCandidateDetailReturnsEvidence(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ambiguous-user",
			Email:          "ambiguous@example.com",
			DisplayName:    "Ambiguous User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		candidateIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Candidate User")
		candidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:           accountID,
			CandidateIdentityID: candidateIdentity,
			ConfidenceBand:      "conflict",
			Score:               40,
			MatchReason:         "ambiguous_primary_email",
			ResolverVersion:     "test",
			ResolverFingerprint: "candidate",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(): %v", err)
		}
		if _, err := q.UpsertCandidateLinkEvidence(ctx, gen.UpsertCandidateLinkEvidenceParams{
			AccountID:     accountID,
			CandidateID:   candidate.ID,
			EvidenceType:  "negative_ambiguous_email",
			EvidenceKey:   "primary_email",
			AccountValue:  pgtype.Text{String: "ambiguous@example.com", Valid: true},
			IdentityValue: pgtype.Text{String: "ambiguous@example.com", Valid: true},
			Strength:      -50,
			IsPositive:    false,
		}); err != nil {
			t.Fatalf("UpsertCandidateLinkEvidence(): %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(candidate.ID, 10))
		(*c).SetPath("/api/identity-resolution/candidates/:id")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(candidate.ID, 10)}})

		if err := h.HandleIdentityResolutionCandidateDetail(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateDetail(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := rec.Body.String()
		for _, want := range []string{
			`"id":` + strconv.FormatInt(candidate.ID, 10),
			`"display_name":"Ambiguous User"`,
			`"display_name":"Candidate User"`,
			`"evidence_type":"negative_ambiguous_email"`,
			`"strength":-50`,
		} {
			assertContains(t, body, want)
		}
	})
}

func TestHandleIdentityResolutionCandidatesFiltersStatusAndGroup(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "anchor-conflict",
			Email:          "anchor@example.com",
			DisplayName:    "Anchor Conflict",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		emailAccountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ambiguous-email",
			Email:          "email@example.com",
			DisplayName:    "Ambiguous Email",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		candidateIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "candidate@example.com", "Candidate")
		anchorCandidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:           accountID,
			CandidateIdentityID: candidateIdentity,
			ConfidenceBand:      "conflict",
			Score:               100,
			MatchReason:         "conflicting_anchors",
			ResolverVersion:     "test",
			ResolverFingerprint: "anchor",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(anchor): %v", err)
		}
		emailCandidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:           emailAccountID,
			CandidateIdentityID: candidateIdentity,
			ConfidenceBand:      "conflict",
			Score:               40,
			MatchReason:         "ambiguous_primary_email",
			ResolverVersion:     "test",
			ResolverFingerprint: "email",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(email): %v", err)
		}
		if err := q.RejectIdentityMatchCandidate(ctx, gen.RejectIdentityMatchCandidateParams{ID: emailCandidate.ID}); err != nil {
			t.Fatalf("RejectIdentityMatchCandidate(email): %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/api/identity-resolution/candidates?status=pending&group=anchor_conflict")
		(*c).SetPath("/api/identity-resolution/candidates")
		if err := h.HandleIdentityResolutionCandidates(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidates(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := rec.Body.String()
		assertContains(t, body, `"id":`+strconv.FormatInt(anchorCandidate.ID, 10))
		assertContains(t, body, `"group":"anchor_conflict"`)
		assertNotContains(t, body, `"id":`+strconv.FormatInt(emailCandidate.ID, 10))
	})
}

func TestHandleIdentityEmailAndAnchorAPIs(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		identityID := insertCommandSearchIdentity(t, ctx, pool, "human", "person@example.com", "Person")

		emailURL := "http://example.com/api/identities/" + strconv.FormatInt(identityID, 10) + "/emails?email=Alias%40Example.com&email_kind=alias&verification_state=manual&is_primary=1"
		emailCtx, emailRec := newTestContext(http.MethodPost, emailURL)
		(*emailCtx).SetPath("/api/identities/:id/emails")
		(*emailCtx).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(identityID, 10)}})
		if err := h.HandleIdentityEmailUpsert(emailCtx); err != nil {
			t.Fatalf("HandleIdentityEmailUpsert(): %v", err)
		}
		if emailRec.Code != http.StatusOK {
			t.Fatalf("email status = %d, want %d; body=%s", emailRec.Code, http.StatusOK, emailRec.Body.String())
		}
		assertContains(t, emailRec.Body.String(), `"normalized_email":"alias@example.com"`)
		assertContains(t, emailRec.Body.String(), `"is_primary":true`)

		anchorURL := "http://example.com/api/identities/" + strconv.FormatInt(identityID, 10) + "/anchors?anchor_kind=okta_user_id&issuer=okta%3Aacme&anchor_value=00U123&trust_level=manual"
		anchorCtx, anchorRec := newTestContext(http.MethodPost, anchorURL)
		(*anchorCtx).SetPath("/api/identities/:id/anchors")
		(*anchorCtx).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(identityID, 10)}})
		if err := h.HandleIdentityAnchorUpsert(anchorCtx); err != nil {
			t.Fatalf("HandleIdentityAnchorUpsert(): %v", err)
		}
		if anchorRec.Code != http.StatusOK {
			t.Fatalf("anchor status = %d, want %d; body=%s", anchorRec.Code, http.StatusOK, anchorRec.Body.String())
		}
		assertContains(t, anchorRec.Body.String(), `"normalized_anchor_value":"00u123"`)

		listEmailCtx, listEmailRec := newTestContext(http.MethodGet, "http://example.com/api/identities/"+strconv.FormatInt(identityID, 10)+"/emails")
		(*listEmailCtx).SetPath("/api/identities/:id/emails")
		(*listEmailCtx).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(identityID, 10)}})
		if err := h.HandleIdentityEmails(listEmailCtx); err != nil {
			t.Fatalf("HandleIdentityEmails(): %v", err)
		}
		assertContains(t, listEmailRec.Body.String(), `"emails":[`)
		assertContains(t, listEmailRec.Body.String(), `"alias@example.com"`)

		listAnchorCtx, listAnchorRec := newTestContext(http.MethodGet, "http://example.com/api/identities/"+strconv.FormatInt(identityID, 10)+"/anchors")
		(*listAnchorCtx).SetPath("/api/identities/:id/anchors")
		(*listAnchorCtx).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(identityID, 10)}})
		if err := h.HandleIdentityAnchors(listAnchorCtx); err != nil {
			t.Fatalf("HandleIdentityAnchors(): %v", err)
		}
		assertContains(t, listAnchorRec.Body.String(), `"anchors":[`)
		assertContains(t, listAnchorRec.Body.String(), `"okta_user_id"`)

		emails, err := q.ListIdentityEmails(ctx, identityID)
		if err != nil {
			t.Fatalf("ListIdentityEmails(): %v", err)
		}
		if len(emails) == 0 {
			t.Fatalf("identity emails not persisted")
		}
	})
}

func TestHandleIdentityResolutionCandidateAcceptLinksAccountAndSupersedesCompetingCandidates(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ambiguous-user",
			Email:          "ambiguous@example.com",
			DisplayName:    "Ambiguous User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		candidateA := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Candidate A")
		candidateB := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Candidate B")
		provisional := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Provisional")

		accepted, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:             accountID,
			CandidateIdentityID:   candidateA,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisional, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 40,
			MatchReason:           "ambiguous_primary_email",
			AmbiguityKey:          pgtype.Text{String: "email:ambiguous@example.com", Valid: true},
			ResolverVersion:       "test",
			ResolverFingerprint:   "candidate-a",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(accepted): %v", err)
		}
		competing, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:             accountID,
			CandidateIdentityID:   candidateB,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisional, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 40,
			MatchReason:           "ambiguous_primary_email",
			AmbiguityKey:          pgtype.Text{String: "email:ambiguous@example.com", Valid: true},
			ResolverVersion:       "test",
			ResolverFingerprint:   "candidate-b",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(competing): %v", err)
		}

		c, rec := newTestContext(http.MethodPost, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(accepted.ID, 10)+"/accept")
		(*c).SetPath("/api/identity-resolution/candidates/:id/accept")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(accepted.ID, 10)}})

		if err := h.HandleIdentityResolutionCandidateAccept(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateAccept(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"status":"accepted"`) {
			t.Fatalf("response missing accepted status: %s", rec.Body.String())
		}

		link, err := q.GetIdentityAccountLinkByAccountID(ctx, accountID)
		if err != nil {
			t.Fatalf("GetIdentityAccountLinkByAccountID(): %v", err)
		}
		if link.IdentityID != candidateA {
			t.Fatalf("linked identity = %d, want %d", link.IdentityID, candidateA)
		}
		if link.LinkState != "manual_confirmed" {
			t.Fatalf("link state = %q, want manual_confirmed", link.LinkState)
		}

		acceptedAfter, err := q.GetIdentityMatchCandidate(ctx, accepted.ID)
		if err != nil {
			t.Fatalf("GetIdentityMatchCandidate(accepted): %v", err)
		}
		if acceptedAfter.Status != "accepted" {
			t.Fatalf("accepted candidate status = %q, want accepted", acceptedAfter.Status)
		}
		competingAfter, err := q.GetIdentityMatchCandidate(ctx, competing.ID)
		if err != nil {
			t.Fatalf("GetIdentityMatchCandidate(competing): %v", err)
		}
		if competingAfter.Status != "superseded" {
			t.Fatalf("competing candidate status = %q, want superseded", competingAfter.Status)
		}
	})
}

func TestHandleIdentityResolutionCandidateAcceptCanMergeProvisionalIdentity(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ambiguous-user",
			Email:          "ambiguous@example.com",
			DisplayName:    "Ambiguous User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		targetIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "target@example.com", "Target")
		provisionalIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Provisional")
		if _, err := pool.Exec(ctx, `UPDATE identities SET resolution_state = 'provisional' WHERE id = $1`, provisionalIdentity); err != nil {
			t.Fatalf("mark provisional identity: %v", err)
		}
		insertCommandSearchIdentityAccountLink(t, ctx, pool, provisionalIdentity, accountID)
		if _, err := q.UpsertIdentityEmail(ctx, gen.UpsertIdentityEmailParams{
			IdentityID:        provisionalIdentity,
			Email:             "ambiguous@example.com",
			NormalizedEmail:   "ambiguous@example.com",
			EmailKind:         "primary",
			VerificationState: "observed",
			LifecycleState:    "active",
			IsPrimary:         true,
		}); err != nil {
			t.Fatalf("UpsertIdentityEmail(provisional): %v", err)
		}

		candidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:             accountID,
			CandidateIdentityID:   targetIdentity,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisionalIdentity, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 40,
			MatchReason:           "ambiguous_primary_email",
			ResolverVersion:       "test",
			ResolverFingerprint:   "candidate",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(): %v", err)
		}

		c, rec := newTestContext(http.MethodPost, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(candidate.ID, 10)+"/accept?merge_provisional=true")
		(*c).SetPath("/api/identity-resolution/candidates/:id/accept")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(candidate.ID, 10)}})
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: 1, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleIdentityResolutionCandidateAccept(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateAccept(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		assertContains(t, rec.Body.String(), `"merged_identity_id":`+strconv.FormatInt(provisionalIdentity, 10))

		redirect, err := q.GetIdentityMergeRedirect(ctx, provisionalIdentity)
		if err != nil {
			t.Fatalf("GetIdentityMergeRedirect(): %v", err)
		}
		if redirect.TargetIdentityID != targetIdentity {
			t.Fatalf("redirect target = %d, want %d", redirect.TargetIdentityID, targetIdentity)
		}
		source, err := q.GetIdentityForIdentityResolutionUpdate(ctx, provisionalIdentity)
		if err != nil {
			t.Fatalf("GetIdentityForIdentityResolutionUpdate(source): %v", err)
		}
		if source.ResolutionState != "merged" {
			t.Fatalf("source resolution state = %q, want merged", source.ResolutionState)
		}
		matches, err := q.FindIdentitiesByNormalizedEmail(ctx, gen.FindIdentitiesByNormalizedEmailParams{
			NormalizedEmail: "ambiguous@example.com",
			LifecycleState:  "active",
		})
		if err != nil {
			t.Fatalf("FindIdentitiesByNormalizedEmail(): %v", err)
		}
		if len(matches) != 1 || matches[0].ID != targetIdentity {
			t.Fatalf("active alias matches = %+v, want only target identity %d", matches, targetIdentity)
		}
		resolved, err := q.FindUnambiguousIdentityByPrimaryEmail(ctx, gen.FindUnambiguousIdentityByPrimaryEmailParams{
			ConfiguredSourceKinds: []string{},
			ConfiguredSourceNames: []string{},
			PrimaryEmail:          "ambiguous@example.com",
		})
		if err != nil {
			t.Fatalf("FindUnambiguousIdentityByPrimaryEmail(merged source legacy email): %v", err)
		}
		if resolved.ID != targetIdentity {
			t.Fatalf("resolved identity ID = %d, want target %d", resolved.ID, targetIdentity)
		}
	})
}

func TestHandleIdentityResolutionCandidateAcceptDoesNotAutoMergeEmptyProvisional(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ambiguous-user",
			Email:          "ambiguous@example.com",
			DisplayName:    "Ambiguous User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		targetIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "target@example.com", "Target")
		provisionalIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Provisional")
		if _, err := pool.Exec(ctx, `UPDATE identities SET resolution_state = 'provisional' WHERE id = $1`, provisionalIdentity); err != nil {
			t.Fatalf("mark provisional identity: %v", err)
		}
		insertCommandSearchIdentityAccountLink(t, ctx, pool, provisionalIdentity, accountID)

		candidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:             accountID,
			CandidateIdentityID:   targetIdentity,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisionalIdentity, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 40,
			MatchReason:           "ambiguous_primary_email",
			ResolverVersion:       "test",
			ResolverFingerprint:   "candidate",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(): %v", err)
		}

		// Note: no merge_provisional query param. Accept should only relink the
		// account; merging the provisional shell is an explicit operator action.
		c, rec := newTestContext(http.MethodPost, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(candidate.ID, 10)+"/accept")
		(*c).SetPath("/api/identity-resolution/candidates/:id/accept")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(candidate.ID, 10)}})
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: 1, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleIdentityResolutionCandidateAccept(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateAccept(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := rec.Body.String()
		assertContains(t, body, `"merged_identity_id":0`)
		assertContains(t, body, `"merged_automatically":false`)

		source, err := q.GetIdentityForIdentityResolutionUpdate(ctx, provisionalIdentity)
		if err != nil {
			t.Fatalf("GetIdentityForIdentityResolutionUpdate(source): %v", err)
		}
		if source.ResolutionState != "provisional" {
			t.Fatalf("source resolution state = %q, want provisional", source.ResolutionState)
		}
	})
}

func TestHandleIdentityResolutionCandidateAcceptSkipsAutoMergeWhenProvisionalStillHasAccounts(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountA := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind: configstore.KindGitHub, SourceName: "acme",
			ExternalID:  "user-a",
			Email:       "ambiguous@example.com",
			DisplayName: "User A",
			Status:      "active", AccountKind: "human", EntityCategory: "user", RawJSON: `{}`,
		})
		accountB := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind: configstore.KindGitHub, SourceName: "acme",
			ExternalID:  "user-b",
			Email:       "ambiguous@example.com",
			DisplayName: "User B",
			Status:      "active", AccountKind: "human", EntityCategory: "user", RawJSON: `{}`,
		})
		targetIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "target@example.com", "Target")
		provisionalIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Provisional")
		if _, err := pool.Exec(ctx, `UPDATE identities SET resolution_state = 'provisional' WHERE id = $1`, provisionalIdentity); err != nil {
			t.Fatalf("mark provisional identity: %v", err)
		}
		insertCommandSearchIdentityAccountLink(t, ctx, pool, provisionalIdentity, accountA)
		insertCommandSearchIdentityAccountLink(t, ctx, pool, provisionalIdentity, accountB)

		candidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:             accountA,
			CandidateIdentityID:   targetIdentity,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisionalIdentity, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 40,
			MatchReason:           "ambiguous_primary_email",
			ResolverVersion:       "test",
			ResolverFingerprint:   "candidate",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(): %v", err)
		}

		c, rec := newTestContext(http.MethodPost, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(candidate.ID, 10)+"/accept")
		(*c).SetPath("/api/identity-resolution/candidates/:id/accept")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(candidate.ID, 10)}})
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: 1, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleIdentityResolutionCandidateAccept(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateAccept(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		assertContains(t, rec.Body.String(), `"merged_identity_id":0`)
		assertContains(t, rec.Body.String(), `"merged_automatically":false`)

		source, err := q.GetIdentityForIdentityResolutionUpdate(ctx, provisionalIdentity)
		if err != nil {
			t.Fatalf("GetIdentityForIdentityResolutionUpdate(source): %v", err)
		}
		if source.ResolutionState != "provisional" {
			t.Fatalf("source resolution state = %q, want provisional", source.ResolutionState)
		}
	})
}

func TestHandleIdentityResolutionCandidateMarkServiceClassifiesCurrentRollupAndRejectsCandidates(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ci-account",
			Email:          "owner@example.com",
			DisplayName:    "CI Account",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		candidateIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Human Candidate")
		currentIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Current Rollup")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, currentIdentity, accountID)
		candidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:           accountID,
			CandidateIdentityID: candidateIdentity,
			ConfidenceBand:      "conflict",
			Score:               40,
			MatchReason:         "service_shared_warning",
			ResolverVersion:     "test",
			ResolverFingerprint: "candidate",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(): %v", err)
		}

		c, rec := newTestContext(http.MethodPost, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(candidate.ID, 10)+"/mark-service?relationship_type=custodian")
		(*c).SetPath("/api/identity-resolution/candidates/:id/mark-service")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(candidate.ID, 10)}})

		if err := h.HandleIdentityResolutionCandidateMarkService(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateMarkService(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		account, err := q.GetAccountForIdentityResolutionUpdate(ctx, accountID)
		if err != nil {
			t.Fatalf("GetAccountForIdentityResolutionUpdate(): %v", err)
		}
		if account.AccountKind != "service" || account.EntityCategory != "service_account" {
			t.Fatalf("account classification = %q/%q, want service/service_account", account.AccountKind, account.EntityCategory)
		}
		identity, err := q.GetIdentityForIdentityResolutionUpdate(ctx, currentIdentity)
		if err != nil {
			t.Fatalf("GetIdentityForIdentityResolutionUpdate(): %v", err)
		}
		if identity.Kind != "service" || identity.IdentityKind != "service" || identity.ResolutionState != "confirmed" {
			t.Fatalf("identity classification = %q/%q/%q, want service/service/confirmed", identity.Kind, identity.IdentityKind, identity.ResolutionState)
		}
		link, err := q.GetIdentityAccountLinkByAccountID(ctx, accountID)
		if err != nil {
			t.Fatalf("GetIdentityAccountLinkByAccountID(): %v", err)
		}
		if link.LinkReason != "manual_service" || link.LinkState != "manual_confirmed" {
			t.Fatalf("link reason/state = %q/%q, want manual_service/manual_confirmed", link.LinkReason, link.LinkState)
		}
		after, err := q.GetIdentityMatchCandidate(ctx, candidate.ID)
		if err != nil {
			t.Fatalf("GetIdentityMatchCandidate(): %v", err)
		}
		if after.Status != "rejected" {
			t.Fatalf("candidate status = %q, want rejected", after.Status)
		}
		relationships, err := q.ListAccountIdentityRelationships(ctx, gen.ListAccountIdentityRelationshipsParams{
			AccountID:      accountID,
			LifecycleState: "active",
		})
		if err != nil {
			t.Fatalf("ListAccountIdentityRelationships(): %v", err)
		}
		if len(relationships) != 1 {
			t.Fatalf("relationship count = %d, want 1", len(relationships))
		}
		if relationships[0].IdentityID != candidateIdentity || relationships[0].RelationshipType != "custodian" {
			t.Fatalf("relationship = %+v, want candidate custodian", relationships[0])
		}

		detailCtx, detailRec := newTestContext(http.MethodGet, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(candidate.ID, 10))
		(*detailCtx).SetPath("/api/identity-resolution/candidates/:id")
		(*detailCtx).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(candidate.ID, 10)}})
		if err := h.HandleIdentityResolutionCandidateDetail(detailCtx); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateDetail(): %v", err)
		}
		if detailRec.Code != http.StatusOK {
			t.Fatalf("detail status = %d, want %d; body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
		}
		assertContains(t, detailRec.Body.String(), `"relationship_type":"custodian"`)
	})
}

func TestHandleIdentityResolutionCandidateRejectMarksPendingCandidateRejected(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ambiguous-user",
			Email:          "ambiguous@example.com",
			DisplayName:    "Ambiguous User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		candidateIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Candidate")
		candidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:           accountID,
			CandidateIdentityID: candidateIdentity,
			ConfidenceBand:      "conflict",
			Score:               40,
			MatchReason:         "ambiguous_primary_email",
			ResolverVersion:     "test",
			ResolverFingerprint: "candidate",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(): %v", err)
		}

		c, rec := newTestContext(http.MethodPost, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(candidate.ID, 10)+"/reject")
		(*c).SetPath("/api/identity-resolution/candidates/:id/reject")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(candidate.ID, 10)}})

		if err := h.HandleIdentityResolutionCandidateReject(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateReject(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		after, err := q.GetIdentityMatchCandidate(ctx, candidate.ID)
		if err != nil {
			t.Fatalf("GetIdentityMatchCandidate(): %v", err)
		}
		if after.Status != "rejected" {
			t.Fatalf("candidate status = %q, want rejected", after.Status)
		}
	})
}

func TestHandleIdentityResolutionCandidateRejectIgnoresReviewedByFormValue(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "ambiguous-user",
			Email:          "ambiguous@example.com",
			DisplayName:    "Ambiguous User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{}`,
		})
		candidateIdentity := insertCommandSearchIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Candidate")
		candidate, err := q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:           accountID,
			CandidateIdentityID: candidateIdentity,
			ConfidenceBand:      "conflict",
			Score:               40,
			MatchReason:         "ambiguous_primary_email",
			ResolverVersion:     "test",
			ResolverFingerprint: "candidate",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(): %v", err)
		}

		c, rec := newTestContext(http.MethodPost, "http://example.com/api/identity-resolution/candidates/"+strconv.FormatInt(candidate.ID, 10)+"/reject?reviewed_by=spoof@example.com")
		(*c).SetPath("/api/identity-resolution/candidates/:id/reject")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(candidate.ID, 10)}})
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: 1, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleIdentityResolutionCandidateReject(c); err != nil {
			t.Fatalf("HandleIdentityResolutionCandidateReject(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		after, err := q.GetIdentityMatchCandidate(ctx, candidate.ID)
		if err != nil {
			t.Fatalf("GetIdentityMatchCandidate(): %v", err)
		}
		if !after.ReviewedBy.Valid || after.ReviewedBy.String != "admin@example.com" {
			t.Fatalf("reviewed_by = %+v, want authenticated principal", after.ReviewedBy)
		}
	})
}
