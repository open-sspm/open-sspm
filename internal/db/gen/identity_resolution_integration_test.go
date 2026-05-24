package gen

import (
	"context"
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIdentityResolutionCoreSchemaAllowsAmbiguousEmailsButExclusiveAnchors(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "seed", "identity-resolution")
		accountA := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "okta",
			SourceName:     "acme.okta.com",
			ExternalID:     "00u1",
			Email:          "shared@example.com",
			DisplayName:    "Shared A",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"id":"00u1"}`,
		})
		accountB := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "github",
			SourceName:     "acme",
			ExternalID:     "gh-1",
			Email:          "shared@example.com",
			DisplayName:    "Shared B",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"id":"gh-1"}`,
		})
		identityA := insertIdentity(t, ctx, pool, "human", "shared@example.com", "Shared A")
		identityB := insertIdentity(t, ctx, pool, "human", "shared@example.com", "Shared B")

		emailA, err := q.UpsertIdentityEmail(ctx, UpsertIdentityEmailParams{
			IdentityID:        identityA,
			Email:             "Shared@Example.com",
			NormalizedEmail:   "Shared@Example.com",
			EmailKind:         "primary",
			VerificationState: "verified_authoritative",
			LifecycleState:    "active",
			IsPrimary:         true,
			SourceKind:        pgtype.Text{String: "okta", Valid: true},
			SourceName:        pgtype.Text{String: "acme.okta.com", Valid: true},
			SourceAccountID:   pgtype.Int8{Int64: accountA, Valid: true},
		})
		if err != nil {
			t.Fatalf("UpsertIdentityEmail(identity A): %v", err)
		}
		if emailA.NormalizedEmail != "shared@example.com" {
			t.Fatalf("normalized email = %q, want shared@example.com", emailA.NormalizedEmail)
		}
		emailAReplay, err := q.UpsertIdentityEmail(ctx, UpsertIdentityEmailParams{
			IdentityID:        identityA,
			Email:             "shared@example.com",
			NormalizedEmail:   "shared@example.com",
			EmailKind:         "login",
			VerificationState: "observed",
			LifecycleState:    "active",
			IsPrimary:         false,
			SourceKind:        pgtype.Text{String: "github", Valid: true},
			SourceName:        pgtype.Text{String: "acme", Valid: true},
			SourceAccountID:   pgtype.Int8{Int64: accountB, Valid: true},
		})
		if err != nil {
			t.Fatalf("UpsertIdentityEmail(identity A lower-trust replay): %v", err)
		}
		if emailAReplay.EmailKind != "primary" || emailAReplay.VerificationState != "verified_authoritative" || !emailAReplay.IsPrimary {
			t.Fatalf("replayed email = %+v, want primary verified_authoritative preserved", emailAReplay)
		}

		if _, err := q.UpsertIdentityEmail(ctx, UpsertIdentityEmailParams{
			IdentityID:        identityB,
			Email:             "shared@example.com",
			NormalizedEmail:   "shared@example.com",
			EmailKind:         "primary",
			VerificationState: "observed",
			LifecycleState:    "active",
			IsPrimary:         true,
			SourceKind:        pgtype.Text{String: "github", Valid: true},
			SourceName:        pgtype.Text{String: "acme", Valid: true},
			SourceAccountID:   pgtype.Int8{Int64: accountB, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertIdentityEmail(identity B duplicate global email): %v", err)
		}

		matches, err := q.FindIdentitiesByNormalizedEmail(ctx, FindIdentitiesByNormalizedEmailParams{
			NormalizedEmail: "shared@example.com",
			LifecycleState:  "active",
		})
		if err != nil {
			t.Fatalf("FindIdentitiesByNormalizedEmail(): %v", err)
		}
		if len(matches) != 2 {
			t.Fatalf("email match count = %d, want 2", len(matches))
		}
		if _, err := q.FindUnambiguousIdentityByNormalizedEmail(ctx, "shared@example.com"); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("FindUnambiguousIdentityByNormalizedEmail() err = %v, want pgx.ErrNoRows", err)
		}

		_, primaryErr := pool.Exec(ctx, `
			INSERT INTO identity_emails (identity_id, email, normalized_email, email_kind, verification_state, lifecycle_state, is_primary)
			VALUES ($1, 'other@example.com', 'other@example.com', 'primary', 'manual', 'active', true)
		`, identityA)
		assertUniqueViolation(t, primaryErr, "second active primary email")

		if _, err := q.UpsertAccountAnchor(ctx, UpsertAccountAnchorParams{
			AccountID:             accountA,
			SourceKind:            "okta",
			SourceName:            "acme.okta.com",
			AnchorKind:            "okta_user_id",
			Issuer:                "okta:acme.okta.com",
			AnchorValue:           "00U1",
			NormalizedAnchorValue: "00U1",
			ExtractionMethod:      "connector",
		}); err != nil {
			t.Fatalf("UpsertAccountAnchor(account A): %v", err)
		}
		if _, err := q.UpsertAccountAnchor(ctx, UpsertAccountAnchorParams{
			AccountID:             accountB,
			SourceKind:            "github",
			SourceName:            "acme",
			AnchorKind:            "okta_user_id",
			Issuer:                "okta:acme.okta.com",
			AnchorValue:           "00U1",
			NormalizedAnchorValue: "00U1",
			ExtractionMethod:      "backfill",
		}); err != nil {
			t.Fatalf("UpsertAccountAnchor(account B duplicate observed anchor): %v", err)
		}

		if _, err := q.UpsertIdentityAnchor(ctx, UpsertIdentityAnchorParams{
			IdentityID:            identityA,
			AnchorKind:            "okta_user_id",
			Issuer:                "okta:acme.okta.com",
			AnchorValue:           "00U1",
			NormalizedAnchorValue: "00U1",
			SourceKind:            pgtype.Text{String: "okta", Valid: true},
			SourceName:            pgtype.Text{String: "acme.okta.com", Valid: true},
			SourceAccountID:       pgtype.Int8{Int64: accountA, Valid: true},
			TrustLevel:            "authoritative",
			LifecycleState:        "active",
		}); err != nil {
			t.Fatalf("UpsertIdentityAnchor(identity A): %v", err)
		}
		duplicateAnchor, err := q.UpsertIdentityAnchor(ctx, UpsertIdentityAnchorParams{
			IdentityID:            identityB,
			AnchorKind:            "okta_user_id",
			Issuer:                "okta:acme.okta.com",
			AnchorValue:           "00U1",
			NormalizedAnchorValue: "00U1",
			TrustLevel:            "authoritative",
			LifecycleState:        "active",
		})
		if err != nil {
			t.Fatalf("UpsertIdentityAnchor(identity B duplicate active anchor): %v", err)
		}
		if duplicateAnchor.IdentityID != identityA {
			t.Fatalf("duplicate anchor owner = %d, want existing owner %d", duplicateAnchor.IdentityID, identityA)
		}
	})
}

func TestIdentityResolutionCandidatesAndEvidenceAreIdempotent(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "github", "acme")
		accountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "github",
			SourceName:     "acme",
			ExternalID:     "gh-ambiguous",
			Email:          "ambiguous@example.com",
			DisplayName:    "Ambiguous GitHub",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"login":"ambiguous"}`,
		})
		candidateIdentityID := insertIdentity(t, ctx, pool, "human", "ambiguous@example.com", "Candidate")
		provisional, err := q.EnsureProvisionalIdentityForAccount(ctx, accountID)
		if err != nil {
			t.Fatalf("EnsureProvisionalIdentityForAccount(): %v", err)
		}
		if provisional.ResolutionState != "provisional" {
			t.Fatalf("provisional resolution_state = %q, want provisional", provisional.ResolutionState)
		}

		params := UpsertIdentityMatchCandidateParams{
			AccountID:             accountID,
			CandidateIdentityID:   candidateIdentityID,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisional.ID, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 40,
			MatchReason:           "ambiguous_email",
			AmbiguityKey:          pgtype.Text{String: "email:ambiguous@example.com", Valid: true},
			ResolverVersion:       "test-resolver-v1",
			ResolverFingerprint:   "email:ambiguous@example.com:candidate",
		}
		first, err := q.UpsertIdentityMatchCandidate(ctx, params)
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(first): %v", err)
		}
		second, err := q.UpsertIdentityMatchCandidate(ctx, params)
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(second): %v", err)
		}
		if first.ID != second.ID {
			t.Fatalf("candidate id after second upsert = %d, want %d", second.ID, first.ID)
		}

		evidence, err := q.UpsertCandidateLinkEvidence(ctx, UpsertCandidateLinkEvidenceParams{
			AccountID:     accountID,
			CandidateID:   first.ID,
			EvidenceType:  "negative_ambiguous_email",
			EvidenceKey:   "email",
			AccountValue:  pgtype.Text{String: "ambiguous@example.com", Valid: true},
			IdentityValue: pgtype.Text{String: "ambiguous@example.com", Valid: true},
			SourceKind:    pgtype.Text{String: "github", Valid: true},
			SourceName:    pgtype.Text{String: "acme", Valid: true},
			Strength:      -50,
			IsPositive:    false,
			Metadata:      []byte(`{"candidate_count":2}`),
		})
		if err != nil {
			t.Fatalf("UpsertCandidateLinkEvidence(): %v", err)
		}
		if evidence.CandidateID.Int64 != first.ID {
			t.Fatalf("evidence candidate_id = %d, want %d", evidence.CandidateID.Int64, first.ID)
		}

		// Replay the same evidence — should update in place, not insert a new row.
		replayed, err := q.UpsertCandidateLinkEvidence(ctx, UpsertCandidateLinkEvidenceParams{
			AccountID:     accountID,
			CandidateID:   first.ID,
			EvidenceType:  "negative_ambiguous_email",
			EvidenceKey:   "email",
			AccountValue:  pgtype.Text{String: "ambiguous@example.com", Valid: true},
			IdentityValue: pgtype.Text{String: "ambiguous@example.com", Valid: true},
			SourceKind:    pgtype.Text{String: "github", Valid: true},
			SourceName:    pgtype.Text{String: "acme", Valid: true},
			Strength:      -75,
			IsPositive:    false,
			Metadata:      []byte(`{"candidate_count":3}`),
		})
		if err != nil {
			t.Fatalf("UpsertCandidateLinkEvidence(replay): %v", err)
		}
		if replayed.ID != evidence.ID {
			t.Fatalf("evidence id after replay = %d, want %d", replayed.ID, evidence.ID)
		}
		if replayed.Strength != -75 {
			t.Fatalf("evidence strength after replay = %d, want -75", replayed.Strength)
		}

		rows, err := q.ListIdentityLinkEvidenceForCandidate(ctx, pgtype.Int8{Int64: first.ID, Valid: true})
		if err != nil {
			t.Fatalf("ListIdentityLinkEvidenceForCandidate(): %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("evidence row count = %d, want 1 (upsert should not duplicate)", len(rows))
		}

		if err := q.RejectIdentityMatchCandidate(ctx, RejectIdentityMatchCandidateParams{
			ID:         first.ID,
			ReviewedBy: pgtype.Text{String: "reviewer@example.com", Valid: true},
			ReviewNote: pgtype.Text{String: "not this identity", Valid: true},
		}); err != nil {
			t.Fatalf("RejectIdentityMatchCandidate(): %v", err)
		}
		rejected, err := q.UpsertIdentityMatchCandidate(ctx, params)
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(rejected fingerprint): %v", err)
		}
		if rejected.ID != first.ID || rejected.Status != "rejected" {
			t.Fatalf("rejected fingerprint returned id/status = %d/%q, want %d/rejected", rejected.ID, rejected.Status, first.ID)
		}
		pendingCount, err := q.CountIdentityMatchCandidatesByStatus(ctx, "pending")
		if err != nil {
			t.Fatalf("CountIdentityMatchCandidatesByStatus(pending): %v", err)
		}
		if pendingCount != 0 {
			t.Fatalf("pending candidate count after rejected replay = %d, want 0", pendingCount)
		}

		changed := params
		changed.ResolverFingerprint = "email:ambiguous@example.com:candidate:v2"
		changedAgain, err := q.UpsertIdentityMatchCandidate(ctx, changed)
		if err != nil {
			t.Fatalf("UpsertIdentityMatchCandidate(changed fingerprint): %v", err)
		}
		if changedAgain.ID == first.ID || changedAgain.Status != "pending" {
			t.Fatalf("changed fingerprint id/status = %d/%q, want new pending candidate", changedAgain.ID, changedAgain.Status)
		}
	})
}

func TestIdentityMergeAndAccountRelationshipTables(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "entra", "tenant-1")
		serviceAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "entra",
			SourceName:     "tenant-1",
			ExternalID:     "spn-1",
			DisplayName:    "Automation Principal",
			Status:         "active",
			AccountKind:    "service",
			EntityCategory: "service_principal",
			RawJSON:        `{"status":"active"}`,
		})
		sourceIdentityID := insertIdentity(t, ctx, pool, "human", "old@example.com", "Old Identity")
		targetIdentityID := insertIdentity(t, ctx, pool, "human", "owner@example.com", "Owner Identity")
		if _, err := q.UpsertIdentityEmail(ctx, UpsertIdentityEmailParams{
			IdentityID:        sourceIdentityID,
			Email:             "old@example.com",
			NormalizedEmail:   "old@example.com",
			EmailKind:         "primary",
			VerificationState: "manual",
			LifecycleState:    "active",
			IsPrimary:         true,
		}); err != nil {
			t.Fatalf("UpsertIdentityEmail(source primary): %v", err)
		}
		if err := q.MoveIdentityEmailsToIdentity(ctx, MoveIdentityEmailsToIdentityParams{
			TargetIdentityID: targetIdentityID,
			SourceIdentityID: sourceIdentityID,
		}); err != nil {
			t.Fatalf("MoveIdentityEmailsToIdentity(): %v", err)
		}
		movedEmails, err := q.ListIdentityEmails(ctx, targetIdentityID)
		if err != nil {
			t.Fatalf("ListIdentityEmails(target): %v", err)
		}
		if len(movedEmails) != 1 || !movedEmails[0].IsPrimary || movedEmails[0].EmailKind != "primary" {
			t.Fatalf("moved emails = %+v, want source primary preserved on empty target", movedEmails)
		}
		if _, err := q.UpsertIdentityAccountLink(ctx, UpsertIdentityAccountLinkParams{
			IdentityID: sourceIdentityID,
			AccountID:  serviceAccountID,
			LinkReason: "auto_provisional_ambiguous_email",
			Confidence: 0.5,
		}); err != nil {
			t.Fatalf("UpsertIdentityAccountLink(needs review): %v", err)
		}
		if err := q.MoveIdentityAccountsToIdentity(ctx, MoveIdentityAccountsToIdentityParams{
			TargetIdentityID: targetIdentityID,
			LinkReason:       "manual_merge",
			SourceIdentityID: sourceIdentityID,
		}); err != nil {
			t.Fatalf("MoveIdentityAccountsToIdentity(): %v", err)
		}
		movedLink, err := q.GetIdentityAccountLinkByAccountID(ctx, serviceAccountID)
		if err != nil {
			t.Fatalf("GetIdentityAccountLinkByAccountID(): %v", err)
		}
		if movedLink.IdentityID != targetIdentityID || movedLink.LinkState != "needs_review" || movedLink.LinkReason != "auto_provisional_ambiguous_email" {
			t.Fatalf("moved link = %+v, want target with needs_review reason preserved", movedLink)
		}

		relationship, err := q.UpsertAccountIdentityRelationship(ctx, UpsertAccountIdentityRelationshipParams{
			AccountID:        serviceAccountID,
			IdentityID:       targetIdentityID,
			RelationshipType: "owner",
			SourceKind:       pgtype.Text{String: "entra", Valid: true},
			SourceName:       pgtype.Text{String: "tenant-1", Valid: true},
			Confidence:       70,
			LifecycleState:   "active",
		})
		if err != nil {
			t.Fatalf("UpsertAccountIdentityRelationship(first): %v", err)
		}
		updated, err := q.UpsertAccountIdentityRelationship(ctx, UpsertAccountIdentityRelationshipParams{
			AccountID:        serviceAccountID,
			IdentityID:       targetIdentityID,
			RelationshipType: "owner",
			SourceKind:       pgtype.Text{String: "entra", Valid: true},
			SourceName:       pgtype.Text{String: "tenant-1", Valid: true},
			Confidence:       95,
			LifecycleState:   "active",
		})
		if err != nil {
			t.Fatalf("UpsertAccountIdentityRelationship(second): %v", err)
		}
		if updated.ID != relationship.ID {
			t.Fatalf("relationship id after second upsert = %d, want %d", updated.ID, relationship.ID)
		}
		if updated.Confidence != 95 {
			t.Fatalf("relationship confidence = %d, want 95", updated.Confidence)
		}

		relationships, err := q.ListAccountIdentityRelationships(ctx, ListAccountIdentityRelationshipsParams{
			AccountID:      serviceAccountID,
			LifecycleState: "active",
		})
		if err != nil {
			t.Fatalf("ListAccountIdentityRelationships(): %v", err)
		}
		if len(relationships) != 1 {
			t.Fatalf("relationship count = %d, want 1", len(relationships))
		}

		mergeEvent, err := q.CreateIdentityMergeEvent(ctx, CreateIdentityMergeEventParams{
			SourceIdentityID: sourceIdentityID,
			TargetIdentityID: targetIdentityID,
			Status:           "pending",
			Reason:           "manual_review",
			ReviewedBy:       pgtype.Text{String: "reviewer@example.com", Valid: true},
			Metadata:         []byte(`{"source":"test"}`),
		})
		if err != nil {
			t.Fatalf("CreateIdentityMergeEvent(): %v", err)
		}
		redirect, err := q.UpsertIdentityMergeRedirect(ctx, UpsertIdentityMergeRedirectParams{
			SourceIdentityID: sourceIdentityID,
			TargetIdentityID: targetIdentityID,
			MergeEventID:     mergeEvent.ID,
		})
		if err != nil {
			t.Fatalf("UpsertIdentityMergeRedirect(): %v", err)
		}
		if redirect.TargetIdentityID != targetIdentityID {
			t.Fatalf("redirect target = %d, want %d", redirect.TargetIdentityID, targetIdentityID)
		}
		applied, err := q.MarkIdentityMergeEventApplied(ctx, mergeEvent.ID)
		if err != nil {
			t.Fatalf("MarkIdentityMergeEventApplied(): %v", err)
		}
		if applied.Status != "applied" || !applied.AppliedAt.Valid {
			t.Fatalf("applied merge event = %+v, want status applied with applied_at", applied)
		}
	})
}

func assertUniqueViolation(t *testing.T, err error, label string) {
	t.Helper()

	if err == nil {
		t.Fatalf("%s succeeded; want unique violation", label)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("%s err = %v, want unique_violation (23505)", label, err)
	}
}
