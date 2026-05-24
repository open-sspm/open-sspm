package identity

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/normalize"
)

const (
	linkReasonAutoAnchor                    = "auto_anchor"
	linkReasonAutoEmail                     = "auto_email"
	linkReasonAutoProvisionalIdentity       = "auto_provisional_identity"
	linkReasonAutoProvisionalAmbiguousEmail = "auto_provisional_ambiguous_email"
	linkReasonAutoProvisionalAnchorConflict = "auto_provisional_conflicting_anchor"
)

type queryRunner interface {
	CountAccountsMissingIdentityLinkByConfiguredSources(context.Context, gen.CountAccountsMissingIdentityLinkByConfiguredSourcesParams) (int64, error)
	ListAccountsMissingIdentityLinkPageByConfiguredSources(context.Context, gen.ListAccountsMissingIdentityLinkPageByConfiguredSourcesParams) ([]gen.Account, error)
	ListProvisionalIdentityLinkAccountsPageByConfiguredSources(context.Context, gen.ListProvisionalIdentityLinkAccountsPageByConfiguredSourcesParams) ([]gen.Account, error)
	ResolveIdentityByPrimaryEmail(context.Context, gen.ResolveIdentityByPrimaryEmailParams) (gen.ResolveIdentityByPrimaryEmailRow, error)
	CountIdentitiesByPrimaryEmail(context.Context, string) (int64, error)
	ListIdentityCandidatesByPrimaryEmail(context.Context, gen.ListIdentityCandidatesByPrimaryEmailParams) ([]gen.ListIdentityCandidatesByPrimaryEmailRow, error)
	CreateIdentity(context.Context, gen.CreateIdentityParams) (gen.Identity, error)
	UpsertIdentityAccountLink(context.Context, gen.UpsertIdentityAccountLinkParams) (gen.IdentityAccount, error)
	GetIdentityAccountLinkByAccountID(context.Context, int64) (gen.IdentityAccount, error)
	ListIdentityEmails(context.Context, int64) ([]gen.IdentityEmail, error)
	UpsertIdentityEmail(context.Context, gen.UpsertIdentityEmailParams) (gen.IdentityEmail, error)
	UpsertAccountAnchor(context.Context, gen.UpsertAccountAnchorParams) (gen.AccountAnchor, error)
	ListIdentityAnchorMatchesForAccount(context.Context, int64) ([]gen.ListIdentityAnchorMatchesForAccountRow, error)
	UpsertIdentityAnchor(context.Context, gen.UpsertIdentityAnchorParams) (gen.IdentityAnchor, error)
	UpsertIdentityMatchCandidate(context.Context, gen.UpsertIdentityMatchCandidateParams) (gen.UpsertIdentityMatchCandidateRow, error)
	UpsertCandidateLinkEvidence(context.Context, gen.UpsertCandidateLinkEvidenceParams) (gen.IdentityLinkEvidence, error)
	UpsertIdentityLinkEvidence(context.Context, gen.UpsertIdentityLinkEvidenceParams) (gen.IdentityLinkEvidence, error)
	CountActiveAccountsForIdentity(context.Context, int64) (int64, error)
	GetIdentityForIdentityResolution(context.Context, int64) (gen.Identity, error)
	CreateIdentityMergeEvent(context.Context, gen.CreateIdentityMergeEventParams) (gen.IdentityMergeEvent, error)
	MoveIdentityAccountsToIdentity(context.Context, gen.MoveIdentityAccountsToIdentityParams) error
	MoveIdentityEmailsToIdentity(context.Context, gen.MoveIdentityEmailsToIdentityParams) error
	MoveIdentityAnchorsToIdentity(context.Context, gen.MoveIdentityAnchorsToIdentityParams) error
	MoveAccountIdentityRelationshipsToIdentity(context.Context, gen.MoveAccountIdentityRelationshipsToIdentityParams) error
	RetireRemainingAccountIdentityRelationships(context.Context, gen.RetireRemainingAccountIdentityRelationshipsParams) error
	MarkIdentityMerged(context.Context, int64) error
	UpsertIdentityMergeRedirect(context.Context, gen.UpsertIdentityMergeRedirectParams) (gen.IdentityMergeRedirect, error)
	MarkIdentityMergeEventApplied(context.Context, int64) (gen.IdentityMergeEvent, error)
	ListAuthoritativeSourcesByConfiguredSources(context.Context, gen.ListAuthoritativeSourcesByConfiguredSourcesParams) ([]gen.IdentitySourceSetting, error)
	ListIdentityAccountAttributesByConfiguredSources(context.Context, gen.ListIdentityAccountAttributesByConfiguredSourcesParams) ([]gen.ListIdentityAccountAttributesByConfiguredSourcesRow, error)
	UpdateIdentityAttributes(context.Context, gen.UpdateIdentityAttributesParams) error
}

type Resolver struct {
	Q                     queryRunner
	Pool                  *pgxpool.Pool
	ConfiguredSourceKinds []string
	ConfiguredSourceNames []string
}

type Stats struct {
	MissingIdentityLinksBefore int64
	ProvisionalIdentities      int64
	AnchorMatchedLinks         int64
	EmailMatchedLinks          int64
	ProvisionalLinks           int64
	UpgradedProvisionalLinks   int64
	UpdatedIdentityEmails      int64
	UpdatedIdentityAnchors     int64
	UpdatedIdentities          int64
}

func Resolve(ctx context.Context, q *gen.Queries) (Stats, error) {
	return ResolveWithConfiguredSources(ctx, q, nil, nil)
}

func ResolveWithConfiguredSources(ctx context.Context, q *gen.Queries, configuredSourceKinds, configuredSourceNames []string) (Stats, error) {
	r := Resolver{Q: q}
	r.ConfiguredSourceKinds = append([]string(nil), configuredSourceKinds...)
	r.ConfiguredSourceNames = append([]string(nil), configuredSourceNames...)
	return r.Resolve(ctx)
}

func ResolveWithConfiguredSourcesTx(ctx context.Context, pool *pgxpool.Pool, configuredSourceKinds, configuredSourceNames []string) (Stats, error) {
	if pool == nil {
		return Stats{}, errors.New("identity resolver database pool is nil")
	}
	r := Resolver{Q: gen.New(pool), Pool: pool}
	r.ConfiguredSourceKinds = append([]string(nil), configuredSourceKinds...)
	r.ConfiguredSourceNames = append([]string(nil), configuredSourceNames...)
	return r.Resolve(ctx)
}

func (s *Stats) add(other Stats) {
	s.MissingIdentityLinksBefore += other.MissingIdentityLinksBefore
	s.ProvisionalIdentities += other.ProvisionalIdentities
	s.AnchorMatchedLinks += other.AnchorMatchedLinks
	s.EmailMatchedLinks += other.EmailMatchedLinks
	s.ProvisionalLinks += other.ProvisionalLinks
	s.UpgradedProvisionalLinks += other.UpgradedProvisionalLinks
	s.UpdatedIdentityEmails += other.UpdatedIdentityEmails
	s.UpdatedIdentityAnchors += other.UpdatedIdentityAnchors
	s.UpdatedIdentities += other.UpdatedIdentities
}

func (r Resolver) withShortTransaction(ctx context.Context, fn func(Resolver) error) error {
	if r.Pool == nil {
		return fn(r)
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	txResolver := r
	txResolver.Pool = nil
	txResolver.Q = gen.New(tx)
	if err := fn(txResolver); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r Resolver) Resolve(ctx context.Context) (Stats, error) {
	if r.Q == nil {
		return Stats{}, errors.New("identity resolver query runner is nil")
	}
	if len(r.ConfiguredSourceKinds) != len(r.ConfiguredSourceNames) {
		return Stats{}, errors.New("identity resolver configured source scope is malformed")
	}

	var out Stats
	if len(r.ConfiguredSourceKinds) == 0 {
		return out, nil
	}

	authoritativeSources, err := r.authoritativeSourceSet(ctx)
	if err != nil {
		return out, err
	}
	updatedAnchors, err := r.refreshIdentityAnchors(ctx, authoritativeSources)
	if err != nil {
		return out, err
	}
	out.UpdatedIdentityAnchors += updatedAnchors

	sourceScope := gen.CountAccountsMissingIdentityLinkByConfiguredSourcesParams{
		ConfiguredSourceKinds: r.ConfiguredSourceKinds,
		ConfiguredSourceNames: r.ConfiguredSourceNames,
	}
	count, err := r.Q.CountAccountsMissingIdentityLinkByConfiguredSources(ctx, sourceScope)
	if err != nil {
		return out, err
	}
	out.MissingIdentityLinksBefore = count

	cursor := int64(0)
	for {
		accounts, err := r.Q.ListAccountsMissingIdentityLinkPageByConfiguredSources(ctx, gen.ListAccountsMissingIdentityLinkPageByConfiguredSourcesParams{
			PageLimit:             500,
			CursorAccountID:       cursor,
			ConfiguredSourceKinds: r.ConfiguredSourceKinds,
			ConfiguredSourceNames: r.ConfiguredSourceNames,
		})
		if err != nil {
			return out, err
		}
		if len(accounts) == 0 {
			break
		}

		for _, account := range accounts {
			if account.ID > cursor {
				cursor = account.ID
			}
			var accountStats Stats
			err := r.withShortTransaction(ctx, func(txResolver Resolver) error {
				var err error
				accountStats, err = txResolver.resolveMissingIdentityLinkForAccount(ctx, account, authoritativeSources)
				return err
			})
			if err != nil {
				return out, err
			}
			out.add(accountStats)
		}
	}

	upgradeStats, err := r.upgradeProvisionalLinksByAnchors(ctx, authoritativeSources)
	if err != nil {
		return out, err
	}
	out.add(upgradeStats)

	updated, err := r.refreshIdentityAttributes(ctx)
	if err != nil {
		return out, err
	}
	out.UpdatedIdentities = updated

	return out, nil
}

func (r Resolver) resolveMissingIdentityLinkForAccount(ctx context.Context, account gen.Account, authoritativeSources map[string]struct{}) (Stats, error) {
	var out Stats
	identityID, reason, createdIdentity, err := r.resolveIdentityIDForAccount(ctx, account)
	if err != nil {
		return out, err
	}
	if createdIdentity {
		out.ProvisionalIdentities++
	}

	_, err = r.Q.UpsertIdentityAccountLink(ctx, gen.UpsertIdentityAccountLinkParams{
		IdentityID: identityID,
		AccountID:  account.ID,
		LinkReason: reason,
		Confidence: 1.0,
	})
	if err != nil {
		return out, err
	}

	_, isAuthoritativeSource := authoritativeSources[sourceKey(account.SourceKind, account.SourceName)]
	emailVerification := "observed"
	if isAuthoritativeSource {
		emailVerification = "verified_authoritative"
	}
	updatedEmail, err := r.persistIdentityEmailForAccount(ctx, identityID, account, emailVerification)
	if err != nil {
		return out, err
	}
	if updatedEmail {
		out.UpdatedIdentityEmails++
	}

	if isAuthoritativeSource {
		updated, err := r.persistIdentityAnchorsForAccount(ctx, identityID, account, "authoritative")
		if err != nil {
			return out, err
		}
		out.UpdatedIdentityAnchors += updated
	}

	if reason == linkReasonAutoEmail {
		out.EmailMatchedLinks++
	} else if reason == linkReasonAutoAnchor {
		out.AnchorMatchedLinks++
	} else {
		out.ProvisionalLinks++
	}
	return out, nil
}

func (r Resolver) upgradeProvisionalLinksByAnchors(ctx context.Context, authoritativeSources map[string]struct{}) (Stats, error) {
	var out Stats
	cursor := int64(0)
	for {
		accounts, err := r.Q.ListProvisionalIdentityLinkAccountsPageByConfiguredSources(ctx, gen.ListProvisionalIdentityLinkAccountsPageByConfiguredSourcesParams{
			PageLimit:             500,
			CursorAccountID:       cursor,
			ConfiguredSourceKinds: r.ConfiguredSourceKinds,
			ConfiguredSourceNames: r.ConfiguredSourceNames,
		})
		if err != nil {
			return out, err
		}
		if len(accounts) == 0 {
			break
		}

		for _, account := range accounts {
			if account.ID > cursor {
				cursor = account.ID
			}
			var accountStats Stats
			err := r.withShortTransaction(ctx, func(txResolver Resolver) error {
				var err error
				accountStats, err = txResolver.upgradeProvisionalLinkByAnchors(ctx, account, authoritativeSources)
				return err
			})
			if err != nil {
				return out, err
			}
			out.add(accountStats)
		}
	}

	return out, nil
}

func (r Resolver) upgradeProvisionalLinkByAnchors(ctx context.Context, account gen.Account, authoritativeSources map[string]struct{}) (Stats, error) {
	var out Stats
	existing, err := r.Q.GetIdentityAccountLinkByAccountID(ctx, account.ID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, nil
		}
		return out, err
	}
	if !isResolverProvisionalReason(existing.LinkReason) {
		return out, nil
	}

	anchors, err := r.persistAccountAnchors(ctx, account)
	if err != nil {
		return out, err
	}
	if len(anchors) == 0 {
		return out, nil
	}

	anchorMatches, err := r.Q.ListIdentityAnchorMatchesForAccount(ctx, account.ID)
	if err != nil {
		return out, err
	}
	if identityID, ok := uniqueAnchorIdentity(anchorMatches); ok {
		if err := r.persistAutoAnchorEvidence(ctx, account, identityID, anchorMatches); err != nil {
			return out, err
		}
		if _, err := r.Q.UpsertIdentityAccountLink(ctx, gen.UpsertIdentityAccountLinkParams{
			IdentityID: identityID,
			AccountID:  account.ID,
			LinkReason: linkReasonAutoAnchor,
			Confidence: 1.0,
		}); err != nil {
			return out, err
		}
		if _, err := r.mergeEmptyProvisionalIdentity(ctx, existing.IdentityID, identityID, "resolver_anchor_upgrade"); err != nil {
			return out, err
		}
		_, isAuthoritativeSource := authoritativeSources[sourceKey(account.SourceKind, account.SourceName)]
		emailVerification := "observed"
		if isAuthoritativeSource {
			emailVerification = "verified_authoritative"
		}
		updatedEmail, err := r.persistIdentityEmailForAccount(ctx, identityID, account, emailVerification)
		if err != nil {
			return out, err
		}
		if updatedEmail {
			out.UpdatedIdentityEmails++
		}
		if isAuthoritativeSource {
			updated, err := r.persistIdentityAnchorsForAccount(ctx, identityID, account, "authoritative")
			if err != nil {
				return out, err
			}
			out.UpdatedIdentityAnchors += updated
		}
		out.UpgradedProvisionalLinks++
		out.AnchorMatchedLinks++
		return out, nil
	}

	if len(anchorMatchIdentitySet(anchorMatches)) > 1 {
		if err := r.persistAnchorConflictCandidates(ctx, account, existing.IdentityID, anchorMatches); err != nil {
			return out, err
		}
		if _, err := r.Q.UpsertIdentityAccountLink(ctx, gen.UpsertIdentityAccountLinkParams{
			IdentityID: existing.IdentityID,
			AccountID:  account.ID,
			LinkReason: linkReasonAutoProvisionalAnchorConflict,
			Confidence: 1.0,
		}); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (r Resolver) mergeEmptyProvisionalIdentity(ctx context.Context, sourceIdentityID, targetIdentityID int64, reason string) (bool, error) {
	if sourceIdentityID <= 0 || targetIdentityID <= 0 || sourceIdentityID == targetIdentityID {
		return false, nil
	}
	source, err := r.Q.GetIdentityForIdentityResolution(ctx, sourceIdentityID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	state := strings.TrimSpace(source.ResolutionState)
	if state != "provisional" && state != "needs_review" {
		return false, nil
	}
	remaining, err := r.Q.CountActiveAccountsForIdentity(ctx, sourceIdentityID)
	if err != nil {
		return false, err
	}
	if remaining != 0 {
		return false, nil
	}

	metadata, _ := json.Marshal(map[string]any{
		"source": "identity_resolver",
		"reason": reason,
	})
	mergeEvent, err := r.Q.CreateIdentityMergeEvent(ctx, gen.CreateIdentityMergeEventParams{
		SourceIdentityID: sourceIdentityID,
		TargetIdentityID: targetIdentityID,
		Status:           "pending",
		Reason:           reason,
		Metadata:         metadata,
	})
	if err != nil {
		return false, err
	}
	if err := r.Q.MoveIdentityAccountsToIdentity(ctx, gen.MoveIdentityAccountsToIdentityParams{
		TargetIdentityID: targetIdentityID,
		LinkReason:       linkReasonAutoAnchor,
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return false, err
	}
	if err := r.Q.MoveIdentityEmailsToIdentity(ctx, gen.MoveIdentityEmailsToIdentityParams{
		TargetIdentityID: targetIdentityID,
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return false, err
	}
	if err := r.Q.MoveIdentityAnchorsToIdentity(ctx, gen.MoveIdentityAnchorsToIdentityParams{
		TargetIdentityID: targetIdentityID,
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return false, err
	}
	if err := r.Q.MoveAccountIdentityRelationshipsToIdentity(ctx, gen.MoveAccountIdentityRelationshipsToIdentityParams{
		TargetIdentityID: targetIdentityID,
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return false, err
	}
	if err := r.Q.RetireRemainingAccountIdentityRelationships(ctx, gen.RetireRemainingAccountIdentityRelationshipsParams{
		TargetIdentityID: targetIdentityID,
		SourceIdentityID: sourceIdentityID,
	}); err != nil {
		return false, err
	}
	if err := r.Q.MarkIdentityMerged(ctx, sourceIdentityID); err != nil {
		return false, err
	}
	if _, err := r.Q.UpsertIdentityMergeRedirect(ctx, gen.UpsertIdentityMergeRedirectParams{
		SourceIdentityID: sourceIdentityID,
		TargetIdentityID: targetIdentityID,
		MergeEventID:     mergeEvent.ID,
	}); err != nil {
		return false, err
	}
	if _, err := r.Q.MarkIdentityMergeEventApplied(ctx, mergeEvent.ID); err != nil {
		return false, err
	}
	return true, nil
}

func (r Resolver) resolveIdentityIDForAccount(ctx context.Context, account gen.Account) (identityID int64, reason string, createdIdentity bool, err error) {
	existing, err := r.Q.GetIdentityAccountLinkByAccountID(ctx, account.ID)
	if err == nil {
		if strings.EqualFold(strings.TrimSpace(existing.LinkReason), "manual") {
			return existing.IdentityID, "manual", false, nil
		}
		return existing.IdentityID, strings.TrimSpace(existing.LinkReason), false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, err
	}

	email := normalizeEmail(account.Email)
	accountKind := registry.NormalizeAccountKind(account.AccountKind)
	anchors, err := r.persistAccountAnchors(ctx, account)
	if err != nil {
		return 0, "", false, err
	}
	if len(anchors) > 0 {
		anchorMatches, err := r.Q.ListIdentityAnchorMatchesForAccount(ctx, account.ID)
		if err != nil {
			return 0, "", false, err
		}
		if identityID, ok := uniqueAnchorIdentity(anchorMatches); ok {
			if err := r.persistAutoAnchorEvidence(ctx, account, identityID, anchorMatches); err != nil {
				return 0, "", false, err
			}
			return identityID, linkReasonAutoAnchor, false, nil
		}
		if len(anchorMatchIdentitySet(anchorMatches)) > 1 {
			identity, err := r.Q.CreateIdentity(ctx, gen.CreateIdentityParams{
				Kind:            accountKind,
				DisplayName:     strings.TrimSpace(account.DisplayName),
				PrimaryEmail:    email,
				ResolutionState: "provisional",
				IdentityKind:    accountKind,
			})
			if err != nil {
				return 0, "", false, err
			}
			if err := r.persistAnchorConflictCandidates(ctx, account, identity.ID, anchorMatches); err != nil {
				return 0, "", false, err
			}
			return identity.ID, linkReasonAutoProvisionalAnchorConflict, true, nil
		}
	}

	provisionalReason := linkReasonAutoProvisionalIdentity
	var ambiguousCandidates []gen.ListIdentityCandidatesByPrimaryEmailRow
	if email != "" && accountKind != registry.AccountKindService && accountKind != registry.AccountKindBot {
		match, matchErr := r.Q.ResolveIdentityByPrimaryEmail(ctx, gen.ResolveIdentityByPrimaryEmailParams{
			ConfiguredSourceKinds: r.ConfiguredSourceKinds,
			ConfiguredSourceNames: r.ConfiguredSourceNames,
			PrimaryEmail:          email,
		})
		if matchErr == nil {
			return match.IdentityID, strings.TrimSpace(match.LinkReason), false, nil
		}
		if !errors.Is(matchErr, pgx.ErrNoRows) {
			return 0, "", false, matchErr
		}
		candidateCount, err := r.Q.CountIdentitiesByPrimaryEmail(ctx, email)
		if err != nil {
			return 0, "", false, err
		}
		if candidateCount > 0 {
			provisionalReason = linkReasonAutoProvisionalAmbiguousEmail
			ambiguousCandidates, err = r.Q.ListIdentityCandidatesByPrimaryEmail(ctx, gen.ListIdentityCandidatesByPrimaryEmailParams{
				ConfiguredSourceKinds: r.ConfiguredSourceKinds,
				ConfiguredSourceNames: r.ConfiguredSourceNames,
				PrimaryEmail:          email,
			})
			if err != nil {
				return 0, "", false, err
			}
		}
	}

	identity, err := r.Q.CreateIdentity(ctx, gen.CreateIdentityParams{
		Kind:            accountKind,
		DisplayName:     strings.TrimSpace(account.DisplayName),
		PrimaryEmail:    email,
		ResolutionState: "provisional",
		IdentityKind:    accountKind,
	})
	if err != nil {
		return 0, "", false, err
	}
	if provisionalReason == linkReasonAutoProvisionalAmbiguousEmail {
		if err := r.persistAmbiguousEmailCandidates(ctx, account, identity.ID, email, ambiguousCandidates); err != nil {
			return 0, "", false, err
		}
	}
	return identity.ID, provisionalReason, true, nil
}

func (r Resolver) persistAmbiguousEmailCandidates(ctx context.Context, account gen.Account, provisionalIdentityID int64, email string, candidates []gen.ListIdentityCandidatesByPrimaryEmailRow) error {
	for _, candidate := range candidates {
		score := int32(40)
		if candidate.IsAuthoritative {
			score = 50
		}
		fingerprint := fmt.Sprintf("ambiguous_primary_email:%s:%d:%d", email, account.ID, candidate.IdentityID)
		row, err := r.Q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:             account.ID,
			CandidateIdentityID:   candidate.IdentityID,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisionalIdentityID, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 score,
			MatchReason:           "ambiguous_primary_email",
			AmbiguityKey:          pgtype.Text{String: "email:" + email, Valid: true},
			ResolverVersion:       "identity-resolver-v1",
			ResolverFingerprint:   fingerprint,
		})
		if err != nil {
			return err
		}
		if row.Status != "pending" {
			continue
		}
		if _, err := r.Q.UpsertCandidateLinkEvidence(ctx, gen.UpsertCandidateLinkEvidenceParams{
			AccountID:     account.ID,
			CandidateID:   row.ID,
			EvidenceType:  "negative_ambiguous_email",
			EvidenceKey:   "primary_email",
			AccountValue:  pgtype.Text{String: email, Valid: true},
			IdentityValue: pgtype.Text{String: candidate.PrimaryEmail, Valid: true},
			SourceKind:    pgtype.Text{String: account.SourceKind, Valid: strings.TrimSpace(account.SourceKind) != ""},
			SourceName:    pgtype.Text{String: account.SourceName, Valid: strings.TrimSpace(account.SourceName) != ""},
			Strength:      -50,
			IsPositive:    false,
			Metadata:      []byte(`{"reason":"ambiguous_primary_email"}`),
		}); err != nil {
			return err
		}
	}
	return nil
}

type accountAnchor struct {
	Kind            string
	Issuer          string
	Value           string
	NormalizedValue string
	Method          string
}

func (r Resolver) persistAccountAnchors(ctx context.Context, account gen.Account) ([]accountAnchor, error) {
	anchors := extractAccountAnchors(account.ID, account.SourceKind, account.SourceName, account.ExternalID, account.AccountKind, account.EntityCategory, account.RawJson)
	for _, anchor := range anchors {
		if _, err := r.Q.UpsertAccountAnchor(ctx, gen.UpsertAccountAnchorParams{
			AccountID:             account.ID,
			SourceKind:            strings.TrimSpace(account.SourceKind),
			SourceName:            strings.TrimSpace(account.SourceName),
			AnchorKind:            anchor.Kind,
			Issuer:                anchor.Issuer,
			AnchorValue:           anchor.Value,
			NormalizedAnchorValue: anchor.NormalizedValue,
			ExtractionMethod:      anchor.Method,
		}); err != nil {
			return nil, err
		}
	}
	return anchors, nil
}

func (r Resolver) persistIdentityEmailForAccount(ctx context.Context, identityID int64, account gen.Account, verificationState string) (bool, error) {
	email := normalizeEmail(account.Email)
	if email == "" {
		return false, nil
	}
	verificationState = strings.TrimSpace(verificationState)
	if verificationState == "" {
		verificationState = "observed"
	}
	params := gen.UpsertIdentityEmailParams{
		IdentityID:        identityID,
		Email:             strings.TrimSpace(account.Email),
		NormalizedEmail:   email,
		EmailKind:         "login",
		VerificationState: verificationState,
		LifecycleState:    "active",
		IsPrimary:         false,
		SourceKind:        pgtype.Text{String: strings.TrimSpace(account.SourceKind), Valid: strings.TrimSpace(account.SourceKind) != ""},
		SourceName:        pgtype.Text{String: strings.TrimSpace(account.SourceName), Valid: strings.TrimSpace(account.SourceName) != ""},
		SourceAccountID:   pgtype.Int8{Int64: account.ID, Valid: account.ID > 0},
	}
	existingEmails, err := r.Q.ListIdentityEmails(ctx, identityID)
	if err != nil {
		return false, err
	}
	updated := identityEmailWouldChange(existingEmails, params)
	_, err = r.Q.UpsertIdentityEmail(ctx, params)
	if err != nil {
		return false, err
	}
	return updated, nil
}

func identityEmailWouldChange(existingEmails []gen.IdentityEmail, params gen.UpsertIdentityEmailParams) bool {
	normalized := normalizeEmail(params.NormalizedEmail)
	for _, existing := range existingEmails {
		if existing.NormalizedEmail != normalized || existing.LifecycleState != "active" {
			continue
		}
		return identityEmailRowWouldChange(existing, params)
	}
	return true
}

func identityEmailRowWouldChange(existing gen.IdentityEmail, params gen.UpsertIdentityEmailParams) bool {
	if existing.Email != params.Email {
		return true
	}
	if mergeIdentityEmailKind(existing.EmailKind, params.EmailKind) != existing.EmailKind {
		return true
	}
	if strongerIdentityEmailVerification(existing.VerificationState, params.VerificationState) != existing.VerificationState {
		return true
	}
	if !existing.IsPrimary && params.IsPrimary {
		return true
	}
	if !nullableTextEqual(coalesceText(params.SourceKind, existing.SourceKind), existing.SourceKind) {
		return true
	}
	if !nullableTextEqual(coalesceText(params.SourceName, existing.SourceName), existing.SourceName) {
		return true
	}
	return !nullableInt8Equal(coalesceInt8(params.SourceAccountID, existing.SourceAccountID), existing.SourceAccountID)
}

func mergeIdentityEmailKind(current, incoming string) string {
	current = strings.TrimSpace(current)
	incoming = strings.TrimSpace(incoming)
	if incoming == "" {
		incoming = "alias"
	}
	switch {
	case current == "primary":
		return current
	case incoming == "primary":
		return incoming
	case current == "login" && incoming == "alias":
		return current
	default:
		return incoming
	}
}

func strongerIdentityEmailVerification(current, incoming string) string {
	current = strings.TrimSpace(current)
	incoming = strings.TrimSpace(incoming)
	if current == "" {
		current = "observed"
	}
	if incoming == "" {
		incoming = "observed"
	}
	if identityEmailVerificationRank(current) <= identityEmailVerificationRank(incoming) {
		return current
	}
	return incoming
}

func identityEmailVerificationRank(value string) int {
	switch strings.TrimSpace(value) {
	case "verified_authoritative":
		return 0
	case "verified_source":
		return 1
	case "manual":
		return 2
	case "observed":
		return 3
	case "inferred_legacy":
		return 4
	default:
		return 5
	}
}

func coalesceText(first, fallback pgtype.Text) pgtype.Text {
	if first.Valid {
		return first
	}
	return fallback
}

func coalesceInt8(first, fallback pgtype.Int8) pgtype.Int8 {
	if first.Valid {
		return first
	}
	return fallback
}

func nullableTextEqual(left, right pgtype.Text) bool {
	if left.Valid != right.Valid {
		return false
	}
	return !left.Valid || left.String == right.String
}

func nullableInt8Equal(left, right pgtype.Int8) bool {
	if left.Valid != right.Valid {
		return false
	}
	return !left.Valid || left.Int64 == right.Int64
}

func (r Resolver) persistIdentityAnchorsForAccount(ctx context.Context, identityID int64, account gen.Account, trustLevel string) (int64, error) {
	anchors := extractAccountAnchors(account.ID, account.SourceKind, account.SourceName, account.ExternalID, account.AccountKind, account.EntityCategory, account.RawJson)
	updated := int64(0)
	for _, anchor := range anchors {
		row, err := r.Q.UpsertIdentityAnchor(ctx, gen.UpsertIdentityAnchorParams{
			IdentityID:            identityID,
			AnchorKind:            anchor.Kind,
			Issuer:                anchor.Issuer,
			AnchorValue:           anchor.Value,
			NormalizedAnchorValue: anchor.NormalizedValue,
			SourceKind:            pgtype.Text{String: strings.TrimSpace(account.SourceKind), Valid: strings.TrimSpace(account.SourceKind) != ""},
			SourceName:            pgtype.Text{String: strings.TrimSpace(account.SourceName), Valid: strings.TrimSpace(account.SourceName) != ""},
			SourceAccountID:       pgtype.Int8{Int64: account.ID, Valid: account.ID > 0},
			TrustLevel:            trustLevel,
			LifecycleState:        "active",
		})
		if err != nil {
			return updated, err
		}
		if row.IdentityID != identityID {
			continue
		}
		updated++
	}
	return updated, nil
}

func (r Resolver) refreshIdentityAnchors(ctx context.Context, authoritativeSources map[string]struct{}) (int64, error) {
	rows, err := r.Q.ListIdentityAccountAttributesByConfiguredSources(ctx, gen.ListIdentityAccountAttributesByConfiguredSourcesParams{
		ConfiguredSourceKinds: r.ConfiguredSourceKinds,
		ConfiguredSourceNames: r.ConfiguredSourceNames,
	})
	if err != nil {
		return 0, err
	}

	updated := int64(0)
	for _, row := range rows {
		account := gen.Account{
			ID:             row.AccountID,
			SourceKind:     row.SourceKind,
			SourceName:     row.SourceName,
			ExternalID:     row.ExternalID,
			AccountKind:    row.AccountKind,
			EntityCategory: row.EntityCategory,
			RawJson:        row.RawJson,
		}
		if _, err := r.persistAccountAnchors(ctx, account); err != nil {
			return updated, err
		}
		if _, ok := authoritativeSources[sourceKey(row.SourceKind, row.SourceName)]; !ok {
			continue
		}
		switch strings.TrimSpace(row.LinkState) {
		case "accepted", "manual_confirmed":
		default:
			continue
		}
		n, err := r.persistIdentityAnchorsForAccount(ctx, row.IdentityID, account, "authoritative")
		if err != nil {
			return updated, err
		}
		updated += n
	}
	return updated, nil
}

func (r Resolver) authoritativeSourceSet(ctx context.Context) (map[string]struct{}, error) {
	sourceScope := gen.ListAuthoritativeSourcesByConfiguredSourcesParams{
		ConfiguredSourceKinds: r.ConfiguredSourceKinds,
		ConfiguredSourceNames: r.ConfiguredSourceNames,
	}
	sources, err := r.Q.ListAuthoritativeSourcesByConfiguredSources(ctx, sourceScope)
	if err != nil {
		return nil, err
	}
	authoritative := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		authoritative[sourceKey(source.SourceKind, source.SourceName)] = struct{}{}
	}
	return authoritative, nil
}

func uniqueAnchorIdentity(matches []gen.ListIdentityAnchorMatchesForAccountRow) (int64, bool) {
	identities := anchorMatchIdentitySet(matches)
	if len(identities) != 1 {
		return 0, false
	}
	for identityID := range identities {
		return identityID, true
	}
	return 0, false
}

func anchorMatchIdentitySet(matches []gen.ListIdentityAnchorMatchesForAccountRow) map[int64]struct{} {
	out := make(map[int64]struct{})
	for _, match := range matches {
		out[match.IdentityID] = struct{}{}
	}
	return out
}

func anchorMatchEvidenceKey(matches []gen.ListIdentityAnchorMatchesForAccountRow) string {
	sorted := append([]gen.ListIdentityAnchorMatchesForAccountRow(nil), matches...)
	sort.Slice(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		if left.AnchorKind != right.AnchorKind {
			return left.AnchorKind < right.AnchorKind
		}
		if left.Issuer != right.Issuer {
			return left.Issuer < right.Issuer
		}
		if left.NormalizedAnchorValue != right.NormalizedAnchorValue {
			return left.NormalizedAnchorValue < right.NormalizedAnchorValue
		}
		return left.TrustLevel < right.TrustLevel
	})

	var b strings.Builder
	for _, match := range sorted {
		b.WriteString(match.AnchorKind)
		b.WriteByte('|')
		b.WriteString(match.Issuer)
		b.WriteByte('|')
		b.WriteString(match.NormalizedAnchorValue)
		b.WriteByte('|')
		b.WriteString(match.TrustLevel)
		b.WriteByte('\n')
	}
	return b.String()
}

func isResolverProvisionalReason(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case linkReasonAutoProvisionalIdentity, linkReasonAutoProvisionalAmbiguousEmail, linkReasonAutoProvisionalAnchorConflict, "seed_orphan":
		return true
	default:
		return false
	}
}

func (r Resolver) persistAutoAnchorEvidence(ctx context.Context, account gen.Account, identityID int64, matches []gen.ListIdentityAnchorMatchesForAccountRow) error {
	for _, match := range matches {
		if match.IdentityID != identityID {
			continue
		}
		if _, err := r.Q.UpsertIdentityLinkEvidence(ctx, gen.UpsertIdentityLinkEvidenceParams{
			AccountID:     account.ID,
			IdentityID:    identityID,
			EvidenceType:  "anchor_exact",
			EvidenceKey:   match.AnchorKind,
			AccountValue:  pgtype.Text{String: match.NormalizedAnchorValue, Valid: match.NormalizedAnchorValue != ""},
			IdentityValue: pgtype.Text{String: match.NormalizedAnchorValue, Valid: match.NormalizedAnchorValue != ""},
			SourceKind:    pgtype.Text{String: account.SourceKind, Valid: strings.TrimSpace(account.SourceKind) != ""},
			SourceName:    pgtype.Text{String: account.SourceName, Valid: strings.TrimSpace(account.SourceName) != ""},
			Strength:      100,
			IsPositive:    true,
			Metadata:      []byte(fmt.Sprintf(`{"issuer":%q,"trust_level":%q}`, match.Issuer, match.TrustLevel)),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r Resolver) persistAnchorConflictCandidates(ctx context.Context, account gen.Account, provisionalIdentityID int64, matches []gen.ListIdentityAnchorMatchesForAccountRow) error {
	byIdentity := make(map[int64][]gen.ListIdentityAnchorMatchesForAccountRow)
	for _, match := range matches {
		byIdentity[match.IdentityID] = append(byIdentity[match.IdentityID], match)
	}
	for identityID, identityMatches := range byIdentity {
		score := int32(90)
		for _, match := range identityMatches {
			if match.TrustLevel == "authoritative" {
				score = 100
				break
			}
		}
		fingerprintHash := sha256.Sum256([]byte(anchorMatchEvidenceKey(identityMatches)))
		fingerprint := fmt.Sprintf("conflicting_anchor:%d:%d:%x", account.ID, identityID, fingerprintHash)
		row, err := r.Q.UpsertIdentityMatchCandidate(ctx, gen.UpsertIdentityMatchCandidateParams{
			AccountID:             account.ID,
			CandidateIdentityID:   identityID,
			ProvisionalIdentityID: pgtype.Int8{Int64: provisionalIdentityID, Valid: true},
			ConfidenceBand:        "conflict",
			Score:                 score,
			MatchReason:           "conflicting_anchors",
			AmbiguityKey:          pgtype.Text{String: "anchor_conflict:" + strconv.FormatInt(account.ID, 10), Valid: true},
			ResolverVersion:       "identity-resolver-v1",
			ResolverFingerprint:   fingerprint,
		})
		if err != nil {
			return err
		}
		if row.Status != "pending" {
			continue
		}
		for _, match := range identityMatches {
			if _, err := r.Q.UpsertCandidateLinkEvidence(ctx, gen.UpsertCandidateLinkEvidenceParams{
				AccountID:     account.ID,
				CandidateID:   row.ID,
				EvidenceType:  "negative_conflicting_anchor",
				EvidenceKey:   match.AnchorKind,
				AccountValue:  pgtype.Text{String: match.NormalizedAnchorValue, Valid: match.NormalizedAnchorValue != ""},
				IdentityValue: pgtype.Text{String: match.NormalizedAnchorValue, Valid: match.NormalizedAnchorValue != ""},
				SourceKind:    pgtype.Text{String: account.SourceKind, Valid: strings.TrimSpace(account.SourceKind) != ""},
				SourceName:    pgtype.Text{String: account.SourceName, Valid: strings.TrimSpace(account.SourceName) != ""},
				Strength:      -100,
				IsPositive:    false,
				Metadata:      []byte(fmt.Sprintf(`{"issuer":%q,"trust_level":%q}`, match.Issuer, match.TrustLevel)),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func extractAccountAnchors(accountID int64, sourceKind, sourceName, externalID, accountKind, entityCategory string, rawJSON []byte) []accountAnchor {
	_ = accountID
	sourceKind = strings.ToLower(strings.TrimSpace(sourceKind))
	sourceName = strings.TrimSpace(sourceName)
	externalID = strings.TrimSpace(externalID)
	accountKind = registry.NormalizeAccountKind(accountKind)
	entityCategory = registry.NormalizeEntityCategory(entityCategory)
	raw := rawJSONMap(rawJSON)

	anchors := make([]accountAnchor, 0, 4)
	add := func(kind, issuer, value, method string) {
		kind = strings.TrimSpace(kind)
		issuer = strings.TrimSpace(issuer)
		value = strings.TrimSpace(value)
		method = strings.TrimSpace(method)
		if kind == "" || issuer == "" || value == "" {
			return
		}
		normalized := strings.ToLower(strings.TrimSpace(value))
		for _, existing := range anchors {
			if existing.Kind == kind && existing.Issuer == issuer && existing.NormalizedValue == normalized {
				return
			}
		}
		anchors = append(anchors, accountAnchor{
			Kind:            kind,
			Issuer:          issuer,
			Value:           value,
			NormalizedValue: normalized,
			Method:          method,
		})
	}

	switch sourceKind {
	case "okta":
		add("okta_user_id", "okta:"+sourceName, firstNonEmptyString(rawString(raw, "id"), externalID), "connector")
	case "entra":
		value := firstNonEmptyString(rawString(raw, "id"), rawString(raw, "objectId"), rawString(raw, "object_id"), stripKnownExternalIDPrefix(externalID))
		switch {
		case entityCategory == registry.EntityCategoryServicePrincipal || strings.HasPrefix(strings.ToLower(externalID), "sp:"):
			add("entra_service_principal_object_id", "entra:"+sourceName, value, "connector")
		case entityCategory == registry.EntityCategoryGroup || strings.HasPrefix(strings.ToLower(externalID), "group:"):
			add("entra_group_id", "entra:"+sourceName, value, "connector")
		default:
			add("entra_object_id", "entra:"+sourceName, value, "connector")
		}
	case "google_workspace":
		value := firstNonEmptyString(rawString(raw, "id"), externalID)
		if entityCategory == registry.EntityCategoryGroup {
			add("google_group_id", "google_workspace:"+sourceName, value, "connector")
		} else {
			add("google_user_id", "google_workspace:"+sourceName, value, "connector")
		}
	case "github":
		if entityCategory == registry.EntityCategoryTeam || strings.HasPrefix(strings.ToLower(externalID), "team:") {
			add("github_team_id", "github:org:"+sourceName, firstNonEmptyString(rawString(raw, "id"), stripKnownExternalIDPrefix(externalID)), "connector")
		} else {
			add("github_user_id", "github:org:"+sourceName, rawString(raw, "id"), "connector")
		}
	case "aws_identity_center", "aws":
		value := firstNonEmptyString(rawString(raw, "UserId"), rawString(raw, "user_id"), rawString(raw, "id"), stripKnownExternalIDPrefix(externalID))
		if entityCategory == registry.EntityCategoryGroup || strings.HasPrefix(strings.ToLower(externalID), "group:") {
			add("aws_identity_center_group_id", "aws_identity_center:"+sourceName, value, "connector")
		} else {
			add("aws_identity_center_user_id", "aws_identity_center:"+sourceName, value, "connector")
		}
	case "datadog":
		value := firstNonEmptyString(rawString(raw, "id"), stripKnownExternalIDPrefix(externalID))
		if entityCategory == registry.EntityCategoryServiceAccount || strings.HasPrefix(strings.ToLower(externalID), "service_account:") {
			add("datadog_service_account_id", "datadog:"+sourceName, value, "connector")
		} else if accountKind == registry.AccountKindHuman || entityCategory == registry.EntityCategoryUser {
			add("datadog_user_id", "datadog:"+sourceName, value, "connector")
		}
	case "vault":
		add("vault_entity_id", "vault:"+sourceName, firstNonEmptyString(rawString(raw, "id"), externalID), "connector")
	}

	addEmbeddedAnchors(raw, add)
	return anchors
}

func addEmbeddedAnchors(raw map[string]any, add func(kind, issuer, value, method string)) {
	if len(raw) == 0 {
		return
	}
	if value := firstNonEmptyString(rawString(raw, "okta_user_id"), rawString(raw, "oktaUserId"), rawString(raw, "okta_id"), rawString(raw, "oktaId")); value != "" {
		if issuer := issuerFromRaw(raw, "okta", "okta_issuer", "okta_source_name", "okta_domain"); issuer != "" {
			add("okta_user_id", issuer, value, "raw_json")
		}
	}
	if value := firstNonEmptyString(rawString(raw, "entra_object_id"), rawString(raw, "entraObjectId"), rawString(raw, "azure_ad_object_id"), rawString(raw, "aad_object_id")); value != "" {
		if issuer := issuerFromRaw(raw, "entra", "entra_issuer", "entra_tenant_id", "tenant_id"); issuer != "" {
			add("entra_object_id", issuer, value, "raw_json")
		}
	}
	if value := firstNonEmptyString(rawString(raw, "scim_external_id"), rawString(raw, "scimExternalId"), rawString(raw, "externalId")); value != "" {
		if issuer := issuerFromRaw(raw, "scim", "scim_issuer", "scim_source", "scim_source_name"); issuer != "" {
			add("scim_external_id", issuer, value, "raw_json")
		}
	}
	if value := firstNonEmptyString(rawString(raw, "saml_nameid_persistent"), rawString(raw, "samlNameID"), rawString(raw, "saml_name_id"), rawString(raw, "name_id")); value != "" {
		if issuer := issuerFromRaw(raw, "saml", "saml_issuer", "saml_idp_entity_id", "idp_entity_id"); issuer != "" {
			add("saml_nameid_persistent", issuer, value, "raw_json")
		}
	}
}

func rawJSONMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func rawString(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := raw[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			if s := strings.TrimSpace(typed); s != "" {
				return s
			}
		case float64:
			if typed == float64(int64(typed)) {
				return strconv.FormatInt(int64(typed), 10)
			}
			return strconv.FormatFloat(typed, 'f', -1, 64)
		case json.Number:
			return typed.String()
		}
	}
	return ""
}

func issuerFromRaw(raw map[string]any, prefix string, keys ...string) string {
	value := rawString(raw, keys...)
	if value == "" {
		return ""
	}
	if strings.Contains(value, ":") {
		return value
	}
	return prefix + ":" + value
}

func stripKnownExternalIDPrefix(value string) string {
	value = strings.TrimSpace(value)
	lower := strings.ToLower(value)
	for _, prefix := range []string{"sp:", "group:", "team:", "service_account:"} {
		if strings.HasPrefix(lower, prefix) {
			return strings.TrimSpace(value[len(prefix):])
		}
	}
	return value
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func (r Resolver) refreshIdentityAttributes(ctx context.Context) (int64, error) {
	authoritative, err := r.authoritativeSourceSet(ctx)
	if err != nil {
		return 0, err
	}

	rows, err := r.Q.ListIdentityAccountAttributesByConfiguredSources(ctx, gen.ListIdentityAccountAttributesByConfiguredSourcesParams{
		ConfiguredSourceKinds: r.ConfiguredSourceKinds,
		ConfiguredSourceNames: r.ConfiguredSourceNames,
	})
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	byIdentity := make(map[int64][]gen.ListIdentityAccountAttributesByConfiguredSourcesRow)
	for _, row := range rows {
		byIdentity[row.IdentityID] = append(byIdentity[row.IdentityID], row)
	}

	updated := int64(0)
	for identityID, candidates := range byIdentity {
		email, displayName := chooseIdentityAttributes(candidates, authoritative)
		kind := chooseIdentityKind(candidates)
		if _, hasEmail := firstNonEmptyEmail(candidates); !hasEmail {
			email = ""
		}
		if err := r.Q.UpdateIdentityAttributes(ctx, gen.UpdateIdentityAttributesParams{
			ID:           identityID,
			DisplayName:  displayName,
			PrimaryEmail: email,
			Kind:         kind,
		}); err != nil {
			return updated, err
		}
		updated++
	}

	return updated, nil
}

func chooseIdentityAttributes(candidates []gen.ListIdentityAccountAttributesByConfiguredSourcesRow, authoritative map[string]struct{}) (email string, displayName string) {
	if len(candidates) == 0 {
		return "", ""
	}

	sorted := append([]gen.ListIdentityAccountAttributesByConfiguredSourcesRow(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		_, leftAuth := authoritative[sourceKey(left.SourceKind, left.SourceName)]
		_, rightAuth := authoritative[sourceKey(right.SourceKind, right.SourceName)]
		if leftAuth != rightAuth {
			return leftAuth
		}
		leftEmail := normalizeEmail(left.Email)
		rightEmail := normalizeEmail(right.Email)
		if (leftEmail != "") != (rightEmail != "") {
			return leftEmail != ""
		}
		return left.AccountID < right.AccountID
	})

	for _, candidate := range sorted {
		if email == "" {
			email = normalizeEmail(candidate.Email)
		}
		if displayName == "" {
			displayName = strings.TrimSpace(candidate.DisplayName)
		}
		if email != "" && displayName != "" {
			break
		}
	}

	return email, displayName
}

func firstNonEmptyEmail(candidates []gen.ListIdentityAccountAttributesByConfiguredSourcesRow) (string, bool) {
	for _, candidate := range candidates {
		email := normalizeEmail(candidate.Email)
		if email != "" {
			return email, true
		}
	}
	return "", false
}

func chooseIdentityKind(candidates []gen.ListIdentityAccountAttributesByConfiguredSourcesRow) string {
	if len(candidates) == 0 {
		return registry.AccountKindUnknown
	}

	accountKinds := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		accountKinds = append(accountKinds, candidate.AccountKind)
	}
	aggregated := registry.AggregateAccountKinds(accountKinds...)
	if aggregated != registry.AccountKindUnknown {
		return aggregated
	}

	return chooseIdentityKindByHeuristic(candidates)
}

func chooseIdentityKindByHeuristic(candidates []gen.ListIdentityAccountAttributesByConfiguredSourcesRow) string {
	hasEmail := false
	hasServiceSignal := false
	for _, candidate := range candidates {
		email := normalizeEmail(candidate.Email)
		if email != "" {
			hasEmail = true
		}

		classifierText := buildIdentityClassifierText(candidate, email)
		if registry.HasIndicator(classifierText, registry.BotIndicators()) {
			return registry.AccountKindBot
		}
		if registry.HasIndicator(classifierText, registry.ServiceIndicators()) {
			hasServiceSignal = true
		}
		if strings.EqualFold(strings.TrimSpace(candidate.SourceKind), "vault") {
			hasServiceSignal = true
		}
	}

	if hasServiceSignal {
		return registry.AccountKindService
	}
	if hasEmail {
		return registry.AccountKindHuman
	}
	return registry.AccountKindUnknown
}

func buildIdentityClassifierText(candidate gen.ListIdentityAccountAttributesByConfiguredSourcesRow, normalizedEmail string) string {
	parts := []string{
		strings.TrimSpace(candidate.DisplayName),
		strings.TrimSpace(candidate.ExternalID),
	}
	if normalizedEmail != "" {
		if local, _, ok := strings.Cut(normalizedEmail, "@"); ok {
			parts = append(parts, local)
		}
	}
	return registry.NormalizeClassifierText(strings.Join(parts, " "))
}

func sourceKey(kind, name string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + "::" + strings.ToLower(strings.TrimSpace(name))
}

func normalizeEmail(email string) string {
	return normalize.Email(email)
}
