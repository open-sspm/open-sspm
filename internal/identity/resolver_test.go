package identity

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type resolverStub struct {
	accounts        map[int64]gen.Account
	identities      map[int64]gen.Identity
	linksByAccount  map[int64]gen.IdentityAccount
	accountAnchors  map[int64][]gen.AccountAnchor
	identityAnchors []gen.IdentityAnchor
	identityEmails  map[int64][]gen.IdentityEmail
	candidates      map[string]gen.UpsertIdentityMatchCandidateRow
	evidence        []gen.IdentityLinkEvidence
	mergeRedirects  map[int64]gen.IdentityMergeRedirect
	sources         []gen.IdentitySourceSetting

	nextIdentityID  int64
	nextLinkID      int64
	nextEmailID     int64
	nextAnchorID    int64
	nextCandidateID int64
	nextEvidenceID  int64
	nextMergeID     int64
}

func newResolverStub() *resolverStub {
	return &resolverStub{
		accounts:        make(map[int64]gen.Account),
		identities:      make(map[int64]gen.Identity),
		linksByAccount:  make(map[int64]gen.IdentityAccount),
		accountAnchors:  make(map[int64][]gen.AccountAnchor),
		identityEmails:  make(map[int64][]gen.IdentityEmail),
		candidates:      make(map[string]gen.UpsertIdentityMatchCandidateRow),
		mergeRedirects:  make(map[int64]gen.IdentityMergeRedirect),
		nextIdentityID:  1,
		nextLinkID:      1,
		nextEmailID:     1,
		nextAnchorID:    1,
		nextCandidateID: 1,
		nextEvidenceID:  1,
		nextMergeID:     1,
	}
}

func resolverForStub(stub *resolverStub) Resolver {
	kinds, names := stub.configuredSourcePairs()
	return Resolver{
		Q:                     stub,
		ConfiguredSourceKinds: kinds,
		ConfiguredSourceNames: names,
	}
}

func (s *resolverStub) configuredSourcePairs() ([]string, []string) {
	type pair struct {
		kind string
		name string
	}
	seen := map[pair]struct{}{}
	pairs := make([]pair, 0)
	for _, account := range s.accounts {
		kind := strings.TrimSpace(account.SourceKind)
		name := strings.TrimSpace(account.SourceName)
		if kind == "" || name == "" {
			continue
		}
		p := pair{kind: kind, name: name}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].kind != pairs[j].kind {
			return pairs[i].kind < pairs[j].kind
		}
		return pairs[i].name < pairs[j].name
	})
	kinds := make([]string, 0, len(pairs))
	names := make([]string, 0, len(pairs))
	for _, p := range pairs {
		kinds = append(kinds, p.kind)
		names = append(names, p.name)
	}
	return kinds, names
}

func sourceInScope(kind, name string, configuredSourceKinds, configuredSourceNames []string) bool {
	kind = strings.TrimSpace(kind)
	name = strings.TrimSpace(name)
	for i := range configuredSourceKinds {
		if i >= len(configuredSourceNames) {
			break
		}
		if strings.TrimSpace(configuredSourceKinds[i]) == kind && strings.TrimSpace(configuredSourceNames[i]) == name {
			return true
		}
	}
	return false
}

func (s *resolverStub) putIdentity(identity gen.Identity) {
	s.identities[identity.ID] = identity
	if identity.ID >= s.nextIdentityID {
		s.nextIdentityID = identity.ID + 1
	}
}

func (s *resolverStub) putLink(link gen.IdentityAccount) {
	if strings.TrimSpace(link.LinkState) == "" {
		link.LinkState = linkStateForReason(link.LinkReason)
	}
	s.linksByAccount[link.AccountID] = link
	if link.ID >= s.nextLinkID {
		s.nextLinkID = link.ID + 1
	}
}

func (s *resolverStub) CountAccountsMissingIdentityLinkByConfiguredSources(_ context.Context, params gen.CountAccountsMissingIdentityLinkByConfiguredSourcesParams) (int64, error) {
	var count int64
	for _, account := range s.accounts {
		if !isActiveAccount(account) {
			continue
		}
		if !sourceInScope(account.SourceKind, account.SourceName, params.ConfiguredSourceKinds, params.ConfiguredSourceNames) {
			continue
		}
		if _, linked := s.linksByAccount[account.ID]; linked {
			continue
		}
		count++
	}
	return count, nil
}

func (s *resolverStub) ListAccountsMissingIdentityLinkPageByConfiguredSources(_ context.Context, params gen.ListAccountsMissingIdentityLinkPageByConfiguredSourcesParams) ([]gen.Account, error) {
	rows := make([]gen.Account, 0)
	for _, account := range s.accounts {
		if !isActiveAccount(account) {
			continue
		}
		if !sourceInScope(account.SourceKind, account.SourceName, params.ConfiguredSourceKinds, params.ConfiguredSourceNames) {
			continue
		}
		if _, linked := s.linksByAccount[account.ID]; linked {
			continue
		}
		if account.ID <= params.CursorAccountID {
			continue
		}
		rows = append(rows, account)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ID < rows[j].ID
	})

	if int(params.PageLimit) < len(rows) {
		rows = rows[:params.PageLimit]
	}
	return rows, nil
}

func (s *resolverStub) ListProvisionalIdentityLinkAccountsPageByConfiguredSources(_ context.Context, params gen.ListProvisionalIdentityLinkAccountsPageByConfiguredSourcesParams) ([]gen.Account, error) {
	rows := make([]gen.Account, 0)
	for _, account := range s.accounts {
		if !isActiveAccount(account) {
			continue
		}
		if !sourceInScope(account.SourceKind, account.SourceName, params.ConfiguredSourceKinds, params.ConfiguredSourceNames) {
			continue
		}
		link, linked := s.linksByAccount[account.ID]
		if !linked || !isResolverProvisionalReason(link.LinkReason) {
			continue
		}
		if account.ID <= params.CursorAccountID {
			continue
		}
		rows = append(rows, account)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ID < rows[j].ID
	})

	if int(params.PageLimit) < len(rows) {
		rows = rows[:params.PageLimit]
	}
	return rows, nil
}

func (s *resolverStub) authoritativeIdentityIDs(configuredSourceKinds, configuredSourceNames []string) map[int64]struct{} {
	authoritativeByIdentity := make(map[int64]struct{})
	authoritativeSources := make(map[string]struct{}, len(s.sources))
	for _, source := range s.sources {
		if !source.IsAuthoritative {
			continue
		}
		if !sourceInScope(source.SourceKind, source.SourceName, configuredSourceKinds, configuredSourceNames) {
			continue
		}
		authoritativeSources[sourceKey(source.SourceKind, source.SourceName)] = struct{}{}
	}
	for _, link := range s.linksByAccount {
		account, ok := s.accounts[link.AccountID]
		if !ok || !isActiveAccount(account) {
			continue
		}
		if !sourceInScope(account.SourceKind, account.SourceName, configuredSourceKinds, configuredSourceNames) {
			continue
		}
		if _, ok := authoritativeSources[sourceKey(account.SourceKind, account.SourceName)]; ok {
			authoritativeByIdentity[link.IdentityID] = struct{}{}
		}
	}
	return authoritativeByIdentity
}

func (s *resolverStub) ResolveIdentityByPrimaryEmail(_ context.Context, params gen.ResolveIdentityByPrimaryEmailParams) (gen.ResolveIdentityByPrimaryEmailRow, error) {
	target := normalizeTestEmail(params.PrimaryEmail)
	if target == "" {
		return gen.ResolveIdentityByPrimaryEmailRow{}, pgx.ErrNoRows
	}

	authoritativeByIdentity := s.authoritativeIdentityIDs(params.ConfiguredSourceKinds, params.ConfiguredSourceNames)

	candidates := make([]gen.Identity, 0)
	anyAuthoritative := false
	for _, identity := range s.identities {
		if normalizeTestEmail(identity.PrimaryEmail) != target {
			continue
		}
		if !isConfirmedTestIdentity(identity) {
			continue
		}
		candidates = append(candidates, identity)
		if _, ok := authoritativeByIdentity[identity.ID]; ok {
			anyAuthoritative = true
		}
	}
	if len(candidates) == 0 {
		return gen.ResolveIdentityByPrimaryEmailRow{}, pgx.ErrNoRows
	}

	topTier := make([]gen.Identity, 0, len(candidates))
	for _, identity := range candidates {
		_, isAuth := authoritativeByIdentity[identity.ID]
		if isAuth == anyAuthoritative {
			topTier = append(topTier, identity)
		}
	}

	sort.Slice(topTier, func(i, j int) bool {
		return topTier[i].ID < topTier[j].ID
	})
	reason := linkReasonAutoEmail
	if len(topTier) != 1 {
		return gen.ResolveIdentityByPrimaryEmailRow{}, pgx.ErrNoRows
	}
	return gen.ResolveIdentityByPrimaryEmailRow{
		IdentityID: topTier[0].ID,
		LinkReason: reason,
	}, nil
}

func (s *resolverStub) CountIdentitiesByPrimaryEmail(_ context.Context, email string) (int64, error) {
	target := normalizeTestEmail(email)
	if target == "" {
		return 0, nil
	}
	var count int64
	for _, identity := range s.identities {
		if normalizeTestEmail(identity.PrimaryEmail) == target {
			if strings.TrimSpace(identity.ResolutionState) == "merged" || strings.TrimSpace(identity.ResolutionState) == "disabled" {
				continue
			}
			count++
		}
	}
	return count, nil
}

func (s *resolverStub) ListIdentityCandidatesByPrimaryEmail(_ context.Context, params gen.ListIdentityCandidatesByPrimaryEmailParams) ([]gen.ListIdentityCandidatesByPrimaryEmailRow, error) {
	target := normalizeTestEmail(params.PrimaryEmail)
	if target == "" {
		return nil, nil
	}

	authoritativeByIdentity := s.authoritativeIdentityIDs(params.ConfiguredSourceKinds, params.ConfiguredSourceNames)
	rows := make([]gen.ListIdentityCandidatesByPrimaryEmailRow, 0)
	for _, identity := range s.identities {
		if normalizeTestEmail(identity.PrimaryEmail) != target {
			continue
		}
		if strings.TrimSpace(identity.ResolutionState) == "merged" || strings.TrimSpace(identity.ResolutionState) == "disabled" {
			continue
		}
		_, isAuthoritative := authoritativeByIdentity[identity.ID]
		rows = append(rows, gen.ListIdentityCandidatesByPrimaryEmailRow{
			IdentityID:      identity.ID,
			IsAuthoritative: isAuthoritative,
			PrimaryEmail:    identity.PrimaryEmail,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].IsAuthoritative != rows[j].IsAuthoritative {
			return rows[i].IsAuthoritative
		}
		return rows[i].IdentityID < rows[j].IdentityID
	})
	return rows, nil
}

func (s *resolverStub) CreateIdentity(_ context.Context, params gen.CreateIdentityParams) (gen.Identity, error) {
	row := gen.Identity{
		ID:              s.nextIdentityID,
		Kind:            strings.TrimSpace(params.Kind),
		DisplayName:     strings.TrimSpace(params.DisplayName),
		PrimaryEmail:    normalizeTestEmail(params.PrimaryEmail),
		ResolutionState: strings.TrimSpace(params.ResolutionState),
		IdentityKind:    strings.TrimSpace(params.IdentityKind),
	}
	if row.Kind == "" {
		row.Kind = "unknown"
	}
	if row.ResolutionState == "" {
		row.ResolutionState = "confirmed"
	}
	if row.IdentityKind == "" {
		row.IdentityKind = row.Kind
	}
	s.identities[row.ID] = row
	s.nextIdentityID++
	return row, nil
}

func (s *resolverStub) UpsertIdentityMatchCandidate(_ context.Context, params gen.UpsertIdentityMatchCandidateParams) (gen.UpsertIdentityMatchCandidateRow, error) {
	key := strings.Join([]string{
		strings.TrimSpace(params.ResolverFingerprint),
		strings.TrimSpace(params.AmbiguityKey.String),
	}, "|")
	row, ok := s.candidates[key]
	if !ok {
		row = gen.UpsertIdentityMatchCandidateRow{
			ID:                  s.nextCandidateID,
			AccountID:           params.AccountID,
			CandidateIdentityID: params.CandidateIdentityID,
			Status:              "pending",
			ResolverFingerprint: params.ResolverFingerprint,
		}
		s.nextCandidateID++
	}
	row.ProvisionalIdentityID = params.ProvisionalIdentityID
	row.ConfidenceBand = params.ConfidenceBand
	row.Score = params.Score
	row.MatchReason = params.MatchReason
	row.AmbiguityKey = params.AmbiguityKey
	row.ResolverVersion = params.ResolverVersion
	s.candidates[key] = row
	return row, nil
}

func (s *resolverStub) UpsertCandidateLinkEvidence(_ context.Context, params gen.UpsertCandidateLinkEvidenceParams) (gen.IdentityLinkEvidence, error) {
	candidateID := pgtype.Int8{Int64: params.CandidateID, Valid: true}
	for i := range s.evidence {
		existing := s.evidence[i]
		if existing.AccountID != params.AccountID {
			continue
		}
		if !existing.CandidateID.Valid || existing.CandidateID.Int64 != params.CandidateID {
			continue
		}
		if existing.EvidenceType != params.EvidenceType || existing.EvidenceKey != params.EvidenceKey {
			continue
		}
		s.evidence[i].IdentityID = params.IdentityID
		s.evidence[i].AccountValue = params.AccountValue
		s.evidence[i].IdentityValue = params.IdentityValue
		s.evidence[i].SourceKind = params.SourceKind
		s.evidence[i].SourceName = params.SourceName
		s.evidence[i].Strength = params.Strength
		s.evidence[i].IsPositive = params.IsPositive
		s.evidence[i].Metadata = params.Metadata
		return s.evidence[i], nil
	}
	row := gen.IdentityLinkEvidence{
		ID:            s.nextEvidenceID,
		AccountID:     params.AccountID,
		IdentityID:    params.IdentityID,
		CandidateID:   candidateID,
		EvidenceType:  params.EvidenceType,
		EvidenceKey:   params.EvidenceKey,
		AccountValue:  params.AccountValue,
		IdentityValue: params.IdentityValue,
		SourceKind:    params.SourceKind,
		SourceName:    params.SourceName,
		Strength:      params.Strength,
		IsPositive:    params.IsPositive,
		Metadata:      params.Metadata,
	}
	s.nextEvidenceID++
	s.evidence = append(s.evidence, row)
	return row, nil
}

func (s *resolverStub) UpsertIdentityLinkEvidence(_ context.Context, params gen.UpsertIdentityLinkEvidenceParams) (gen.IdentityLinkEvidence, error) {
	identityID := pgtype.Int8{Int64: params.IdentityID, Valid: true}
	for i := range s.evidence {
		existing := s.evidence[i]
		if existing.AccountID != params.AccountID {
			continue
		}
		if existing.CandidateID.Valid {
			continue
		}
		if !existing.IdentityID.Valid || existing.IdentityID.Int64 != params.IdentityID {
			continue
		}
		if existing.EvidenceType != params.EvidenceType || existing.EvidenceKey != params.EvidenceKey {
			continue
		}
		s.evidence[i].AccountValue = params.AccountValue
		s.evidence[i].IdentityValue = params.IdentityValue
		s.evidence[i].SourceKind = params.SourceKind
		s.evidence[i].SourceName = params.SourceName
		s.evidence[i].Strength = params.Strength
		s.evidence[i].IsPositive = params.IsPositive
		s.evidence[i].Metadata = params.Metadata
		return s.evidence[i], nil
	}
	row := gen.IdentityLinkEvidence{
		ID:            s.nextEvidenceID,
		AccountID:     params.AccountID,
		IdentityID:    identityID,
		EvidenceType:  params.EvidenceType,
		EvidenceKey:   params.EvidenceKey,
		AccountValue:  params.AccountValue,
		IdentityValue: params.IdentityValue,
		SourceKind:    params.SourceKind,
		SourceName:    params.SourceName,
		Strength:      params.Strength,
		IsPositive:    params.IsPositive,
		Metadata:      params.Metadata,
	}
	s.nextEvidenceID++
	s.evidence = append(s.evidence, row)
	return row, nil
}

func (s *resolverStub) UpsertIdentityAccountLink(_ context.Context, params gen.UpsertIdentityAccountLinkParams) (gen.IdentityAccount, error) {
	current, exists := s.linksByAccount[params.AccountID]
	if !exists {
		current = gen.IdentityAccount{
			ID:         s.nextLinkID,
			AccountID:  params.AccountID,
			IdentityID: params.IdentityID,
			LinkReason: params.LinkReason,
			Confidence: params.Confidence,
			LinkState:  linkStateForReason(params.LinkReason),
		}
		s.nextLinkID++
	} else {
		current.IdentityID = params.IdentityID
		current.LinkReason = params.LinkReason
		current.Confidence = params.Confidence
		current.LinkState = linkStateForReason(params.LinkReason)
	}
	s.linksByAccount[params.AccountID] = current
	return current, nil
}

func linkStateForReason(reason string) string {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "manual":
		return "manual_confirmed"
	case linkReasonAutoProvisionalAmbiguousEmail, linkReasonAutoProvisionalAnchorConflict:
		return "needs_review"
	case linkReasonAutoProvisionalIdentity, "seed_orphan":
		return "provisional"
	default:
		return "accepted"
	}
}

func (s *resolverStub) GetIdentityAccountLinkByAccountID(_ context.Context, accountID int64) (gen.IdentityAccount, error) {
	row, ok := s.linksByAccount[accountID]
	if !ok {
		return gen.IdentityAccount{}, pgx.ErrNoRows
	}
	return row, nil
}

func (s *resolverStub) UpsertIdentityEmail(_ context.Context, params gen.UpsertIdentityEmailParams) (gen.IdentityEmail, error) {
	normalized := normalizeTestEmail(params.NormalizedEmail)
	for i, row := range s.identityEmails[params.IdentityID] {
		if row.NormalizedEmail == normalized && row.LifecycleState == "active" {
			row.Email = params.Email
			row.EmailKind = params.EmailKind
			row.VerificationState = params.VerificationState
			row.IsPrimary = row.IsPrimary || params.IsPrimary
			row.SourceKind = params.SourceKind
			row.SourceName = params.SourceName
			row.SourceAccountID = params.SourceAccountID
			s.identityEmails[params.IdentityID][i] = row
			return row, nil
		}
	}
	row := gen.IdentityEmail{
		ID:                s.nextEmailID,
		IdentityID:        params.IdentityID,
		Email:             params.Email,
		NormalizedEmail:   normalized,
		EmailKind:         params.EmailKind,
		VerificationState: params.VerificationState,
		LifecycleState:    params.LifecycleState,
		IsPrimary:         params.IsPrimary,
		SourceKind:        params.SourceKind,
		SourceName:        params.SourceName,
		SourceAccountID:   params.SourceAccountID,
	}
	if row.LifecycleState == "" {
		row.LifecycleState = "active"
	}
	s.nextEmailID++
	s.identityEmails[params.IdentityID] = append(s.identityEmails[params.IdentityID], row)
	return row, nil
}

func (s *resolverStub) UpsertAccountAnchor(_ context.Context, params gen.UpsertAccountAnchorParams) (gen.AccountAnchor, error) {
	normalized := strings.ToLower(strings.TrimSpace(params.NormalizedAnchorValue))
	for i, row := range s.accountAnchors[params.AccountID] {
		if row.AnchorKind == params.AnchorKind && row.Issuer == params.Issuer && row.NormalizedAnchorValue == normalized {
			row.AnchorValue = params.AnchorValue
			row.ExtractionMethod = params.ExtractionMethod
			s.accountAnchors[params.AccountID][i] = row
			return row, nil
		}
	}
	row := gen.AccountAnchor{
		ID:                    s.nextAnchorID,
		AccountID:             params.AccountID,
		SourceKind:            params.SourceKind,
		SourceName:            params.SourceName,
		AnchorKind:            params.AnchorKind,
		Issuer:                params.Issuer,
		AnchorValue:           params.AnchorValue,
		NormalizedAnchorValue: normalized,
		ExtractionMethod:      params.ExtractionMethod,
	}
	s.nextAnchorID++
	s.accountAnchors[params.AccountID] = append(s.accountAnchors[params.AccountID], row)
	return row, nil
}

func (s *resolverStub) UpsertIdentityAnchor(_ context.Context, params gen.UpsertIdentityAnchorParams) (gen.IdentityAnchor, error) {
	normalized := strings.ToLower(strings.TrimSpace(params.NormalizedAnchorValue))
	for i, row := range s.identityAnchors {
		if row.AnchorKind == params.AnchorKind && row.Issuer == params.Issuer && row.NormalizedAnchorValue == normalized && row.LifecycleState == "active" {
			if row.IdentityID != params.IdentityID {
				return row, nil
			}
			row.AnchorValue = params.AnchorValue
			row.TrustLevel = params.TrustLevel
			row.SourceKind = params.SourceKind
			row.SourceName = params.SourceName
			row.SourceAccountID = params.SourceAccountID
			s.identityAnchors[i] = row
			return row, nil
		}
	}
	row := gen.IdentityAnchor{
		ID:                    s.nextAnchorID,
		IdentityID:            params.IdentityID,
		AnchorKind:            params.AnchorKind,
		Issuer:                params.Issuer,
		AnchorValue:           params.AnchorValue,
		NormalizedAnchorValue: normalized,
		SourceKind:            params.SourceKind,
		SourceName:            params.SourceName,
		SourceAccountID:       params.SourceAccountID,
		TrustLevel:            params.TrustLevel,
		LifecycleState:        params.LifecycleState,
	}
	if row.LifecycleState == "" {
		row.LifecycleState = "active"
	}
	s.nextAnchorID++
	s.identityAnchors = append(s.identityAnchors, row)
	return row, nil
}

func (s *resolverStub) ListIdentityAnchorMatchesForAccount(_ context.Context, accountID int64) ([]gen.ListIdentityAnchorMatchesForAccountRow, error) {
	accountAnchors := s.accountAnchors[accountID]
	out := make([]gen.ListIdentityAnchorMatchesForAccountRow, 0)
	for _, accountAnchor := range accountAnchors {
		for _, identityAnchor := range s.identityAnchors {
			if identityAnchor.LifecycleState != "active" {
				continue
			}
			if accountAnchor.AnchorKind != identityAnchor.AnchorKind || accountAnchor.Issuer != identityAnchor.Issuer || accountAnchor.NormalizedAnchorValue != identityAnchor.NormalizedAnchorValue {
				continue
			}
			out = append(out, gen.ListIdentityAnchorMatchesForAccountRow{
				AccountID:             accountID,
				IdentityID:            identityAnchor.IdentityID,
				AnchorKind:            identityAnchor.AnchorKind,
				Issuer:                identityAnchor.Issuer,
				NormalizedAnchorValue: identityAnchor.NormalizedAnchorValue,
				TrustLevel:            identityAnchor.TrustLevel,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IdentityID != out[j].IdentityID {
			return out[i].IdentityID < out[j].IdentityID
		}
		return out[i].AnchorKind < out[j].AnchorKind
	})
	return out, nil
}

func (s *resolverStub) ListAuthoritativeSourcesByConfiguredSources(_ context.Context, params gen.ListAuthoritativeSourcesByConfiguredSourcesParams) ([]gen.IdentitySourceSetting, error) {
	out := make([]gen.IdentitySourceSetting, 0, len(s.sources))
	for _, source := range s.sources {
		if !source.IsAuthoritative {
			continue
		}
		if !sourceInScope(source.SourceKind, source.SourceName, params.ConfiguredSourceKinds, params.ConfiguredSourceNames) {
			continue
		}
		out = append(out, source)
	}
	return out, nil
}

func (s *resolverStub) ListIdentityAccountAttributesByConfiguredSources(_ context.Context, params gen.ListIdentityAccountAttributesByConfiguredSourcesParams) ([]gen.ListIdentityAccountAttributesByConfiguredSourcesRow, error) {
	out := make([]gen.ListIdentityAccountAttributesByConfiguredSourcesRow, 0, len(s.linksByAccount))
	for accountID, link := range s.linksByAccount {
		account, ok := s.accounts[accountID]
		if !ok || !isActiveAccount(account) {
			continue
		}
		if !sourceInScope(account.SourceKind, account.SourceName, params.ConfiguredSourceKinds, params.ConfiguredSourceNames) {
			continue
		}
		identity := s.identities[link.IdentityID]
		out = append(out, gen.ListIdentityAccountAttributesByConfiguredSourcesRow{
			IdentityID:     link.IdentityID,
			LinkState:      link.LinkState,
			LinkReason:     link.LinkReason,
			IdentityKind:   identity.Kind,
			AccountID:      account.ID,
			SourceKind:     account.SourceKind,
			SourceName:     account.SourceName,
			ExternalID:     account.ExternalID,
			AccountKind:    account.AccountKind,
			EntityCategory: account.EntityCategory,
			Email:          account.Email,
			DisplayName:    account.DisplayName,
			RawJson:        account.RawJson,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IdentityID != out[j].IdentityID {
			return out[i].IdentityID < out[j].IdentityID
		}
		return out[i].AccountID < out[j].AccountID
	})
	return out, nil
}

func (s *resolverStub) UpdateIdentityAttributes(_ context.Context, params gen.UpdateIdentityAttributesParams) error {
	row, ok := s.identities[params.ID]
	if !ok {
		return nil
	}
	if strings.TrimSpace(params.DisplayName) != "" {
		row.DisplayName = strings.TrimSpace(params.DisplayName)
	}
	if normalizeTestEmail(params.PrimaryEmail) != "" {
		row.PrimaryEmail = normalizeTestEmail(params.PrimaryEmail)
	}
	if strings.TrimSpace(params.Kind) != "" {
		row.Kind = strings.TrimSpace(params.Kind)
	}
	s.identities[params.ID] = row
	return nil
}

func (s *resolverStub) CountActiveAccountsForIdentity(_ context.Context, identityID int64) (int64, error) {
	var count int64
	for accountID, link := range s.linksByAccount {
		if link.IdentityID != identityID {
			continue
		}
		if account, ok := s.accounts[accountID]; ok && isActiveAccount(account) {
			count++
		}
	}
	return count, nil
}

func (s *resolverStub) GetIdentityForIdentityResolution(_ context.Context, identityID int64) (gen.Identity, error) {
	row, ok := s.identities[identityID]
	if !ok {
		return gen.Identity{}, pgx.ErrNoRows
	}
	return row, nil
}

func (s *resolverStub) CreateIdentityMergeEvent(_ context.Context, params gen.CreateIdentityMergeEventParams) (gen.IdentityMergeEvent, error) {
	row := gen.IdentityMergeEvent{
		ID:               s.nextMergeID,
		SourceIdentityID: params.SourceIdentityID,
		TargetIdentityID: params.TargetIdentityID,
		Status:           params.Status,
		Reason:           params.Reason,
		RequestedBy:      params.RequestedBy,
		ReviewedBy:       params.ReviewedBy,
		Metadata:         params.Metadata,
	}
	s.nextMergeID++
	return row, nil
}

func (s *resolverStub) MoveIdentityAccountsToIdentity(_ context.Context, params gen.MoveIdentityAccountsToIdentityParams) error {
	for accountID, link := range s.linksByAccount {
		if link.IdentityID != params.SourceIdentityID {
			continue
		}
		link.IdentityID = params.TargetIdentityID
		if link.LinkState != "needs_review" {
			link.LinkReason = strings.TrimSpace(params.LinkReason)
		}
		s.linksByAccount[accountID] = link
	}
	return nil
}

func (s *resolverStub) MoveIdentityEmailsToIdentity(_ context.Context, params gen.MoveIdentityEmailsToIdentityParams) error {
	rows := s.identityEmails[params.SourceIdentityID]
	delete(s.identityEmails, params.SourceIdentityID)
	for _, row := range rows {
		row.IdentityID = params.TargetIdentityID
		row.IsPrimary = false
		s.identityEmails[params.TargetIdentityID] = append(s.identityEmails[params.TargetIdentityID], row)
	}
	return nil
}

func (s *resolverStub) MoveIdentityAnchorsToIdentity(_ context.Context, params gen.MoveIdentityAnchorsToIdentityParams) error {
	for i := range s.identityAnchors {
		if s.identityAnchors[i].IdentityID == params.SourceIdentityID {
			s.identityAnchors[i].IdentityID = params.TargetIdentityID
		}
	}
	return nil
}

func (s *resolverStub) MoveAccountIdentityRelationshipsToIdentity(context.Context, gen.MoveAccountIdentityRelationshipsToIdentityParams) error {
	return nil
}

func (s *resolverStub) RetireRemainingAccountIdentityRelationships(context.Context, gen.RetireRemainingAccountIdentityRelationshipsParams) error {
	return nil
}

func (s *resolverStub) MarkIdentityMerged(_ context.Context, sourceIdentityID int64) error {
	row, ok := s.identities[sourceIdentityID]
	if !ok {
		return nil
	}
	row.ResolutionState = "merged"
	s.identities[sourceIdentityID] = row
	return nil
}

func (s *resolverStub) UpsertIdentityMergeRedirect(_ context.Context, params gen.UpsertIdentityMergeRedirectParams) (gen.IdentityMergeRedirect, error) {
	row := gen.IdentityMergeRedirect{
		SourceIdentityID: params.SourceIdentityID,
		TargetIdentityID: params.TargetIdentityID,
		MergeEventID:     params.MergeEventID,
	}
	s.mergeRedirects[params.SourceIdentityID] = row
	return row, nil
}

func (s *resolverStub) MarkIdentityMergeEventApplied(_ context.Context, id int64) (gen.IdentityMergeEvent, error) {
	return gen.IdentityMergeEvent{ID: id, Status: "applied"}, nil
}

func isActiveAccount(account gen.Account) bool {
	return !account.ExpiredAt.Valid && account.LastObservedRunID.Valid
}

func normalizeTestEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func isConfirmedTestIdentity(identity gen.Identity) bool {
	state := strings.TrimSpace(identity.ResolutionState)
	return state == "" || state == "confirmed"
}

func makeActiveAccount(id int64, sourceKind, sourceName, email, displayName string) gen.Account {
	return gen.Account{
		ID:                id,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		ExternalID:        strings.ToLower(strings.TrimSpace(displayName)),
		AccountKind:       "unknown",
		EntityCategory:    "user",
		Email:             email,
		DisplayName:       displayName,
		LastObservedRunID: pgtype.Int8{Int64: 1, Valid: true},
	}
}

func TestResolverResolveExactEmailLink(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, PrimaryEmail: "alice@example.com", DisplayName: "Alice"})
	stub.accounts[10] = makeActiveAccount(10, "github", "acme", "ALICE@example.com", "Alice GH")

	stats, err := resolverForStub(stub).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.EmailMatchedLinks != 1 {
		t.Fatalf("EmailMatchedLinks = %d, want 1", stats.EmailMatchedLinks)
	}
	if stats.ProvisionalIdentities != 0 {
		t.Fatalf("ProvisionalIdentities = %d, want 0", stats.ProvisionalIdentities)
	}
	if stats.UpdatedIdentityEmails != 1 {
		t.Fatalf("UpdatedIdentityEmails = %d, want 1", stats.UpdatedIdentityEmails)
	}

	link, ok := stub.linksByAccount[10]
	if !ok {
		t.Fatalf("missing link for account 10")
	}
	if link.IdentityID != 1 {
		t.Fatalf("link.IdentityID = %d, want 1", link.IdentityID)
	}
	if link.LinkReason != linkReasonAutoEmail {
		t.Fatalf("link.LinkReason = %q, want %q", link.LinkReason, linkReasonAutoEmail)
	}
	if got := stub.identityEmails[1]; len(got) != 1 || got[0].NormalizedEmail != "alice@example.com" {
		t.Fatalf("identity emails = %+v, want observed alice@example.com", got)
	}
}

func TestResolverResolveSkipsEmailMatchedAnchorForNonHumanAccountKinds(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, PrimaryEmail: "svc@example.com", DisplayName: "Human Owner"})

	account := makeActiveAccount(20, "github", "acme", "svc@example.com", "CI Service Account")
	account.AccountKind = "service"
	stub.accounts[20] = account

	stats, err := resolverForStub(stub).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.ProvisionalIdentities != 1 {
		t.Fatalf("ProvisionalIdentities = %d, want 1", stats.ProvisionalIdentities)
	}

	link := stub.linksByAccount[20]
	if link.IdentityID == 1 {
		t.Fatalf("expected non-human account not to anchor by email match")
	}
	if link.LinkReason != linkReasonAutoProvisionalIdentity {
		t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoProvisionalIdentity)
	}
}

func TestResolverResolveNewIdentityKindInitializedFromAccountKind(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	account := makeActiveAccount(10, "entra", "tenant", "", "Automation Principal")
	account.AccountKind = "service"
	stub.accounts[10] = account

	stats, err := resolverForStub(stub).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.ProvisionalIdentities != 1 {
		t.Fatalf("ProvisionalIdentities = %d, want 1", stats.ProvisionalIdentities)
	}

	link := stub.linksByAccount[10]
	identity := stub.identities[link.IdentityID]
	if identity.Kind != "service" {
		t.Fatalf("identity kind = %q, want %q", identity.Kind, "service")
	}
}

func TestResolverResolveAuthoritativeAccountUsesExistingIdentity(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.sources = []gen.IdentitySourceSetting{
		{SourceKind: "okta", SourceName: "example.okta.com", IsAuthoritative: true},
	}

	stub.putIdentity(gen.Identity{ID: 1, PrimaryEmail: "person@example.com", DisplayName: "Person"})
	stub.accounts[11] = makeActiveAccount(11, "github", "acme", "person@example.com", "Person GH")
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 11, LinkReason: "seed_orphan", Confidence: 1})

	stub.accounts[20] = makeActiveAccount(20, "okta", "example.okta.com", "person@example.com", "Person Okta")

	stats, err := resolverForStub(stub).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.ProvisionalIdentities != 0 {
		t.Fatalf("ProvisionalIdentities = %d, want 0", stats.ProvisionalIdentities)
	}

	link := stub.linksByAccount[20]
	if link.IdentityID != 1 {
		t.Fatalf("okta account linked to identity %d, want 1", link.IdentityID)
	}
}

func TestResolverResolveManualLinkIsNotOverridden(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 7, PrimaryEmail: "manual@example.com"})
	account := makeActiveAccount(55, "github", "acme", "manual@example.com", "Manual")
	stub.accounts[account.ID] = account
	stub.putLink(gen.IdentityAccount{
		ID:         9,
		IdentityID: 7,
		AccountID:  account.ID,
		LinkReason: "manual",
		Confidence: 1,
	})

	identityID, reason, created, err := (resolverForStub(stub)).resolveIdentityIDForAccount(context.Background(), account)
	if err != nil {
		t.Fatalf("resolveIdentityIDForAccount() error = %v", err)
	}
	if identityID != 7 {
		t.Fatalf("identityID = %d, want 7", identityID)
	}
	if reason != "manual" {
		t.Fatalf("reason = %q, want %q", reason, "manual")
	}
	if created {
		t.Fatalf("created = true, want false")
	}
}

func TestResolverResolveEmptyEmailCreatesUniqueIdentities(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.accounts[1] = makeActiveAccount(1, "github", "acme", "", "Bot A")
	stub.accounts[2] = makeActiveAccount(2, "datadog", "datadoghq.com", " ", "Bot B")

	stats, err := resolverForStub(stub).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.ProvisionalIdentities != 2 {
		t.Fatalf("ProvisionalIdentities = %d, want 2", stats.ProvisionalIdentities)
	}

	linkA := stub.linksByAccount[1]
	linkB := stub.linksByAccount[2]
	if linkA.IdentityID == linkB.IdentityID {
		t.Fatalf("accounts linked to same identity %d, want different identities", linkA.IdentityID)
	}
}

func TestResolverResolveDuplicateEmailRequiresUnambiguousWinner(t *testing.T) {
	t.Parallel()

	t.Run("authoritative preferred", func(t *testing.T) {
		stub := newResolverStub()
		stub.sources = []gen.IdentitySourceSetting{
			{SourceKind: "okta", SourceName: "example.okta.com", IsAuthoritative: true},
		}

		stub.putIdentity(gen.Identity{ID: 1, PrimaryEmail: "team@example.com"})
		stub.putIdentity(gen.Identity{ID: 2, PrimaryEmail: "team@example.com"})
		stub.accounts[100] = makeActiveAccount(100, "okta", "example.okta.com", "team@example.com", "Authoritative")
		stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 2, AccountID: 100, LinkReason: "seed_migration", Confidence: 1})
		stub.accounts[200] = makeActiveAccount(200, "github", "acme", "team@example.com", "GitHub")

		if _, err := (resolverForStub(stub)).Resolve(context.Background()); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if got := stub.linksByAccount[200].IdentityID; got != 2 {
			t.Fatalf("identity = %d, want authoritative identity 2", got)
		}
	})

	t.Run("ambiguous when no authoritative winner", func(t *testing.T) {
		// Two existing identities share an email and neither has an
		// authoritative anchor. The new GitHub account should get a safe
		// provisional identity rather than being attached to a deterministic
		// low-id candidate.
		stub := newResolverStub()
		stub.putIdentity(gen.Identity{ID: 2, PrimaryEmail: "team@example.com"})
		stub.putIdentity(gen.Identity{ID: 5, PrimaryEmail: "team@example.com"})
		stub.accounts[201] = makeActiveAccount(201, "github", "acme", "team@example.com", "GitHub")

		stats, err := (resolverForStub(stub)).Resolve(context.Background())
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if stats.ProvisionalIdentities != 1 {
			t.Fatalf("ProvisionalIdentities = %d, want 1", stats.ProvisionalIdentities)
		}
		if len(stub.identities) != 3 {
			t.Fatalf("identities count = %d, want 3 (provisional identity should be minted)", len(stub.identities))
		}
		link := stub.linksByAccount[201]
		if link.IdentityID == 2 || link.IdentityID == 5 {
			t.Fatalf("ambiguous email linked to existing candidate identity %d", link.IdentityID)
		}
		if link.LinkReason != linkReasonAutoProvisionalAmbiguousEmail {
			t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoProvisionalAmbiguousEmail)
		}
		if link.LinkState != "needs_review" {
			t.Fatalf("link state = %q, want needs_review", link.LinkState)
		}
		if len(stub.candidates) != 2 {
			t.Fatalf("candidate count = %d, want 2", len(stub.candidates))
		}
		for _, candidate := range stub.candidates {
			if candidate.ProvisionalIdentityID.Int64 != link.IdentityID || !candidate.ProvisionalIdentityID.Valid {
				t.Fatalf("candidate provisional identity = %+v, want %d", candidate.ProvisionalIdentityID, link.IdentityID)
			}
			if candidate.MatchReason != "ambiguous_primary_email" {
				t.Fatalf("candidate match reason = %q, want ambiguous_primary_email", candidate.MatchReason)
			}
		}
		if len(stub.evidence) != 2 {
			t.Fatalf("evidence count = %d, want 2", len(stub.evidence))
		}
	})

	t.Run("ambiguous when two authoritative anchors tie", func(t *testing.T) {
		// Two existing identities are each anchored by a different
		// authoritative source. The GitHub account should get a safe
		// provisional identity rather than being attached to either
		// authoritative candidate.
		stub := newResolverStub()
		stub.sources = []gen.IdentitySourceSetting{
			{SourceKind: "okta", SourceName: "example.okta.com", IsAuthoritative: true},
			{SourceKind: "entra", SourceName: "tenant", IsAuthoritative: true},
		}

		stub.putIdentity(gen.Identity{ID: 1, PrimaryEmail: "team@example.com"})
		stub.putIdentity(gen.Identity{ID: 2, PrimaryEmail: "team@example.com"})
		stub.accounts[100] = makeActiveAccount(100, "okta", "example.okta.com", "team@example.com", "Okta")
		stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 100, LinkReason: "seed_migration", Confidence: 1})
		stub.accounts[101] = makeActiveAccount(101, "entra", "tenant", "team@example.com", "Entra")
		stub.putLink(gen.IdentityAccount{ID: 2, IdentityID: 2, AccountID: 101, LinkReason: "seed_migration", Confidence: 1})
		stub.accounts[202] = makeActiveAccount(202, "github", "acme", "team@example.com", "GitHub")

		identitiesBefore := len(stub.identities)
		if _, err := (resolverForStub(stub)).Resolve(context.Background()); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if len(stub.identities) != identitiesBefore+1 {
			t.Fatalf("identities count = %d, want %d (provisional identity should be minted)", len(stub.identities), identitiesBefore+1)
		}
		link := stub.linksByAccount[202]
		if link.IdentityID == 1 || link.IdentityID == 2 {
			t.Fatalf("ambiguous authoritative tie linked to existing identity %d", link.IdentityID)
		}
		if link.LinkReason != linkReasonAutoProvisionalAmbiguousEmail {
			t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoProvisionalAmbiguousEmail)
		}
		if link.LinkState != "needs_review" {
			t.Fatalf("link state = %q, want needs_review", link.LinkState)
		}
	})
}

func TestResolverResolveExactAnchorWinsOverEmail(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, PrimaryEmail: "owner@example.com", DisplayName: "Email Candidate"})
	stub.putIdentity(gen.Identity{ID: 2, PrimaryEmail: "other@example.com", DisplayName: "Anchor Candidate"})
	stub.identityAnchors = append(stub.identityAnchors, gen.IdentityAnchor{
		ID:                    1,
		IdentityID:            2,
		AnchorKind:            "okta_user_id",
		Issuer:                "okta:example.okta.com",
		AnchorValue:           "00u-anchor",
		NormalizedAnchorValue: "00u-anchor",
		TrustLevel:            "authoritative",
		LifecycleState:        "active",
	})
	account := makeActiveAccount(301, "github", "acme", "owner@example.com", "GitHub User")
	account.RawJson = []byte(`{"okta_user_id":"00u-anchor","okta_source_name":"example.okta.com"}`)
	stub.accounts[301] = account

	stats, err := (resolverForStub(stub)).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	link := stub.linksByAccount[301]
	if link.IdentityID != 2 {
		t.Fatalf("linked identity = %d, want anchor identity 2", link.IdentityID)
	}
	if link.LinkReason != linkReasonAutoAnchor {
		t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoAnchor)
	}
	if stats.AnchorMatchedLinks != 1 || stats.EmailMatchedLinks != 0 {
		t.Fatalf("anchor/email stats = %d/%d, want 1/0", stats.AnchorMatchedLinks, stats.EmailMatchedLinks)
	}
	if len(stub.evidence) != 1 || stub.evidence[0].EvidenceType != "anchor_exact" {
		t.Fatalf("evidence = %+v, want one anchor_exact row", stub.evidence)
	}
}

func TestResolverResolveAnchorConflictCreatesReviewCandidates(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, PrimaryEmail: "okta@example.com"})
	stub.putIdentity(gen.Identity{ID: 2, PrimaryEmail: "entra@example.com"})
	stub.identityAnchors = append(stub.identityAnchors,
		gen.IdentityAnchor{
			ID:                    1,
			IdentityID:            1,
			AnchorKind:            "okta_user_id",
			Issuer:                "okta:example.okta.com",
			AnchorValue:           "00u-conflict",
			NormalizedAnchorValue: "00u-conflict",
			TrustLevel:            "authoritative",
			LifecycleState:        "active",
		},
		gen.IdentityAnchor{
			ID:                    2,
			IdentityID:            2,
			AnchorKind:            "entra_object_id",
			Issuer:                "entra:tenant-1",
			AnchorValue:           "user-conflict",
			NormalizedAnchorValue: "user-conflict",
			TrustLevel:            "authoritative",
			LifecycleState:        "active",
		},
	)
	account := makeActiveAccount(302, "github", "acme", "", "Conflicting GitHub User")
	account.RawJson = []byte(`{"okta_user_id":"00u-conflict","okta_source_name":"example.okta.com","entra_object_id":"user-conflict","entra_tenant_id":"tenant-1"}`)
	stub.accounts[302] = account

	stats, err := (resolverForStub(stub)).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	link := stub.linksByAccount[302]
	if link.IdentityID == 1 || link.IdentityID == 2 {
		t.Fatalf("conflicting anchors linked to existing identity %d", link.IdentityID)
	}
	if link.LinkReason != linkReasonAutoProvisionalAnchorConflict {
		t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoProvisionalAnchorConflict)
	}
	if link.LinkState != "needs_review" {
		t.Fatalf("link state = %q, want needs_review", link.LinkState)
	}
	if stats.ProvisionalIdentities != 1 {
		t.Fatalf("ProvisionalIdentities = %d, want 1", stats.ProvisionalIdentities)
	}
	if len(stub.candidates) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(stub.candidates))
	}
	if len(stub.evidence) != 2 {
		t.Fatalf("evidence count = %d, want 2", len(stub.evidence))
	}
	for _, evidence := range stub.evidence {
		if evidence.EvidenceType != "negative_conflicting_anchor" {
			t.Fatalf("evidence type = %q, want negative_conflicting_anchor", evidence.EvidenceType)
		}
	}
}

func TestAnchorMatchEvidenceKeyChangesWhenEvidenceChanges(t *testing.T) {
	first := []gen.ListIdentityAnchorMatchesForAccountRow{{
		IdentityID:            1,
		AnchorKind:            "okta_user_id",
		Issuer:                "okta:example",
		NormalizedAnchorValue: "00u-one",
		TrustLevel:            "authoritative",
	}}
	second := []gen.ListIdentityAnchorMatchesForAccountRow{{
		IdentityID:            1,
		AnchorKind:            "okta_user_id",
		Issuer:                "okta:example",
		NormalizedAnchorValue: "00u-two",
		TrustLevel:            "authoritative",
	}}
	if anchorMatchEvidenceKey(first) == anchorMatchEvidenceKey(second) {
		t.Fatalf("anchor evidence key did not change when anchor value changed")
	}
}

func TestResolverBackfillsAuthoritativeAnchorsBeforeResolvingMissingAccounts(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.sources = []gen.IdentitySourceSetting{
		{SourceKind: "okta", SourceName: "example.okta.com", IsAuthoritative: true},
	}
	stub.putIdentity(gen.Identity{ID: 7, PrimaryEmail: "person@example.com", DisplayName: "Person"})
	oktaAccount := makeActiveAccount(401, "okta", "example.okta.com", "person@example.com", "Okta Person")
	oktaAccount.ExternalID = "00u-backfill"
	oktaAccount.RawJson = []byte(`{"id":"00u-backfill"}`)
	stub.accounts[401] = oktaAccount
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 7, AccountID: 401, LinkReason: "seed_migration", Confidence: 1})

	githubAccount := makeActiveAccount(402, "github", "acme", "", "GitHub Person")
	githubAccount.RawJson = []byte(`{"okta_user_id":"00u-backfill","okta_source_name":"example.okta.com"}`)
	stub.accounts[402] = githubAccount

	resolver := Resolver{
		Q:                     stub,
		ConfiguredSourceKinds: []string{"okta", "github"},
		ConfiguredSourceNames: []string{"example.okta.com", "acme"},
	}
	stats, err := resolver.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	link := stub.linksByAccount[402]
	if link.IdentityID != 7 {
		t.Fatalf("linked identity = %d, want authoritative anchor identity 7", link.IdentityID)
	}
	if link.LinkReason != linkReasonAutoAnchor {
		t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoAnchor)
	}
	if stats.UpdatedIdentityAnchors == 0 {
		t.Fatalf("UpdatedIdentityAnchors = 0, want authoritative anchor backfill")
	}
}

func TestResolverUpgradesExistingProvisionalLinkWithExactAnchor(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 8, PrimaryEmail: "temporary@example.com", DisplayName: "Temporary", ResolutionState: "provisional"})
	stub.putIdentity(gen.Identity{ID: 9, PrimaryEmail: "anchored@example.com", DisplayName: "Anchored"})
	stub.identityAnchors = append(stub.identityAnchors, gen.IdentityAnchor{
		ID:                    1,
		IdentityID:            9,
		AnchorKind:            "okta_user_id",
		Issuer:                "okta:example.okta.com",
		AnchorValue:           "00u-upgrade",
		NormalizedAnchorValue: "00u-upgrade",
		TrustLevel:            "authoritative",
		LifecycleState:        "active",
	})
	account := makeActiveAccount(501, "github", "acme", "", "GitHub Existing")
	account.RawJson = []byte(`{"okta_user_id":"00u-upgrade","okta_source_name":"example.okta.com"}`)
	stub.accounts[501] = account
	stub.putLink(gen.IdentityAccount{
		ID:         1,
		IdentityID: 8,
		AccountID:  501,
		LinkReason: linkReasonAutoProvisionalAmbiguousEmail,
		LinkState:  "needs_review",
		Confidence: 1,
	})

	stats, err := resolverForStub(stub).Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	link := stub.linksByAccount[501]
	if link.IdentityID != 9 {
		t.Fatalf("linked identity = %d, want anchored identity 9", link.IdentityID)
	}
	if link.LinkReason != linkReasonAutoAnchor {
		t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoAnchor)
	}
	if link.LinkState != "accepted" {
		t.Fatalf("link state = %q, want accepted", link.LinkState)
	}
	if stats.UpgradedProvisionalLinks != 1 || stats.AnchorMatchedLinks != 1 {
		t.Fatalf("upgrade/anchor stats = %d/%d, want 1/1", stats.UpgradedProvisionalLinks, stats.AnchorMatchedLinks)
	}
	if stub.identities[8].ResolutionState != "merged" {
		t.Fatalf("old provisional state = %q, want merged", stub.identities[8].ResolutionState)
	}
	if redirect := stub.mergeRedirects[8]; redirect.TargetIdentityID != 9 {
		t.Fatalf("merge redirect = %+v, want target 9", redirect)
	}
	if len(stub.evidence) != 1 || stub.evidence[0].EvidenceType != "anchor_exact" {
		t.Fatalf("evidence = %+v, want one anchor_exact row", stub.evidence)
	}
}

func TestResolverResolveScopesAuthoritativeSources(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.sources = []gen.IdentitySourceSetting{
		{SourceKind: "okta", SourceName: "retired.okta.com", IsAuthoritative: true},
	}
	stub.putIdentity(gen.Identity{ID: 1, Kind: "human", PrimaryEmail: "team@example.com", DisplayName: "Retired Anchor"})
	stub.putIdentity(gen.Identity{ID: 2, Kind: "human", PrimaryEmail: "team@example.com", DisplayName: "Current Identity"})
	stub.accounts[100] = makeActiveAccount(100, "okta", "retired.okta.com", "team@example.com", "Okta")
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 100, LinkReason: "seed_migration", Confidence: 1})
	stub.accounts[101] = makeActiveAccount(101, "github", "acme", "team@example.com", "GitHub Existing")
	stub.putLink(gen.IdentityAccount{ID: 2, IdentityID: 2, AccountID: 101, LinkReason: "seed_migration", Confidence: 1})
	stub.accounts[102] = makeActiveAccount(102, "github", "acme", "team@example.com", "GitHub New")

	resolver := Resolver{
		Q:                     stub,
		ConfiguredSourceKinds: []string{"github"},
		ConfiguredSourceNames: []string{"acme"},
	}
	if _, err := resolver.Resolve(context.Background()); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	link := stub.linksByAccount[102]
	if link.LinkReason != linkReasonAutoProvisionalAmbiguousEmail {
		t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoProvisionalAmbiguousEmail)
	}
	if link.IdentityID == 1 || link.IdentityID == 2 {
		t.Fatalf("ambiguous scoped email linked to existing identity %d", link.IdentityID)
	}
}

func TestResolverRefreshAttributesIgnoresRetiredAuthoritativeSource(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.sources = []gen.IdentitySourceSetting{
		{SourceKind: "okta", SourceName: "retired.okta.com", IsAuthoritative: true},
	}
	stub.putIdentity(gen.Identity{ID: 1, Kind: "unknown", PrimaryEmail: "old@example.com", DisplayName: "Old Name"})
	stub.accounts[10] = makeActiveAccount(10, "okta", "retired.okta.com", "old@example.com", "Retired Okta")
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 10, LinkReason: "seed_migration", Confidence: 1})
	stub.accounts[20] = makeActiveAccount(20, "github", "acme", "current@example.com", "Current GitHub")
	stub.putLink(gen.IdentityAccount{ID: 2, IdentityID: 1, AccountID: 20, LinkReason: "seed_migration", Confidence: 1})

	resolver := Resolver{
		Q:                     stub,
		ConfiguredSourceKinds: []string{"github"},
		ConfiguredSourceNames: []string{"acme"},
	}
	if _, err := resolver.Resolve(context.Background()); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	identity := stub.identities[1]
	if identity.PrimaryEmail != "current@example.com" {
		t.Fatalf("primary email = %q, want current@example.com", identity.PrimaryEmail)
	}
	if identity.DisplayName != "Current GitHub" {
		t.Fatalf("display name = %q, want Current GitHub", identity.DisplayName)
	}
}

func TestResolverRefreshIdentityKindClassifiesHuman(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, Kind: "unknown", PrimaryEmail: "alice@example.com", DisplayName: "Alice"})
	stub.accounts[10] = makeActiveAccount(10, "okta", "example.okta.com", "alice@example.com", "Alice Admin")
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 10, LinkReason: "seed_migration", Confidence: 1})

	if _, err := (resolverForStub(stub)).Resolve(context.Background()); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got := stub.identities[1].Kind; got != "human" {
		t.Fatalf("identity kind = %q, want %q", got, "human")
	}
}

func TestResolverRefreshIdentityKindClassifiesBot(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, Kind: "unknown"})
	stub.accounts[10] = makeActiveAccount(10, "github", "acme", "", "Dependabot")
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 10, LinkReason: "seed_migration", Confidence: 1})

	if _, err := (resolverForStub(stub)).Resolve(context.Background()); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got := stub.identities[1].Kind; got != "bot" {
		t.Fatalf("identity kind = %q, want %q", got, "bot")
	}
}

func TestResolverRefreshIdentityKindClassifiesService(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, Kind: "unknown"})
	stub.accounts[10] = makeActiveAccount(10, "entra", "tenant", "", "Build Service Account")
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 10, LinkReason: "seed_migration", Confidence: 1})

	if _, err := (resolverForStub(stub)).Resolve(context.Background()); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got := stub.identities[1].Kind; got != "service" {
		t.Fatalf("identity kind = %q, want %q", got, "service")
	}
}

func TestResolverRefreshIdentityKindUsesAccountKindPrecedence(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, Kind: "unknown"})

	human := makeActiveAccount(10, "okta", "example.okta.com", "person@example.com", "Person")
	human.AccountKind = "human"
	stub.accounts[10] = human
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 10, LinkReason: "seed_migration", Confidence: 1})

	service := makeActiveAccount(11, "entra", "tenant", "", "Service Principal")
	service.AccountKind = "service"
	stub.accounts[11] = service
	stub.putLink(gen.IdentityAccount{ID: 2, IdentityID: 1, AccountID: 11, LinkReason: "seed_migration", Confidence: 1})

	bot := makeActiveAccount(12, "github", "acme", "", "Dependabot")
	bot.AccountKind = "bot"
	stub.accounts[12] = bot
	stub.putLink(gen.IdentityAccount{ID: 3, IdentityID: 1, AccountID: 12, LinkReason: "seed_migration", Confidence: 1})

	if _, err := (resolverForStub(stub)).Resolve(context.Background()); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got := stub.identities[1].Kind; got != "bot" {
		t.Fatalf("identity kind = %q, want %q", got, "bot")
	}
}

func TestResolverRefreshIdentityKindFallbackOnlyWhenAllAccountKindsUnknown(t *testing.T) {
	t.Parallel()

	t.Run("known account kind disables heuristic override", func(t *testing.T) {
		stub := newResolverStub()
		stub.putIdentity(gen.Identity{ID: 1, Kind: "unknown"})
		account := makeActiveAccount(10, "github", "acme", "", "Dependabot")
		account.AccountKind = "human"
		stub.accounts[10] = account
		stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 10, LinkReason: "seed_migration", Confidence: 1})

		if _, err := (resolverForStub(stub)).Resolve(context.Background()); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if got := stub.identities[1].Kind; got != "human" {
			t.Fatalf("identity kind = %q, want %q", got, "human")
		}
	})

	t.Run("unknown account kind uses heuristic", func(t *testing.T) {
		stub := newResolverStub()
		stub.putIdentity(gen.Identity{ID: 1, Kind: "unknown"})
		stub.accounts[10] = makeActiveAccount(10, "github", "acme", "", "Dependabot")
		stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 10, LinkReason: "seed_migration", Confidence: 1})

		if _, err := (resolverForStub(stub)).Resolve(context.Background()); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if got := stub.identities[1].Kind; got != "bot" {
			t.Fatalf("identity kind = %q, want %q", got, "bot")
		}
	})
}
