package identity

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	linkReasonAutoEmail                     = "auto_email"
	linkReasonAutoProvisionalIdentity       = "auto_provisional_identity"
	linkReasonAutoProvisionalAmbiguousEmail = "auto_provisional_ambiguous_email"
)

type queryRunner interface {
	CountAccountsMissingIdentityLinkByConfiguredSources(context.Context, gen.CountAccountsMissingIdentityLinkByConfiguredSourcesParams) (int64, error)
	ListAccountsMissingIdentityLinkPageByConfiguredSources(context.Context, gen.ListAccountsMissingIdentityLinkPageByConfiguredSourcesParams) ([]gen.Account, error)
	ResolveIdentityByPrimaryEmail(context.Context, gen.ResolveIdentityByPrimaryEmailParams) (gen.ResolveIdentityByPrimaryEmailRow, error)
	CreateIdentity(context.Context, gen.CreateIdentityParams) (gen.Identity, error)
	UpsertIdentityAccountLink(context.Context, gen.UpsertIdentityAccountLinkParams) (gen.IdentityAccount, error)
	GetIdentityAccountLinkByAccountID(context.Context, int64) (gen.IdentityAccount, error)
	ListAuthoritativeSourcesByConfiguredSources(context.Context, gen.ListAuthoritativeSourcesByConfiguredSourcesParams) ([]gen.IdentitySourceSetting, error)
	ListIdentityAccountAttributesByConfiguredSources(context.Context, gen.ListIdentityAccountAttributesByConfiguredSourcesParams) ([]gen.ListIdentityAccountAttributesByConfiguredSourcesRow, error)
	UpdateIdentityAttributes(context.Context, gen.UpdateIdentityAttributesParams) error
}

type Resolver struct {
	Q                     queryRunner
	ConfiguredSourceKinds []string
	ConfiguredSourceNames []string
}

type Stats struct {
	MissingIdentityLinksBefore int64
	ProvisionalIdentities      int64
	EmailMatchedLinks          int64
	ProvisionalLinks           int64
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

	sourceScope := gen.CountAccountsMissingIdentityLinkByConfiguredSourcesParams{
		ConfiguredSourceKinds: r.ConfiguredSourceKinds,
		ConfiguredSourceNames: r.ConfiguredSourceNames,
	}
	count, err := r.Q.CountAccountsMissingIdentityLinkByConfiguredSources(ctx, sourceScope)
	if err != nil {
		return out, err
	}
	out.MissingIdentityLinksBefore = count

	for {
		accounts, err := r.Q.ListAccountsMissingIdentityLinkPageByConfiguredSources(ctx, gen.ListAccountsMissingIdentityLinkPageByConfiguredSourcesParams{
			PageLimit:             500,
			PageOffset:            0,
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

			if reason == linkReasonAutoEmail {
				out.EmailMatchedLinks++
			} else {
				out.ProvisionalLinks++
			}
		}
	}

	updated, err := r.refreshIdentityAttributes(ctx)
	if err != nil {
		return out, err
	}
	out.UpdatedIdentities = updated

	return out, nil
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
	}

	identity, err := r.Q.CreateIdentity(ctx, gen.CreateIdentityParams{
		Kind:         accountKind,
		DisplayName:  strings.TrimSpace(account.DisplayName),
		PrimaryEmail: email,
	})
	if err != nil {
		return 0, "", false, err
	}
	return identity.ID, linkReasonAutoProvisionalIdentity, true, nil
}

func (r Resolver) refreshIdentityAttributes(ctx context.Context) (int64, error) {
	sourceScope := gen.ListAuthoritativeSourcesByConfiguredSourcesParams{
		ConfiguredSourceKinds: r.ConfiguredSourceKinds,
		ConfiguredSourceNames: r.ConfiguredSourceNames,
	}
	sources, err := r.Q.ListAuthoritativeSourcesByConfiguredSources(ctx, sourceScope)
	if err != nil {
		return 0, err
	}
	authoritative := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		key := sourceKey(source.SourceKind, source.SourceName)
		authoritative[key] = struct{}{}
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
	return strings.ToLower(strings.TrimSpace(email))
}
