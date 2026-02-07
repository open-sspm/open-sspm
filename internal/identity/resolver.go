package identity

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	linkReasonAutoEmail  = "auto_email"
	linkReasonAutoCreate = "auto_create"
)

type queryRunner interface {
	CountUnlinkedAccounts(context.Context) (int64, error)
	ListUnlinkedAccountsPage(context.Context, gen.ListUnlinkedAccountsPageParams) ([]gen.Account, error)
	GetPreferredIdentityByPrimaryEmail(context.Context, string) (gen.Identity, error)
	CreateIdentity(context.Context, gen.CreateIdentityParams) (gen.Identity, error)
	UpsertIdentityAccountLink(context.Context, gen.UpsertIdentityAccountLinkParams) (gen.IdentityAccount, error)
	GetIdentityAccountLinkByAccountID(context.Context, int64) (gen.IdentityAccount, error)
	ListAuthoritativeSources(context.Context) ([]gen.IdentitySourceSetting, error)
	ListIdentityAccountAttributes(context.Context) ([]gen.ListIdentityAccountAttributesRow, error)
	UpdateIdentityAttributes(context.Context, gen.UpdateIdentityAttributesParams) error
}

type Resolver struct {
	Q queryRunner
}

type Stats struct {
	UnlinkedBefore   int64
	NewIdentities    int64
	AutoLinked       int64
	AutoCreatedLinks int64
	UpdatedIdentites int64
}

func Resolve(ctx context.Context, q *gen.Queries) (Stats, error) {
	r := Resolver{Q: q}
	return r.Resolve(ctx)
}

func (r Resolver) Resolve(ctx context.Context) (Stats, error) {
	if r.Q == nil {
		return Stats{}, errors.New("identity resolver query runner is nil")
	}

	var out Stats

	count, err := r.Q.CountUnlinkedAccounts(ctx)
	if err != nil {
		return out, err
	}
	out.UnlinkedBefore = count

	for {
		accounts, err := r.Q.ListUnlinkedAccountsPage(ctx, gen.ListUnlinkedAccountsPageParams{
			PageLimit:  500,
			PageOffset: 0,
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
				out.NewIdentities++
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
				out.AutoLinked++
			} else {
				out.AutoCreatedLinks++
			}
		}
	}

	updated, err := r.refreshIdentityAttributes(ctx)
	if err != nil {
		return out, err
	}
	out.UpdatedIdentites = updated

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
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, err
	}

	email := normalizeEmail(account.Email)
	if email != "" {
		identity, findErr := r.Q.GetPreferredIdentityByPrimaryEmail(ctx, email)
		if findErr == nil {
			return identity.ID, linkReasonAutoEmail, false, nil
		}
		if !errors.Is(findErr, pgx.ErrNoRows) {
			return 0, "", false, findErr
		}
	}

	identity, err := r.Q.CreateIdentity(ctx, gen.CreateIdentityParams{
		Kind:         "unknown",
		DisplayName:  strings.TrimSpace(account.DisplayName),
		PrimaryEmail: email,
	})
	if err != nil {
		return 0, "", false, err
	}
	return identity.ID, linkReasonAutoCreate, true, nil
}

func (r Resolver) refreshIdentityAttributes(ctx context.Context) (int64, error) {
	sources, err := r.Q.ListAuthoritativeSources(ctx)
	if err != nil {
		return 0, err
	}
	authoritative := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		key := sourceKey(source.SourceKind, source.SourceName)
		authoritative[key] = struct{}{}
	}

	rows, err := r.Q.ListIdentityAccountAttributes(ctx)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	byIdentity := make(map[int64][]gen.ListIdentityAccountAttributesRow)
	for _, row := range rows {
		byIdentity[row.IdentityID] = append(byIdentity[row.IdentityID], row)
	}

	updated := int64(0)
	for identityID, candidates := range byIdentity {
		email, displayName := chooseIdentityAttributes(candidates, authoritative)
		if _, hasEmail := firstNonEmptyEmail(candidates); !hasEmail {
			email = ""
		}
		if err := r.Q.UpdateIdentityAttributes(ctx, gen.UpdateIdentityAttributesParams{
			ID:          identityID,
			DisplayName: displayName,
			PrimaryEmail: email,
			Kind:        "",
		}); err != nil {
			return updated, err
		}
		updated++
	}

	return updated, nil
}

func chooseIdentityAttributes(candidates []gen.ListIdentityAccountAttributesRow, authoritative map[string]struct{}) (email string, displayName string) {
	if len(candidates) == 0 {
		return "", ""
	}

	sorted := append([]gen.ListIdentityAccountAttributesRow(nil), candidates...)
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

func firstNonEmptyEmail(candidates []gen.ListIdentityAccountAttributesRow) (string, bool) {
	for _, candidate := range candidates {
		email := normalizeEmail(candidate.Email)
		if email != "" {
			return email, true
		}
	}
	return "", false
}

func sourceKey(kind, name string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + "::" + strings.ToLower(strings.TrimSpace(name))
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}
