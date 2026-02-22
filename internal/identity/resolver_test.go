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
	accounts       map[int64]gen.Account
	identities     map[int64]gen.Identity
	linksByAccount map[int64]gen.IdentityAccount
	sources        []gen.IdentitySourceSetting

	nextIdentityID int64
	nextLinkID     int64
}

func newResolverStub() *resolverStub {
	return &resolverStub{
		accounts:       make(map[int64]gen.Account),
		identities:     make(map[int64]gen.Identity),
		linksByAccount: make(map[int64]gen.IdentityAccount),
		nextIdentityID: 1,
		nextLinkID:     1,
	}
}

func (s *resolverStub) putIdentity(identity gen.Identity) {
	s.identities[identity.ID] = identity
	if identity.ID >= s.nextIdentityID {
		s.nextIdentityID = identity.ID + 1
	}
}

func (s *resolverStub) putLink(link gen.IdentityAccount) {
	s.linksByAccount[link.AccountID] = link
	if link.ID >= s.nextLinkID {
		s.nextLinkID = link.ID + 1
	}
}

func (s *resolverStub) CountUnlinkedAccounts(context.Context) (int64, error) {
	var count int64
	for _, account := range s.accounts {
		if !isActiveAccount(account) {
			continue
		}
		if _, linked := s.linksByAccount[account.ID]; linked {
			continue
		}
		count++
	}
	return count, nil
}

func (s *resolverStub) ListUnlinkedAccountsPage(_ context.Context, params gen.ListUnlinkedAccountsPageParams) ([]gen.Account, error) {
	rows := make([]gen.Account, 0)
	for _, account := range s.accounts {
		if !isActiveAccount(account) {
			continue
		}
		if _, linked := s.linksByAccount[account.ID]; linked {
			continue
		}
		rows = append(rows, account)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ID < rows[j].ID
	})

	start := int(params.PageOffset)
	if start >= len(rows) {
		return nil, nil
	}
	end := min(start+int(params.PageLimit), len(rows))
	return rows[start:end], nil
}

func (s *resolverStub) GetPreferredIdentityByPrimaryEmail(_ context.Context, email string) (gen.Identity, error) {
	target := normalizeTestEmail(email)
	if target == "" {
		return gen.Identity{}, pgx.ErrNoRows
	}

	authoritativeByIdentity := make(map[int64]struct{})
	authoritativeSources := make(map[string]struct{}, len(s.sources))
	for _, source := range s.sources {
		if !source.IsAuthoritative {
			continue
		}
		authoritativeSources[sourceKey(source.SourceKind, source.SourceName)] = struct{}{}
	}
	for _, link := range s.linksByAccount {
		account, ok := s.accounts[link.AccountID]
		if !ok || !isActiveAccount(account) {
			continue
		}
		if _, ok := authoritativeSources[sourceKey(account.SourceKind, account.SourceName)]; ok {
			authoritativeByIdentity[link.IdentityID] = struct{}{}
		}
	}

	candidates := make([]gen.Identity, 0)
	for _, identity := range s.identities {
		if normalizeTestEmail(identity.PrimaryEmail) == target {
			candidates = append(candidates, identity)
		}
	}
	if len(candidates) == 0 {
		return gen.Identity{}, pgx.ErrNoRows
	}

	sort.Slice(candidates, func(i, j int) bool {
		_, leftAuth := authoritativeByIdentity[candidates[i].ID]
		_, rightAuth := authoritativeByIdentity[candidates[j].ID]
		if leftAuth != rightAuth {
			return leftAuth
		}
		return candidates[i].ID < candidates[j].ID
	})
	return candidates[0], nil
}

func (s *resolverStub) CreateIdentity(_ context.Context, params gen.CreateIdentityParams) (gen.Identity, error) {
	row := gen.Identity{
		ID:           s.nextIdentityID,
		Kind:         strings.TrimSpace(params.Kind),
		DisplayName:  strings.TrimSpace(params.DisplayName),
		PrimaryEmail: normalizeTestEmail(params.PrimaryEmail),
	}
	if row.Kind == "" {
		row.Kind = "unknown"
	}
	s.identities[row.ID] = row
	s.nextIdentityID++
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
		}
		s.nextLinkID++
	} else {
		current.IdentityID = params.IdentityID
		current.LinkReason = params.LinkReason
		current.Confidence = params.Confidence
	}
	s.linksByAccount[params.AccountID] = current
	return current, nil
}

func (s *resolverStub) GetIdentityAccountLinkByAccountID(_ context.Context, accountID int64) (gen.IdentityAccount, error) {
	row, ok := s.linksByAccount[accountID]
	if !ok {
		return gen.IdentityAccount{}, pgx.ErrNoRows
	}
	return row, nil
}

func (s *resolverStub) ListAuthoritativeSources(context.Context) ([]gen.IdentitySourceSetting, error) {
	out := make([]gen.IdentitySourceSetting, 0, len(s.sources))
	for _, source := range s.sources {
		if source.IsAuthoritative {
			out = append(out, source)
		}
	}
	return out, nil
}

func (s *resolverStub) ListIdentityAccountAttributes(context.Context) ([]gen.ListIdentityAccountAttributesRow, error) {
	out := make([]gen.ListIdentityAccountAttributesRow, 0, len(s.linksByAccount))
	for accountID, link := range s.linksByAccount {
		account, ok := s.accounts[accountID]
		if !ok || !isActiveAccount(account) {
			continue
		}
		identity := s.identities[link.IdentityID]
		out = append(out, gen.ListIdentityAccountAttributesRow{
			IdentityID:   link.IdentityID,
			IdentityKind: identity.Kind,
			AccountID:    account.ID,
			SourceKind:   account.SourceKind,
			SourceName:   account.SourceName,
			ExternalID:   account.ExternalID,
			AccountKind:  account.AccountKind,
			Email:        account.Email,
			DisplayName:  account.DisplayName,
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
	row.DisplayName = strings.TrimSpace(params.DisplayName)
	row.PrimaryEmail = normalizeTestEmail(params.PrimaryEmail)
	if strings.TrimSpace(params.Kind) != "" {
		row.Kind = strings.TrimSpace(params.Kind)
	}
	s.identities[params.ID] = row
	return nil
}

func isActiveAccount(account gen.Account) bool {
	return !account.ExpiredAt.Valid && account.LastObservedRunID.Valid
}

func normalizeTestEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func makeActiveAccount(id int64, sourceKind, sourceName, email, displayName string) gen.Account {
	return gen.Account{
		ID:                id,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		ExternalID:        strings.ToLower(strings.TrimSpace(displayName)),
		AccountKind:       "unknown",
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

	stats, err := Resolver{Q: stub}.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.AutoLinked != 1 {
		t.Fatalf("AutoLinked = %d, want 1", stats.AutoLinked)
	}
	if stats.NewIdentities != 0 {
		t.Fatalf("NewIdentities = %d, want 0", stats.NewIdentities)
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
}

func TestResolverResolveSkipsEmailAutoLinkForNonHumanAccountKinds(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, PrimaryEmail: "svc@example.com", DisplayName: "Human Owner"})

	account := makeActiveAccount(20, "github", "acme", "svc@example.com", "CI Service Account")
	account.AccountKind = "service"
	stub.accounts[20] = account

	stats, err := Resolver{Q: stub}.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.NewIdentities != 1 {
		t.Fatalf("NewIdentities = %d, want 1", stats.NewIdentities)
	}

	link := stub.linksByAccount[20]
	if link.IdentityID == 1 {
		t.Fatalf("expected non-human account not to auto-link by email")
	}
	if link.LinkReason != linkReasonAutoCreate {
		t.Fatalf("link reason = %q, want %q", link.LinkReason, linkReasonAutoCreate)
	}
}

func TestResolverResolveNewIdentityKindInitializedFromAccountKind(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	account := makeActiveAccount(10, "entra", "tenant", "", "Automation Principal")
	account.AccountKind = "service"
	stub.accounts[10] = account

	stats, err := Resolver{Q: stub}.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.NewIdentities != 1 {
		t.Fatalf("NewIdentities = %d, want 1", stats.NewIdentities)
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

	stats, err := Resolver{Q: stub}.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.NewIdentities != 0 {
		t.Fatalf("NewIdentities = %d, want 0", stats.NewIdentities)
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

	identityID, reason, created, err := (Resolver{Q: stub}).resolveIdentityIDForAccount(context.Background(), account)
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

	stats, err := Resolver{Q: stub}.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if stats.NewIdentities != 2 {
		t.Fatalf("NewIdentities = %d, want 2", stats.NewIdentities)
	}

	linkA := stub.linksByAccount[1]
	linkB := stub.linksByAccount[2]
	if linkA.IdentityID == linkB.IdentityID {
		t.Fatalf("accounts linked to same identity %d, want different identities", linkA.IdentityID)
	}
}

func TestResolverResolveDuplicateEmailChoosesAuthoritativeThenLowestID(t *testing.T) {
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

		if _, err := (Resolver{Q: stub}).Resolve(context.Background()); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if got := stub.linksByAccount[200].IdentityID; got != 2 {
			t.Fatalf("identity = %d, want authoritative identity 2", got)
		}
	})

	t.Run("lowest id when no authoritative", func(t *testing.T) {
		stub := newResolverStub()
		stub.putIdentity(gen.Identity{ID: 2, PrimaryEmail: "team@example.com"})
		stub.putIdentity(gen.Identity{ID: 5, PrimaryEmail: "team@example.com"})
		stub.accounts[201] = makeActiveAccount(201, "github", "acme", "team@example.com", "GitHub")

		if _, err := (Resolver{Q: stub}).Resolve(context.Background()); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if got := stub.linksByAccount[201].IdentityID; got != 2 {
			t.Fatalf("identity = %d, want lowest identity 2", got)
		}
	})
}

func TestResolverRefreshIdentityKindClassifiesHuman(t *testing.T) {
	t.Parallel()

	stub := newResolverStub()
	stub.putIdentity(gen.Identity{ID: 1, Kind: "unknown", PrimaryEmail: "alice@example.com", DisplayName: "Alice"})
	stub.accounts[10] = makeActiveAccount(10, "okta", "example.okta.com", "alice@example.com", "Alice Admin")
	stub.putLink(gen.IdentityAccount{ID: 1, IdentityID: 1, AccountID: 10, LinkReason: "seed_migration", Confidence: 1})

	if _, err := (Resolver{Q: stub}).Resolve(context.Background()); err != nil {
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

	if _, err := (Resolver{Q: stub}).Resolve(context.Background()); err != nil {
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

	if _, err := (Resolver{Q: stub}).Resolve(context.Background()); err != nil {
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

	if _, err := (Resolver{Q: stub}).Resolve(context.Background()); err != nil {
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

		if _, err := (Resolver{Q: stub}).Resolve(context.Background()); err != nil {
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

		if _, err := (Resolver{Q: stub}).Resolve(context.Background()); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if got := stub.identities[1].Kind; got != "bot" {
			t.Fatalf("identity kind = %q, want %q", got, "bot")
		}
	})
}
