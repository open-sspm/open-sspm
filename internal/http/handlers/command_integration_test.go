package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	connregistry "github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/readmodels"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

const commandSearchTestConnectorSecretKey = "0123456789abcdef0123456789abcdef"

type commandSearchFixture struct {
	azureIdentityID            int64
	azureAppAssetID            int64
	servicePrincipalIdentityID int64
	servicePrincipalAssetID    int64
	githubAppAssetID           int64
	googleConnectedAppID       int64
}

type commandSearchTestDefinition struct {
	kind        string
	displayName string
	role        connregistry.IntegrationRole
}

func (d commandSearchTestDefinition) Kind() string                       { return d.kind }
func (d commandSearchTestDefinition) DisplayName() string                { return d.displayName }
func (d commandSearchTestDefinition) Role() connregistry.IntegrationRole { return d.role }
func (d commandSearchTestDefinition) ValidateConfig(any) error           { return nil }
func (d commandSearchTestDefinition) MetricsProvider() connregistry.MetricsProvider {
	return nil
}
func (d commandSearchTestDefinition) NewIntegration(any) (connregistry.Integration, error) {
	return nil, nil
}

func (d commandSearchTestDefinition) DecodeConfig(raw []byte) (any, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		raw = []byte("{}")
	}

	switch d.kind {
	case configstore.KindOkta:
		var cfg configstore.OktaConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		return cfg.Normalized(), nil
	case configstore.KindEntra:
		var cfg configstore.EntraConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		return cfg.Normalized(), nil
	case configstore.KindGoogleWorkspace:
		var cfg configstore.GoogleWorkspaceConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		return cfg.Normalized(), nil
	case configstore.KindGitHub:
		var cfg configstore.GitHubConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		return cfg.Normalized(), nil
	case configstore.KindDatadog:
		var cfg configstore.DatadogConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		return cfg.Normalized(), nil
	case configstore.KindAWSIdentityCenter:
		var cfg configstore.AWSIdentityCenterConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		return cfg.Normalized(), nil
	case configstore.KindVault:
		var cfg configstore.VaultConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		return cfg.Normalized(), nil
	default:
		return struct{}{}, nil
	}
}

func (d commandSearchTestDefinition) IsConfigured(cfg any) bool {
	switch cfg := cfg.(type) {
	case configstore.OktaConfig:
		return strings.TrimSpace(cfg.Domain) != ""
	case configstore.EntraConfig:
		return strings.TrimSpace(cfg.TenantID) != ""
	case configstore.GoogleWorkspaceConfig:
		return strings.TrimSpace(cfg.CustomerID) != ""
	case configstore.GitHubConfig:
		return strings.TrimSpace(cfg.Org) != ""
	case configstore.DatadogConfig:
		return strings.TrimSpace(cfg.Site) != "" && strings.TrimSpace(cfg.APIKey) != "" && strings.TrimSpace(cfg.AppKey) != ""
	case configstore.AWSIdentityCenterConfig:
		if strings.TrimSpace(cfg.Region) == "" {
			return false
		}
		switch strings.TrimSpace(cfg.AuthType) {
		case "", configstore.AWSIdentityCenterAuthTypeDefaultChain:
			return true
		case configstore.AWSIdentityCenterAuthTypeAccessKey:
			return strings.TrimSpace(cfg.AccessKeyID) != "" && strings.TrimSpace(cfg.SecretAccessKey) != ""
		default:
			return false
		}
	case configstore.VaultConfig:
		if strings.TrimSpace(cfg.Address) == "" {
			return false
		}
		switch strings.TrimSpace(cfg.AuthType) {
		case "", configstore.VaultAuthTypeToken:
			return strings.TrimSpace(cfg.Token) != ""
		case configstore.VaultAuthTypeAppRole:
			return strings.TrimSpace(cfg.AppRoleRoleID) != "" && strings.TrimSpace(cfg.AppRoleSecretID) != ""
		default:
			return false
		}
	default:
		return false
	}
}

func (d commandSearchTestDefinition) SourceName(cfg any) string {
	switch cfg := cfg.(type) {
	case configstore.OktaConfig:
		return strings.TrimSpace(cfg.Domain)
	case configstore.EntraConfig:
		return strings.TrimSpace(cfg.TenantID)
	case configstore.GoogleWorkspaceConfig:
		return strings.TrimSpace(cfg.CustomerID)
	case configstore.GitHubConfig:
		return strings.TrimSpace(cfg.Org)
	case configstore.DatadogConfig:
		return strings.TrimSpace(cfg.Site)
	case configstore.AWSIdentityCenterConfig:
		if name := strings.TrimSpace(cfg.Name); name != "" {
			return name
		}
		return strings.TrimSpace(cfg.Region)
	case configstore.VaultConfig:
		return strings.TrimSpace(cfg.SourceName())
	default:
		return ""
	}
}

func TestHandleCommandSearchShellAndShortQuery(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{Org: "acme"})

		body := renderCommandSearch(t, h, "http://example.com/command/search?q=")
		if !strings.Contains(body, `id="command-search-input"`) {
			t.Fatalf("empty-query body missing command input: %s", body)
		}
		if strings.Contains(body, `role="heading">Actions`) {
			t.Fatalf("empty-query body unexpectedly rendered actions: %s", body)
		}
		if strings.Contains(body, "Type at least 2 characters for direct matches.") {
			t.Fatalf("empty-query body unexpectedly rendered short-query notice: %s", body)
		}

		body = renderCommandSearch(t, h, "http://example.com/command/search?q=g")
		if !strings.Contains(body, "Type at least 2 characters for direct matches.") {
			t.Fatalf("short-query body missing notice: %s", body)
		}
		if !strings.Contains(body, `href="/identities?q=g"`) {
			t.Fatalf("short-query body missing identities action: %s", body)
		}
		if !strings.Contains(body, `href="/non-human-identities?q=g"`) {
			t.Fatalf("short-query body missing non-human-identities action: %s", body)
		}
		if strings.Contains(body, `href="/oauth-apps?q=g"`) {
			t.Fatalf("short-query body unexpectedly rendered oauth-apps action: %s", body)
		}
		if strings.Contains(body, `href="/assigned-apps?q=g"`) {
			t.Fatalf("short-query body unexpectedly rendered assigned-apps action: %s", body)
		}
	})
}

func TestHandleCommandSearchShowsNonHumanIdentitiesActionForIdentityOnlySource(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{Domain: "acme.okta.com"})

		body := renderCommandSearch(t, h, "http://example.com/command/search?q=ac")
		if !strings.Contains(body, `href="/non-human-identities?q=ac"`) {
			t.Fatalf("identity-only body missing non-human-identities action: %s", body)
		}
	})
}

func TestHandleCommandSearchCrossInventoryResults(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{Domain: "acme.okta.com"})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{TenantID: "tenant-1"})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGoogleWorkspace, true, configstore.GoogleWorkspaceConfig{CustomerID: "C0123"})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{Org: "acme"})

		fixture := seedCommandSearchFixture(t, ctx, pool, q)

		t.Run("azure returns live entra inventory and not unrelated okta rows", func(t *testing.T) {
			body := renderCommandSearch(t, h, "http://example.com/command/search?q=azure")

			if !strings.Contains(body, `role="heading">Identities`) {
				t.Fatalf("azure body missing identities section: %s", body)
			}
			if !strings.Contains(body, `/identities/`+strconv.FormatInt(fixture.azureIdentityID, 10)) {
				t.Fatalf("azure body missing identity href: %s", body)
			}
			if !strings.Contains(body, `/app-assets/`+strconv.FormatInt(fixture.azureAppAssetID, 10)) {
				t.Fatalf("azure body missing app asset href: %s", body)
			}
			if strings.Contains(body, "Salesforce Reference") {
				t.Fatalf("azure body leaked unrelated okta app row: %s", body)
			}
		})

		t.Run("github returns github inventory instead of defaulting to okta", func(t *testing.T) {
			body := renderCommandSearch(t, h, "http://example.com/command/search?q=github")

			if !strings.Contains(body, `/app-assets/`+strconv.FormatInt(fixture.githubAppAssetID, 10)) {
				t.Fatalf("github body missing github app asset href: %s", body)
			}
			if strings.Contains(body, `role="heading">Assigned Apps`) {
				t.Fatalf("github body unexpectedly rendered assigned-apps direct matches: %s", body)
			}
		})

		t.Run("service principal returns entra service identities and assets", func(t *testing.T) {
			body := renderCommandSearch(t, h, "http://example.com/command/search?q=service%20principal")

			if !strings.Contains(body, `/identities/`+strconv.FormatInt(fixture.servicePrincipalIdentityID, 10)) {
				t.Fatalf("service principal body missing identity href: %s", body)
			}
			if !strings.Contains(body, `/app-assets/`+strconv.FormatInt(fixture.servicePrincipalAssetID, 10)) {
				t.Fatalf("service principal body missing app asset href: %s", body)
			}
		})

		t.Run("google oauth client appears once under app assets", func(t *testing.T) {
			body := renderCommandSearch(t, h, "http://example.com/command/search?q=oauth")

			if !strings.Contains(body, `role="heading">App Assets`) {
				t.Fatalf("oauth body missing app-assets section: %s", body)
			}
			if !strings.Contains(body, `/app-assets/`+strconv.FormatInt(fixture.googleConnectedAppID, 10)) {
				t.Fatalf("oauth body missing app asset href: %s", body)
			}
			if strings.Contains(body, `role="heading">OAuth Apps`) {
				t.Fatalf("oauth body unexpectedly rendered oauth-apps section: %s", body)
			}
		})

		t.Run("okta app rows link to direct destinations", func(t *testing.T) {
			referenceBody := renderCommandSearch(t, h, "http://example.com/command/search?q=reference")
			if !strings.Contains(referenceBody, `href="/assigned-apps/example.okta.com/reference-app"`) {
				t.Fatalf("current okta body missing source-scoped assigned app link: %s", referenceBody)
			}

			mappedBody := renderCommandSearch(t, h, "http://example.com/command/search?q=mapped")
			if !strings.Contains(mappedBody, `href="/accounts/github"`) {
				t.Fatalf("mapped okta body missing integrated destination link: %s", mappedBody)
			}
		})
	})
}

func TestHandleCommandSearchQueryFailureFallsBack(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{TenantID: "tenant-1"})

		if _, err := pool.Exec(ctx, `DROP TABLE accounts CASCADE`); err != nil {
			t.Fatalf("drop accounts: %v", err)
		}

		body := renderCommandSearch(t, h, "http://example.com/command/search?q=azure")
		if !strings.Contains(body, "Search unavailable. Open an inventory below.") {
			t.Fatalf("query-failure body missing degraded notice: %s", body)
		}
		if !strings.Contains(body, `href="/identities?q=azure"`) {
			t.Fatalf("query-failure body missing identities action: %s", body)
		}
		if !strings.Contains(body, `href="/non-human-identities?q=azure"`) {
			t.Fatalf("query-failure body missing non-human-identities action: %s", body)
		}
	})
}

func TestHandleCommandSearchUsesLiveDiscoveryPostureBadges(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			runID,
			configstore.KindEntra,
			"tenant-1",
			"orphaned-portal",
			"Orphaned Portal",
			"example.com",
			"Example",
			"orphaned-portal",
		)

		body := renderCommandSearch(t, h, "http://example.com/command/search?q=orphaned")
		if !strings.Contains(body, `role="heading">Apps &amp; Discovery`) {
			t.Fatalf("orphaned body missing apps and discovery section: %s", body)
		}
		if !strings.Contains(body, "Orphaned Portal") {
			t.Fatalf("orphaned body missing discovery app row: %s", body)
		}
		if !strings.Contains(body, "Unmanaged") {
			t.Fatalf("orphaned body missing live unmanaged badge: %s", body)
		}
		if !strings.Contains(body, "High") {
			t.Fatalf("orphaned body missing live risk badge: %s", body)
		}
	})
}

func withCommandSearchTestDatabase(t *testing.T, fn func(context.Context, *pgxpool.Pool, *gen.Queries, *Handlers)) {
	t.Helper()

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_command_search"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		fn(ctx, pool, q, &Handlers{
			Cfg: config.Config{
				ConnectorSecretKey:   []byte(commandSearchTestConnectorSecretKey),
				SyncDiscoveryEnabled: true,
				EventInboxEnabled:    true,
			},
			Q:        q,
			Pool:     pool,
			Registry: newCommandSearchTestRegistry(t),
		})
	})
}

func newCommandSearchTestRegistry(t *testing.T) *connregistry.ConnectorRegistry {
	t.Helper()

	reg := connregistry.NewRegistry()
	reg.SetConnectorSecretKey([]byte(commandSearchTestConnectorSecretKey))
	defs := []commandSearchTestDefinition{
		{kind: configstore.KindOkta, displayName: "Okta", role: connregistry.RoleIdP},
		{kind: configstore.KindEntra, displayName: "Microsoft Entra", role: connregistry.RoleIdP},
		{kind: configstore.KindGoogleWorkspace, displayName: "Google Workspace", role: connregistry.RoleApp},
		{kind: configstore.KindGitHub, displayName: "GitHub", role: connregistry.RoleApp},
		{kind: configstore.KindDatadog, displayName: "Datadog", role: connregistry.RoleApp},
		{kind: configstore.KindAWSIdentityCenter, displayName: "AWS Identity Center", role: connregistry.RoleApp},
		{kind: configstore.KindVault, displayName: "Vault", role: connregistry.RoleApp},
	}
	for _, def := range defs {
		if err := reg.Register(def); err != nil {
			t.Fatalf("register %s: %v", def.kind, err)
		}
	}
	return reg
}

func renderCommandSearch(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleCommandSearch(c); err != nil {
		t.Fatalf("HandleCommandSearch(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func upsertCommandSearchConnectorConfig(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string, enabled bool, cfg any) {
	t.Helper()

	switch kind {
	case configstore.KindOkta:
		if typed, ok := cfg.(configstore.OktaConfig); ok {
			cfg = typed.Normalized()
		}
	case configstore.KindEntra:
		if typed, ok := cfg.(configstore.EntraConfig); ok {
			cfg = typed.Normalized()
		}
	case configstore.KindGoogleWorkspace:
		if typed, ok := cfg.(configstore.GoogleWorkspaceConfig); ok {
			cfg = typed.Normalized()
		}
	case configstore.KindGitHub:
		if typed, ok := cfg.(configstore.GitHubConfig); ok {
			cfg = typed.Normalized()
		}
	case configstore.KindDatadog:
		if typed, ok := cfg.(configstore.DatadogConfig); ok {
			cfg = typed.Normalized()
		}
	case configstore.KindAWSIdentityCenter:
		if typed, ok := cfg.(configstore.AWSIdentityCenterConfig); ok {
			cfg = typed.Normalized()
		}
	case configstore.KindVault:
		if typed, ok := cfg.(configstore.VaultConfig); ok {
			cfg = typed.Normalized()
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin connector config tx %s: %v", kind, err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	qtx := gen.New(pool).WithTx(tx)
	if _, err := qtx.UpdateConnectorConfigEnabled(ctx, gen.UpdateConnectorConfigEnabledParams{
		Kind:    kind,
		Enabled: enabled,
	}); err != nil {
		t.Fatalf("update connector config enabled %s: %v", kind, err)
	}
	store := configstore.NewStore(nil, qtx, []byte(commandSearchTestConnectorSecretKey))
	if err := store.SaveConnectorConfigTx(ctx, qtx, kind, cfg); err != nil {
		t.Fatalf("save connector config %s: %v", kind, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit connector config %s: %v", kind, err)
	}
	refreshCommandSearchSourceState(t, ctx, pool)
}

func seedCommandSearchFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) commandSearchFixture {
	t.Helper()

	oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
	entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
	googleRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGoogleWorkspace, "C0123")
	githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

	insertCommandSearchIdentitySourceSetting(t, ctx, pool, configstore.KindEntra, "tenant-1", true)
	insertCommandSearchIdentitySourceSetting(t, ctx, pool, configstore.KindGitHub, "acme", true)

	azureAccountID := insertCommandSearchAccount(t, ctx, pool, entraRunID, commandSearchAccountSeed{
		SourceKind:     configstore.KindEntra,
		SourceName:     "tenant-1",
		ExternalID:     "azure-user-1",
		Email:          "azure.admin@example.com",
		DisplayName:    "Azure Admin",
		Status:         "active",
		AccountKind:    "human",
		EntityCategory: "user",
		RawJSON:        `{"status":"active"}`,
	})
	azureIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "azure.admin@example.com", "Azure Admin")
	insertCommandSearchIdentityAccountLink(t, ctx, pool, azureIdentityID, azureAccountID)

	servicePrincipalAccountID := insertCommandSearchAccount(t, ctx, pool, entraRunID, commandSearchAccountSeed{
		SourceKind:     configstore.KindEntra,
		SourceName:     "tenant-1",
		ExternalID:     "sp:svc-123",
		Email:          "service.principal@example.com",
		DisplayName:    "Azure Service Principal",
		Status:         "active",
		AccountKind:    "service",
		EntityCategory: "service_principal",
		RawJSON:        `{"status":"active"}`,
	})
	servicePrincipalIdentityID := insertCommandSearchIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
	insertCommandSearchIdentityAccountLink(t, ctx, pool, servicePrincipalIdentityID, servicePrincipalAccountID)

	azureAppAssetID := insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_application", "azure-enterprise-app", "", "Azure Enterprise App", "active")
	servicePrincipalAssetID := insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_service_principal", "svc-123", "azure-enterprise-app", "Azure Service Principal", "active")
	githubAppAssetID := insertCommandSearchAppAsset(t, ctx, q, githubRunID, configstore.KindGitHub, "acme", "github_app", "github-actions", "", "GitHub Actions", "active")
	googleConnectedAppID := insertCommandSearchAppAsset(t, ctx, q, googleRunID, configstore.KindGoogleWorkspace, "C0123", "google_oauth_client", "client-123.apps.googleusercontent.com", "", "OAuth Approval Client", "active")

	if _, err := q.UpsertAppAssetGovernance(ctx, gen.UpsertAppAssetGovernanceParams{
		AppAssetID:      googleConnectedAppID,
		GovernanceState: "action_required",
	}); err != nil {
		t.Fatalf("UpsertAppAssetGovernance: %v", err)
	}

	insertCommandSearchDiscoveryApp(t, ctx, pool, q, entraRunID, configstore.KindEntra, "tenant-1", "azure-cloud", "Azure Cloud", "azure.com", "Microsoft", "azure-cloud")

	insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "salesforce-reference", "Salesforce Reference", "salesforce", "active")
	insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "reference-app", "Reference HR App", "reference-hr", "active")
	insertCommandSearchOktaApp(t, ctx, q, oktaRunID, "mapped-okta-app", "Mapped Directory App", "mapped-directory", "active")
	if err := q.UpsertIntegrationOktaAppMap(ctx, gen.UpsertIntegrationOktaAppMapParams{
		IntegrationKind:   configstore.KindGitHub,
		OktaSourceKind:    configstore.KindOkta,
		OktaSourceName:    "example.okta.com",
		OktaAppExternalID: "mapped-okta-app",
	}); err != nil {
		t.Fatalf("UpsertIntegrationOktaAppMap: %v", err)
	}

	return commandSearchFixture{
		azureIdentityID:            azureIdentityID,
		azureAppAssetID:            azureAppAssetID,
		servicePrincipalIdentityID: servicePrincipalIdentityID,
		servicePrincipalAssetID:    servicePrincipalAssetID,
		githubAppAssetID:           githubAppAssetID,
		googleConnectedAppID:       googleConnectedAppID,
	}
}

type commandSearchAccountSeed struct {
	SourceKind     string
	SourceName     string
	ExternalID     string
	Email          string
	DisplayName    string
	Status         string
	AccountKind    string
	EntityCategory string
	RawJSON        string
}

func insertCommandSearchSyncRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName string) int64 {
	t.Helper()

	return insertCommandSearchSyncRunWithMode(t, ctx, pool, sourceKind, sourceName, connregistry.RunModeFull)
}

func insertCommandSearchSyncRunWithMode(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName string, runMode connregistry.RunMode) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO sync_runs (source_kind, source_name, run_mode, status, started_at, finished_at, message)
		VALUES ($1, $2, $3, 'success', now(), now(), '')
		RETURNING id
	`, strings.ToLower(strings.TrimSpace(sourceKind)), sourceName, string(runMode.Normalize())).Scan(&id)
	if err != nil {
		t.Fatalf("insert sync run %s/%s: %v", sourceKind, sourceName, err)
	}
	refreshCommandSearchSourceState(t, ctx, pool)
	return id
}

func insertCommandSearchIdentitySourceSetting(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName string, authoritative bool) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative, created_at, updated_at)
		VALUES ($1, $2, $3, now(), now())
		ON CONFLICT (source_kind, source_name) DO UPDATE SET
			is_authoritative = EXCLUDED.is_authoritative,
			updated_at = EXCLUDED.updated_at
	`, sourceKind, sourceName, authoritative); err != nil {
		t.Fatalf("insert identity source setting %s/%s: %v", sourceKind, sourceName, err)
	}
}

func insertCommandSearchIdentity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind, email, displayName string) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO identities (kind, display_name, primary_email, created_at, updated_at)
		VALUES ($1, $2, $3, now(), now())
		RETURNING id
	`, kind, displayName, email).Scan(&id)
	if err != nil {
		t.Fatalf("insert identity %s: %v", displayName, err)
	}
	if strings.TrimSpace(email) != "" {
		if _, err := pool.Exec(ctx, `
			INSERT INTO identity_emails (
				identity_id,
				email,
				normalized_email,
				email_kind,
				verification_state,
				lifecycle_state,
				is_primary,
				last_seen_at,
				updated_at
			)
			VALUES ($1, $2, lower(trim($2)), 'primary', 'manual', 'active', true, now(), now())
			ON CONFLICT (identity_id, normalized_email)
			WHERE lifecycle_state = 'active'
			DO NOTHING
		`, id, email); err != nil {
			t.Fatalf("insert identity email %s: %v", displayName, err)
		}
	}
	return id
}

func insertCommandSearchIdentityAccountLink(t *testing.T, ctx context.Context, pool *pgxpool.Pool, identityID, accountID int64) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, created_at, updated_at)
		VALUES ($1, $2, 'seed', 1.0, now(), now())
	`, identityID, accountID); err != nil {
		t.Fatalf("insert identity account link %d/%d: %v", identityID, accountID, err)
	}
}

func insertCommandSearchAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, seed commandSearchAccountSeed) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO accounts (
			source_kind,
			source_name,
			external_id,
			email,
			display_name,
			status,
			account_kind,
			entity_category,
			raw_json,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, now(), $10, now(), now())
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.ExternalID, seed.Email, seed.DisplayName, seed.Status, seed.AccountKind, seed.EntityCategory, seed.RawJSON, runID).Scan(&id)
	if err != nil {
		t.Fatalf("insert account %s/%s: %v", seed.SourceKind, seed.ExternalID, err)
	}
	return id
}

func insertCommandSearchAppAsset(t *testing.T, ctx context.Context, q *gen.Queries, runID int64, sourceKind, sourceName, assetKind, externalID, parentExternalID, displayName, status string) int64 {
	t.Helper()

	now := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	if _, err := q.UpsertAppAssetsBulkBySource(ctx, gen.UpsertAppAssetsBulkBySourceParams{
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		SeenInRunID:       runID,
		AssetKinds:        []string{assetKind},
		ExternalIds:       []string{externalID},
		ParentExternalIds: []string{parentExternalID},
		DisplayNames:      []string{displayName},
		Statuses:          []string{status},
		CreatedAtSources:  []pgtype.Timestamptz{now},
		UpdatedAtSources:  []pgtype.Timestamptz{now},
		RawJsons:          [][]byte{[]byte(`{}`)},
	}); err != nil {
		t.Fatalf("UpsertAppAssetsBulkBySource %s/%s: %v", sourceKind, externalID, err)
	}
	if _, err := q.PromoteAppAssetsSeenInRunBySource(ctx, gen.PromoteAppAssetsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	}); err != nil {
		t.Fatalf("PromoteAppAssetsSeenInRunBySource %s/%s: %v", sourceKind, externalID, err)
	}

	appAsset, err := q.GetAppAssetBySourceAndKindAndExternalID(ctx, gen.GetAppAssetBySourceAndKindAndExternalIDParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
		AssetKind:  assetKind,
		ExternalID: externalID,
	})
	if err != nil {
		t.Fatalf("GetAppAssetBySourceAndKindAndExternalID %s/%s: %v", sourceKind, externalID, err)
	}
	refreshCommandSearchSourceReadModels(t, ctx, q, sourceKind, sourceName)
	return appAsset.ID
}

func insertCommandSearchDiscoveryApp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, runID int64, sourceKind, sourceName, canonicalKey, displayName, primaryDomain, vendorName, sourceAppID string) int64 {
	t.Helper()

	now := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	if _, err := q.UpsertSaaSAppsBulk(ctx, gen.UpsertSaaSAppsBulkParams{
		CanonicalKeys:  []string{canonicalKey},
		DisplayNames:   []string{displayName},
		PrimaryDomains: []string{primaryDomain},
		VendorNames:    []string{vendorName},
		Categories:     []string{""},
		FirstSeenAts:   []pgtype.Timestamptz{now},
		LastSeenAts:    []pgtype.Timestamptz{now},
	}); err != nil {
		t.Fatalf("UpsertSaaSAppsBulk %s: %v", canonicalKey, err)
	}

	var id int64
	if err := pool.QueryRow(ctx, `
		SELECT id
		FROM saas_apps
		WHERE canonical_key = $1
	`, canonicalKey).Scan(&id); err != nil {
		t.Fatalf("select saas app %s: %v", canonicalKey, err)
	}

	if _, err := q.UpsertSaaSAppSourcesBulkBySource(ctx, gen.UpsertSaaSAppSourcesBulkBySourceParams{
		SeenInRunID:      runID,
		SourceKind:       sourceKind,
		SourceName:       sourceName,
		CanonicalKeys:    []string{canonicalKey},
		SourceAppIds:     []string{sourceAppID},
		SourceAppNames:   []string{displayName},
		SourceAppDomains: []string{primaryDomain},
		SeenAts:          []pgtype.Timestamptz{now},
	}); err != nil {
		t.Fatalf("UpsertSaaSAppSourcesBulkBySource %s: %v", canonicalKey, err)
	}
	if _, err := q.PromoteSaaSAppSourcesSeenInRunBySource(ctx, gen.PromoteSaaSAppSourcesSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	}); err != nil {
		t.Fatalf("PromoteSaaSAppSourcesSeenInRunBySource %s: %v", canonicalKey, err)
	}
	refreshCommandSearchSourceReadModels(t, ctx, q, sourceKind, sourceName)

	return id
}

func refreshHandlerReadModels(t *testing.T, h *Handlers) {
	t.Helper()
	if h == nil || h.Q == nil {
		return
	}
	projector := readmodels.NewProjector(h.Pool, h.Q, readmodels.RefreshConfigFromConfig(h.Cfg))
	if err := projector.RebuildAllReadModels(context.Background()); err != nil {
		t.Fatalf("RebuildAllReadModels(): %v", err)
	}
}

func refreshCommandSearchSourceState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	projector := readmodels.NewProjector(pool, gen.New(pool), readmodels.RefreshConfig{})
	if err := projector.RefreshConnectorSourceState(ctx); err != nil {
		t.Fatalf("RefreshConnectorSourceState(): %v", err)
	}
}

func refreshCommandSearchSourceReadModels(t *testing.T, ctx context.Context, q *gen.Queries, sourceKind, sourceName string) {
	t.Helper()
	projector := readmodels.NewProjector(nil, q, readmodels.RefreshConfig{})
	if err := projector.RefreshDiscoverySource(ctx, sourceKind, sourceName); err != nil {
		t.Fatalf("RefreshDiscoverySource(%s/%s): %v", sourceKind, sourceName, err)
	}
	if err := projector.RefreshAppAssetSource(ctx, sourceKind, sourceName); err != nil {
		t.Fatalf("RefreshAppAssetSource(%s/%s): %v", sourceKind, sourceName, err)
	}
	if err := projector.RefreshConnectorSourceState(ctx); err != nil {
		t.Fatalf("RefreshConnectorSourceState(): %v", err)
	}
	if err := projector.RefreshSaaSAppRiskReadModelsBySource(ctx, sourceKind, sourceName); err != nil {
		t.Fatalf("RefreshSaaSAppRiskReadModelsBySource(%s/%s): %v", sourceKind, sourceName, err)
	}
	if err := projector.RefreshNonHumanPrincipalSourceReadModels(ctx, sourceKind, sourceName); err != nil {
		t.Fatalf("RefreshNonHumanPrincipalSourceReadModels(%s/%s): %v", sourceKind, sourceName, err)
	}
}

func insertCommandSearchOktaApp(t *testing.T, ctx context.Context, q *gen.Queries, runID int64, externalID, label, name, status string) {
	t.Helper()

	if _, err := q.UpsertOktaAppsBulk(ctx, gen.UpsertOktaAppsBulkParams{
		SeenInRunID: runID,
		SourceKind:  "okta",
		SourceName:  "example.okta.com",
		ExternalIds: []string{externalID},
		Labels:      []string{label},
		Names:       []string{name},
		Statuses:    []string{status},
		SignOnModes: []string{"bookmark"},
		RawJsons:    [][]byte{[]byte(`{}`)},
	}); err != nil {
		t.Fatalf("UpsertOktaAppsBulk %s: %v", externalID, err)
	}
	if _, err := q.PromoteOktaAppsSeenInRunBySource(ctx, gen.PromoteOktaAppsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        "okta",
		SourceName:        "example.okta.com",
	}); err != nil {
		t.Fatalf("PromoteOktaAppsSeenInRunBySource %s: %v", externalID, err)
	}
}
