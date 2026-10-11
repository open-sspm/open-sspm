package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
)

func TestHandleNonHumanIdentitiesRendersUnifiedInventory(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		ownerID := insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Owner User")
		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		insertCommandSearchIdentitySourceSetting(t, ctx, pool, configstore.KindEntra, "tenant-1", true)

		serviceAccountID := insertCommandSearchAccount(t, ctx, pool, entraRunID, commandSearchAccountSeed{
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
		serviceIdentityID := insertCommandSearchIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, serviceIdentityID, serviceAccountID)

		serviceAssetID := insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_service_principal", "svc-123", "azure-enterprise-app", "Azure Service Principal", "active")
		if _, err := q.UpsertAppAssetGovernance(ctx, gen.UpsertAppAssetGovernanceParams{
			AppAssetID:      serviceAssetID,
			GovernanceState: "approved",
			OwnerIdentityID: pgtype.Int8{Int64: ownerID, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertAppAssetGovernance(service): %v", err)
		}

		githubAssetID := insertCommandSearchAppAsset(t, ctx, q, githubRunID, configstore.KindGitHub, "acme", "github_app", "github-actions", "", "GitHub Actions", "active")
		insertNonHumanCredentialArtifact(t, ctx, pool, githubRunID, nonHumanCredentialArtifactSeed{
			SourceKind:           configstore.KindGitHub,
			SourceName:           "acme",
			AssetRefKind:         "app_asset",
			AssetRefExternalID:   "github_app:github-actions",
			CredentialKind:       "github_pat_fine_grained",
			ExternalID:           "pat-123",
			DisplayName:          "GitHub Actions PAT",
			Status:               "active",
			LastUsedAtSource:     time.Now().UTC().Add(-120 * 24 * time.Hour),
			CreatedByExternalID:  "ci-bot@example.com",
			CreatedByDisplayName: "CI Bot",
		})
		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindGitHub, "acme")

		c, rec := newTestContext(http.MethodGet, "http://example.com/non-human-identities?owner_presence=unknown")

		if err := h.HandleNonHumanIdentities(c); err != nil {
			t.Fatalf("HandleNonHumanIdentities(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Non-Human Identities")
		assertContains(t, body, `id="non-human-identities-results"`)
		assertContains(t, body, `id="non-human-identities-filters"`)
		assertContains(t, body, `id="non-human-identities-inventory"`)
		assertContains(t, body, `data-enter-only-query="q"`)
		assertContains(t, body, `hx-target="#non-human-identities-inventory"`)
		assertContains(t, body, `hx-get="/non-human-identities?owner_presence=unknown&amp;page=1"`)
		assertContains(t, body, "/non-human-identities/app-asset-"+fmt.Sprint(githubAssetID))
		assertContains(t, body, "GitHub Actions")
		assertContains(t, body, "Unknown")
		assertNotContains(t, body, "/non-human-identities/identity-"+fmt.Sprint(serviceIdentityID))
	})
}

func TestHandleNonHumanIdentitiesHTMXReturnsInventoryOnly(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		c, rec := newTestContext(http.MethodGet, "http://example.com/non-human-identities")
		(*c).Request().Header.Set("HX-Request", "true")
		(*c).Request().Header.Set("HX-Target", "non-human-identities-inventory")

		if err := h.HandleNonHumanIdentities(c); err != nil {
			t.Fatalf("HandleNonHumanIdentities(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, `id="non-human-identities-inventory"`)
		assertContains(t, body, `data-busy-inline-indicator`)
		assertNotContains(t, body, `id="non-human-identities-filters"`)
		assertNotContains(t, body, `hx-swap-oob="outerHTML"`)
		assertNotContains(t, body, `data-osspm-askbar`)
		assertNotContains(t, body, "<!doctype html>")
	})
}

func TestRefreshCommandSearchSourceReadModelsPopulatesNonHumanPrincipals(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, _ *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		insertCommandSearchAppAsset(t, ctx, q, runID, configstore.KindGitHub, "acme", "github_app", "github-actions", "", "GitHub Actions", "active")
		insertNonHumanCredentialArtifact(t, ctx, pool, runID, nonHumanCredentialArtifactSeed{
			SourceKind:         configstore.KindGitHub,
			SourceName:         "acme",
			AssetRefKind:       "app_asset",
			AssetRefExternalID: "github_app:github-actions",
			CredentialKind:     "github_pat_fine_grained",
			ExternalID:         "pat-123",
			DisplayName:        "GitHub Actions PAT",
			Status:             "active",
		})

		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindGitHub, "acme")

		rows, err := q.ListNonHumanPrincipalsPageByFilters(ctx, gen.ListNonHumanPrincipalsPageByFiltersParams{
			PageLimit:             20,
			ConfiguredSourceKinds: []string{configstore.KindGitHub},
			ConfiguredSourceNames: []string{"acme"},
			SourceKind:            configstore.KindGitHub,
			SourceName:            "acme",
		})
		if err != nil {
			t.Fatalf("ListNonHumanPrincipalsPageByFilters(): %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("rows len = %d, want 1 (%+v)", len(rows), rows)
		}
		if rows[0].DisplayName != "GitHub Actions" {
			t.Fatalf("display name = %q, want %q", rows[0].DisplayName, "GitHub Actions")
		}
	})
}

func TestHandleNonHumanIdentitiesFallsBackWhenConfiguredSourceNameDrifts(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "reference-acme")
		githubAssetID := insertCommandSearchAppAsset(t, ctx, q, runID, configstore.KindGitHub, "reference-acme", "github_app", "github-actions", "", "GitHub Actions", "active")
		insertNonHumanCredentialArtifact(t, ctx, pool, runID, nonHumanCredentialArtifactSeed{
			SourceKind:         configstore.KindGitHub,
			SourceName:         "reference-acme",
			AssetRefKind:       "app_asset",
			AssetRefExternalID: "github_app:github-actions",
			CredentialKind:     "github_pat_fine_grained",
			ExternalID:         "pat-123",
			DisplayName:        "GitHub Actions PAT",
			Status:             "active",
		})

		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindGitHub, "reference-acme")

		rows, err := q.ListNonHumanPrincipalsPageByFilters(ctx, gen.ListNonHumanPrincipalsPageByFiltersParams{
			PageLimit:             20,
			ConfiguredSourceKinds: []string{configstore.KindGitHub},
			ConfiguredSourceNames: []string{"acme"},
			SourceKind:            configstore.KindGitHub,
		})
		if err != nil {
			t.Fatalf("ListNonHumanPrincipalsPageByFilters(): %v", err)
		}
		if len(rows) != 1 || rows[0].PrincipalRef != "app-asset-"+fmt.Sprint(githubAssetID) {
			t.Fatalf("rows = %+v, want app-asset-%d", rows, githubAssetID)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/non-human-identities")

		if err := h.HandleNonHumanIdentities(c); err != nil {
			t.Fatalf("HandleNonHumanIdentities(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "/non-human-identities/app-asset-"+fmt.Sprint(githubAssetID))
		assertContains(t, body, "GitHub Actions")
	})
}

func TestHandleNonHumanIdentityShowRendersDetailLinksAndRiskReasons(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		ownerID := insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Owner User")
		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		insertCommandSearchIdentitySourceSetting(t, ctx, pool, configstore.KindEntra, "tenant-1", true)

		serviceAccountID := insertCommandSearchAccount(t, ctx, pool, entraRunID, commandSearchAccountSeed{
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
		serviceIdentityID := insertCommandSearchIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, serviceIdentityID, serviceAccountID)

		serviceAssetID := insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_service_principal", "svc-123", "azure-enterprise-app", "Azure Service Principal", "active")
		if _, err := q.UpsertAppAssetGovernance(ctx, gen.UpsertAppAssetGovernanceParams{
			AppAssetID:      serviceAssetID,
			GovernanceState: "action_required",
			OwnerIdentityID: pgtype.Int8{Int64: ownerID, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertAppAssetGovernance(service): %v", err)
		}

		credentialID := insertNonHumanCredentialArtifact(t, ctx, pool, entraRunID, nonHumanCredentialArtifactSeed{
			SourceKind:            configstore.KindEntra,
			SourceName:            "tenant-1",
			AssetRefKind:          "app_asset",
			AssetRefExternalID:    "entra_service_principal:svc-123",
			CredentialKind:        "entra_client_secret",
			ExternalID:            "secret-123",
			DisplayName:           "Azure Client Secret",
			Status:                "active",
			ExpiresAtSource:       time.Now().UTC().Add(-24 * time.Hour),
			LastUsedAtSource:      time.Now().UTC().Add(-120 * 24 * time.Hour),
			CreatedByExternalID:   "owner@example.com",
			CreatedByDisplayName:  "Owner User",
			ApprovedByExternalID:  "owner@example.com",
			ApprovedByDisplayName: "Owner User",
		})
		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindEntra, "tenant-1")

		c, rec := newTestContext(http.MethodGet, "http://example.com/non-human-identities/identity-"+fmt.Sprint(serviceIdentityID))
		(*c).SetPath("/non-human-identities/:ref")
		(*c).SetPathValues(echo.PathValues{{Name: "ref", Value: "identity-" + fmt.Sprint(serviceIdentityID)}})

		if err := h.HandleNonHumanIdentityShow(c); err != nil {
			t.Fatalf("HandleNonHumanIdentityShow(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Azure Service Principal")
		assertContains(t, body, "Owner User")
		assertContains(t, body, "Linked credential is expired")
		assertContains(t, body, "Principal has stale or aging evidence")
		assertContains(t, body, "/identities/"+fmt.Sprint(serviceIdentityID))
		assertContains(t, body, "/app-assets/"+fmt.Sprint(serviceAssetID))
		assertContains(t, body, "/credentials/"+fmt.Sprint(credentialID))
	})
}

func TestHandleNonHumanIdentityShowRendersAccountRelationships(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		ownerID := insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Service Owner")
		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		insertCommandSearchIdentitySourceSetting(t, ctx, pool, configstore.KindEntra, "tenant-1", true)

		serviceAccountID := insertCommandSearchAccount(t, ctx, pool, entraRunID, commandSearchAccountSeed{
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
		serviceIdentityID := insertCommandSearchIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, serviceIdentityID, serviceAccountID)

		insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_service_principal", "svc-123", "azure-enterprise-app", "Azure Service Principal", "active")
		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindEntra, "tenant-1")

		if _, err := q.UpsertAccountIdentityRelationship(ctx, gen.UpsertAccountIdentityRelationshipParams{
			AccountID:        serviceAccountID,
			IdentityID:       ownerID,
			RelationshipType: "custodian",
			SourceKind:       pgtype.Text{String: configstore.KindEntra, Valid: true},
			SourceName:       pgtype.Text{String: "tenant-1", Valid: true},
			Confidence:       70,
			LifecycleState:   "active",
		}); err != nil {
			t.Fatalf("UpsertAccountIdentityRelationship(): %v", err)
		}

		adminUserID := insertDiscoveryAuthUser(t, ctx, pool, "admin@example.com", "admin")
		c, rec := newTestContext(http.MethodGet, "http://example.com/non-human-identities/identity-"+fmt.Sprint(serviceIdentityID))
		(*c).SetPath("/non-human-identities/:ref")
		(*c).SetPathValues(echo.PathValues{{Name: "ref", Value: "identity-" + fmt.Sprint(serviceIdentityID)}})
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminUserID, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleNonHumanIdentityShow(c); err != nil {
			t.Fatalf("HandleNonHumanIdentityShow(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Account relationships")
		assertContains(t, body, "Service Owner")
		assertContains(t, body, "owner@example.com")
		assertContains(t, body, "Custodian")
		assertContains(t, body, "Identity email")
		assertContains(t, body, "/non-human-identities/identity-"+fmt.Sprint(serviceIdentityID)+"/relationships")
	})
}

func TestHandleNonHumanIdentityRelationshipCreateStoresAccountScopedRelationship(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		ownerID := insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Service Owner")
		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		insertCommandSearchIdentitySourceSetting(t, ctx, pool, configstore.KindEntra, "tenant-1", true)

		serviceAccountID := insertCommandSearchAccount(t, ctx, pool, entraRunID, commandSearchAccountSeed{
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
		serviceIdentityID := insertCommandSearchIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, serviceIdentityID, serviceAccountID)
		insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_service_principal", "svc-123", "azure-enterprise-app", "Azure Service Principal", "active")
		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindEntra, "tenant-1")

		form := url.Values{"identity_email": {"owner@example.com"}, "relationship_type": {"owner"}}
		c, rec := newFormTestContext(http.MethodPost, "http://example.com/non-human-identities/identity-"+fmt.Sprint(serviceIdentityID)+"/relationships", form)
		(*c).SetPath("/non-human-identities/:ref/relationships")
		(*c).SetPathValues(echo.PathValues{{Name: "ref", Value: "identity-" + fmt.Sprint(serviceIdentityID)}})

		if err := h.HandleNonHumanIdentityRelationshipCreate(c); err != nil {
			t.Fatalf("HandleNonHumanIdentityRelationshipCreate(): %v", err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
		}

		relationships, err := q.ListIdentityScopedAccountRelationships(ctx, gen.ListIdentityScopedAccountRelationshipsParams{
			IdentityID:     serviceIdentityID,
			LifecycleState: "active",
		})
		if err != nil {
			t.Fatalf("ListIdentityScopedAccountRelationships(): %v", err)
		}
		if len(relationships) != 1 {
			t.Fatalf("relationships len = %d, want 1 (%+v)", len(relationships), relationships)
		}
		if relationships[0].AccountID != serviceAccountID || relationships[0].IdentityID != ownerID || relationships[0].RelationshipType != "owner" {
			t.Fatalf("relationship = %+v", relationships[0])
		}

		ownedRows, err := q.ListNonHumanPrincipalsPageByFilters(ctx, gen.ListNonHumanPrincipalsPageByFiltersParams{
			PageLimit:             20,
			OwnerPresence:         "owned",
			ConfiguredSourceKinds: []string{configstore.KindEntra},
			ConfiguredSourceNames: []string{"tenant-1"},
		})
		if err != nil {
			t.Fatalf("ListNonHumanPrincipalsPageByFilters(owned): %v", err)
		}
		if len(ownedRows) != 1 {
			t.Fatalf("owned rows len = %d, want 1 (%+v)", len(ownedRows), ownedRows)
		}
		if ownedRows[0].AccountableOwnerIdentityID != ownerID || ownedRows[0].AccountableOwnerDisplayName != "Service Owner" || ownedRows[0].OwnerPresence != "owned" {
			t.Fatalf("owned row = %+v", ownedRows[0])
		}
		unknownCount, err := q.CountNonHumanPrincipalsByFilters(ctx, gen.CountNonHumanPrincipalsByFiltersParams{
			OwnerPresence:         "unknown",
			ConfiguredSourceKinds: []string{configstore.KindEntra},
			ConfiguredSourceNames: []string{"tenant-1"},
		})
		if err != nil {
			t.Fatalf("CountNonHumanPrincipalsByFilters(unknown): %v", err)
		}
		if unknownCount != 0 {
			t.Fatalf("unknown count = %d, want 0", unknownCount)
		}
		coverage, err := q.CountConfiguredNonHumanPrincipalOwnerCoverage(ctx)
		if err != nil {
			t.Fatalf("CountConfiguredNonHumanPrincipalOwnerCoverage(): %v", err)
		}
		if coverage.PrincipalCount != 1 || coverage.WithOwnerCount != 1 {
			t.Fatalf("coverage = %+v, want 1/1", coverage)
		}
	})
}

func TestHandleNonHumanIdentityRelationshipCreateRejectsAmbiguousEmail(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Owner A")
		insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Owner B")
		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		insertCommandSearchIdentitySourceSetting(t, ctx, pool, configstore.KindEntra, "tenant-1", true)

		serviceAccountID := insertCommandSearchAccount(t, ctx, pool, entraRunID, commandSearchAccountSeed{
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
		serviceIdentityID := insertCommandSearchIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, serviceIdentityID, serviceAccountID)
		insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_service_principal", "svc-123", "azure-enterprise-app", "Azure Service Principal", "active")
		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindEntra, "tenant-1")

		form := url.Values{"identity_email": {"owner@example.com"}, "relationship_type": {"owner"}}
		c, rec := newFormTestContext(http.MethodPost, "http://example.com/non-human-identities/identity-"+fmt.Sprint(serviceIdentityID)+"/relationships", form)
		(*c).SetPath("/non-human-identities/:ref/relationships")
		(*c).SetPathValues(echo.PathValues{{Name: "ref", Value: "identity-" + fmt.Sprint(serviceIdentityID)}})

		if err := h.HandleNonHumanIdentityRelationshipCreate(c); err != nil {
			t.Fatalf("HandleNonHumanIdentityRelationshipCreate(): %v", err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
		}

		relationships, err := q.ListIdentityScopedAccountRelationships(ctx, gen.ListIdentityScopedAccountRelationshipsParams{
			IdentityID:     serviceIdentityID,
			LifecycleState: "active",
		})
		if err != nil {
			t.Fatalf("ListIdentityScopedAccountRelationships(): %v", err)
		}
		if len(relationships) != 0 {
			t.Fatalf("relationships len = %d, want 0 (%+v)", len(relationships), relationships)
		}
	})
}

func TestHandleNonHumanIdentityShowUsesBestAvailableAttribution(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		attributionIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "builder@example.com", "Build User")
		githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		githubAssetID := insertCommandSearchAppAsset(t, ctx, q, githubRunID, configstore.KindGitHub, "acme", "github_app", "github-actions", "", "GitHub Actions", "active")

		insertNonHumanCredentialArtifact(t, ctx, pool, githubRunID, nonHumanCredentialArtifactSeed{
			SourceKind:           configstore.KindGitHub,
			SourceName:           "acme",
			AssetRefKind:         "app_asset",
			AssetRefExternalID:   "github_app:github-actions",
			CredentialKind:       "github_pat_fine_grained",
			ExternalID:           "pat-123",
			DisplayName:          "GitHub Actions PAT",
			Status:               "active",
			CreatedByExternalID:  "builder@example.com",
			CreatedByDisplayName: "Build User",
		})
		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindGitHub, "acme")

		c, rec := newTestContext(http.MethodGet, "http://example.com/non-human-identities/app-asset-"+fmt.Sprint(githubAssetID))
		(*c).SetPath("/non-human-identities/:ref")
		(*c).SetPathValues(echo.PathValues{{Name: "ref", Value: "app-asset-" + fmt.Sprint(githubAssetID)}})

		if err := h.HandleNonHumanIdentityShow(c); err != nil {
			t.Fatalf("HandleNonHumanIdentityShow(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Best-available attribution")
		assertContains(t, body, "Build User")
		assertContains(t, body, "/identities/"+fmt.Sprint(attributionIdentityID))
	})
}

func TestNonHumanIdentitiesHandlersTrackUsageEvents(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		adminUserID := insertDiscoveryAuthUser(t, ctx, pool, "admin@example.com", "admin")
		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		insertCommandSearchIdentitySourceSetting(t, ctx, pool, configstore.KindEntra, "tenant-1", true)

		serviceAccountID := insertCommandSearchAccount(t, ctx, pool, entraRunID, commandSearchAccountSeed{
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
		serviceIdentityID := insertCommandSearchIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, serviceIdentityID, serviceAccountID)

		insertCommandSearchAppAsset(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "entra_service_principal", "svc-123", "azure-enterprise-app", "Azure Service Principal", "active")
		credentialID := insertNonHumanCredentialArtifact(t, ctx, pool, entraRunID, nonHumanCredentialArtifactSeed{
			SourceKind:           configstore.KindEntra,
			SourceName:           "tenant-1",
			AssetRefKind:         "app_asset",
			AssetRefExternalID:   "entra_service_principal:svc-123",
			CredentialKind:       "entra_client_secret",
			ExternalID:           "secret-123",
			DisplayName:          "Azure Client Secret",
			Status:               "active",
			CreatedByExternalID:  "owner@example.com",
			CreatedByDisplayName: "Owner User",
		})
		refreshCommandSearchSourceReadModels(t, ctx, q, configstore.KindEntra, "tenant-1")

		listCtx, listRec := newTestContext(http.MethodGet, "http://example.com/non-human-identities?owner_presence=unknown")
		(*listCtx).Request().Header.Set("Referer", "http://example.com/non-human-identities?freshness_state=stale")
		(*listCtx).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminUserID, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleNonHumanIdentities(listCtx); err != nil {
			t.Fatalf("HandleNonHumanIdentities(): %v", err)
		}
		if listRec.Code != http.StatusOK {
			t.Fatalf("list status = %d, want %d", listRec.Code, http.StatusOK)
		}

		showCtx, showRec := newTestContext(http.MethodGet, "http://example.com/non-human-identities/identity-"+fmt.Sprint(serviceIdentityID))
		(*showCtx).SetPath("/non-human-identities/:ref")
		(*showCtx).SetPathValues(echo.PathValues{{Name: "ref", Value: "identity-" + fmt.Sprint(serviceIdentityID)}})
		(*showCtx).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminUserID, Email: "admin@example.com", Role: "admin"})
		if err := h.HandleNonHumanIdentityShow(showCtx); err != nil {
			t.Fatalf("HandleNonHumanIdentityShow(): %v", err)
		}
		if showRec.Code != http.StatusOK {
			t.Fatalf("show status = %d, want %d", showRec.Code, http.StatusOK)
		}

		credentialCtx, credentialRec := newTestContext(http.MethodGet, "http://example.com/credentials/"+fmt.Sprint(credentialID))
		(*credentialCtx).Request().Header.Set("Referer", "http://example.com/non-human-identities/identity-"+fmt.Sprint(serviceIdentityID))
		(*credentialCtx).SetPath("/credentials/:id")
		(*credentialCtx).SetPathValues(echo.PathValues{{Name: "id", Value: fmt.Sprint(credentialID)}})
		(*credentialCtx).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminUserID, Email: "admin@example.com", Role: "admin"})
		if err := h.HandleCredentialShow(credentialCtx); err != nil {
			t.Fatalf("HandleCredentialShow(): %v", err)
		}
		if credentialRec.Code != http.StatusOK {
			t.Fatalf("credential status = %d, want %d", credentialRec.Code, http.StatusOK)
		}

		events := listNonHumanAccessEvents(t, ctx, pool)
		if len(events) != 4 {
			t.Fatalf("events len = %d, want 4 (%+v)", len(events), events)
		}
		if events[0].EventKind != "inventory_view" {
			t.Fatalf("event[0] = %+v", events[0])
		}
		if events[1].EventKind != "filter_use" || events[1].FilterSignature != "owner_presence=unknown" {
			t.Fatalf("event[1] = %+v", events[1])
		}
		if events[2].EventKind != "detail_open" || events[2].PrincipalRef != "identity-"+fmt.Sprint(serviceIdentityID) {
			t.Fatalf("event[2] = %+v", events[2])
		}
		if events[3].EventKind != "outbound_click" || events[3].PrincipalRef != "identity-"+fmt.Sprint(serviceIdentityID) || events[3].TargetKind != "credential" || events[3].TargetRef != fmt.Sprint(credentialID) {
			t.Fatalf("event[3] = %+v", events[3])
		}
		if events[3].AuthUserID != adminUserID || events[3].AuthUserRole != "admin" {
			t.Fatalf("event[3] auth = %+v", events[3])
		}
	})
}

type nonHumanCredentialArtifactSeed struct {
	SourceKind            string
	SourceName            string
	AssetRefKind          string
	AssetRefExternalID    string
	CredentialKind        string
	ExternalID            string
	DisplayName           string
	Status                string
	ExpiresAtSource       time.Time
	LastUsedAtSource      time.Time
	CreatedByExternalID   string
	CreatedByDisplayName  string
	ApprovedByExternalID  string
	ApprovedByDisplayName string
}

func insertNonHumanCredentialArtifact(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, seed nonHumanCredentialArtifactSeed) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO credential_artifacts (
			source_kind,
			source_name,
			asset_ref_kind,
			asset_ref_external_id,
			credential_kind,
			external_id,
			display_name,
			scope_json,
			raw_json,
			lineage_key,
			status,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			expires_at_source,
			last_used_at_source,
			created_by_kind,
			created_by_external_id,
			created_by_display_name,
			approved_by_kind,
			approved_by_external_id,
			approved_by_display_name,
			updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, '{}'::jsonb,
			md5(
				coalesce($1, '') || '|' ||
				coalesce($2, '') || '|' ||
				coalesce($3, '') || '|' ||
				coalesce($4, '') || '|' ||
				coalesce($5, '') || '|' ||
				CASE
					WHEN coalesce($7, '') ~ '\s*\[\d{4}\]\s*$' THEN
						'cohort:' || lower(trim(regexp_replace(coalesce($7, ''), '\s*\[\d{4}\]\s*$', '')))
					ELSE
						'id:' || coalesce($6, '') || '|' || lower(trim(coalesce($7, '')))
				END
			),
			$8, $9, now(), $9, now(), $10, $11, 'user', $12, $13, 'user', $14, $15, now()
		)
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.AssetRefKind, seed.AssetRefExternalID, seed.CredentialKind, seed.ExternalID, seed.DisplayName, seed.Status, runID, nonHumanNullableTime(seed.ExpiresAtSource), nonHumanNullableTime(seed.LastUsedAtSource), seed.CreatedByExternalID, seed.CreatedByDisplayName, seed.ApprovedByExternalID, seed.ApprovedByDisplayName).Scan(&id)
	if err != nil {
		t.Fatalf("insert credential artifact %s/%s: %v", seed.SourceKind, seed.ExternalID, err)
	}

	return id
}

func nonHumanNullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

type nonHumanAccessEventRow struct {
	EventKind       string
	PrincipalRef    string
	TargetKind      string
	TargetRef       string
	FilterSignature string
	AuthUserID      int64
	AuthUserRole    string
}

func listNonHumanAccessEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []nonHumanAccessEventRow {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT event_kind, principal_ref, target_kind, target_ref, filter_signature, auth_user_id, auth_user_role
		FROM non_human_access_events
		ORDER BY id ASC
	`)
	if err != nil {
		t.Fatalf("query non_human_access_events: %v", err)
	}
	defer rows.Close()

	out := make([]nonHumanAccessEventRow, 0)
	for rows.Next() {
		var row nonHumanAccessEventRow
		if err := rows.Scan(&row.EventKind, &row.PrincipalRef, &row.TargetKind, &row.TargetRef, &row.FilterSignature, &row.AuthUserID, &row.AuthUserRole); err != nil {
			t.Fatalf("scan non_human_access_events: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate non_human_access_events: %v", err)
	}
	return out
}
