package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
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

func TestHandleDiscoveryAppShowUsesLivePostureWithoutPersisting(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		setSyncRunFinishedAt(t, ctx, pool, runID, time.Now().UTC().Add(-2*time.Hour))

		appID := insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			runID,
			configstore.KindEntra,
			"tenant-1",
			"azure-cloud",
			"Azure Cloud",
			"azure.com",
			"Microsoft",
			"azure-cloud",
		)
		upsertDiscoveryPrimaryBinding(t, ctx, q, appID, configstore.KindEntra, "tenant-1")

		body := renderDiscoveryAppShow(t, h, appID)
		assertContains(t, body, "Azure Cloud")
		assertContains(t, body, "Bound connector sync is stale")
		assertContains(t, body, "Score 60")

		var postureColumnCount int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM information_schema.columns
			WHERE table_name = 'saas_apps'
			  AND column_name IN ('managed_state', 'managed_reason', 'risk_score', 'risk_level')
		`).Scan(&postureColumnCount); err != nil {
			t.Fatalf("count posture columns: %v", err)
		}
		if postureColumnCount != 0 {
			t.Fatalf("saas_apps still exposes %d stored posture columns", postureColumnCount)
		}
	})
}

func TestHandleDiscoveryAppShowManagedBindingsAcrossConnectorKinds(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		t.Run("github binding discovered from okta is managed when github is fresh", func(t *testing.T) {
			upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
				Domain: "acme.okta.com",
				Token:  "token-1",
			})
			upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
				Org:   "acme",
				Token: "github-token",
			})

			oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
			_ = insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

			appID := insertCommandSearchDiscoveryApp(
				t,
				ctx,
				pool,
				q,
				oktaRunID,
				configstore.KindOkta,
				"acme.okta.com",
				"github-actions",
				"GitHub Actions",
				"github.com",
				"GitHub",
				"github-actions",
			)
			upsertDiscoveryPrimaryBinding(t, ctx, q, appID, configstore.KindGitHub, "acme")

			body := renderDiscoveryAppShow(t, h, appID)
			assertContains(t, body, "GitHub Actions")
			assertContains(t, body, "Primary binding has fresh sync")
			assertContains(t, body, "Score 15")
		})

		t.Run("datadog binding discovered from okta is managed when datadog is fresh", func(t *testing.T) {
			upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
				Domain: "acme.okta.com",
				Token:  "token-1",
			})
			upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindDatadog, true, configstore.DatadogConfig{
				Site:   "datadoghq.com",
				APIKey: "api-key",
				AppKey: "app-key",
			})

			oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
			_ = insertCommandSearchSyncRun(t, ctx, pool, configstore.KindDatadog, "datadoghq.com")

			appID := insertCommandSearchDiscoveryApp(
				t,
				ctx,
				pool,
				q,
				oktaRunID,
				configstore.KindOkta,
				"acme.okta.com",
				"datadog-ci",
				"Datadog CI",
				"datadoghq.com",
				"Datadog",
				"datadog-ci",
			)
			upsertDiscoveryPrimaryBinding(t, ctx, q, appID, configstore.KindDatadog, "datadoghq.com")

			body := renderDiscoveryAppShow(t, h, appID)
			assertContains(t, body, "Datadog CI")
			assertContains(t, body, "Primary binding has fresh sync")
			assertContains(t, body, "Score 15")
		})

		t.Run("entra discovery sync alias counts as fresh for entra binding", func(t *testing.T) {
			upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
				TenantID:     "tenant-1",
				ClientID:     "client-1",
				ClientSecret: "secret-1",
			})

			discoveryRunID := insertCommandSearchSyncRun(t, ctx, pool, "entra_discovery", "tenant-1")
			appID := insertCommandSearchDiscoveryApp(
				t,
				ctx,
				pool,
				q,
				discoveryRunID,
				configstore.KindEntra,
				"tenant-1",
				"azure-portal",
				"Azure Portal",
				"portal.azure.com",
				"Microsoft",
				"azure-portal",
			)
			upsertDiscoveryPrimaryBinding(t, ctx, q, appID, configstore.KindEntra, "tenant-1")

			body := renderDiscoveryAppShow(t, h, appID)
			assertContains(t, body, "Azure Portal")
			assertContains(t, body, "Primary binding has fresh sync")
			assertContains(t, body, "Score 15")
		})

		t.Run("aws sync alias counts as fresh for aws identity center binding", func(t *testing.T) {
			upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
				Domain: "acme.okta.com",
				Token:  "token-1",
			})
			upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindAWSIdentityCenter, true, configstore.AWSIdentityCenterConfig{
				Region: "eu-west-1",
			})

			oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
			_ = insertCommandSearchSyncRun(t, ctx, pool, "aws", "eu-west-1")

			appID := insertCommandSearchDiscoveryApp(
				t,
				ctx,
				pool,
				q,
				oktaRunID,
				configstore.KindOkta,
				"acme.okta.com",
				"aws-console",
				"AWS Console",
				"aws.amazon.com",
				"Amazon",
				"aws-console",
			)
			upsertDiscoveryPrimaryBinding(t, ctx, q, appID, configstore.KindAWSIdentityCenter, "eu-west-1")

			body := renderDiscoveryAppShow(t, h, appID)
			assertContains(t, body, "AWS Console")
			assertContains(t, body, "Primary binding has fresh sync")
			assertContains(t, body, "Score 15")
		})
	})
}

func TestHandleDiscoveryAppShowDisabledConnectorIsUnmanaged(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "acme.okta.com",
			Token:  "token-1",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, false, configstore.GitHubConfig{
			Org:   "acme",
			Token: "github-token",
		})

		oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		_ = insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

		appID := insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			oktaRunID,
			configstore.KindOkta,
			"acme.okta.com",
			"disabled-github-app",
			"Disabled GitHub App",
			"github.com",
			"GitHub",
			"disabled-github-app",
		)
		upsertDiscoveryPrimaryBinding(t, ctx, q, appID, configstore.KindGitHub, "acme")

		body := renderDiscoveryAppShow(t, h, appID)
		assertContains(t, body, "Disabled GitHub App")
		assertContains(t, body, "Bound connector is disabled")
		assertContains(t, body, "Score 60")
	})
}

func TestListTopActorsForSaaSAppByIDPrefersDisplayNameForMixedActorEvidence(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		appID := insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			runID,
			configstore.KindEntra,
			"tenant-1",
			"shared-actor-app",
			"Shared Actor App",
			"shared.example.com",
			"Example",
			"shared-actor-app",
		)

		now := time.Now().UTC()
		if _, err := q.UpsertSaaSAppEventsBulkBySource(ctx, gen.UpsertSaaSAppEventsBulkBySourceParams{
			SeenInRunID:       runID,
			SourceKind:        configstore.KindEntra,
			SourceName:        "tenant-1",
			CanonicalKeys:     []string{"shared-actor-app", "shared-actor-app"},
			SignalKinds:       []string{"oauth_grant", "oauth_grant"},
			EventExternalIds:  []string{"shared-actor-app-event-1", "shared-actor-app-event-2"},
			SourceAppIds:      []string{"shared-actor-app", "shared-actor-app"},
			SourceAppNames:    []string{"Shared Actor App", "Shared Actor App"},
			SourceAppDomains:  []string{"shared.example.com", "shared.example.com"},
			ActorExternalIds:  []string{"actor-1", "actor-1"},
			ActorEmails:       []string{"alice@example.com", "alice@example.com"},
			ActorDisplayNames: []string{"", "Alice Example"},
			ObservedAts: []pgtype.Timestamptz{
				{Time: now.Add(-time.Minute), Valid: true},
				{Time: now, Valid: true},
			},
			ScopesJsons: [][]byte{[]byte(`["mail.read"]`), []byte(`["mail.read"]`)},
			RawJsons:    [][]byte{[]byte(`{}`), []byte(`{}`)},
		}); err != nil {
			t.Fatalf("UpsertSaaSAppEventsBulkBySource shared actor app: %v", err)
		}
		if _, err := q.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
			LastObservedRunID: runID,
			SourceKind:        configstore.KindEntra,
			SourceName:        "tenant-1",
		}); err != nil {
			t.Fatalf("PromoteSaaSAppEventsSeenInRunBySource shared actor app: %v", err)
		}

		actors, err := q.ListTopActorsForSaaSAppByID(ctx, gen.ListTopActorsForSaaSAppByIDParams{
			SaasAppID: appID,
			LimitRows: 25,
		})
		if err != nil {
			t.Fatalf("ListTopActorsForSaaSAppByID(%d): %v", appID, err)
		}
		if len(actors) != 1 {
			t.Fatalf("actor rows = %d, want 1", len(actors))
		}
		if actors[0].ActorLabel != "Alice Example" {
			t.Fatalf("actor label = %q, want %q", actors[0].ActorLabel, "Alice Example")
		}
		if actors[0].ActorEmail != "alice@example.com" {
			t.Fatalf("actor email = %q, want %q", actors[0].ActorEmail, "alice@example.com")
		}
		if actors[0].ActorExternalID != "actor-1" {
			t.Fatalf("actor external id = %q, want %q", actors[0].ActorExternalID, "actor-1")
		}
		if actors[0].EventCount != 2 {
			t.Fatalf("event count = %d, want 2", actors[0].EventCount)
		}

		body := renderDiscoveryAppShow(t, h, appID)
		assertContains(t, body, "Alice Example")
	})
}

func TestHandleDiscoveryAppsFiltersManagedState(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "acme.okta.com",
			Token:  "token-1",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "github-token",
		})

		oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		_ = insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

		managedAppID := insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			oktaRunID,
			configstore.KindOkta,
			"acme.okta.com",
			"managed-gh-app",
			"Managed GitHub App",
			"github.com",
			"GitHub",
			"managed-gh-app",
		)
		upsertDiscoveryPrimaryBinding(t, ctx, q, managedAppID, configstore.KindGitHub, "acme")

		_ = insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			oktaRunID,
			configstore.KindOkta,
			"acme.okta.com",
			"unmanaged-no-binding",
			"Unmanaged No Binding",
			"example.com",
			"Example",
			"unmanaged-no-binding",
		)

		body := renderDiscoveryApps(t, h, "http://example.com/discovery/apps?managed_state=managed")
		assertContains(t, body, "Managed GitHub App")
		assertNotContains(t, body, "Unmanaged No Binding")
	})
}

func TestHandleDiscoveryAppsHTMXReturnsResultsShellOnly(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "acme.okta.com",
			Token:  "token-1",
		})

		oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			oktaRunID,
			configstore.KindOkta,
			"acme.okta.com",
			"managed-gh-app",
			"Managed GitHub App",
			"github.com",
			"GitHub",
			"managed-gh-app",
		)

		c, rec := newTestContext(http.MethodGet, "http://example.com/discovery/apps")
		(*c).Request().Header.Set("HX-Request", "true")
		(*c).Request().Header.Set("HX-Target", "discovery-apps-results")

		if err := h.HandleDiscoveryApps(c); err != nil {
			t.Fatalf("HandleDiscoveryApps(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, `id="discovery-apps-results"`)
		assertContains(t, body, `data-busy-inline-indicator`)
		assertContains(t, body, `data-enter-only-query="q"`)
		assertContains(t, body, `hx-get="/discovery/apps?page=1"`)
		assertContains(t, body, `hx-trigger="change delay:150ms from:select, submit"`)
		assertContains(t, body, `class="space-y-3 lg:hidden"`)
		assertContains(t, body, `class="hidden lg:block"`)
		assertContains(t, body, `aria-label="View details for Managed GitHub App"`)
		assertNotContains(t, body, "<!doctype html>")
	})
}

func TestHandleDiscoveryAppsRendersFullPageWithoutHTMX(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "acme.okta.com",
			Token:  "token-1",
		})

		oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		insertCommandSearchDiscoveryApp(
			t,
			ctx,
			pool,
			q,
			oktaRunID,
			configstore.KindOkta,
			"acme.okta.com",
			"managed-gh-app",
			"Managed GitHub App",
			"github.com",
			"GitHub",
			"managed-gh-app",
		)

		c, rec := newTestContext(http.MethodGet, "http://example.com/discovery/apps")

		if err := h.HandleDiscoveryApps(c); err != nil {
			t.Fatalf("HandleDiscoveryApps(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "<!doctype html>")
		assertContains(t, body, `id="discovery-apps-results"`)
	})
}

func TestGetSaaSAppByIDComputesLivePostureScenarios(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org: "acme-unconfigured",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindAWSIdentityCenter, true, configstore.AWSIdentityCenterConfig{
			Region: "eu-west-1",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindDatadog, false, configstore.DatadogConfig{
			Site:   "datadoghq.com",
			APIKey: "api-key",
			AppKey: "app-key",
		})

		entraRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		githubUnconfiguredRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme-unconfigured")
		awsStaleRunID := insertCommandSearchSyncRun(t, ctx, pool, "aws", "eu-west-1")
		datadogRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindDatadog, "datadoghq.com")
		setSyncRunFinishedAt(t, ctx, pool, awsStaleRunID, time.Now().UTC().Add(-2*time.Hour))

		freshLowID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, entraRunID, configstore.KindEntra, "tenant-1", "fresh-low", "Fresh Low", "fresh.example.com", "Example", "fresh-low")
		upsertDiscoveryPrimaryBinding(t, ctx, q, freshLowID, configstore.KindEntra, "tenant-1")

		freshMediumID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, entraRunID, configstore.KindEntra, "tenant-1", "fresh-medium", "Fresh Medium", "medium.example.com", "Example", "fresh-medium")
		upsertDiscoveryPrimaryBinding(t, ctx, q, freshMediumID, configstore.KindEntra, "tenant-1")
		insertDiscoveryOAuthEvents(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "fresh-medium", "fresh-medium", "Fresh Medium", "medium.example.com", 50, true)

		noBindingCriticalID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, entraRunID, configstore.KindEntra, "tenant-1", "orphan-critical", "Orphan Critical", "critical.example.com", "Example", "orphan-critical")
		insertDiscoveryOAuthEvents(t, ctx, q, entraRunID, configstore.KindEntra, "tenant-1", "orphan-critical", "orphan-critical", "Orphan Critical", "critical.example.com", 200, true)

		staleID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, entraRunID, configstore.KindEntra, "tenant-1", "stale-app", "Stale App", "stale.example.com", "Example", "stale-app")
		upsertDiscoveryPrimaryBinding(t, ctx, q, staleID, configstore.KindAWSIdentityCenter, "eu-west-1")

		notConfiguredID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, githubUnconfiguredRunID, configstore.KindGitHub, "acme-unconfigured", "not-configured", "Not Configured", "nocfg.example.com", "Example", "not-configured")
		upsertDiscoveryPrimaryBinding(t, ctx, q, notConfiguredID, configstore.KindGitHub, "acme-unconfigured")

		disabledID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, datadogRunID, configstore.KindDatadog, "datadoghq.com", "disabled-app", "Disabled App", "disabled.example.com", "Example", "disabled-app")
		upsertDiscoveryPrimaryBinding(t, ctx, q, disabledID, configstore.KindDatadog, "datadoghq.com")

		freshLow := getDiscoveryAppByIDForTest(t, ctx, q, h, freshLowID)
		if freshLow.ManagedState != "managed" || freshLow.ManagedReason != "active_binding_fresh_sync" || freshLow.RiskScore != 15 || freshLow.RiskLevel != "low" {
			t.Fatalf("fresh low posture = (%q, %q, %d, %q)", freshLow.ManagedState, freshLow.ManagedReason, freshLow.RiskScore, freshLow.RiskLevel)
		}

		freshMedium := getDiscoveryAppByIDForTest(t, ctx, q, h, freshMediumID)
		if freshMedium.ManagedState != "managed" || freshMedium.RiskScore != 45 || freshMedium.RiskLevel != "medium" {
			t.Fatalf("fresh medium posture = (%q, %d, %q)", freshMedium.ManagedState, freshMedium.RiskScore, freshMedium.RiskLevel)
		}

		noBindingCritical := getDiscoveryAppByIDForTest(t, ctx, q, h, noBindingCriticalID)
		if noBindingCritical.ManagedState != "unmanaged" || noBindingCritical.ManagedReason != "no_binding" {
			t.Fatalf("no-binding posture = (%q, %q)", noBindingCritical.ManagedState, noBindingCritical.ManagedReason)
		}
		if noBindingCritical.RiskScore != 100 || noBindingCritical.RiskLevel != "critical" {
			t.Fatalf("no-binding risk = (%d, %q)", noBindingCritical.RiskScore, noBindingCritical.RiskLevel)
		}
		if noBindingCritical.SuggestedBusinessCriticality != "critical" || noBindingCritical.SuggestedDataClassification != "restricted" {
			t.Fatalf("no-binding suggestions = (%q, %q)", noBindingCritical.SuggestedBusinessCriticality, noBindingCritical.SuggestedDataClassification)
		}

		stale := getDiscoveryAppByIDForTest(t, ctx, q, h, staleID)
		if stale.ManagedState != "unmanaged" || stale.ManagedReason != "stale_sync" || stale.RiskScore != 60 || stale.RiskLevel != "high" {
			t.Fatalf("stale posture = (%q, %q, %d, %q)", stale.ManagedState, stale.ManagedReason, stale.RiskScore, stale.RiskLevel)
		}

		notConfigured := getDiscoveryAppByIDForTest(t, ctx, q, h, notConfiguredID)
		if notConfigured.ManagedState != "unmanaged" || notConfigured.ManagedReason != "connector_not_configured" || notConfigured.RiskScore != 60 || notConfigured.RiskLevel != "high" {
			t.Fatalf("not-configured posture = (%q, %q, %d, %q)", notConfigured.ManagedState, notConfigured.ManagedReason, notConfigured.RiskScore, notConfigured.RiskLevel)
		}

		disabled := getDiscoveryAppByIDForTest(t, ctx, q, h, disabledID)
		if disabled.ManagedState != "unmanaged" || disabled.ManagedReason != "connector_disabled" || disabled.RiskScore != 60 || disabled.RiskLevel != "high" {
			t.Fatalf("disabled posture = (%q, %q, %d, %q)", disabled.ManagedState, disabled.ManagedReason, disabled.RiskScore, disabled.RiskLevel)
		}
	})
}

func TestHandleDiscoveryAppGovernanceUpdatePersistsDecisionHistory(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "github-token",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		currentID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindGitHub, "acme", "shadow-app", "Shadow App", "shadow.example.com", "Example", "shadow-app")
		replacementID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindGitHub, "acme", "approved-app", "Approved App", "approved.example.com", "Example", "approved-app")
		upsertDiscoveryPrimaryBinding(t, ctx, q, replacementID, configstore.KindGitHub, "acme")

		adminUserID := insertDiscoveryAuthUser(t, ctx, pool, "admin@example.com", "admin")
		insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Owner User")
		insertCommandSearchIdentity(t, ctx, pool, "human", "reviewer@example.com", "Reviewer User")

		form := url.Values{
			"owner_email":             []string{"owner@example.com"},
			"review_owner_email":      []string{"reviewer@example.com"},
			"review_disposition":      []string{"replace"},
			"follow_up_due_date":      []string{"2026-04-20"},
			"ticket_ref":              []string{"SEC-123"},
			"notes":                   []string{"Needs migration"},
			"replacement_saas_app_id": []string{strconv.FormatInt(replacementID, 10)},
		}
		c, rec := newFormTestContext(http.MethodPost, "http://example.com/discovery/apps/"+strconv.FormatInt(currentID, 10)+"/governance", form)
		(*c).SetPath("/discovery/apps/:id/governance")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(currentID, 10)}})
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminUserID, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleDiscoveryAppGovernanceUpdate(c); err != nil {
			t.Fatalf("HandleDiscoveryAppGovernanceUpdate(): %v", err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusSeeOther, rec.Body.String())
		}

		row := getDiscoveryAppByIDForTest(t, ctx, q, h, currentID)
		if row.ReviewDisposition != "replace" {
			t.Fatalf("review disposition = %q, want replace", row.ReviewDisposition)
		}
		if row.OwnerPrimaryEmail != "owner@example.com" {
			t.Fatalf("owner email = %q, want owner@example.com", row.OwnerPrimaryEmail)
		}
		if row.ReviewOwnerPrimaryEmail != "reviewer@example.com" {
			t.Fatalf("review owner email = %q, want reviewer@example.com", row.ReviewOwnerPrimaryEmail)
		}
		if !row.FollowUpDueDate.Valid || row.FollowUpDueDate.Time.UTC().Format("2006-01-02") != "2026-04-20" {
			t.Fatalf("follow-up date = %+v, want 2026-04-20", row.FollowUpDueDate)
		}
		if row.TicketRef != "SEC-123" || row.Notes != "Needs migration" {
			t.Fatalf("ticket/notes = %q/%q, want SEC-123/Needs migration", row.TicketRef, row.Notes)
		}
		if row.ReplacementSaasAppID != replacementID || row.ReplacementDisplayName != "Approved App" {
			t.Fatalf("replacement = %d/%q, want %d/Approved App", row.ReplacementSaasAppID, row.ReplacementDisplayName, replacementID)
		}

		history, err := q.ListSaaSAppReviewDecisionsBySaaSAppID(ctx, gen.ListSaaSAppReviewDecisionsBySaaSAppIDParams{
			SaasAppID: currentID,
			LimitRows: 10,
		})
		if err != nil {
			t.Fatalf("ListSaaSAppReviewDecisionsBySaaSAppID(): %v", err)
		}
		if len(history) != 1 {
			t.Fatalf("history len = %d, want 1", len(history))
		}
		if history[0].ChangedByAuthUserEmail != "admin@example.com" || history[0].ReviewDisposition != "replace" {
			t.Fatalf("history row = %+v", history[0])
		}
	})
}

func TestHandleDiscoveryAppGovernanceUpdateValidation(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "github-token",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindDatadog, true, configstore.DatadogConfig{
			Site:   "datadoghq.com",
			APIKey: "api-key",
			AppKey: "app-key",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		datadogRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindDatadog, "datadoghq.com")
		currentID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindGitHub, "acme", "shadow-app", "Shadow App", "shadow.example.com", "Example", "shadow-app")
		unmanagedReplacementID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindGitHub, "acme", "legacy-app", "Legacy App", "legacy.example.com", "Example", "legacy-app")
		hiddenManagedReplacementID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, datadogRunID, configstore.KindDatadog, "datadoghq.com", "hidden-app", "Hidden App", "hidden.example.com", "Example", "hidden-app")
		upsertDiscoveryPrimaryBinding(t, ctx, q, hiddenManagedReplacementID, configstore.KindDatadog, "datadoghq.com")
		if _, err := pool.Exec(ctx, `
			UPDATE connector_source_state
			SET discovery_enabled = false,
			    updated_at = now()
			WHERE source_kind = $1
			  AND source_name = $2
		`, configstore.KindDatadog, "datadoghq.com"); err != nil {
			t.Fatalf("disable datadog discovery source: %v", err)
		}

		adminUserID := insertDiscoveryAuthUser(t, ctx, pool, "admin@example.com", "admin")
		insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Owner User")

		tests := []struct {
			name     string
			form     url.Values
			want     string
			wantBody []string
		}{
			{
				name: "review owner not found",
				form: url.Values{
					"owner_email":        []string{"owner@example.com"},
					"review_disposition": []string{"under_review"},
					"review_owner_email": []string{"missing@example.com"},
					"follow_up_due_date": []string{"2026-04-22"},
					"ticket_ref":         []string{"SEC-404"},
					"notes":              []string{"Keep this note"},
				},
				want: "Review owner not found",
				wantBody: []string{
					`value="owner@example.com"`,
					`value="missing@example.com"`,
					`value="2026-04-22"`,
					`value="SEC-404"`,
					"Keep this note",
				},
			},
			{
				name: "owner required",
				form: url.Values{
					"review_disposition": []string{"sanctioned"},
				},
				want: "Owner required",
			},
			{
				name: "past due date rejected",
				form: url.Values{
					"owner_email":        []string{"owner@example.com"},
					"review_disposition": []string{"under_review"},
					"follow_up_due_date": []string{"2024-01-01"},
				},
				want: "Invalid due date",
				wantBody: []string{
					"Follow-up date must be today or in the future.",
					`value="2024-01-01"`,
				},
			},
			{
				name: "replacement required",
				form: url.Values{
					"owner_email":        []string{"owner@example.com"},
					"review_disposition": []string{"replace"},
				},
				want: "Replacement required",
			},
			{
				name: "replacement not found when unmanaged",
				form: url.Values{
					"owner_email":             []string{"owner@example.com"},
					"review_disposition":      []string{"replace"},
					"replacement_saas_app_id": []string{strconv.FormatInt(unmanagedReplacementID, 10)},
				},
				want: "Replacement not found",
			},
			{
				name: "replacement not found when outside discovery scope",
				form: url.Values{
					"owner_email":             []string{"owner@example.com"},
					"review_disposition":      []string{"replace"},
					"replacement_saas_app_id": []string{strconv.FormatInt(hiddenManagedReplacementID, 10)},
				},
				want: "Replacement not found",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				c, rec := newFormTestContext(http.MethodPost, "http://example.com/discovery/apps/"+strconv.FormatInt(currentID, 10)+"/governance", tc.form)
				(*c).SetPath("/discovery/apps/:id/governance")
				(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(currentID, 10)}})
				(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminUserID, Email: "admin@example.com", Role: "admin"})

				if err := h.HandleDiscoveryAppGovernanceUpdate(c); err != nil {
					t.Fatalf("HandleDiscoveryAppGovernanceUpdate(): %v", err)
				}
				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
				}
				body := rec.Body.String()
				assertContains(t, body, tc.want)
				assertContains(t, body, `id="discovery-governance-dialog"`)
				assertContains(t, body, `data-open`)
				for _, want := range tc.wantBody {
					assertContains(t, body, want)
				}
			})
		}
	})
}

func TestHandleDiscoveryAppGovernanceUpdateValidationPreservesClearedOwners(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		appID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindGitHub, "acme", "shadow-app", "Shadow App", "shadow.example.com", "Example", "shadow-app")

		adminUserID := insertDiscoveryAuthUser(t, ctx, pool, "admin@example.com", "admin")
		ownerID := insertCommandSearchIdentity(t, ctx, pool, "human", "owner@example.com", "Owner User")
		reviewOwnerID := insertCommandSearchIdentity(t, ctx, pool, "human", "reviewer@example.com", "Reviewer User")

		if _, err := q.UpsertSaaSAppReviewGovernance(ctx, gen.UpsertSaaSAppReviewGovernanceParams{
			SaasAppID:             appID,
			OwnerIdentityID:       pgtype.Int8{Int64: ownerID, Valid: true},
			ReviewOwnerIdentityID: pgtype.Int8{Int64: reviewOwnerID, Valid: true},
			ReviewDisposition:     "under_review",
			UpdatedByAuthUserID:   pgtype.Int8{Int64: adminUserID, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertSaaSAppReviewGovernance(): %v", err)
		}

		form := url.Values{
			"owner_email":        []string{""},
			"review_owner_email": []string{""},
			"review_disposition": []string{"under_review"},
			"follow_up_due_date": []string{"not-a-date"},
		}
		c, rec := newFormTestContext(http.MethodPost, "http://example.com/discovery/apps/"+strconv.FormatInt(appID, 10)+"/governance", form)
		(*c).SetPath("/discovery/apps/:id/governance")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(appID, 10)}})
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminUserID, Email: "admin@example.com", Role: "admin"})

		if err := h.HandleDiscoveryAppGovernanceUpdate(c); err != nil {
			t.Fatalf("HandleDiscoveryAppGovernanceUpdate(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Invalid due date")
		assertContains(t, body, `id="discovery-governance-dialog"`)
		assertContains(t, body, `data-open`)
		assertContains(t, body, `name="owner_email" value=""`)
		assertContains(t, body, `name="review_owner_email" value=""`)
		assertNotContains(t, body, `name="owner_email" value="owner@example.com"`)
		assertNotContains(t, body, `name="review_owner_email" value="reviewer@example.com"`)
	})
}

func TestHandleDiscoveryAppGovernanceUpdateRequiresPrincipal(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		appID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindEntra, "tenant-1", "shadow-app", "Shadow App", "shadow.example.com", "Example", "shadow-app")

		form := url.Values{
			"review_disposition": []string{"under_review"},
		}
		c, rec := newFormTestContext(http.MethodPost, "http://example.com/discovery/apps/"+strconv.FormatInt(appID, 10)+"/governance", form)
		(*c).SetPath("/discovery/apps/:id/governance")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(appID, 10)}})

		if err := h.HandleDiscoveryAppGovernanceUpdate(c); err != nil {
			t.Fatalf("HandleDiscoveryAppGovernanceUpdate(): %v", err)
		}
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
		}
	})
}

func TestHandleDiscoveryAppShowRendersGovernanceForAdminAndViewer(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		appID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindEntra, "tenant-1", "shadow-app", "Shadow App", "shadow.example.com", "Example", "shadow-app")

		adminUserID := insertDiscoveryAuthUser(t, ctx, pool, "admin@example.com", "admin")
		if _, err := q.UpsertSaaSAppReviewGovernance(ctx, gen.UpsertSaaSAppReviewGovernanceParams{
			SaasAppID:           appID,
			TicketRef:           "SEC-999",
			Notes:               "Escalated to app owner",
			ReviewDisposition:   "under_review",
			UpdatedByAuthUserID: pgtype.Int8{Int64: adminUserID, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertSaaSAppReviewGovernance(): %v", err)
		}
		if err := q.InsertSaaSAppReviewDecision(ctx, gen.InsertSaaSAppReviewDecisionParams{
			SaasAppID:           appID,
			ReviewDisposition:   "under_review",
			TicketRef:           "SEC-999",
			Notes:               "Escalated to app owner",
			ChangedByAuthUserID: pgtype.Int8{Int64: adminUserID, Valid: true},
		}); err != nil {
			t.Fatalf("InsertSaaSAppReviewDecision(): %v", err)
		}

		viewerBody := renderDiscoveryAppShow(t, h, appID)
		assertContains(t, viewerBody, "Decision History")
		assertContains(t, viewerBody, "Governance changes require an admin account.")
		assertContains(t, viewerBody, `data-dialog-open="#discovery-governance-dialog"`)
		assertContains(t, viewerBody, `id="discovery-governance-dialog"`)

		adminBody := renderDiscoveryAppShowAsPrincipal(t, h, appID, auth.Principal{UserID: adminUserID, Email: "admin@example.com", Role: "admin"})
		assertContains(t, adminBody, "/discovery/apps/"+strconv.FormatInt(appID, 10)+"/governance")
		assertContains(t, adminBody, "Decision History")
		assertContains(t, adminBody, "SEC-999")
		assertContains(t, adminBody, `data-dialog-open="#discovery-governance-dialog"`)
	})
}

func TestHandleDiscoveryAppShowDoesNotMarkHistoricalFollowUpAsOverdue(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		appID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindEntra, "tenant-1", "shadow-app", "Shadow App", "shadow.example.com", "Example", "shadow-app")

		adminUserID := insertDiscoveryAuthUser(t, ctx, pool, "admin@example.com", "admin")
		if err := q.InsertSaaSAppReviewDecision(ctx, gen.InsertSaaSAppReviewDecisionParams{
			SaasAppID:           appID,
			ReviewDisposition:   "sanctioned",
			FollowUpDueDate:     pgtype.Date{Time: time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC), Valid: true},
			ChangedByAuthUserID: pgtype.Int8{Int64: adminUserID, Valid: true},
		}); err != nil {
			t.Fatalf("InsertSaaSAppReviewDecision(): %v", err)
		}

		body := renderDiscoveryAppShow(t, h, appID)
		assertContains(t, body, "Jan 2, 2020")
		assertNotContains(t, body, "Overdue")
	})
}

func TestHandleDiscoveryReplacementCandidatesOnlyReturnsManagedAlternatives(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "github-token",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		currentID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindGitHub, "acme", "current-app", "Current App", "current.example.com", "Example", "current-app")
		managedID := insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindGitHub, "acme", "approved-app", "Approved App", "approved.example.com", "Example", "approved-app")
		_ = insertCommandSearchDiscoveryApp(t, ctx, pool, q, runID, configstore.KindGitHub, "acme", "legacy-app", "Legacy App", "legacy.example.com", "Example", "legacy-app")
		upsertDiscoveryPrimaryBinding(t, ctx, q, managedID, configstore.KindGitHub, "acme")

		target := "http://example.com/discovery/apps/replacement-candidates?q=app&exclude_id=" + strconv.FormatInt(currentID, 10)
		c, rec := newTestContext(http.MethodGet, target)
		(*c).Set(authn.ContextKeyPrincipal, auth.Principal{UserID: 1, Email: "viewer@example.com", Role: "viewer"})

		if err := h.HandleDiscoveryReplacementCandidates(c); err != nil {
			t.Fatalf("HandleDiscoveryReplacementCandidates(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := rec.Body.String()
		assertContains(t, body, "Approved App")
		assertNotContains(t, body, "Current App")
		assertNotContains(t, body, "Legacy App")
	})
}

func renderDiscoveryAppShow(t *testing.T, h *Handlers, appID int64) string {
	t.Helper()

	return renderDiscoveryAppShowAsPrincipal(t, h, appID, auth.Principal{})
}

func renderDiscoveryAppShowAsPrincipal(t *testing.T, h *Handlers, appID int64, principal auth.Principal) string {
	t.Helper()

	target := "http://example.com/discovery/apps/" + strconv.FormatInt(appID, 10)
	c, rec := newTestContext(http.MethodGet, target)
	(*c).SetPath("/discovery/apps/:id")
	(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(appID, 10)}})
	if principal.UserID > 0 {
		(*c).Set(authn.ContextKeyPrincipal, principal)
	}

	if err := h.HandleDiscoveryAppShow(c); err != nil {
		t.Fatalf("HandleDiscoveryAppShow(%d): %v", appID, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderDiscoveryApps(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleDiscoveryApps(c); err != nil {
		t.Fatalf("HandleDiscoveryApps(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func newFormTestContext(method, target string, form url.Values) (*echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest(method, target, strings.NewReader(form.Encode()))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	return c, rec
}

func upsertDiscoveryPrimaryBinding(t *testing.T, ctx context.Context, q *gen.Queries, appID int64, connectorKind, sourceName string) {
	t.Helper()

	if err := q.UpsertSaaSAppBinding(ctx, gen.UpsertSaaSAppBindingParams{
		SaasAppID:           appID,
		ConnectorKind:       connectorKind,
		ConnectorSourceName: sourceName,
		BindingSource:       "manual",
		Confidence:          1,
		IsPrimary:           true,
		CreatedByAuthUserID: pgtype.Int8{},
	}); err != nil {
		t.Fatalf("UpsertSaaSAppBinding %s/%s: %v", connectorKind, sourceName, err)
	}
}

func setSyncRunFinishedAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, finishedAt time.Time) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		UPDATE sync_runs
		SET started_at = $2 - interval '5 minutes',
		    finished_at = $2
		WHERE id = $1
	`, runID, finishedAt); err != nil {
		t.Fatalf("update sync_runs finished_at: %v", err)
	}
	refreshCommandSearchSourceState(t, ctx, pool)
}

func getDiscoveryAppByIDForTest(t *testing.T, ctx context.Context, q *gen.Queries, h *Handlers, appID int64) gen.GetSaaSAppByIDRow {
	t.Helper()
	row, err := q.GetSaaSAppByID(ctx, appID)
	if err != nil {
		t.Fatalf("GetSaaSAppByID(%d): %v", appID, err)
	}
	return row
}

func insertDiscoveryAuthUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool, email, role string) int64 {
	t.Helper()

	q := gen.New(pool)
	user, err := q.CreateAuthUser(ctx, gen.CreateAuthUserParams{
		Email:        email,
		PasswordHash: "test-password-hash",
		Role:         role,
		IsActive:     true,
	})
	if err != nil {
		t.Fatalf("CreateAuthUser(%q): %v", email, err)
	}
	return user.ID
}

func insertDiscoveryOAuthEvents(t *testing.T, ctx context.Context, q *gen.Queries, runID int64, sourceKind, sourceName, canonicalKey, sourceAppID, sourceAppName, sourceAppDomain string, actorCount int, privileged bool) {
	t.Helper()

	signalKinds := make([]string, 0, actorCount)
	eventExternalIDs := make([]string, 0, actorCount)
	sourceAppIDs := make([]string, 0, actorCount)
	sourceAppNames := make([]string, 0, actorCount)
	sourceAppDomains := make([]string, 0, actorCount)
	actorExternalIDs := make([]string, 0, actorCount)
	actorEmails := make([]string, 0, actorCount)
	actorDisplayNames := make([]string, 0, actorCount)
	observedAts := make([]pgtype.Timestamptz, 0, actorCount)
	scopesJSONs := make([][]byte, 0, actorCount)
	rawJSONs := make([][]byte, 0, actorCount)

	scopesJSON := `["mail.read"]`
	if privileged {
		scopesJSON = `["directory.readwrite.all","mail.read"]`
	}

	now := time.Now().UTC()
	for i := 0; i < actorCount; i++ {
		suffix := strconv.Itoa(i)
		signalKinds = append(signalKinds, "oauth_grant")
		eventExternalIDs = append(eventExternalIDs, sourceAppID+"-event-"+suffix)
		sourceAppIDs = append(sourceAppIDs, sourceAppID)
		sourceAppNames = append(sourceAppNames, sourceAppName)
		sourceAppDomains = append(sourceAppDomains, sourceAppDomain)
		actorExternalIDs = append(actorExternalIDs, "actor-"+suffix)
		actorEmails = append(actorEmails, "actor-"+suffix+"@example.com")
		actorDisplayNames = append(actorDisplayNames, "Actor "+suffix)
		observedAts = append(observedAts, pgtype.Timestamptz{Time: now, Valid: true})
		scopesJSONs = append(scopesJSONs, []byte(scopesJSON))
		rawJSONs = append(rawJSONs, []byte(`{}`))
	}

	if _, err := q.UpsertSaaSAppEventsBulkBySource(ctx, gen.UpsertSaaSAppEventsBulkBySourceParams{
		SeenInRunID:       runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		CanonicalKeys:     repeatStringForTest(canonicalKey, actorCount),
		SignalKinds:       signalKinds,
		EventExternalIds:  eventExternalIDs,
		SourceAppIds:      sourceAppIDs,
		SourceAppNames:    sourceAppNames,
		SourceAppDomains:  sourceAppDomains,
		ActorExternalIds:  actorExternalIDs,
		ActorEmails:       actorEmails,
		ActorDisplayNames: actorDisplayNames,
		ObservedAts:       observedAts,
		ScopesJsons:       scopesJSONs,
		RawJsons:          rawJSONs,
	}); err != nil {
		t.Fatalf("UpsertSaaSAppEventsBulkBySource %s: %v", canonicalKey, err)
	}
	if _, err := q.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	}); err != nil {
		t.Fatalf("PromoteSaaSAppEventsSeenInRunBySource %s: %v", canonicalKey, err)
	}
	refreshCommandSearchSourceReadModels(t, ctx, q, sourceKind, sourceName)
}

func repeatStringForTest(value string, count int) []string {
	items := make([]string, count)
	for i := range items {
		items[i] = value
	}
	return items
}

func assertContains(t *testing.T, body, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Fatalf("body missing %q: %s", want, body)
	}
}

func assertNotContains(t *testing.T, body, want string) {
	t.Helper()
	if strings.Contains(body, want) {
		t.Fatalf("body unexpectedly contained %q: %s", want, body)
	}
}
