package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
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
		assertNotContains(t, body, ">Columns<")

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
		assertContains(t, body, "Primary binding has fresh sync")
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

func renderDiscoveryAppShow(t *testing.T, h *Handlers, appID int64) string {
	t.Helper()

	target := "http://example.com/discovery/apps/" + strconv.FormatInt(appID, 10)
	c, rec := newTestContext(http.MethodGet, target)
	(*c).SetPath("/discovery/apps/:id")
	(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(appID, 10)}})

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
}

func getDiscoveryAppByIDForTest(t *testing.T, ctx context.Context, q *gen.Queries, h *Handlers, appID int64) gen.GetSaaSAppByIDRow {
	t.Helper()

	cutoffs := h.discoveryPostureCutoffs(time.Now().UTC())
	row, err := q.GetSaaSAppByID(ctx, gen.GetSaaSAppByIDParams{
		ID:                        appID,
		OktaFreshAfter:            cutoffs.OktaFreshAfter,
		EntraFreshAfter:           cutoffs.EntraFreshAfter,
		GoogleWorkspaceFreshAfter: cutoffs.GoogleWorkspaceFreshAfter,
		GithubFreshAfter:          cutoffs.GithubFreshAfter,
		DatadogFreshAfter:         cutoffs.DatadogFreshAfter,
		AwsFreshAfter:             cutoffs.AwsFreshAfter,
		DefaultFreshAfter:         cutoffs.DefaultFreshAfter,
	})
	if err != nil {
		t.Fatalf("GetSaaSAppByID(%d): %v", appID, err)
	}
	return row
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
