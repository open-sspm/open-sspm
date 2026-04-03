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
			"managed",
			"low",
			0,
			"azure-cloud",
		)
		upsertDiscoveryPrimaryBinding(t, ctx, q, appID, configstore.KindEntra, "tenant-1")

		body := renderDiscoveryAppShow(t, h, appID)
		assertContains(t, body, "Azure Cloud")
		assertContains(t, body, "Bound connector sync is stale")
		assertContains(t, body, "Score 60")

		var (
			storedManagedState string
			storedRiskLevel    string
			storedRiskScore    int32
		)
		if err := pool.QueryRow(ctx, `
			SELECT managed_state, risk_level, risk_score
			FROM saas_apps
			WHERE id = $1
		`, appID).Scan(&storedManagedState, &storedRiskLevel, &storedRiskScore); err != nil {
			t.Fatalf("select stored posture: %v", err)
		}
		if storedManagedState != "managed" {
			t.Fatalf("stored managed_state = %q, want %q", storedManagedState, "managed")
		}
		if storedRiskLevel != "low" {
			t.Fatalf("stored risk_level = %q, want %q", storedRiskLevel, "low")
		}
		if storedRiskScore != 0 {
			t.Fatalf("stored risk_score = %d, want 0", storedRiskScore)
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
				"unmanaged",
				"low",
				0,
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
				"unmanaged",
				"low",
				0,
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
				"unmanaged",
				"low",
				0,
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
				"unmanaged",
				"low",
				0,
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
			"managed",
			"low",
			0,
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
			"unmanaged",
			"low",
			0,
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
			"managed",
			"low",
			0,
			"unmanaged-no-binding",
		)

		body := renderDiscoveryApps(t, h, "http://example.com/discovery/apps?managed_state=managed")
		assertContains(t, body, "Managed GitHub App")
		assertNotContains(t, body, "Unmanaged No Binding")
		assertContains(t, body, "Primary binding has fresh sync")
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
