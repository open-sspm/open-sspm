package handlers

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestBuildSourceAccountsNeedingAnchorPageUnavailable(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/needs-anchor/github/acme?q=alice")
		(*c).SetPath("/accounts/needs-anchor/github/:org")
		(*c).SetPathValues(echo.PathValues{{Name: "org", Value: "acme"}})

		result, err := h.buildSourceAccountsNeedingAnchorPage(c, githubNeedsAnchorOptions())
		if err != nil {
			t.Fatalf("buildSourceAccountsNeedingAnchorPage(): %v", err)
		}

		if result.PageData.Query.Q != "alice" {
			t.Fatalf("Query.Q = %q, want alice", result.PageData.Query.Q)
		}
		if result.PageData.EmptyStateMsg != "GitHub is not configured yet. Add credentials in Connectors." {
			t.Fatalf("EmptyStateMsg = %q", result.PageData.EmptyStateMsg)
		}
		if result.PageData.EmptyStateHref != "/settings/connectors?open=github" {
			t.Fatalf("EmptyStateHref = %q", result.PageData.EmptyStateHref)
		}
		if result.PageData.Page != 1 || result.PageData.TotalPages != 1 {
			t.Fatalf("page data = (%d, %d), want (1, 1)", result.PageData.Page, result.PageData.TotalPages)
		}
		if result.PageData.HasUsers {
			t.Fatalf("HasUsers = true, want false")
		}
	})
}

func TestBuildSourceAccountsNeedingAnchorPageEmptyStateMessages(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		t.Run("uses synced empty state when query is blank", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/needs-anchor/entra")
			result, err := h.buildSourceAccountsNeedingAnchorPage(c, entraNeedsAnchorOptions())
			if err != nil {
				t.Fatalf("buildSourceAccountsNeedingAnchorPage(): %v", err)
			}
			if result.PageData.EmptyStateMsg != "No Microsoft Entra ID users need an anchor." {
				t.Fatalf("EmptyStateMsg = %q", result.PageData.EmptyStateMsg)
			}
		})

		t.Run("uses filtered empty state when query is present", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/needs-anchor/entra?q=alice")
			result, err := h.buildSourceAccountsNeedingAnchorPage(c, entraNeedsAnchorOptions())
			if err != nil {
				t.Fatalf("buildSourceAccountsNeedingAnchorPage(): %v", err)
			}
			if result.PageData.EmptyStateMsg != "No Microsoft Entra ID users needing an anchor match the current search." {
				t.Fatalf("EmptyStateMsg = %q", result.PageData.EmptyStateMsg)
			}
		})
	})
}

func TestBuildSourceAccountsNeedingAnchorPageValidatesRouteParams(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindDatadog, true, configstore.DatadogConfig{
			Site:   "datadoghq.com",
			APIKey: "api-key",
			AppKey: "app-key",
		})

		t.Run("missing github org returns not found", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/needs-anchor/github")
			_, err := h.buildSourceAccountsNeedingAnchorPage(c, githubNeedsAnchorOptions())
			if !errors.Is(err, errSourceAccountNeedsAnchorNotFound) {
				t.Fatalf("error = %v, want errSourceAccountNeedsAnchorNotFound", err)
			}
		})

		t.Run("unknown github org returns source validation error", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/needs-anchor/github/other")
			(*c).SetPath("/accounts/needs-anchor/github/:org")
			(*c).SetPathValues(echo.PathValues{{Name: "org", Value: "other"}})

			_, err := h.buildSourceAccountsNeedingAnchorPage(c, githubNeedsAnchorOptions())
			var sourceErr sourceAccountNeedsAnchorNameError
			if !errors.As(err, &sourceErr) || sourceErr != sourceAccountNeedsAnchorNameError("unknown org") {
				t.Fatalf("error = %v, want unknown org validation error", err)
			}
		})

		t.Run("unknown datadog site returns source validation error", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/needs-anchor/datadog/us5.datadoghq.com")
			(*c).SetPath("/accounts/needs-anchor/datadog/:site")
			(*c).SetPathValues(echo.PathValues{{Name: "site", Value: "us5.datadoghq.com"}})

			_, err := h.buildSourceAccountsNeedingAnchorPage(c, datadogNeedsAnchorOptions())
			var sourceErr sourceAccountNeedsAnchorNameError
			if !errors.As(err, &sourceErr) || sourceErr != sourceAccountNeedsAnchorNameError("unknown site") {
				t.Fatalf("error = %v, want unknown site validation error", err)
			}
		})
	})
}

func TestHandleGitHubAccountsNeedingAnchorRendersAccounts(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "token-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "octocat",
			Email:          "octocat@example.com",
			DisplayName:    "Octocat",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: registry.EntityCategoryUser,
			RawJSON:        `{"login":"octocat"}`,
		})

		c, rec := newTestContext(http.MethodGet, "http://example.com/accounts/needs-anchor/github/acme")
		(*c).SetPath("/accounts/needs-anchor/github/:org")
		(*c).SetPathValues(echo.PathValues{{Name: "org", Value: "acme"}})

		if err := h.HandleGitHubAccountsNeedingAnchor(c); err != nil {
			t.Fatalf("HandleGitHubAccountsNeedingAnchor(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "octocat")
		assertContains(t, body, "Showing 1-1 of 1")
		assertNotContains(t, body, "No GitHub accounts need an anchor.")
	})
}

func TestHandleDatadogAccountsNeedingAnchorOnlyIncludesUsers(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindDatadog, true, configstore.DatadogConfig{
			Site:   "datadoghq.com",
			APIKey: "api-key",
			AppKey: "app-key",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindDatadog, "datadoghq.com")
		insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindDatadog,
			SourceName:     "datadoghq.com",
			ExternalID:     "user-1",
			Email:          "alice@example.com",
			DisplayName:    "Alice",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: registry.EntityCategoryUser,
			RawJSON:        `{}`,
		})
		insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindDatadog,
			SourceName:     "datadoghq.com",
			ExternalID:     "service_account:sa-1",
			Email:          "deploy-bot@example.com",
			DisplayName:    "Deploy Bot",
			Status:         "active",
			AccountKind:    "service",
			EntityCategory: registry.EntityCategoryServiceAccount,
			RawJSON:        `{}`,
		})
		insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindDatadog,
			SourceName:     "datadoghq.com",
			ExternalID:     "role:admin",
			DisplayName:    "Datadog Admin",
			Status:         "active",
			AccountKind:    "service",
			EntityCategory: registry.EntityCategoryRole,
			RawJSON:        `{}`,
		})

		c, rec := newTestContext(http.MethodGet, "http://example.com/accounts/needs-anchor/datadog/datadoghq.com")
		(*c).SetPath("/accounts/needs-anchor/datadog/:site")
		(*c).SetPathValues(echo.PathValues{{Name: "site", Value: "datadoghq.com"}})

		if err := h.HandleDatadogAccountsNeedingAnchor(c); err != nil {
			t.Fatalf("HandleDatadogAccountsNeedingAnchor(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Alice")
		assertContains(t, body, "Showing 1-1 of 1")
		assertNotContains(t, body, "Deploy Bot")
		assertNotContains(t, body, "Datadog Admin")
	})
}

func githubNeedsAnchorOptions() sourceAccountsNeedingAnchorOptions {
	return sourceAccountsNeedingAnchorOptions{
		Title:              "GitHub Accounts Needing Anchor",
		ConnectorName:      "GitHub",
		ConnectorKind:      "github",
		SourceKind:         "github",
		EntityCategory:     registry.EntityCategoryUser,
		EmptyStateHref:     "/settings/connectors?open=github",
		SyncedEmptyState:   "No GitHub accounts need an anchor.",
		FilteredEmptyState: "No GitHub accounts needing an anchor match the current search.",
		ResolveSourceName: func(c *echo.Context, configuredSourceName string) (string, error) {
			org := routeParamOrWildcard(c, "org")
			if org == "" {
				return "", errSourceAccountNeedsAnchorNotFound
			}
			if org != configuredSourceName {
				return "", sourceAccountNeedsAnchorNameError("unknown org")
			}
			return org, nil
		},
	}
}

func entraNeedsAnchorOptions() sourceAccountsNeedingAnchorOptions {
	return sourceAccountsNeedingAnchorOptions{
		Title:              "Microsoft Entra ID Users Needing Anchor",
		ConnectorName:      "Microsoft Entra ID",
		ConnectorKind:      "entra",
		SourceKind:         "entra",
		EntityCategory:     registry.EntityCategoryUser,
		EmptyStateHref:     "/settings/connectors?open=entra",
		SyncedEmptyState:   "No Microsoft Entra ID users need an anchor.",
		FilteredEmptyState: "No Microsoft Entra ID users needing an anchor match the current search.",
		UnavailableMessageFn: func(configured, enabled bool) string {
			if configured && !enabled {
				return "Microsoft Entra ID sync is disabled. Enable it in Connectors."
			}
			return "Microsoft Entra ID is not configured yet. Add settings in Connectors."
		},
		ResolveSourceName: func(_ *echo.Context, configuredSourceName string) (string, error) {
			return configuredSourceName, nil
		},
	}
}

func datadogNeedsAnchorOptions() sourceAccountsNeedingAnchorOptions {
	return sourceAccountsNeedingAnchorOptions{
		Title:              "Datadog Accounts Needing Anchor",
		ConnectorName:      "Datadog",
		ConnectorKind:      "datadog",
		SourceKind:         "datadog",
		EntityCategory:     registry.EntityCategoryUser,
		EmptyStateHref:     "/settings/connectors?open=datadog",
		SyncedEmptyState:   "No Datadog accounts need an anchor.",
		FilteredEmptyState: "No Datadog accounts needing an anchor match the current search.",
		ResolveSourceName: func(c *echo.Context, configuredSourceName string) (string, error) {
			site := routeParamOrWildcard(c, "site")
			if site == "" {
				return "", errSourceAccountNeedsAnchorNotFound
			}
			if site != configuredSourceName {
				return "", sourceAccountNeedsAnchorNameError("unknown site")
			}
			return site, nil
		},
	}
}
