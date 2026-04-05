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

func TestBuildUnmatchedSourceAccountsPageUnavailable(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/unlinked/github/acme?q=alice")
		(*c).SetPath("/accounts/unlinked/github/:org")
		(*c).SetPathValues(echo.PathValues{{Name: "org", Value: "acme"}})

		result, err := h.buildUnmatchedSourceAccountsPage(c, githubUnmatchedOptions())
		if err != nil {
			t.Fatalf("buildUnmatchedSourceAccountsPage(): %v", err)
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

func TestBuildUnmatchedSourceAccountsPageEmptyStateMessages(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:     "tenant-1",
			ClientID:     "client-1",
			ClientSecret: "secret-1",
		})

		t.Run("uses synced empty state when query is blank", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/unlinked/entra")
			result, err := h.buildUnmatchedSourceAccountsPage(c, entraUnmatchedOptions())
			if err != nil {
				t.Fatalf("buildUnmatchedSourceAccountsPage(): %v", err)
			}
			if result.PageData.EmptyStateMsg != "No unlinked Microsoft Entra ID users." {
				t.Fatalf("EmptyStateMsg = %q", result.PageData.EmptyStateMsg)
			}
		})

		t.Run("uses filtered empty state when query is present", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/unlinked/entra?q=alice")
			result, err := h.buildUnmatchedSourceAccountsPage(c, entraUnmatchedOptions())
			if err != nil {
				t.Fatalf("buildUnmatchedSourceAccountsPage(): %v", err)
			}
			if result.PageData.EmptyStateMsg != "No unlinked Microsoft Entra ID users match the current search." {
				t.Fatalf("EmptyStateMsg = %q", result.PageData.EmptyStateMsg)
			}
		})
	})
}

func TestBuildUnmatchedSourceAccountsPageValidatesRouteParams(t *testing.T) {
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
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/unlinked/github")
			_, err := h.buildUnmatchedSourceAccountsPage(c, githubUnmatchedOptions())
			if !errors.Is(err, errUnmatchedSourceAccountNotFound) {
				t.Fatalf("error = %v, want errUnmatchedSourceAccountNotFound", err)
			}
		})

		t.Run("unknown github org returns source validation error", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/unlinked/github/other")
			(*c).SetPath("/accounts/unlinked/github/:org")
			(*c).SetPathValues(echo.PathValues{{Name: "org", Value: "other"}})

			_, err := h.buildUnmatchedSourceAccountsPage(c, githubUnmatchedOptions())
			var sourceErr unmatchedSourceNameError
			if !errors.As(err, &sourceErr) || sourceErr != unmatchedSourceNameError("unknown org") {
				t.Fatalf("error = %v, want unknown org validation error", err)
			}
		})

		t.Run("unknown datadog site returns source validation error", func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/accounts/unlinked/datadog/us5.datadoghq.com")
			(*c).SetPath("/accounts/unlinked/datadog/:site")
			(*c).SetPathValues(echo.PathValues{{Name: "site", Value: "us5.datadoghq.com"}})

			_, err := h.buildUnmatchedSourceAccountsPage(c, datadogUnmatchedOptions())
			var sourceErr unmatchedSourceNameError
			if !errors.As(err, &sourceErr) || sourceErr != unmatchedSourceNameError("unknown site") {
				t.Fatalf("error = %v, want unknown site validation error", err)
			}
		})
	})
}

func TestHandleUnmatchedGitHubRendersUnlinkedAccounts(t *testing.T) {
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
			EntityCategory: "unknown",
			RawJSON:        `{"login":"octocat"}`,
		})

		c, rec := newTestContext(http.MethodGet, "http://example.com/accounts/unlinked/github/acme")
		(*c).SetPath("/accounts/unlinked/github/:org")
		(*c).SetPathValues(echo.PathValues{{Name: "org", Value: "acme"}})

		if err := h.HandleUnmatchedGitHub(c); err != nil {
			t.Fatalf("HandleUnmatchedGitHub(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "octocat")
		assertContains(t, body, "Showing 1-1 of 1")
		assertNotContains(t, body, "No unlinked GitHub accounts.")
	})
}

func githubUnmatchedOptions() unmatchedSourceAccountOptions {
	return unmatchedSourceAccountOptions{
		Title:              "Unlinked GitHub Accounts",
		ConnectorName:      "GitHub",
		ConnectorKind:      "github",
		SourceKind:         "github",
		EmptyStateHref:     "/settings/connectors?open=github",
		SyncedEmptyState:   "No unlinked GitHub accounts.",
		FilteredEmptyState: "No unlinked GitHub accounts match the current search.",
		ResolveSourceName: func(c *echo.Context, configuredSourceName string) (string, error) {
			org := routeParamOrWildcard(c, "org")
			if org == "" {
				return "", errUnmatchedSourceAccountNotFound
			}
			if org != configuredSourceName {
				return "", unmatchedSourceNameError("unknown org")
			}
			return org, nil
		},
	}
}

func entraUnmatchedOptions() unmatchedSourceAccountOptions {
	return unmatchedSourceAccountOptions{
		Title:              "Unlinked Microsoft Entra ID Users",
		ConnectorName:      "Microsoft Entra ID",
		ConnectorKind:      "entra",
		SourceKind:         "entra",
		EntityCategory:     registry.EntityCategoryUser,
		EmptyStateHref:     "/settings/connectors?open=entra",
		SyncedEmptyState:   "No unlinked Microsoft Entra ID users.",
		FilteredEmptyState: "No unlinked Microsoft Entra ID users match the current search.",
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

func datadogUnmatchedOptions() unmatchedSourceAccountOptions {
	return unmatchedSourceAccountOptions{
		Title:              "Unlinked Datadog Accounts",
		ConnectorName:      "Datadog",
		ConnectorKind:      "datadog",
		SourceKind:         "datadog",
		EmptyStateHref:     "/settings/connectors?open=datadog",
		SyncedEmptyState:   "No unlinked Datadog accounts.",
		FilteredEmptyState: "No unlinked Datadog accounts match the current search.",
		ResolveSourceName: func(c *echo.Context, configuredSourceName string) (string, error) {
			site := routeParamOrWildcard(c, "site")
			if site == "" {
				return "", errUnmatchedSourceAccountNotFound
			}
			if site != configuredSourceName {
				return "", unmatchedSourceNameError("unknown site")
			}
			return site, nil
		},
	}
}
