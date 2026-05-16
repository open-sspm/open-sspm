package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleIdentitiesClampsOutOfRangePage(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID: "tenant-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindEntra,
			SourceName:     "tenant-1",
			ExternalID:     "user-1",
			Email:          "person@example.com",
			DisplayName:    "Example Person",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		identityID := insertCommandSearchIdentity(t, ctx, pool, "human", "person@example.com", "Example Person")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, identityID, accountID)

		tests := []struct {
			name string
			page string
		}{
			{name: "valid offset beyond last page", page: "2"},
			{name: "overflowing offset", page: "200000000"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c, rec := newTestContext(http.MethodGet, "http://example.com/identities?page="+tt.page)
				if err := h.HandleIdentities(c); err != nil {
					t.Fatalf("HandleIdentities() error = %v", err)
				}

				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
				}

				body := rec.Body.String()
				if !strings.Contains(body, "Example Person") {
					t.Fatalf("body missing identity row: %s", body)
				}
				if !strings.Contains(body, "Showing 1-1 of 1") {
					t.Fatalf("body missing showing summary: %s", body)
				}
				if strings.Contains(body, "No identities found") {
					t.Fatalf("body unexpectedly rendered empty state: %s", body)
				}
			})
		}
	})
}

func TestHandleIdentityShowRendersEntitlementDetails(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		identityID := insertCommandSearchIdentity(t, ctx, pool, "human", "person@example.com", "Example Person")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "github-user-1",
			Email:          "person@example.com",
			DisplayName:    "Example Person",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		insertCommandSearchIdentityAccountLink(t, ctx, pool, identityID, accountID)
		insertDashboardEntitlement(t, ctx, pool, runID, accountID, "github_team_repo_permission", "github_repo:acme/private-repo", "admin", `{}`)

		body := renderIdentityShow(t, h, identityID)
		for _, want := range []string{
			"Access grants",
			"github_team_repo_permission",
			"acme/private-repo",
			"admin",
			"person@example.com",
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("identity show missing %q: %s", want, body)
			}
		}
	})
}

func TestHandleIdentitiesPinsToHumanKindOnly(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID: "tenant-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindEntra, "tenant-1")

		humanAccountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindEntra,
			SourceName:     "tenant-1",
			ExternalID:     "user-1",
			Email:          "person@example.com",
			DisplayName:    "Example Person",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		humanIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "person@example.com", "Example Person")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, humanIdentityID, humanAccountID)

		serviceAccountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
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

		unknownAccountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindEntra,
			SourceName:     "tenant-1",
			ExternalID:     "user-unknown",
			Email:          "mystery@example.com",
			DisplayName:    "Mystery Identity",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		unknownIdentityID := insertCommandSearchIdentity(t, ctx, pool, "unknown", "mystery@example.com", "Mystery Identity")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, unknownIdentityID, unknownAccountID)

		t.Run("default request hides service kinds but keeps unknown", func(t *testing.T) {
			c, rec := newTestContext(http.MethodGet, "http://example.com/identities")
			if err := h.HandleIdentities(c); err != nil {
				t.Fatalf("HandleIdentities() error = %v", err)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}

			body := rec.Body.String()
			if !strings.Contains(body, "Example Person") {
				t.Fatalf("body missing human row: %s", body)
			}
			if !strings.Contains(body, "Mystery Identity") {
				t.Fatalf("body missing unknown-kind row: %s", body)
			}
			if strings.Contains(body, "Azure Service Principal") {
				t.Fatalf("body unexpectedly rendered service identity row: %s", body)
			}
		})

		t.Run("identity_type=service in deeplink is ignored", func(t *testing.T) {
			c, rec := newTestContext(http.MethodGet, "http://example.com/identities?identity_type=service")
			if err := h.HandleIdentities(c); err != nil {
				t.Fatalf("HandleIdentities() error = %v", err)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}

			body := rec.Body.String()
			if !strings.Contains(body, "Example Person") {
				t.Fatalf("body missing human row: %s", body)
			}
			if strings.Contains(body, "Azure Service Principal") {
				t.Fatalf("body unexpectedly rendered service identity row: %s", body)
			}
		})
	})
}

func TestHandleIdentitiesHTMXResultsIncludesSavedQueryState(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID: "tenant-1",
		})

		c, rec := newTestContext(http.MethodGet, "http://example.com/identities?row_state=action_required")
		(*c).Request().Header.Set("HX-Request", "true")
		(*c).Request().Header.Set("HX-Target", "identities-results")
		if err := h.HandleIdentities(c); err != nil {
			t.Fatalf("HandleIdentities() error = %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, `id="identities-results"`)
		assertContains(t, body, `aria-label="Saved queries"`)
		assertContains(t, body, `aria-current="page">Needs action`)
		assertNotContains(t, body, `data-osspm-askbar`)
		assertNotContains(t, body, "<!doctype html>")
	})
}

func TestHandleIdentityShowRedirectsServiceIdentitiesToNonHumanRoute(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		serviceIdentityID := insertCommandSearchIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
		botIdentityID := insertCommandSearchIdentity(t, ctx, pool, "bot", "bot@example.com", "Automation Bot")
		unknownIdentityID := insertCommandSearchIdentity(t, ctx, pool, "unknown", "mystery@example.com", "Mystery Identity")

		tests := []struct {
			name           string
			id             int64
			wantStatus     int
			wantLocation   string
			wantRedirected bool
		}{
			{name: "service redirects", id: serviceIdentityID, wantStatus: http.StatusSeeOther, wantLocation: "/non-human-identities/identity-" + strconv.FormatInt(serviceIdentityID, 10), wantRedirected: true},
			{name: "bot redirects", id: botIdentityID, wantStatus: http.StatusSeeOther, wantLocation: "/non-human-identities/identity-" + strconv.FormatInt(botIdentityID, 10), wantRedirected: true},
			{name: "unknown does not redirect", id: unknownIdentityID, wantStatus: http.StatusOK},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				target := "http://example.com/identities/" + strconv.FormatInt(tt.id, 10)
				c, rec := newTestContext(http.MethodGet, target)
				(*c).SetPath("/identities/:id")
				(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(tt.id, 10)}})

				if err := h.HandleIdentityShow(c); err != nil {
					t.Fatalf("HandleIdentityShow(%s): %v", target, err)
				}
				if rec.Code != tt.wantStatus {
					t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
				}
				if tt.wantRedirected {
					if got := rec.Header().Get("Location"); got != tt.wantLocation {
						t.Fatalf("Location = %q, want %q", got, tt.wantLocation)
					}
				}
			})
		}
	})
}

func renderIdentityShow(t *testing.T, h *Handlers, identityID int64) string {
	t.Helper()

	target := "http://example.com/identities/" + strconv.FormatInt(identityID, 10)
	c, rec := newTestContext(http.MethodGet, target)
	(*c).SetPath("/identities/:id")
	(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(identityID, 10)}})
	if err := h.HandleIdentityShow(c); err != nil {
		t.Fatalf("HandleIdentityShow(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}
