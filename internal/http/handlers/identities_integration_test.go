package handlers

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestRenderIdentityShowSectionErrorReturnsRetryableHTML(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/identities/42")
	(*c).Set(ContextKeyRequestID, "request-123")
	h := &Handlers{}

	if err := h.renderIdentityShowSectionError(
		c,
		errors.New("database unavailable"),
		"identity-entitlements-section",
		"Access grants",
		"/identities/42",
	); err != nil {
		t.Fatalf("renderIdentityShowSectionError(): %v", err)
	}

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if contentType := rec.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/html") {
		t.Fatalf("Content-Type = %q, want HTML", contentType)
	}
	for _, want := range []string{`data-hx-lazy-error`, `data-hx-lazy-retry`, "request-123"} {
		assertContains(t, rec.Body.String(), want)
	}
	assertNotContains(t, rec.Body.String(), "database unavailable")
}

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
			RawJSON:        `{"status":"active","manager":"Jane Manager","department":"Engineering","title":"Admin","mfa":true,"location":"Paris"}`,
		})
		insertCommandSearchIdentityAccountLink(t, ctx, pool, identityID, accountID)
		insertDashboardEntitlement(t, ctx, pool, runID, accountID, "github_team_repo_permission", "github_repo:acme/private-repo", "admin", `{}`)

		body := renderIdentityShow(t, h, identityID)
		for _, want := range []string{
			"Access grants",
			`id="identity-entitlements-section"`,
			"person@example.com",
			`href="/non-human-identities?q=person%40example.com"`,
			"Search non-human identities",
			"Active",
			"Privileged access",
			"Jane Manager",
			"Engineering · Admin",
			"Enabled",
			"Paris",
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("identity show missing %q: %s", want, body)
			}
		}
		assertContains(t, body, "github_team_repo_permission")
		assertNotContains(t, body, `hx-trigger="intersect once, oss-panel-visible"`)

		boosted := renderIdentityShowBoosted(t, h, identityID)
		assertContains(t, boosted, `hx-trigger="intersect once, oss-panel-visible"`)
		assertContains(t, boosted, `hx-request=`)
		assertContains(t, boosted, `timeout`)
		assertContains(t, boosted, `data-hx-lazy-error-template`)
		assertNotContains(t, boosted, "github_team_repo_permission")

		entitlements := renderIdentityShowFragment(t, h, identityID, "identity-entitlements-section")
		assertContains(t, entitlements, `id="identity-entitlements-section"`)
		assertContains(t, entitlements, "github_team_repo_permission")
		assertContains(t, entitlements, "acme/private-repo")
		assertContains(t, entitlements, "admin")
		assertNotContains(t, entitlements, "<!doctype html>")

		linkedAccounts := renderIdentityShowFragment(t, h, identityID, "identity-linked-accounts-section")
		assertContains(t, linkedAccounts, `id="identity-linked-accounts-section"`)
		assertContains(t, linkedAccounts, "github-user-1")
		assertContains(t, linkedAccounts, "1</td>")
		assertNotContains(t, linkedAccounts, "<!doctype html>")
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

func TestHandleIdentityShowRedirectsMergedIdentities(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		sourceIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "old@example.com", "Old Identity")
		targetIdentityID := insertCommandSearchIdentity(t, ctx, pool, "human", "new@example.com", "New Identity")
		mergeEvent, err := q.CreateIdentityMergeEvent(ctx, gen.CreateIdentityMergeEventParams{
			SourceIdentityID: sourceIdentityID,
			TargetIdentityID: targetIdentityID,
			Status:           "pending",
			Reason:           "manual_review",
		})
		if err != nil {
			t.Fatalf("CreateIdentityMergeEvent(): %v", err)
		}
		if _, err := q.UpsertIdentityMergeRedirect(ctx, gen.UpsertIdentityMergeRedirectParams{
			SourceIdentityID: sourceIdentityID,
			TargetIdentityID: targetIdentityID,
			MergeEventID:     mergeEvent.ID,
		}); err != nil {
			t.Fatalf("UpsertIdentityMergeRedirect(): %v", err)
		}

		target := "http://example.com/identities/" + strconv.FormatInt(sourceIdentityID, 10)
		c, rec := newTestContext(http.MethodGet, target)
		(*c).SetPath("/identities/:id")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(sourceIdentityID, 10)}})

		if err := h.HandleIdentityShow(c); err != nil {
			t.Fatalf("HandleIdentityShow(%s): %v", target, err)
		}
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusSeeOther)
		}
		if got, want := rec.Header().Get("Location"), "/identities/"+strconv.FormatInt(targetIdentityID, 10); got != want {
			t.Fatalf("Location = %q, want %q", got, want)
		}
	})
}

func TestHandleIdentityShowRendersIdentityGraphFacts(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme")
		accountID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindOkta,
			SourceName:     "acme",
			ExternalID:     "00u123",
			Email:          "person@example.com",
			DisplayName:    "Person",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"id":"00u123"}`,
		})
		identityID := insertCommandSearchIdentity(t, ctx, pool, "human", "person@example.com", "Person")
		insertCommandSearchIdentityAccountLink(t, ctx, pool, identityID, accountID)
		if _, err := q.UpsertIdentityEmail(ctx, gen.UpsertIdentityEmailParams{
			IdentityID:        identityID,
			Email:             "alias@example.com",
			NormalizedEmail:   "alias@example.com",
			EmailKind:         "alias",
			VerificationState: "manual",
			LifecycleState:    "active",
			IsPrimary:         false,
		}); err != nil {
			t.Fatalf("UpsertIdentityEmail(): %v", err)
		}
		if _, err := q.UpsertIdentityAnchor(ctx, gen.UpsertIdentityAnchorParams{
			IdentityID:            identityID,
			AnchorKind:            "okta_user_id",
			Issuer:                "okta:acme",
			AnchorValue:           "00u123",
			NormalizedAnchorValue: "00u123",
			TrustLevel:            "manual",
			LifecycleState:        "active",
		}); err != nil {
			t.Fatalf("UpsertIdentityAnchor(): %v", err)
		}

		body := renderIdentityShow(t, h, identityID)
		assertContains(t, body, "Identity graph facts")
		assertContains(t, body, "alias@example.com")
		assertContains(t, body, "Okta User Id")
		assertContains(t, body, "okta:acme")
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

func renderIdentityShowFragment(t *testing.T, h *Handlers, identityID int64, hxTarget string) string {
	t.Helper()

	target := "http://example.com/identities/" + strconv.FormatInt(identityID, 10)
	c, rec := newTestContext(http.MethodGet, target)
	(*c).SetPath("/identities/:id")
	(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(identityID, 10)}})
	(*c).Request().Header.Set("HX-Request", "true")
	(*c).Request().Header.Set("HX-Target", hxTarget)
	if err := h.HandleIdentityShow(c); err != nil {
		t.Fatalf("HandleIdentityShow(%s, target %s): %v", target, hxTarget, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderIdentityShowBoosted(t *testing.T, h *Handlers, identityID int64) string {
	t.Helper()

	target := "http://example.com/identities/" + strconv.FormatInt(identityID, 10)
	c, rec := newTestContext(http.MethodGet, target)
	(*c).SetPath("/identities/:id")
	(*c).SetPathValues(echo.PathValues{{Name: "id", Value: strconv.FormatInt(identityID, 10)}})
	(*c).Request().Header.Set("HX-Request", "true")
	(*c).Request().Header.Set("HX-Boosted", "true")
	if err := h.HandleIdentityShow(c); err != nil {
		t.Fatalf("HandleIdentityShow(%s, boosted): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}
