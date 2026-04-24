package handlers

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
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

func TestBuildIdentityShowOverviewMapCountsCurrentIdentityPerSource(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		oktaRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		githubRunID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")

		identityID := insertCommandSearchIdentity(t, ctx, pool, "human", "person@example.com", "Example Person")
		oktaAccountID := insertCommandSearchAccount(t, ctx, pool, oktaRunID, commandSearchAccountSeed{
			SourceKind:     configstore.KindOkta,
			SourceName:     "acme.okta.com",
			ExternalID:     "okta-user-1",
			Email:          "person@example.com",
			DisplayName:    "Example Person",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		secondOktaAccountID := insertCommandSearchAccount(t, ctx, pool, oktaRunID, commandSearchAccountSeed{
			SourceKind:     configstore.KindOkta,
			SourceName:     "acme.okta.com",
			ExternalID:     "okta-user-2",
			Email:          "person@example.com",
			DisplayName:    "Example Person Admin",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		githubAccountID := insertCommandSearchAccount(t, ctx, pool, githubRunID, commandSearchAccountSeed{
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
		insertCommandSearchIdentityAccountLink(t, ctx, pool, identityID, oktaAccountID)
		insertCommandSearchIdentityAccountLink(t, ctx, pool, identityID, secondOktaAccountID)
		insertCommandSearchIdentityAccountLink(t, ctx, pool, identityID, githubAccountID)

		graph, err := h.buildIdentityShowOverviewMap(ctx, identityID)
		if err != nil {
			t.Fatalf("buildIdentityShowOverviewMap() error = %v", err)
		}
		if graph.AccountCount != 3 {
			t.Fatalf("graph account count = %d, want 3", graph.AccountCount)
		}
		if len(graph.Sources) != 2 {
			t.Fatalf("sources length = %d, want 2: %+v", len(graph.Sources), graph.Sources)
		}

		accountCountsByKind := map[string]int64{}
		for _, source := range graph.Sources {
			if source.IdentityCount != 1 {
				t.Fatalf("%s identity count = %d, want 1: %+v", source.Kind, source.IdentityCount, source)
			}
			accountCountsByKind[source.Kind] = source.AccountCount
		}
		if accountCountsByKind[configstore.KindOkta] != 2 {
			t.Fatalf("okta account count = %d, want 2", accountCountsByKind[configstore.KindOkta])
		}
		if accountCountsByKind[configstore.KindGitHub] != 1 {
			t.Fatalf("github account count = %d, want 1", accountCountsByKind[configstore.KindGitHub])
		}
	})
}
