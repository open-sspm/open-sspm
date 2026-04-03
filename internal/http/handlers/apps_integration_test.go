package handlers

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleAppsHTMXUsesSharedPaginationState(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "acme.okta.com",
			Token:  "token-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		for i := 1; i <= 21; i++ {
			insertCommandSearchOktaApp(
				t,
				ctx,
				q,
				runID,
				fmt.Sprintf("app-%02d", i),
				fmt.Sprintf("App %02d", i),
				fmt.Sprintf("app-%02d", i),
				"active",
			)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/assigned-apps?page=2")
		(*c).Request().Header.Set("HX-Request", "true")
		(*c).Request().Header.Set("HX-Target", "apps-results")

		if err := h.HandleApps(c); err != nil {
			t.Fatalf("HandleApps(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "App 21")
		assertContains(t, body, "Page 2 of 2")
		assertNotContains(t, body, "App 01")
	})
}

func TestHandleOktaAppShowPaginatesAssignedAccounts(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, true, configstore.OktaConfig{
			Domain: "acme.okta.com",
			Token:  "token-1",
		})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "acme.okta.com")
		insertCommandSearchOktaApp(t, ctx, q, runID, "pager-app", "Pager App", "pager-app", "active")

		accountExternalIDs := make([]string, 0, 21)
		for i := 1; i <= 21; i++ {
			externalID := fmt.Sprintf("user-%02d", i)
			accountExternalIDs = append(accountExternalIDs, externalID)
			insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
				SourceKind:     configstore.KindOkta,
				SourceName:     "acme.okta.com",
				ExternalID:     externalID,
				Email:          fmt.Sprintf("user%02d@example.com", i),
				DisplayName:    fmt.Sprintf("User %02d", i),
				Status:         "ACTIVE",
				AccountKind:    "human",
				EntityCategory: "user",
				RawJSON:        `{"status":"ACTIVE"}`,
			})
		}
		insertCommandSearchOktaAppAssignments(t, ctx, q, runID, accountExternalIDs, "pager-app")

		c, rec := newTestContext(http.MethodGet, "http://example.com/assigned-apps/pager-app?page=2")
		(*c).SetPath("/assigned-apps/:externalID")
		(*c).SetPathValues(echo.PathValues{{Name: "externalID", Value: "pager-app"}})

		if err := h.HandleOktaAppShow(c); err != nil {
			t.Fatalf("HandleOktaAppShow(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "User 21")
		assertContains(t, body, "Page 2 of 2")
		assertNotContains(t, body, "User 01")
	})
}

func insertCommandSearchOktaAppAssignments(t *testing.T, ctx context.Context, q *gen.Queries, runID int64, oktaAccountExternalIDs []string, oktaAppExternalID string) {
	t.Helper()

	oktaAppExternalIDs := make([]string, len(oktaAccountExternalIDs))
	scopes := make([]string, len(oktaAccountExternalIDs))
	profileJSONs := make([][]byte, len(oktaAccountExternalIDs))
	rawJSONs := make([][]byte, len(oktaAccountExternalIDs))
	for i := range oktaAccountExternalIDs {
		oktaAppExternalIDs[i] = oktaAppExternalID
		scopes[i] = "USER"
		profileJSONs[i] = []byte(`{}`)
		rawJSONs[i] = []byte(`{}`)
	}

	if _, err := q.UpsertOktaAppAssignmentsBulkByOktaAccountExternalIDs(ctx, gen.UpsertOktaAppAssignmentsBulkByOktaAccountExternalIDsParams{
		SeenInRunID:            runID,
		OktaAccountExternalIds: oktaAccountExternalIDs,
		OktaAppExternalIds:     oktaAppExternalIDs,
		Scopes:                 scopes,
		ProfileJsons:           profileJSONs,
		RawJsons:               rawJSONs,
	}); err != nil {
		t.Fatalf("UpsertOktaAppAssignmentsBulkByOktaAccountExternalIDs: %v", err)
	}
	if _, err := q.PromoteOktaAppAssignmentsSeenInRun(ctx, pgtype.Int8{Int64: runID, Valid: true}); err != nil {
		t.Fatalf("PromoteOktaAppAssignmentsSeenInRun: %v", err)
	}
}
