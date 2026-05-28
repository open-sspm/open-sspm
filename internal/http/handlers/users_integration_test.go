package handlers

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleOktaAccountShowReadsAssignmentsFromGenericEntitlements(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindOkta, "example.okta.com")
		insertCommandSearchOktaApp(t, ctx, q, runID, "direct-app", "Direct App", "direct", "active")
		insertCommandSearchOktaApp(t, ctx, q, runID, "group-app", "Group App", "group", "active")

		userID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     "okta",
			SourceName:     "example.okta.com",
			ExternalID:     "00u-alice",
			Email:          "alice@example.com",
			DisplayName:    "Alice Example",
			Status:         "ACTIVE",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"ACTIVE"}`,
		})
		groupID := insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     "okta",
			SourceName:     "example.okta.com",
			ExternalID:     "group:00g-eng",
			DisplayName:    "Engineering",
			Status:         "ACTIVE",
			AccountKind:    "service",
			EntityCategory: "group",
			RawJSON:        `{"entity_category":"group"}`,
		})

		insertDashboardEntitlement(t, ctx, pool, runID, userID, "application_assignment", "direct-app", "USER", `{"attributes":{"profile":{"appUserName":"alice.direct"}}}`)
		insertDashboardEntitlement(t, ctx, pool, runID, userID, "application_assignment", "group-app", "GROUP", `{"attributes":{"profile":{"appUserName":"alice.group"}}}`)
		insertDashboardEntitlement(t, ctx, pool, runID, userID, "group_membership", "group:00g-eng", "member", `{"attributes":{"target":{"external_id":"00g-eng","display_name":"Engineering"}}}`)
		insertDashboardEntitlement(t, ctx, pool, runID, groupID, "application_assignment", "group-app", "1", `{"attributes":{"target":{"external_id":"group-app","display_name":"Group App"}}}`)

		c, rec := newTestContext(http.MethodGet, "http://example.com/accounts/okta/"+fmt.Sprint(userID))
		(*c).SetPath("/accounts/okta/:id")
		(*c).SetPathValues(echo.PathValues{{Name: "id", Value: fmt.Sprint(userID)}})

		if err := h.HandleOktaAccountShow(c); err != nil {
			t.Fatalf("HandleOktaAccountShow(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		assertContains(t, body, "Alice Example")
		assertContains(t, body, "Engineering")
		assertContains(t, body, "Direct App")
		assertContains(t, body, "Group App")
		assertContains(t, body, "Direct")
		assertContains(t, body, "Group")
		assertContains(t, body, "appUserName: alice.direct")
		assertContains(t, body, "appUserName: alice.group")
		assertNotContains(t, body, "No apps assigned to this account.")
	})
}
