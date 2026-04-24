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

		c, rec := newTestContext(http.MethodGet, "http://example.com/identities?page=200000000")
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
