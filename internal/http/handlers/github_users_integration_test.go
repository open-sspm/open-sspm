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

func TestHandleGitHubUsersIncludesLegacyUnknownUserRows(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{Org: "acme"})

		runID := insertCommandSearchSyncRun(t, ctx, pool, configstore.KindGitHub, "acme")
		insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "github-member-legacy",
			Email:          "member@example.com",
			DisplayName:    "Legacy Member",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "unknown",
			RawJSON:        `{"login":"github-member-legacy","type":"User","status":"active"}`,
		})
		insertCommandSearchAccount(t, ctx, pool, runID, commandSearchAccountSeed{
			SourceKind:     configstore.KindGitHub,
			SourceName:     "acme",
			ExternalID:     "team:platform",
			DisplayName:    "Platform",
			Status:         "active",
			AccountKind:    "service",
			EntityCategory: "unknown",
			RawJSON:        `{"slug":"platform","type":"Team","status":"active"}`,
		})

		c, rec := newTestContext(http.MethodGet, "http://example.com/github-users")
		if err := h.HandleGitHubUsers(c); err != nil {
			t.Fatalf("HandleGitHubUsers(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		body := rec.Body.String()
		if !strings.Contains(body, "github-member-legacy") {
			t.Fatalf("body missing legacy github user: %s", body)
		}
		if strings.Contains(body, "team:platform") {
			t.Fatalf("body unexpectedly included github team row: %s", body)
		}
		if strings.Contains(body, "No GitHub users synced yet.") {
			t.Fatalf("body unexpectedly rendered empty state: %s", body)
		}
	})
}
