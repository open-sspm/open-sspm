package handlers

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestDemoSeedContractsPopulateBreadthSurfaces(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		applyDemoSeedFiles(t, ctx, pool)

		discoveryListBody := renderDiscoveryApps(t, h, "http://example.com/discovery/apps")
		assertContains(t, discoveryListBody, "Finance Sync Audit Bot")
		assertContains(t, discoveryListBody, "GitHub Actions Control Plane")

		discoveryHotspotsBody := renderDiscoveryHotspots(t, h, "http://example.com/discovery/hotspots")
		assertContains(t, discoveryHotspotsBody, "Finance Sync Audit Bot")
		assertContains(t, discoveryHotspotsBody, "Drive Mirror Exporter")

		discoveryAppID := lookupSaaSAppIDByCanonicalKey(t, ctx, pool, "finance-sync-audit-bot")
		discoveryDetailBody := renderDiscoveryAppShow(t, h, discoveryAppID)
		assertContains(t, discoveryDetailBody, "Finance Sync Audit Bot")
		assertContains(t, discoveryDetailBody, "Action Required")

		googleUsersBody := renderGoogleWorkspaceUsers(t, h, "http://example.com/accounts/google-workspace")
		assertContains(t, googleUsersBody, "Workspace User 001")

		googleGroupsBody := renderGoogleWorkspaceGroups(t, h, "http://example.com/accounts/google-workspace/groups")
		assertContains(t, googleGroupsBody, "Workspace Engineering")

		googleUnlinkedBody := renderGoogleWorkspaceUnlinkedUsers(t, h, "http://example.com/accounts/unlinked/google-workspace")
		assertContains(t, googleUnlinkedBody, "Workspace User 061")

		googleOAuthBody := renderAppAssetsPage(t, h, "http://example.com/app-assets?source_kind=google_workspace&asset_kind=google_oauth_client")
		assertContains(t, googleOAuthBody, "Finance Sync Audit Bot")
		assertContains(t, googleOAuthBody, "Analytics Studio")

		awsUsersBody := renderAWSUsers(t, h, "http://example.com/accounts/aws")
		assertContains(t, awsUsersBody, "AWS User 001")

		awsUnlinkedBody := renderAWSUnlinkedUsers(t, h, "http://example.com/accounts/unlinked/aws")
		assertContains(t, awsUnlinkedBody, "AWS User 036")

		vaultAppAssetsBody := renderAppAssetsPage(t, h, "http://example.com/app-assets?source_kind=vault&asset_kind=vault_auth_role")
		assertContains(t, vaultAppAssetsBody, "platform-admin")
		assertContains(t, vaultAppAssetsBody, "release-bot")
	})
}

func applyDemoSeedFiles(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	dir := filepath.Join(repoRootFromDemoSeedContractTest(t), "demo", "data")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}

	seedFiles := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		seedFiles = append(seedFiles, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(seedFiles)

	for _, path := range seedFiles {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", path, err)
		}
		if _, err := pool.Exec(ctx, string(raw)); err != nil {
			t.Fatalf("Exec(%s): %v", path, err)
		}
	}
}

func repoRootFromDemoSeedContractTest(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func lookupSaaSAppIDByCanonicalKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, canonicalKey string) int64 {
	t.Helper()

	var appID int64
	if err := pool.QueryRow(ctx, `
		SELECT id
		FROM saas_apps
		WHERE canonical_key = $1
	`, canonicalKey).Scan(&appID); err != nil {
		t.Fatalf("lookup saas app %s: %v", canonicalKey, err)
	}
	return appID
}

func renderDiscoveryHotspots(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleDiscoveryHotspots(c); err != nil {
		t.Fatalf("HandleDiscoveryHotspots(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderGoogleWorkspaceUsers(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleGoogleWorkspaceUsers(c); err != nil {
		t.Fatalf("HandleGoogleWorkspaceUsers(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderGoogleWorkspaceGroups(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleGoogleWorkspaceGroups(c); err != nil {
		t.Fatalf("HandleGoogleWorkspaceGroups(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderGoogleWorkspaceUnlinkedUsers(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleUnmatchedGoogleWorkspace(c); err != nil {
		t.Fatalf("HandleUnmatchedGoogleWorkspace(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderAWSUsers(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleAWSUsers(c); err != nil {
		t.Fatalf("HandleAWSUsers(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderAWSUnlinkedUsers(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleUnmatchedAWS(c); err != nil {
		t.Fatalf("HandleUnmatchedAWS(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}

func renderAppAssetsPage(t *testing.T, h *Handlers, target string) string {
	t.Helper()

	c, rec := newTestContext(http.MethodGet, target)
	if err := h.HandleAppAssets(c); err != nil {
		t.Fatalf("HandleAppAssets(%s): %v", target, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec.Body.String()
}
