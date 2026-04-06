package handlers

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestDemoSeedContractsHandleDuplicateOwnerIdentityEmails(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, _ *Handlers) {
		insertCommandSearchIdentity(t, ctx, pool, "human", "demo.user009@example.com", "Preexisting Demo User 009 A")
		insertCommandSearchIdentity(t, ctx, pool, "human", "demo.user009@example.com", "Preexisting Demo User 009 B")

		applyDemoSeedFiles(t, ctx, pool)

		appID := lookupSaaSAppIDByCanonicalKey(t, ctx, pool, "azure-legacy-ops-portal")
		ownerIdentityID := lookupGovernanceOwnerIdentityID(t, ctx, pool, "saas_app", appID)
		if ownerIdentityID == 0 {
			t.Fatalf("owner_identity_id for azure-legacy-ops-portal = 0, want non-zero")
		}
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

func lookupGovernanceOwnerIdentityID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, subjectKind string, subjectID int64) int64 {
	t.Helper()

	var ownerIdentityID int64
	if err := pool.QueryRow(ctx, `
		SELECT owner_identity_id
		FROM governance_subject_overrides
		WHERE subject_kind = $1
		  AND subject_id = $2
	`, subjectKind, subjectID).Scan(&ownerIdentityID); err != nil {
		t.Fatalf("lookup governance owner %s/%d: %v", subjectKind, subjectID, err)
	}
	return ownerIdentityID
}
