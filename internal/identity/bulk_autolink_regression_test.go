package identity

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// selfFilename is the basename of this test file; we self-skip by suffix so
// path comparisons survive symlinks, -trimpath builds, and different working
// directories (running `go test ./...` from a parent does not produce the
// same absolute path the linker would record for runtime.Caller).
const selfFilename = "bulk_autolink_regression_test.go"

func TestBulkAutoLinkByEmailIsNotReferencedOutsideGeneratedCode(t *testing.T) {
	t.Parallel()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))

	// Search every top-level dir that could plausibly contain a reference to
	// the obsolete name: Go sources, SQL queries, docs, scripts, and the
	// frontend bundle. Restrict to existing dirs so the test stays portable
	// against future directory renames.
	candidateRoots := []string{
		"cmd",
		"db/queries",
		"internal",
		"docs",
		"scripts",
		"web",
		"plans",
		"helm",
	}
	searchRoots := make([]string, 0, len(candidateRoots))
	for _, candidate := range candidateRoots {
		root := filepath.Join(repoRoot, candidate)
		if info, err := os.Stat(root); err == nil && info.IsDir() {
			searchRoots = append(searchRoots, root)
		}
	}
	if len(searchRoots) == 0 {
		t.Fatalf("no candidate search roots exist under %s", repoRoot)
	}

	genDir := filepath.Join(repoRoot, "internal", "db", "gen")

	for _, root := range searchRoots {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if path == genDir {
					return filepath.SkipDir
				}
				return nil
			}
			// Self-skip by suffix instead of byte-exact path equality so the
			// test survives symlinks, -trimpath builds, and tooling that
			// rewrites paths.
			if strings.HasSuffix(filepath.ToSlash(path), "/"+selfFilename) {
				return nil
			}
			name := entry.Name()
			if !strings.HasSuffix(name, ".go") &&
				!strings.HasSuffix(name, ".sql") &&
				!strings.HasSuffix(name, ".md") &&
				!strings.HasSuffix(name, ".ts") &&
				!strings.HasSuffix(name, ".tsx") &&
				!strings.HasSuffix(name, ".js") &&
				!strings.HasSuffix(name, ".mjs") &&
				!strings.HasSuffix(name, ".templ") {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.Contains(string(body), "BulkAutoLinkByEmail") || strings.Contains(string(body), "bulkAutoLinkByEmail") {
				t.Fatalf("obsolete BulkAutoLinkByEmail reference found in %s", path)
			}
			return nil
		})
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
}
