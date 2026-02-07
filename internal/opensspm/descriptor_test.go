package opensspm

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/opensspm/specassets"
)

func TestDescriptorV2_Loads(t *testing.T) {
	t.Parallel()

	desc, err := DescriptorV2()
	if err != nil {
		t.Fatalf("load embedded descriptor: %v", err)
	}
	if len(desc.Rulesets) == 0 {
		t.Fatal("embedded descriptor has no rulesets")
	}
}

func TestLockfileHashMatchesEmbeddedDescriptor(t *testing.T) {
	t.Parallel()

	lock, err := specassets.Lockfile()
	if err != nil {
		t.Fatalf("parse lockfile: %v", err)
	}

	sum := sha256.Sum256(specassets.DescriptorV2YAML())
	got := strings.TrimSpace(lock.DescriptorHash)
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("descriptor hash mismatch: lockfile=%s computed=%s", got, want)
	}
	if strings.TrimSpace(lock.DescriptorHashAlgorithm) != "sha256" {
		t.Fatalf(
			"descriptor hash algorithm mismatch: lockfile=%s expected=%s",
			strings.TrimSpace(lock.DescriptorHashAlgorithm),
			"sha256",
		)
	}
	if filepath.IsAbs(strings.TrimSpace(lock.UpstreamRepo)) {
		t.Fatalf("lockfile upstream_repo must be a canonical repo identifier, got absolute path %q", lock.UpstreamRepo)
	}
}

func TestLockfileUpstreamMatchesGoMod(t *testing.T) {
	t.Parallel()

	lock, err := specassets.Lockfile()
	if err != nil {
		t.Fatalf("parse lockfile: %v", err)
	}

	moduleVersion, ok := findGoModRequiredVersion(t, "github.com/open-sspm/open-sspm-spec")
	if !ok {
		t.Fatalf("missing required module version for github.com/open-sspm/open-sspm-spec")
	}

	if replaceTarget, ok := findGoModReplaceTarget(t, "github.com/open-sspm/open-sspm-spec"); ok {
		t.Fatalf("go.mod must not replace github.com/open-sspm/open-sspm-spec (found target %q); pin a remote module version instead", replaceTarget)
	}

	if commit, ok := pseudoVersionCommitSuffix(moduleVersion); ok {
		want := strings.TrimSpace(lock.UpstreamCommit)
		if want == "" {
			want = strings.TrimSpace(lock.UpstreamRef)
		}
		if want == "" {
			t.Fatalf("lockfile missing upstream_commit/upstream_ref to match go.mod pseudo-version")
		}
		if !strings.HasPrefix(strings.ToLower(want), strings.ToLower(commit)) {
			t.Fatalf("open-sspm-spec version mismatch: go.mod suffix=%s lockfile commit/ref=%s", commit, want)
		}
		return
	}

	want := strings.TrimSpace(lock.UpstreamRef)
	if want == "" {
		t.Fatalf("lockfile missing upstream_ref to match go.mod version")
	}
	if moduleVersion != want {
		t.Fatalf("open-sspm-spec version mismatch: go.mod=%s lockfile.upstream_ref=%s", moduleVersion, want)
	}
}

func findGoModRequiredVersion(t *testing.T, modulePath string) (string, bool) {
	t.Helper()

	repoRoot := repoRootFromThisFile(t)

	goModPath := filepath.Join(repoRoot, "go.mod")
	b, err := os.ReadFile(goModPath)
	if err != nil {
		t.Fatalf("read %s: %v", goModPath, err)
	}

	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		line, _, _ = strings.Cut(line, "//")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[0] != modulePath {
			continue
		}
		return fields[1], true
	}

	return "", false
}

func findGoModReplaceTarget(t *testing.T, modulePath string) (string, bool) {
	t.Helper()

	repoRoot := repoRootFromThisFile(t)

	goModPath := filepath.Join(repoRoot, "go.mod")
	b, err := os.ReadFile(goModPath)
	if err != nil {
		t.Fatalf("read %s: %v", goModPath, err)
	}

	lines := strings.Split(string(b), "\n")
	inReplaceBlock := false
	for _, line := range lines {
		line, _, _ = strings.Cut(line, "//")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		switch line {
		case "replace (":
			inReplaceBlock = true
			continue
		case ")":
			if inReplaceBlock {
				inReplaceBlock = false
				continue
			}
		}

		if !inReplaceBlock {
			if !strings.HasPrefix(line, "replace ") {
				continue
			}
			line = strings.TrimSpace(strings.TrimPrefix(line, "replace "))
		}

		before, after, found := strings.Cut(line, "=>")
		if !found {
			continue
		}
		leftFields := strings.Fields(strings.TrimSpace(before))
		if len(leftFields) == 0 || leftFields[0] != modulePath {
			continue
		}

		rightFields := strings.Fields(strings.TrimSpace(after))
		if len(rightFields) == 0 {
			continue
		}
		return strings.TrimSpace(rightFields[0]), true
	}

	return "", false
}

func repoRootFromThisFile(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}

	dir := filepath.Dir(thisFile)
	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root (go.mod) starting from %s", thisFile)
		}
		dir = parent
	}
}

func pseudoVersionCommitSuffix(version string) (string, bool) {
	i := strings.LastIndex(version, "-")
	if i == -1 || i+1 >= len(version) {
		return "", false
	}

	suffix := version[i+1:]
	if len(suffix) != 12 {
		return "", false
	}

	for _, r := range suffix {
		switch {
		case '0' <= r && r <= '9':
		case 'a' <= r && r <= 'f':
		case 'A' <= r && r <= 'F':
		default:
			return "", false
		}
	}

	return suffix, true
}
