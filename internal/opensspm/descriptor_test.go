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

func TestDescriptorV1_Parses(t *testing.T) {
	t.Parallel()

	_, err := DescriptorV1()
	if err != nil {
		t.Fatalf("parse embedded descriptor: %v", err)
	}
}

func TestLockfileHashMatchesEmbeddedDescriptor(t *testing.T) {
	t.Parallel()

	lock, err := specassets.Lockfile()
	if err != nil {
		t.Fatalf("parse lockfile: %v", err)
	}

	sum := sha256.Sum256(specassets.DescriptorV1JSON())
	got := hex.EncodeToString(sum[:])
	if got != lock.DescriptorHash {
		t.Fatalf("descriptor hash mismatch: lockfile=%s computed=%s", lock.DescriptorHash, got)
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

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "../.."))

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
