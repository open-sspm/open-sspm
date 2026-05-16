package helm_test

import (
	"os/exec"
	"strings"
	"testing"
)

func TestFullWorkerDeploymentFollowsSyncFullEnabled(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	defaultRender := renderHelmTemplate(t, helm)
	fullWorkerName := "name: test-open-sspm-worker\n"
	fullWorkerBlock := renderedManifestBlock(t, defaultRender, fullWorkerName)
	if !strings.Contains(fullWorkerBlock, "- name: SYNC_FULL_ENABLED\n              value: \"1\"") {
		t.Fatalf("default render does not set SYNC_FULL_ENABLED=1 for the full worker")
	}

	disabledRender := renderHelmTemplate(t, helm, "--set", "config.syncFullEnabled=false")
	if strings.Contains(disabledRender, fullWorkerName) {
		t.Fatalf("render with config.syncFullEnabled=false still contains full worker Deployment %q", fullWorkerName)
	}
}

func renderHelmTemplate(t *testing.T, helm string, extraArgs ...string) string {
	t.Helper()
	args := []string{
		"template",
		"test",
		".",
		"--set", "database.existingSecret.name=db",
		"--set", "database.existingSecret.key=url",
	}
	args = append(args, extraArgs...)
	cmd := exec.Command(helm, args...)
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	return string(out)
}

func renderedManifestBlock(t *testing.T, render, marker string) string {
	t.Helper()
	start := strings.Index(render, marker)
	if start < 0 {
		t.Fatalf("render does not contain manifest marker %q", marker)
	}
	end := strings.Index(render[start:], "\n---")
	if end < 0 {
		return render[start:]
	}
	return render[start : start+end]
}
