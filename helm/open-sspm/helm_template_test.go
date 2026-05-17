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

func TestRealtimeWorkerDeploymentsRenderByDefault(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	rendered := renderHelmTemplate(t, helm)
	tailBlock := renderedManifestBlock(t, rendered, "name: test-open-sspm-worker-tail\n")
	if !strings.Contains(tailBlock, "- worker-tail") {
		t.Fatalf("tail worker deployment does not run worker-tail")
	}
	if !strings.Contains(tailBlock, "- name: SYNC_TAIL_INTERVAL\n              value: \"5m\"") {
		t.Fatalf("tail worker deployment does not set SYNC_TAIL_INTERVAL")
	}
	if !strings.Contains(tailBlock, "- name: EVENT_RETENTION_DAYS\n              value: \"90\"") {
		t.Fatalf("tail worker deployment does not set event retention")
	}
	if !strings.Contains(tailBlock, "livenessProbe:") || !strings.Contains(tailBlock, "http://127.0.0.1:9090/healthz") {
		t.Fatalf("tail worker deployment does not render metrics health probes")
	}

	riskpolicyBlock := renderedManifestBlock(t, rendered, "name: test-open-sspm-worker-riskpolicy\n")
	if !strings.Contains(riskpolicyBlock, "- worker-riskpolicy") {
		t.Fatalf("riskpolicy worker deployment does not run worker-riskpolicy")
	}
	if !strings.Contains(riskpolicyBlock, "- name: RISKPOLICY_EVENT_WORKER_BATCH_SIZE\n              value: \"100\"") {
		t.Fatalf("riskpolicy worker deployment does not set RISKPOLICY_EVENT_WORKER_BATCH_SIZE")
	}
	if !strings.Contains(riskpolicyBlock, "- name: RISKPOLICY_EVENT_WORKER_MAX_ATTEMPTS\n              value: \"10\"") {
		t.Fatalf("riskpolicy worker deployment does not set RISKPOLICY_EVENT_WORKER_MAX_ATTEMPTS")
	}
	if !strings.Contains(riskpolicyBlock, "readinessProbe:") || !strings.Contains(riskpolicyBlock, "http://127.0.0.1:9090/healthz") {
		t.Fatalf("riskpolicy worker deployment does not render metrics health probes")
	}
}

func TestRealtimeWorkerDeploymentsCanBeDisabled(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	rendered := renderHelmTemplate(t, helm, "--set", "tailWorker.enabled=false", "--set", "riskpolicyWorker.enabled=false")
	if strings.Contains(rendered, "name: test-open-sspm-worker-tail\n") {
		t.Fatalf("render with tailWorker.enabled=false still contains tail worker Deployment")
	}
	if strings.Contains(rendered, "name: test-open-sspm-worker-riskpolicy\n") {
		t.Fatalf("render with riskpolicyWorker.enabled=false still contains riskpolicy worker Deployment")
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
