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
	fullWorkerName := "name: test-open-sspm-worker-lane-full\n"
	fullWorkerBlock := renderedManifestBlock(t, defaultRender, fullWorkerName)
	if !strings.Contains(fullWorkerBlock, "- name: SYNC_FULL_ENABLED\n              value: \"1\"") {
		t.Fatalf("default render does not set SYNC_FULL_ENABLED=1 for the full worker")
	}
	if !strings.Contains(fullWorkerBlock, "app.kubernetes.io/component: worker") ||
		!strings.Contains(fullWorkerBlock, "open-sspm.io/worker-lane: full") {
		t.Fatalf("full worker deployment does not render worker component and lane labels")
	}

	disabledRender := renderHelmTemplate(t, helm, "--set", "config.syncFullEnabled=false")
	if strings.Contains(disabledRender, fullWorkerName) {
		t.Fatalf("render with config.syncFullEnabled=false still contains full worker Deployment %q", fullWorkerName)
	}

	laneDisabledRender := renderHelmTemplate(t, helm, "--set", "worker.lanes.full.enabled=false")
	if strings.Contains(laneDisabledRender, fullWorkerName) {
		t.Fatalf("render with worker.lanes.full.enabled=false still contains full worker Deployment %q", fullWorkerName)
	}
}

func TestRealtimeWorkerDeploymentsRenderByDefault(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	rendered := renderHelmTemplate(t, helm)
	eventInboxBlock := renderedManifestBlock(t, rendered, "name: test-open-sspm-worker-lane-event-inbox\n")
	if !strings.Contains(eventInboxBlock, "- worker\n            - --lane=event-inbox") {
		t.Fatalf("event inbox worker deployment does not run the event-inbox worker lane")
	}
	if !strings.Contains(eventInboxBlock, "app.kubernetes.io/component: worker") ||
		!strings.Contains(eventInboxBlock, "open-sspm.io/worker-lane: event-inbox") {
		t.Fatalf("event inbox worker deployment does not render worker component and lane labels")
	}
	if !strings.Contains(eventInboxBlock, "- name: EVENT_INBOX_BATCH_SIZE\n              value: \"500\"") {
		t.Fatalf("event inbox worker deployment does not set EVENT_INBOX_BATCH_SIZE")
	}

	tailBlock := renderedManifestBlock(t, rendered, "name: test-open-sspm-worker-lane-tail\n")
	if !strings.Contains(tailBlock, "- worker\n            - --lane=tail") {
		t.Fatalf("tail worker deployment does not run the tail worker lane")
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

	evaluatorBlock := renderedManifestBlock(t, rendered, "name: test-open-sspm-worker-lane-evaluator\n")
	if !strings.Contains(evaluatorBlock, "- worker\n            - --lane=evaluator") {
		t.Fatalf("evaluator worker deployment does not run the evaluator worker lane")
	}
	if !strings.Contains(evaluatorBlock, "- name: EVENT_EVALUATOR_WORKER_BATCH_SIZE\n              value: \"100\"") {
		t.Fatalf("evaluator worker deployment does not set EVENT_EVALUATOR_WORKER_BATCH_SIZE")
	}
	if !strings.Contains(evaluatorBlock, "- name: EVENT_EVALUATOR_WORKER_MAX_ATTEMPTS\n              value: \"10\"") {
		t.Fatalf("evaluator worker deployment does not set EVENT_EVALUATOR_WORKER_MAX_ATTEMPTS")
	}
	if !strings.Contains(evaluatorBlock, "readinessProbe:") || !strings.Contains(evaluatorBlock, "http://127.0.0.1:9090/healthz") {
		t.Fatalf("evaluator worker deployment does not render metrics health probes")
	}
}

func TestMetricsServiceCanSelectAWorkerLane(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	rendered := renderHelmTemplate(t, helm, "--set", "metrics.service.enabled=true", "--set", "metrics.service.workerLane=tail")
	metricsBlock := renderedManifestBlock(t, rendered, "name: test-open-sspm-metrics\n")
	if !strings.Contains(metricsBlock, "app.kubernetes.io/component: worker") ||
		!strings.Contains(metricsBlock, "open-sspm.io/worker-lane: tail") {
		t.Fatalf("metrics service does not select the requested worker lane")
	}
}

func TestRealtimeWorkerDeploymentsCanBeDisabled(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	rendered := renderHelmTemplate(t, helm, "--set", "worker.lanes.eventInbox.enabled=false", "--set", "worker.lanes.tail.enabled=false", "--set", "worker.lanes.evaluator.enabled=false")
	if strings.Contains(rendered, "name: test-open-sspm-worker-lane-event-inbox\n") {
		t.Fatalf("render with worker.lanes.eventInbox.enabled=false still contains event inbox worker Deployment")
	}
	if strings.Contains(rendered, "name: test-open-sspm-worker-lane-tail\n") {
		t.Fatalf("render with worker.lanes.tail.enabled=false still contains tail worker Deployment")
	}
	if strings.Contains(rendered, "name: test-open-sspm-worker-lane-evaluator\n") {
		t.Fatalf("render with worker.lanes.evaluator.enabled=false still contains evaluator worker Deployment")
	}
}

func TestDiscoveryLaneDisableAlsoDisablesAPIQueuing(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	rendered := renderHelmTemplate(t, helm, "--set", "worker.lanes.discovery.enabled=false")
	if strings.Contains(rendered, "name: test-open-sspm-worker-lane-discovery\n") {
		t.Fatal("render with worker.lanes.discovery.enabled=false still contains discovery worker Deployment")
	}
	apiBlock := renderedManifestBlock(t, rendered, "name: test-open-sspm-api\n")
	if !strings.Contains(apiBlock, "- name: SYNC_DISCOVERY_ENABLED\n              value: \"0\"") {
		t.Fatalf("api deployment did not disable discovery queuing when discovery lane is disabled")
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
