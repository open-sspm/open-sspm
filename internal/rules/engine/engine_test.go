package engine

import (
	"context"
	"encoding/json"
	"testing"

	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
)

type fakeDatasets struct {
	data map[string][]any
	errs map[string]runtimev2.DatasetErrorKind
}

func (f *fakeDatasets) Capabilities(ctx context.Context) []runtimev2.DatasetRef {
	_ = ctx
	return nil
}

func (f *fakeDatasets) GetDataset(ctx context.Context, eval runtimev2.EvalContext, ref runtimev2.DatasetRef) runtimev2.DatasetResult {
	_, _ = ctx, eval

	key := ref.Dataset
	if f.errs != nil {
		if kind, ok := f.errs[key]; ok {
			return runtimev2.DatasetResult{Error: &runtimev2.DatasetError{Kind: kind}}
		}
	}

	rows, ok := f.data[key]
	if !ok {
		return runtimev2.DatasetResult{Error: &runtimev2.DatasetError{Kind: runtimev2.DatasetErrorKind_MISSING_DATASET}}
	}

	raw := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			return runtimev2.DatasetResult{Error: &runtimev2.DatasetError{Kind: runtimev2.DatasetErrorKind_ENGINE_ERROR, Message: err.Error()}}
		}
		raw = append(raw, json.RawMessage(b))
	}
	return runtimev2.DatasetResult{Rows: raw}
}

func TestEvalCheck_RegoPass(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:policies/sign-on": {
					map[string]any{"id": "a", "session": map[string]any{"max_idle_minutes": float64(10)}},
					map[string]any{"id": "b", "session": map[string]any{"max_idle_minutes": float64(15)}},
				},
			},
		},
	}

	rule := regoRule("Idle timeout", []string{"okta:policies/sign-on"}, idleTimeoutRego, map[string]any{"max_idle_minutes": float64(15)})

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "pass" {
		t.Fatalf("expected pass, got %q", ev.Status)
	}
}

func TestEvalCheck_RegoFail(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:policies/sign-on": {
					map[string]any{"id": "a", "session": map[string]any{"max_idle_minutes": float64(30)}},
				},
			},
		},
	}

	rule := regoRule("Idle timeout", []string{"okta:policies/sign-on"}, idleTimeoutRego, map[string]any{"max_idle_minutes": float64(15)})

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "fail" {
		t.Fatalf("expected fail, got %q", ev.Status)
	}
}

func TestEvalCheck_RegoCountComparePass(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:apps": {
					map[string]any{"id": "a", "active": true},
					map[string]any{"id": "b", "active": true},
					map[string]any{"id": "c", "active": false},
				},
			},
		},
	}

	rule := regoRule("Count active apps", []string{"okta:apps"}, activeAppsRego, nil)

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "pass" {
		t.Fatalf("expected pass, got %q", ev.Status)
	}
}

func TestEvalCheck_MissingDatasetIsPolicyUnknown(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{},
			errs: map[string]runtimev2.DatasetErrorKind{
				"missing": runtimev2.DatasetErrorKind_MISSING_DATASET,
			},
		},
	}

	rule := regoRule("Missing dataset", []string{"missing"}, datasetErrorRego, nil)

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "unknown" || ev.ErrorKind != "" {
		t.Fatalf("expected unknown policy result, got %q/%q", ev.Status, ev.ErrorKind)
	}
}

func TestEvalCheck_ManualRuleUnknown(t *testing.T) {
	e := &Engine{Datasets: &fakeDatasets{data: map[string][]any{}}}

	rule := osspecv2.Rule{
		Title:      "Manual control",
		Monitoring: osspecv2.Monitoring{Status: osspecv2.MonitoringStatus_MANUAL},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "unknown" {
		t.Fatalf("expected unknown, got %q", ev.Status)
	}
}

func regoRule(title string, requiredData []string, rego string, defaults map[string]any) osspecv2.Rule {
	rule := osspecv2.Rule{
		Key:          "rule",
		Title:        title,
		Monitoring:   osspecv2.Monitoring{Status: osspecv2.MonitoringStatus_AUTOMATED},
		RequiredData: requiredData,
		Check: &osspecv2.Check{
			Engine:  osspecv2.CheckEngine_REGO,
			Package: "opensspm.tests",
			Query:   "data.opensspm.tests.result",
			Rego:    rego,
		},
	}
	if defaults != nil {
		rule.Parameters = &osspecv2.Parameters{Defaults: defaults}
	}
	return rule
}

const idleTimeoutRego = `package opensspm.tests

rows := object.get(object.get(input.datasets, "okta:policies/sign-on", {}), "rows", [])
passed := [r |
  r := rows[_]
  object.get(object.get(r, "session", {}), "max_idle_minutes", 999999) <= input.params.max_idle_minutes
]

result := {
  "status": "pass",
  "selected_count": count(rows),
  "passed_count": count(passed),
  "count_value": count(passed),
} if {
  count(rows) > 0
  count(passed) == count(rows)
}

result := {
  "status": "fail",
  "selected_count": count(rows),
  "passed_count": count(passed),
  "count_value": count(passed),
} if {
  count(rows) > 0
  count(passed) != count(rows)
}`

const activeAppsRego = `package opensspm.tests

rows := object.get(object.get(input.datasets, "okta:apps", {}), "rows", [])
active := [r | r := rows[_]; object.get(r, "active", false) == true]

result := {
  "status": "pass",
  "selected_count": count(active),
  "passed_count": 1,
  "count_value": count(active),
  "target_value": 2,
} if {
  count(active) == 2
}

result := {
  "status": "fail",
  "selected_count": count(active),
  "passed_count": 0,
  "count_value": count(active),
  "target_value": 2,
} if {
  count(active) != 2
}`

const datasetErrorRego = `package opensspm.tests

result := {"status": "unknown", "reason_code": sprintf("dataset_%s", [kind])} if {
  err := object.get(object.get(input.datasets, "missing", {}), "error", null)
  err != null
  kind := object.get(err, "kind", "engine_error")
}`
