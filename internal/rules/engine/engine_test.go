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

func TestEvalCheck_FieldComparePass(t *testing.T) {
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

	rule := osspecv2.Rule{
		Title:      "Idle timeout",
		Monitoring: osspecv2.Monitoring{Status: osspecv2.MonitoringStatus_AUTOMATED},
		RequiredData: []string{
			"okta:policies/sign-on",
		},
		Check: &osspecv2.Check{
			Engine: osspecv2.CheckEngine_CEL_PLAN,
			Plan: &osspecv2.CheckPlan{
				Type:             "dataset.field_compare",
				Dataset:          "okta:policies/sign-on",
				AssertExpression: `field(r, "session.max_idle_minutes") <= param("max_idle_minutes")`,
				Expect: &osspecv2.CheckPlanExpect{
					Match:       "all",
					MinSelected: 1,
					OnEmpty:     "fail",
				},
			},
		},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{"max_idle_minutes": float64(15)})
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

func TestEvalCheck_FieldCompareFail(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:policies/sign-on": {
					map[string]any{"id": "a", "session": map[string]any{"max_idle_minutes": float64(30)}},
				},
			},
		},
	}

	rule := osspecv2.Rule{
		Title:      "Idle timeout",
		Monitoring: osspecv2.Monitoring{Status: osspecv2.MonitoringStatus_AUTOMATED},
		RequiredData: []string{
			"okta:policies/sign-on",
		},
		Check: &osspecv2.Check{
			Engine: osspecv2.CheckEngine_CEL_PLAN,
			Plan: &osspecv2.CheckPlan{
				Type:             "dataset.field_compare",
				Dataset:          "okta:policies/sign-on",
				AssertExpression: `field(r, "session.max_idle_minutes") <= 15`,
				Expect: &osspecv2.CheckPlanExpect{
					Match:       "all",
					MinSelected: 1,
					OnEmpty:     "fail",
				},
			},
		},
	}

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

func TestEvalCheck_CountComparePass(t *testing.T) {
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

	rule := osspecv2.Rule{
		Title:      "Count active apps",
		Monitoring: osspecv2.Monitoring{Status: osspecv2.MonitoringStatus_AUTOMATED},
		RequiredData: []string{
			"okta:apps",
		},
		Check: &osspecv2.Check{
			Engine: osspecv2.CheckEngine_CEL_PLAN,
			Plan: &osspecv2.CheckPlan{
				Type:            "dataset.count_compare",
				Dataset:         "okta:apps",
				WhereExpression: `field(r, "active") == true`,
				Compare: &osspecv2.CheckPlanCompare{
					Op:    "eq",
					Value: 2,
				},
			},
		},
	}

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

func TestEvalCheck_MissingDatasetPolicyError(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{},
			errs: map[string]runtimev2.DatasetErrorKind{
				"missing": runtimev2.DatasetErrorKind_MISSING_DATASET,
			},
		},
	}

	rule := osspecv2.Rule{
		Title:      "Missing dataset",
		Monitoring: osspecv2.Monitoring{Status: osspecv2.MonitoringStatus_AUTOMATED},
		RequiredData: []string{
			"missing",
		},
		Check: &osspecv2.Check{
			Engine: osspecv2.CheckEngine_CEL_PLAN,
			Plan: &osspecv2.CheckPlan{
				Type:             "dataset.count_compare",
				Dataset:          "missing",
				OnMissingDataset: "error",
				Compare: &osspecv2.CheckPlanCompare{
					Op:    "eq",
					Value: 0,
				},
			},
		},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "rule", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "error" || ev.ErrorKind != "missing_dataset" {
		t.Fatalf("expected error/missing_dataset, got %q/%q", ev.Status, ev.ErrorKind)
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
