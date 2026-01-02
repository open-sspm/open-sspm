package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
	osspecv1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v1"
)

type fakeDatasets struct {
	data map[string][]any
	errs map[string]runtimev1.DatasetErrorKind
}

func (f *fakeDatasets) Capabilities(ctx context.Context) []runtimev1.DatasetRef {
	_ = ctx
	return nil
}

func (f *fakeDatasets) GetDataset(ctx context.Context, eval runtimev1.EvalContext, ref runtimev1.DatasetRef) runtimev1.DatasetResult {
	_, _ = ctx, eval

	key := ref.Dataset
	if f.errs != nil {
		if kind, ok := f.errs[key]; ok {
			return runtimev1.DatasetResult{Error: &runtimev1.DatasetError{Kind: kind}}
		}
	}

	if f.data == nil {
		return runtimev1.DatasetResult{Error: &runtimev1.DatasetError{Kind: runtimev1.DatasetErrorKind_MISSING_DATASET}}
	}
	rows, ok := f.data[key]
	if !ok {
		return runtimev1.DatasetResult{Error: &runtimev1.DatasetError{Kind: runtimev1.DatasetErrorKind_MISSING_DATASET}}
	}

	raw := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			return runtimev1.DatasetResult{Error: &runtimev1.DatasetError{Kind: runtimev1.DatasetErrorKind_ENGINE_ERROR, Message: err.Error()}}
		}
		raw = append(raw, json.RawMessage(b))
	}
	return runtimev1.DatasetResult{Rows: raw}
}

func TestEvalDatasetFieldCompare_AllPass(t *testing.T) {
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

	rule := osspecv1.Rule{
		Title: "Idle timeout",
		Parameters: &osspecv1.Parameters{
			Defaults: map[string]any{"max_idle_minutes": float64(15)},
		},
		Check: &osspecv1.Check{
			Type:    "dataset.field_compare",
			Dataset: "okta:policies/sign-on",
			Assert: &osspecv1.Predicate{
				Path:       "/session/max_idle_minutes",
				Op:         "lte",
				ValueParam: "max_idle_minutes",
			},
			Expect: &osspecv1.FieldCompareExpect{
				Match:   "all",
				OnEmpty: "error",
			},
		},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "r", Context{}, rule, rule.Parameters.Defaults)
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

func TestEvalDatasetFieldCompare_Violation(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:policies/sign-on": {
					map[string]any{"id": "a", "session": map[string]any{"max_idle_minutes": float64(30)}},
					map[string]any{"id": "b", "session": map[string]any{"max_idle_minutes": float64(10)}},
				},
			},
		},
	}

	params := map[string]any{"max_idle_minutes": float64(15)}
	rule := osspecv1.Rule{
		Title: "Idle timeout",
		Check: &osspecv1.Check{
			Type:    "dataset.field_compare",
			Dataset: "okta:policies/sign-on",
			Assert: &osspecv1.Predicate{
				Path:       "/session/max_idle_minutes",
				Op:         "lte",
				ValueParam: "max_idle_minutes",
			},
		},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "r", Context{}, rule, params)
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "fail" {
		t.Fatalf("expected fail, got %q", ev.Status)
	}
	if len(ev.AffectedResourceIDs) != 1 || ev.AffectedResourceIDs[0] != "a" {
		t.Fatalf("expected affected ids [a], got %v", ev.AffectedResourceIDs)
	}
}

func TestEvalDatasetCountCompare(t *testing.T) {
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

	rule := osspecv1.Rule{
		Title: "Count active apps",
		Check: &osspecv1.Check{
			Type:    "dataset.count_compare",
			Dataset: "okta:apps",
			Where: []osspecv1.Predicate{
				{Path: "/active", Op: "eq", Value: true},
			},
			Compare: &osspecv1.Compare{Op: "eq", Value: intPtr(2)},
		},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "r", Context{}, rule, map[string]any{})
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

func TestEvalDatasetJoinCountCompare_OnUnmatchedLeftError(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"left":  {map[string]any{"id": "x"}},
				"right": {},
			},
		},
	}

	rule := osspecv1.Rule{
		Title: "Join required",
		Check: &osspecv1.Check{
			Type:            "dataset.join_count_compare",
			Left:            &osspecv1.JoinSide{Dataset: "left", KeyPath: "/id"},
			Right:           &osspecv1.JoinSide{Dataset: "right", KeyPath: "/id"},
			OnUnmatchedLeft: "error",
			Compare:         &osspecv1.Compare{Op: "eq", Value: intPtr(0)},
		},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "r", Context{}, rule, map[string]any{})
	if err != nil {
		t.Fatalf("evalCheck error: %v", err)
	}
	if ev == nil {
		t.Fatalf("expected evaluation, got nil")
	}
	if ev.Status != "error" || ev.ErrorKind != "join_unmatched" {
		t.Fatalf("expected error/join_unmatched, got %q/%q", ev.Status, ev.ErrorKind)
	}
}

func TestEvalManualAttestation_NoAttestation(t *testing.T) {
	e := &Engine{Datasets: &fakeDatasets{data: map[string][]any{}}}

	rule := osspecv1.Rule{
		Title: "Manual control",
		Check: &osspecv1.Check{Type: "manual.attestation"},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "r", Context{}, rule, map[string]any{})
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

func TestDatasetErrorPolicy_MissingDatasetCanBeError(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			errs: map[string]runtimev1.DatasetErrorKind{
				"missing": runtimev1.DatasetErrorKind_MISSING_DATASET,
			},
		},
	}

	rule := osspecv1.Rule{
		Title: "Missing dataset",
		Check: &osspecv1.Check{
			Type:             "dataset.count_compare",
			Dataset:          "missing",
			OnMissingDataset: "error",
			Compare:          &osspecv1.Compare{Op: "eq", Value: intPtr(0)},
		},
	}

	ev, err := e.evalCheck(context.Background(), "rs", "r", Context{}, rule, map[string]any{})
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

func TestValueParamMissing_ReturnsParamError(t *testing.T) {
	e := &Engine{
		Datasets: &fakeDatasets{
			data: map[string][]any{
				"okta:policies/sign-on": {
					map[string]any{"id": "a", "session": map[string]any{"max_idle_minutes": float64(10)}},
				},
			},
		},
	}

	rule := osspecv1.Rule{
		Title: "Idle timeout",
		Check: &osspecv1.Check{
			Type:    "dataset.field_compare",
			Dataset: "okta:policies/sign-on",
			Assert: &osspecv1.Predicate{
				Path:       "/session/max_idle_minutes",
				Op:         "lte",
				ValueParam: "max_idle_minutes",
			},
		},
	}

	_, err := e.evalCheck(context.Background(), "rs", "r", Context{}, rule, map[string]any{})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	var pe ParamError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ParamError, got %T: %v", err, err)
	}
}

func intPtr(v int) *int { return &v }
