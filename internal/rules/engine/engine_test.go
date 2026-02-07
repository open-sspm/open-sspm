package engine

import (
	"context"
	"encoding/json"
	"errors"
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

	if f.data == nil {
		return runtimev2.DatasetResult{Error: &runtimev2.DatasetError{Kind: runtimev2.DatasetErrorKind_MISSING_DATASET}}
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

	rule := osspecv2.Rule{
		Title: "Idle timeout",
		Parameters: &osspecv2.Parameters{
			Defaults: map[string]any{"max_idle_minutes": float64(15)},
		},
		Check: &osspecv2.Check{
			Type:    "dataset.field_compare",
			Dataset: "okta:policies/sign-on",
			Assert: &osspecv2.Predicate{
				Path:       "/session/max_idle_minutes",
				Op:         "lte",
				ValueParam: "max_idle_minutes",
			},
			Expect: &osspecv2.FieldCompareExpect{
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
	rule := osspecv2.Rule{
		Title: "Idle timeout",
		Check: &osspecv2.Check{
			Type:    "dataset.field_compare",
			Dataset: "okta:policies/sign-on",
			Assert: &osspecv2.Predicate{
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

	rule := osspecv2.Rule{
		Title: "Count active apps",
		Check: &osspecv2.Check{
			Type:    "dataset.count_compare",
			Dataset: "okta:apps",
			Where: []osspecv2.Predicate{
				{Path: "/active", Op: "eq", Value: true},
			},
			Compare: &osspecv2.Compare{Op: "eq", Value: intPtr(2)},
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

	rule := osspecv2.Rule{
		Title: "Join required",
		Check: &osspecv2.Check{
			Type:            "dataset.join_count_compare",
			Left:            &osspecv2.JoinSide{Dataset: "left", KeyPath: "/id"},
			Right:           &osspecv2.JoinSide{Dataset: "right", KeyPath: "/id"},
			OnUnmatchedLeft: "error",
			Compare:         &osspecv2.Compare{Op: "eq", Value: intPtr(0)},
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

	rule := osspecv2.Rule{
		Title: "Manual control",
		Check: &osspecv2.Check{Type: "manual.attestation"},
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
			errs: map[string]runtimev2.DatasetErrorKind{
				"missing": runtimev2.DatasetErrorKind_MISSING_DATASET,
			},
		},
	}

	rule := osspecv2.Rule{
		Title: "Missing dataset",
		Check: &osspecv2.Check{
			Type:             "dataset.count_compare",
			Dataset:          "missing",
			OnMissingDataset: "error",
			Compare:          &osspecv2.Compare{Op: "eq", Value: intPtr(0)},
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

	rule := osspecv2.Rule{
		Title: "Idle timeout",
		Check: &osspecv2.Check{
			Type:    "dataset.field_compare",
			Dataset: "okta:policies/sign-on",
			Assert: &osspecv2.Predicate{
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
