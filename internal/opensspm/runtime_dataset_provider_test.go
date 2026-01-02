package opensspm

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

type fakeEngineDatasetProvider struct {
	gotKey     string
	gotVersion *int
	rows       []any
	err        error
}

func (p *fakeEngineDatasetProvider) GetDataset(ctx context.Context, datasetKey string, datasetVersion *int) ([]any, error) {
	_ = ctx

	p.gotKey = datasetKey
	if datasetVersion == nil {
		p.gotVersion = nil
	} else {
		v := *datasetVersion
		p.gotVersion = &v
	}

	return p.rows, p.err
}

func TestRuntimeDatasetProviderAdapter_ErrorMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		adapter  RuntimeDatasetProviderAdapter
		ref      runtimev1.DatasetRef
		wantKind runtimev1.DatasetErrorKind
		wantMsg  string
	}{
		{
			name:     "nil provider",
			adapter:  RuntimeDatasetProviderAdapter{},
			ref:      runtimev1.DatasetRef{Dataset: "d"},
			wantKind: runtimev1.DatasetErrorKind_MISSING_INTEGRATION,
			wantMsg:  "dataset provider not configured",
		},
		{
			name:     "missing dataset key",
			adapter:  RuntimeDatasetProviderAdapter{Provider: &fakeEngineDatasetProvider{}},
			ref:      runtimev1.DatasetRef{},
			wantKind: runtimev1.DatasetErrorKind_MISSING_DATASET,
			wantMsg:  "dataset ref is missing dataset key",
		},
		{
			name: "engine dataset error",
			adapter: RuntimeDatasetProviderAdapter{
				Provider: &fakeEngineDatasetProvider{
					err: engine.DatasetError{
						Kind: engine.DatasetErrorPermissionDenied,
						Err:  errors.New("nope"),
					},
				},
			},
			ref:      runtimev1.DatasetRef{Dataset: "d"},
			wantKind: runtimev1.DatasetErrorKind_PERMISSION_DENIED,
			wantMsg:  "nope",
		},
		{
			name: "generic error",
			adapter: RuntimeDatasetProviderAdapter{
				Provider: &fakeEngineDatasetProvider{err: errors.New(" boom ")},
			},
			ref:      runtimev1.DatasetRef{Dataset: "d"},
			wantKind: runtimev1.DatasetErrorKind_ENGINE_ERROR,
			wantMsg:  "boom",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := tc.adapter.GetDataset(context.Background(), runtimev1.EvalContext{}, tc.ref)
			if res.Error == nil {
				t.Fatalf("expected error, got nil")
			}
			if res.Error.Kind != tc.wantKind {
				t.Fatalf("expected kind %q, got %q", tc.wantKind, res.Error.Kind)
			}
			if res.Error.Message != tc.wantMsg {
				t.Fatalf("expected message %q, got %q", tc.wantMsg, res.Error.Message)
			}
		})
	}
}

func TestRuntimeDatasetProviderAdapter_RowsAndVersion(t *testing.T) {
	t.Parallel()

	p := &fakeEngineDatasetProvider{
		rows: []any{
			json.RawMessage(`{"x":1}`),
			[]byte(`{"y":2}`),
			map[string]any{"z": 3},
		},
	}
	adapter := RuntimeDatasetProviderAdapter{Provider: p}

	res := adapter.GetDataset(context.Background(), runtimev1.EvalContext{}, runtimev1.DatasetRef{Dataset: "d", Version: 2})
	if res.Error != nil {
		t.Fatalf("expected nil error, got %v", res.Error)
	}
	if p.gotKey != "d" {
		t.Fatalf("expected dataset key %q, got %q", "d", p.gotKey)
	}
	if p.gotVersion == nil || *p.gotVersion != 2 {
		t.Fatalf("expected dataset version 2, got %v", p.gotVersion)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}

	for i, row := range res.Rows {
		if !json.Valid(row) {
			t.Fatalf("row %d is not valid JSON", i)
		}
	}
}

func TestRuntimeDatasetProviderAdapter_CapabilitiesReturnsCopy(t *testing.T) {
	t.Parallel()

	adapter := RuntimeDatasetProviderAdapter{
		CapabilitiesV: []runtimev1.DatasetRef{{Dataset: "a", Version: 1}},
	}

	caps := adapter.Capabilities(context.Background())
	if len(caps) != 1 {
		t.Fatalf("expected 1 capability, got %d", len(caps))
	}

	caps[0].Dataset = "b"
	if adapter.CapabilitiesV[0].Dataset != "a" {
		t.Fatalf("Capabilities mutated adapter state")
	}
}
