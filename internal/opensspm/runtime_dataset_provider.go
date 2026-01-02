package opensspm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

// RuntimeDatasetProviderAdapter adapts our legacy dataset provider interface
// (internal/rules/engine.DatasetProvider) to the Open SSPM runtime DatasetProvider.
//
// This lets the rules engine evaluate checks using the Open SSPM dataset ABI
// without rewriting existing connector dataset providers.
type RuntimeDatasetProviderAdapter struct {
	Provider      engine.DatasetProvider
	CapabilitiesV []runtimev1.DatasetRef
}

func (p RuntimeDatasetProviderAdapter) Capabilities(ctx context.Context) []runtimev1.DatasetRef {
	_ = ctx
	return append([]runtimev1.DatasetRef(nil), p.CapabilitiesV...)
}

func (p RuntimeDatasetProviderAdapter) GetDataset(ctx context.Context, eval runtimev1.EvalContext, ref runtimev1.DatasetRef) runtimev1.DatasetResult {
	_ = eval

	if p.Provider == nil {
		return runtimev1.DatasetResult{
			Error: &runtimev1.DatasetError{
				Kind:    runtimev1.DatasetErrorKind_MISSING_INTEGRATION,
				Message: "dataset provider not configured",
			},
		}
	}

	datasetKey := strings.TrimSpace(ref.Dataset)
	if datasetKey == "" {
		return runtimev1.DatasetResult{
			Error: &runtimev1.DatasetError{
				Kind:    runtimev1.DatasetErrorKind_MISSING_DATASET,
				Message: "dataset ref is missing dataset key",
			},
		}
	}

	version := ref.Version
	var versionPtr *int
	if version > 0 {
		versionPtr = &version
	}

	rows, err := p.Provider.GetDataset(ctx, datasetKey, versionPtr)
	if err != nil {
		var de engine.DatasetError
		if errors.As(err, &de) {
			msg := ""
			if de.Err != nil {
				msg = strings.TrimSpace(de.Err.Error())
			}
			return runtimev1.DatasetResult{
				Error: &runtimev1.DatasetError{
					Kind:    runtimev1.DatasetErrorKind(de.Kind),
					Message: msg,
				},
			}
		}

		return runtimev1.DatasetResult{
			Error: &runtimev1.DatasetError{
				Kind:    runtimev1.DatasetErrorKind_ENGINE_ERROR,
				Message: strings.TrimSpace(err.Error()),
			},
		}
	}

	if rows == nil {
		rows = []any{}
	}

	rawRows := make([]json.RawMessage, 0, len(rows))
	for i, row := range rows {
		switch v := row.(type) {
		case json.RawMessage:
			rawRows = append(rawRows, v)
			continue
		case []byte:
			rawRows = append(rawRows, json.RawMessage(v))
			continue
		default:
		}

		b, err := json.Marshal(row)
		if err != nil {
			return runtimev1.DatasetResult{
				Error: &runtimev1.DatasetError{
					Kind:    runtimev1.DatasetErrorKind_ENGINE_ERROR,
					Message: fmt.Sprintf("marshal row %d: %v", i, err),
				},
			}
		}
		rawRows = append(rawRows, json.RawMessage(b))
	}

	return runtimev1.DatasetResult{Rows: rawRows}
}
