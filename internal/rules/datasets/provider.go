package datasets

import (
	"context"
	"strings"

	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
)

type RouterProvider struct {
	Okta       runtimev2.DatasetProvider
	Normalized runtimev2.DatasetProvider
}

func (p RouterProvider) Capabilities(ctx context.Context) []runtimev2.DatasetRef {
	_ = ctx
	return RuntimeCapabilities(p)
}

func (p RouterProvider) GetDataset(ctx context.Context, eval runtimev2.EvalContext, ref runtimev2.DatasetRef) runtimev2.DatasetResult {
	_ = eval

	key := strings.TrimSpace(ref.Dataset)
	if key == "" {
		return runtimev2.DatasetResult{
			Error: &runtimev2.DatasetError{
				Kind:    runtimev2.DatasetErrorKind_MISSING_DATASET,
				Message: "dataset ref is missing dataset key",
			},
		}
	}

	switch {
	case strings.HasPrefix(key, "okta:"):
		if p.Okta == nil {
			return runtimev2.DatasetResult{
				Error: &runtimev2.DatasetError{
					Kind:    runtimev2.DatasetErrorKind_MISSING_DATASET,
					Message: "okta dataset provider not configured",
				},
			}
		}
		return p.Okta.GetDataset(ctx, eval, ref)
	case strings.HasPrefix(key, "normalized:"):
		if p.Normalized == nil {
			return runtimev2.DatasetResult{
				Error: &runtimev2.DatasetError{
					Kind:    runtimev2.DatasetErrorKind_MISSING_DATASET,
					Message: "normalized dataset provider not configured",
				},
			}
		}
		return p.Normalized.GetDataset(ctx, eval, ref)
	default:
		return runtimev2.DatasetResult{
			Error: &runtimev2.DatasetError{
				Kind:    runtimev2.DatasetErrorKind_MISSING_DATASET,
				Message: "unknown dataset prefix",
			},
		}
	}
}
