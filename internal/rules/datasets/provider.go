package datasets

import (
	"context"
	"strings"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
)

type RouterProvider struct {
	Okta       runtimev1.DatasetProvider
	Normalized runtimev1.DatasetProvider
}

func (p RouterProvider) Capabilities(ctx context.Context) []runtimev1.DatasetRef {
	_ = ctx
	return RuntimeCapabilities(p)
}

func (p RouterProvider) GetDataset(ctx context.Context, eval runtimev1.EvalContext, ref runtimev1.DatasetRef) runtimev1.DatasetResult {
	_ = eval

	key := strings.TrimSpace(ref.Dataset)
	if key == "" {
		return runtimev1.DatasetResult{
			Error: &runtimev1.DatasetError{
				Kind:    runtimev1.DatasetErrorKind_MISSING_DATASET,
				Message: "dataset ref is missing dataset key",
			},
		}
	}

	switch {
	case strings.HasPrefix(key, "okta:"):
		if p.Okta == nil {
			return runtimev1.DatasetResult{
				Error: &runtimev1.DatasetError{
					Kind:    runtimev1.DatasetErrorKind_MISSING_DATASET,
					Message: "okta dataset provider not configured",
				},
			}
		}
		return p.Okta.GetDataset(ctx, eval, ref)
	case strings.HasPrefix(key, "normalized:"):
		if p.Normalized == nil {
			return runtimev1.DatasetResult{
				Error: &runtimev1.DatasetError{
					Kind:    runtimev1.DatasetErrorKind_MISSING_DATASET,
					Message: "normalized dataset provider not configured",
				},
			}
		}
		return p.Normalized.GetDataset(ctx, eval, ref)
	default:
		return runtimev1.DatasetResult{
			Error: &runtimev1.DatasetError{
				Kind:    runtimev1.DatasetErrorKind_MISSING_DATASET,
				Message: "unknown dataset prefix",
			},
		}
	}
}
