package datasets

import (
	"context"
	"errors"
	"strings"

	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

type RouterProvider struct {
	Okta       engine.DatasetProvider
	Normalized engine.DatasetProvider
}

func (p RouterProvider) GetDataset(ctx context.Context, datasetKey string, datasetVersion *int) ([]any, error) {
	key := strings.TrimSpace(datasetKey)
	switch {
	case strings.HasPrefix(key, "okta:"):
		if p.Okta == nil {
			return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: errors.New("okta dataset provider not configured")}
		}
		return p.Okta.GetDataset(ctx, key, datasetVersion)
	case strings.HasPrefix(key, "normalized:"):
		if p.Normalized == nil {
			return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: errors.New("normalized dataset provider not configured")}
		}
		return p.Normalized.GetDataset(ctx, key, datasetVersion)
	default:
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: errors.New("unknown dataset prefix")}
	}
}
