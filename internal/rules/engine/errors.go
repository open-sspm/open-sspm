package engine

import (
	"errors"
	"fmt"
)

type DatasetErrorKind string

const (
	DatasetErrorMissingIntegration DatasetErrorKind = "missing_integration"
	DatasetErrorMissingDataset     DatasetErrorKind = "missing_dataset"
	DatasetErrorPermissionDenied   DatasetErrorKind = "permission_denied"
	DatasetErrorSyncFailed         DatasetErrorKind = "sync_failed"
)

type DatasetError struct {
	Kind DatasetErrorKind
	Err  error
}

func (e DatasetError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("dataset error: %s", e.Kind)
	}
	return fmt.Sprintf("dataset error: %s: %v", e.Kind, e.Err)
}

func (e DatasetError) Unwrap() error { return e.Err }

func asDatasetError(err error) (DatasetError, bool) {
	if err == nil {
		return DatasetError{}, false
	}
	var de DatasetError
	if errors.As(err, &de) {
		return de, true
	}
	return DatasetError{}, false
}
