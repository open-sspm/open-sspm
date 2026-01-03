package datasets

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

func runtimeResultFromRowsOrError(rows []any, err error) runtimev1.DatasetResult {
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
