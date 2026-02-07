package datasets

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

func runtimeResultFromRowsOrError(rows []any, err error) runtimev2.DatasetResult {
	if err != nil {
		var de engine.DatasetError
		if errors.As(err, &de) {
			msg := ""
			if de.Err != nil {
				msg = strings.TrimSpace(de.Err.Error())
			}
			return runtimev2.DatasetResult{
				Error: &runtimev2.DatasetError{
					Kind:    runtimev2.DatasetErrorKind(de.Kind),
					Message: msg,
				},
			}
		}

		return runtimev2.DatasetResult{
			Error: &runtimev2.DatasetError{
				Kind:    runtimev2.DatasetErrorKind_ENGINE_ERROR,
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
			return runtimev2.DatasetResult{
				Error: &runtimev2.DatasetError{
					Kind:    runtimev2.DatasetErrorKind_ENGINE_ERROR,
					Message: fmt.Sprintf("marshal row %d: %v", i, err),
				},
			}
		}
		rawRows = append(rawRows, json.RawMessage(b))
	}

	return runtimev2.DatasetResult{Rows: rawRows}
}
