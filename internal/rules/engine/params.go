package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"

	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
)

func parseJSONObject(b []byte) (map[string]any, error) {
	if len(b) == 0 {
		return map[string]any{}, nil
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	if v == nil {
		return map[string]any{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected json object, got %T", v)
	}
	return m, nil
}

func deepMerge(dst, src map[string]any) map[string]any {
	out := make(map[string]any, len(dst)+len(src))
	for k, v := range dst {
		out[k] = deepCopyJSONValue(v)
	}
	for k, v := range src {
		if vMap, ok := v.(map[string]any); ok {
			if existing, ok := out[k].(map[string]any); ok {
				out[k] = deepMerge(existing, vMap)
			} else {
				out[k] = deepCopyJSONValue(vMap)
			}
			continue
		}
		out[k] = deepCopyJSONValue(v)
	}
	return out
}

func deepCopyJSONValue(v any) any {
	switch vv := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(vv))
		for k, v := range vv {
			out[k] = deepCopyJSONValue(v)
		}
		return out
	case []any:
		out := make([]any, len(vv))
		for i := range vv {
			out[i] = deepCopyJSONValue(vv[i])
		}
		return out
	default:
		return v
	}
}

func validateParams(params map[string]any, schema map[string]osspecv2.ParameterSchema) error {
	var errs []error
	for key, sch := range schema {
		val, ok := params[key]
		if !ok {
			continue
		}
		if err := validateParamValue(val, sch); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

func ValidateParams(params map[string]any, schema map[string]osspecv2.ParameterSchema) error {
	return validateParams(params, schema)
}

func validateParamValue(val any, sch osspecv2.ParameterSchema) error {
	switch sch.Type {
	case "string":
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected string, got %T", val)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", val)
		}
	case "integer":
		n, ok := asFloat(val)
		if !ok {
			return fmt.Errorf("expected integer, got %T", val)
		}
		if math.Trunc(n) != n {
			return fmt.Errorf("expected integer, got %v", n)
		}
		if sch.Minimum != nil && n < *sch.Minimum {
			return fmt.Errorf("must be >= %v", *sch.Minimum)
		}
		if sch.Maximum != nil && n > *sch.Maximum {
			return fmt.Errorf("must be <= %v", *sch.Maximum)
		}
	case "number":
		n, ok := asFloat(val)
		if !ok {
			return fmt.Errorf("expected number, got %T", val)
		}
		if sch.Minimum != nil && n < *sch.Minimum {
			return fmt.Errorf("must be >= %v", *sch.Minimum)
		}
		if sch.Maximum != nil && n > *sch.Maximum {
			return fmt.Errorf("must be <= %v", *sch.Maximum)
		}
	case "array":
		if _, ok := val.([]any); !ok {
			return fmt.Errorf("expected array, got %T", val)
		}
	case "object":
		if _, ok := val.(map[string]any); !ok {
			return fmt.Errorf("expected object, got %T", val)
		}
	default:
		return fmt.Errorf("unsupported schema type %q", sch.Type)
	}

	if len(sch.Enum) > 0 {
		var ok bool
		for _, allowed := range sch.Enum {
			if reflect.DeepEqual(val, allowed) {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("must be one of %v", sch.Enum)
		}
	}

	return nil
}

func asFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
}
