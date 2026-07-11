package engine

import (
	"encoding/json"
	"errors"
	"fmt"
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

// ValidateParamOverrides verifies that overrides only target parameters declared
// by the rule and preserve the JSON type of each default value.
func ValidateParamOverrides(defaults, overrides map[string]any) error {
	var errs []error
	for key, override := range overrides {
		defaultValue, ok := defaults[key]
		if !ok {
			errs = append(errs, fmt.Errorf("%s: unknown parameter", key))
			continue
		}
		if err := validateParamOverrideType(defaultValue, override); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

func validateParamOverrideType(defaultValue, override any) error {
	if defaultValue == nil || override == nil {
		return nil
	}

	switch defaultValue.(type) {
	case string:
		if _, ok := override.(string); !ok {
			return fmt.Errorf("expected string, got %T", override)
		}
	case bool:
		if _, ok := override.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", override)
		}
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		if !isJSONNumber(override) {
			return fmt.Errorf("expected number, got %T", override)
		}
	case []any:
		if _, ok := override.([]any); !ok {
			return fmt.Errorf("expected array, got %T", override)
		}
	case map[string]any:
		if _, ok := override.(map[string]any); !ok {
			return fmt.Errorf("expected object, got %T", override)
		}
	default:
		return fmt.Errorf("unsupported default value type %T", defaultValue)
	}
	return nil
}

func isJSONNumber(v any) bool {
	switch v.(type) {
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return true
	default:
		return false
	}
}
