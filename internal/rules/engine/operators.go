package engine

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type ParamError struct {
	Param string
	Msg   string
}

func (e ParamError) Error() string {
	if strings.TrimSpace(e.Param) == "" {
		return e.Msg
	}
	if strings.TrimSpace(e.Msg) == "" {
		return fmt.Sprintf("invalid params: %s", e.Param)
	}
	return fmt.Sprintf("invalid params: %s: %s", e.Param, e.Msg)
}

type predicateValue struct {
	Value      any
	ValueParam string
}

func resolvePredicateValue(v predicateValue, params map[string]any) (any, error) {
	if strings.TrimSpace(v.ValueParam) == "" {
		return v.Value, nil
	}
	if params == nil {
		return nil, ParamError{Param: v.ValueParam, Msg: "missing parameters"}
	}
	p, ok := params[v.ValueParam]
	if !ok {
		return nil, ParamError{Param: v.ValueParam, Msg: "missing parameters.defaults value"}
	}
	return p, nil
}

func evalSinglePredicate(doc any, path string, op string, v predicateValue, params map[string]any) (bool, error) {
	val, ok, err := getByJSONPointer(doc, path)
	if err != nil {
		return false, err
	}

	switch strings.TrimSpace(op) {
	case "exists":
		return ok, nil
	case "absent":
		return !ok, nil
	}

	if !ok {
		return false, nil
	}

	want, err := resolvePredicateValue(v, params)
	if err != nil {
		return false, err
	}

	return evalOp(val, strings.TrimSpace(op), want)
}

func evalOp(actual any, op string, expected any) (bool, error) {
	switch op {
	case "eq":
		return valuesEqual(actual, expected), nil
	case "neq":
		return !valuesEqual(actual, expected), nil
	case "lt", "lte", "gt", "gte":
		cmp, ok := compareOrdered(actual, expected)
		if !ok {
			return false, nil
		}
		switch op {
		case "lt":
			return cmp < 0, nil
		case "lte":
			return cmp <= 0, nil
		case "gt":
			return cmp > 0, nil
		case "gte":
			return cmp >= 0, nil
		default:
			return false, nil
		}
	case "in":
		arr, ok := expected.([]any)
		if !ok {
			return false, errors.New("in expects array value")
		}
		for _, v := range arr {
			if valuesEqual(actual, v) {
				return true, nil
			}
		}
		return false, nil
	case "contains":
		switch a := actual.(type) {
		case []any:
			for _, v := range a {
				if valuesEqual(v, expected) {
					return true, nil
				}
			}
			return false, nil
		case string:
			s, ok := expected.(string)
			if !ok {
				return false, errors.New("contains expects string value when actual is string")
			}
			return strings.Contains(a, s), nil
		default:
			return false, nil
		}
	default:
		return false, fmt.Errorf("unsupported op %q", op)
	}
}

func valuesEqual(a, b any) bool {
	if af, ok := asFloat(a); ok {
		if bf, ok := asFloat(b); ok {
			return af == bf
		}
	}
	return reflect.DeepEqual(a, b)
}

func compareOrdered(a, b any) (int, bool) {
	if af, ok := asFloat(a); ok {
		if bf, ok := asFloat(b); ok {
			switch {
			case af < bf:
				return -1, true
			case af > bf:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	as, okA := a.(string)
	bs, okB := b.(string)
	if okA && okB {
		switch {
		case as < bs:
			return -1, true
		case as > bs:
			return 1, true
		default:
			return 0, true
		}
	}
	return 0, false
}
