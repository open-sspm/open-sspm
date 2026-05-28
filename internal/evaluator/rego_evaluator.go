package evaluator

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/open-policy-agent/opa/v1/rego"
	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
)

type entityPolicyEvaluator struct {
	query rego.PreparedEvalQuery
}

func prepareEntityPolicyEvaluator(name string, pack PolicyPack) (entityPolicyEvaluator, error) {
	query, err := rego.New(
		rego.Query(pack.Policy.Query),
		rego.Module(pack.Metadata.ID+".rego", pack.Policy.Rego),
		rego.Strict(true),
		rego.StrictBuiltinErrors(true),
	).PrepareForEval(context.Background())
	if err != nil {
		return entityPolicyEvaluator{}, fmt.Errorf("%s: prepare Rego policy: %w", name, err)
	}
	return entityPolicyEvaluator{query: query}, nil
}

func (e entityPolicyEvaluator) Evaluate(pack PolicyPack, entity map[string]any) (osspecv2.EntityPolicyEvaluateResult, error) {
	result, err := e.evaluateRego(map[string]any{
		"entity": entity,
		"policy": map[string]any{
			"id":     pack.Metadata.ID,
			"domain": pack.Metadata.Domain,
		},
	})
	if err != nil {
		return osspecv2.EntityPolicyEvaluateResult{}, err
	}
	return entityPolicyEvaluateResultFromMap(result)
}

func (e entityPolicyEvaluator) evaluateRego(input any) (map[string]any, error) {
	rs, err := e.query.Eval(context.Background(), rego.EvalInput(input))
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 || len(rs[0].Expressions) == 0 {
		return nil, fmt.Errorf("rego query is undefined")
	}
	if len(rs) != 1 || len(rs[0].Expressions) != 1 {
		return nil, fmt.Errorf("rego query must return exactly one object result, got %d results", len(rs))
	}
	result, ok := rs[0].Expressions[0].Value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("rego query must return object, got %T", rs[0].Expressions[0].Value)
	}
	return result, nil
}

func entityPolicyEvaluateResultFromMap(result map[string]any) (osspecv2.EntityPolicyEvaluateResult, error) {
	out := osspecv2.EntityPolicyEvaluateResult{
		RiskLevel: strings.TrimSpace(fmt.Sprint(result["risk_level"])),
	}
	out.RiskScore, _ = intFromAny(result["risk_score"])
	signals, ok := result["signals"].([]any)
	if !ok {
		return out, nil
	}
	for _, raw := range signals {
		signal, ok := raw.(map[string]any)
		if !ok {
			return osspecv2.EntityPolicyEvaluateResult{}, fmt.Errorf("entity policy signal must be object, got %T", raw)
		}
		out.Signals = append(out.Signals, osspecv2.EntityPolicyTestSignal{
			ID:       strings.TrimSpace(fmt.Sprint(signal["id"])),
			Severity: strings.TrimSpace(fmt.Sprint(signal["severity"])),
			Title:    strings.TrimSpace(fmt.Sprint(signal["title"])),
		})
	}
	return out, nil
}

func intFromAny(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return intFromInt64(v)
	case float64:
		return intFromFloat64(v)
	case json.Number:
		return intFromString(v.String())
	case string:
		return intFromString(v)
	default:
		return 0, false
	}
}

func intFromString(value string) (int, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if i, err := strconv.ParseInt(value, 10, strconv.IntSize); err == nil {
		return intFromInt64(i)
	}
	if strings.Contains(value, "/") {
		return 0, false
	}
	rat, ok := new(big.Rat).SetString(value)
	if !ok || !rat.IsInt() {
		return 0, false
	}
	if rat.Cmp(big.NewRat(int64(math.MinInt), 1)) < 0 || rat.Cmp(big.NewRat(int64(math.MaxInt), 1)) > 0 {
		return 0, false
	}
	return intFromInt64(rat.Num().Int64())
}

func intFromInt64(value int64) (int, bool) {
	if value < int64(math.MinInt) || value > int64(math.MaxInt) {
		return 0, false
	}
	return int(value), true
}

func intFromFloat64(value float64) (int, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value {
		return 0, false
	}
	return intFromInt64(int64(value))
}
