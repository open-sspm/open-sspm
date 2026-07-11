package engine

import (
	"strings"
	"testing"
)

func TestValidateParamOverrides(t *testing.T) {
	t.Parallel()

	defaults := map[string]any{
		"enabled": true,
		"limit":   float64(15),
		"labels":  []any{"admin"},
		"options": map[string]any{"strict": true},
	}

	if err := ValidateParamOverrides(defaults, map[string]any{
		"enabled": false,
		"limit":   float64(30),
		"labels":  []any{"owner"},
		"options": map[string]any{"strict": false},
	}); err != nil {
		t.Fatalf("ValidateParamOverrides() error = %v", err)
	}

	tests := []struct {
		name     string
		override map[string]any
		want     string
	}{
		{name: "unknown key", override: map[string]any{"missing": true}, want: "unknown parameter"},
		{name: "wrong scalar type", override: map[string]any{"enabled": "true"}, want: "expected boolean"},
		{name: "wrong collection type", override: map[string]any{"labels": map[string]any{}}, want: "expected array"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateParamOverrides(defaults, tt.override)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateParamOverrides() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}
