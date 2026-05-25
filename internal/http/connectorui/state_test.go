package connectorui

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type testDefinition struct {
	kind string
	role registry.IntegrationRole
}

func (d testDefinition) Kind() string                                     { return d.kind }
func (d testDefinition) DisplayName() string                              { return d.kind }
func (d testDefinition) Role() registry.IntegrationRole                   { return d.role }
func (d testDefinition) DecodeConfig([]byte) (any, error)                 { return nil, nil }
func (d testDefinition) ValidateConfig(any) error                         { return nil }
func (d testDefinition) IsConfigured(any) bool                            { return false }
func (d testDefinition) SourceName(any) string                            { return "" }
func (d testDefinition) DefaultSubtitle() string                          { return "default subtitle" }
func (d testDefinition) ConfiguredSubtitle(any) string                    { return "configured subtitle" }
func (d testDefinition) SettingsHref() string                             { return "/settings/connectors/" + d.kind + "/dialog" }
func (d testDefinition) MetricsProvider() registry.MetricsProvider        { return nil }
func (d testDefinition) NewIntegration(any) (registry.Integration, error) { return nil, nil }

func TestStatePresenterActionsForConfiguredOkta(t *testing.T) {
	p := NewStatePresenter(registry.ConnectorState{
		Definition: testDefinition{kind: "okta", role: registry.RoleIdP},
		Configured: true,
		Enabled:    true,
		SourceName: "example.okta.com",
		Metrics: &registry.ConnectorMetrics{
			Total: 42,
			Extras: map[string]int64{
				"apps": 7,
			},
		},
	})

	if got := p.PrimaryHref(); got != "/accounts/okta" {
		t.Fatalf("PrimaryHref() = %q, want /accounts/okta", got)
	}
	if got := p.SecondaryHref(); got != "/assigned-apps" {
		t.Fatalf("SecondaryHref() = %q, want /assigned-apps", got)
	}
	if got := p.SecondaryLabel(); got != "Browse apps" {
		t.Fatalf("SecondaryLabel() = %q, want Browse apps", got)
	}
	if got := p.Subtitle(); got != "configured subtitle" {
		t.Fatalf("Subtitle() = %q, want configured subtitle", got)
	}

	metrics := p.MetricsKV()
	if len(metrics) != 3 || metrics[0].Label != "Users" || metrics[0].Value != "42" || metrics[1].Label != "Apps" || metrics[1].Value != "7" {
		t.Fatalf("MetricsKV() = %+v", metrics)
	}
}

func TestStatePresenterStatusClasses(t *testing.T) {
	tests := []struct {
		name  string
		state registry.ConnectorState
		want  string
	}{
		{
			name:  "invalid",
			state: registry.ConnectorState{ConfigError: "bad"},
			want:  "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100",
		},
		{
			name:  "unconfigured",
			state: registry.ConnectorState{},
			want:  "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100",
		},
		{
			name:  "disabled",
			state: registry.ConnectorState{Configured: true},
			want:  "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100",
		},
		{
			name:  "enabled",
			state: registry.ConnectorState{Configured: true, Enabled: true},
			want:  "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.Definition = testDefinition{kind: "github"}
			if got := NewStatePresenter(tt.state).StatusClass(); got != tt.want {
				t.Fatalf("StatusClass() = %q, want %q", got, tt.want)
			}
		})
	}
}
