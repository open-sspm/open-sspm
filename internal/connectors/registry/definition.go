package registry

// ConnectorDefinition defines the behavior and metadata for a connector.
type ConnectorDefinition interface {
	// Identity
	Kind() string          // e.g., "github", "okta"
	DisplayName() string   // e.g., "GitHub", "Okta"
	Role() IntegrationRole // IdP or App

	// Configuration
	DecodeConfig(raw []byte) (any, error)
	ValidateConfig(cfg any) error
	IsConfigured(cfg any) bool
	SourceName(cfg any) string // e.g., org name, domain

	// UI Metadata
	DefaultSubtitle() string
	ConfiguredSubtitle(cfg any) string
	SettingsHref() string

	// Metrics (optional - returns nil if not applicable)
	MetricsProvider() MetricsProvider

	// Sync Integration (optional - returns nil if not syncable)
	NewIntegration(cfg any) (Integration, error)
}
