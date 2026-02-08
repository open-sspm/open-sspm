package discovery

import "time"

const (
	SignalKindIDPSSO   = "idp_sso"
	SignalKindOAuth    = "oauth_grant"
	ManagedStateManaged   = "managed"
	ManagedStateUnmanaged = "unmanaged"

	ManagedReasonActiveBindingFreshSync = "active_binding_fresh_sync"
	ManagedReasonNoBinding              = "no_binding"
	ManagedReasonConnectorDisabled      = "connector_disabled"
	ManagedReasonConnectorNotConfigured = "connector_not_configured"
	ManagedReasonStaleSync              = "stale_sync"
)

type CanonicalInput struct {
	SourceKind    string
	SourceName    string
	SourceAppID   string
	SourceAppName string
	SourceDomain  string
	EntraAppID    string
}

type AppMetadata struct {
	CanonicalKey string
	DisplayName  string
	Domain       string
	VendorName   string
}

type ManagedStateInput struct {
	HasPrimaryBinding bool
	ConnectorEnabled  bool
	ConnectorConfigured bool
	LastSuccessfulSyncAt time.Time
	HasLastSuccessfulSync bool
	FreshnessWindow time.Duration
	Now time.Time
}

type RiskInput struct {
	ManagedState         string
	HasPrivilegedScopes  bool
	HasConfidentialScopes bool
	HasOwner             bool
	Actors30d            int64
	BusinessCriticality  string
	DataClassification   string
}
