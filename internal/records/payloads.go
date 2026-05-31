package records

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type ResourcePayload interface {
	ResourceName() ResourceName
	SchemaVersion() int
	Validate() error
	ToEnvelope() ResourceEnvelope
}

type ProviderFields map[string]any

type ResourceEnvelope struct {
	Resource      ResourceName   `json:"resource"`
	SchemaVersion int            `json:"schema_version"`
	ExternalID    string         `json:"external_id"`
	DisplayName   string         `json:"display_name,omitempty"`
	Provider      ProviderFields `json:"provider"`
	Attributes    map[string]any `json:"attributes"`
	Raw           map[string]any `json:"raw,omitempty"`
}

type ResourceRef struct {
	Resource    ResourceName `json:"resource"`
	ExternalID  string       `json:"external_id"`
	DisplayName string       `json:"display_name,omitempty"`
}

type IdentityPayload struct {
	ExternalID    string
	Email         string
	DisplayName   string
	Status        string
	IsPrivileged  bool
	Groups        []ResourceRef
	ProviderAttrs map[string]any
	Raw           map[string]any
}

func (p IdentityPayload) ResourceName() ResourceName { return ResourceIdentity }
func (p IdentityPayload) SchemaVersion() int         { return 1 }

func (p IdentityPayload) Validate() error {
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("identity external_id is required")
	}
	return nil
}

func (p IdentityPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["email"] = strings.TrimSpace(p.Email)
	attrs["status"] = strings.TrimSpace(p.Status)
	attrs["is_privileged"] = p.IsPrivileged
	if len(p.Groups) > 0 {
		attrs["groups"] = p.Groups
	}
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.DisplayName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

type GroupPayload struct {
	ExternalID    string
	DisplayName   string
	Type          string
	ProviderAttrs map[string]any
	Raw           map[string]any
}

func (p GroupPayload) ResourceName() ResourceName { return ResourceGroup }
func (p GroupPayload) SchemaVersion() int         { return 1 }

func (p GroupPayload) Validate() error {
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("group external_id is required")
	}
	return nil
}

func (p GroupPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["type"] = strings.TrimSpace(p.Type)
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.DisplayName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

type ApplicationPayload struct {
	ExternalID    string
	DisplayName   string
	Name          string
	Status        string
	SignOnMode    string
	ProviderAttrs map[string]any
	Raw           map[string]any
}

func (p ApplicationPayload) ResourceName() ResourceName { return ResourceApplication }
func (p ApplicationPayload) SchemaVersion() int         { return 1 }

func (p ApplicationPayload) Validate() error {
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("application external_id is required")
	}
	return nil
}

func (p ApplicationPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["name"] = strings.TrimSpace(p.Name)
	attrs["status"] = strings.TrimSpace(p.Status)
	attrs["sign_on_mode"] = strings.TrimSpace(p.SignOnMode)
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.DisplayName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

type ServicePrincipalPayload struct {
	ExternalID    string
	Email         string
	DisplayName   string
	Status        string
	ProviderAttrs map[string]any
	Raw           map[string]any
}

func (p ServicePrincipalPayload) ResourceName() ResourceName { return ResourceServicePrincipal }
func (p ServicePrincipalPayload) SchemaVersion() int         { return 1 }

func (p ServicePrincipalPayload) Validate() error {
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("service principal external_id is required")
	}
	return nil
}

func (p ServicePrincipalPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["email"] = strings.TrimSpace(p.Email)
	attrs["status"] = strings.TrimSpace(p.Status)
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.DisplayName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

type AppAssetPayload struct {
	AssetKind        string
	ExternalID       string
	ParentExternalID string
	DisplayName      string
	Status           string
	CreatedAtSource  time.Time
	UpdatedAtSource  time.Time
	ProviderAttrs    map[string]any
	Raw              map[string]any
}

func (p AppAssetPayload) ResourceName() ResourceName { return ResourceAppAsset }
func (p AppAssetPayload) SchemaVersion() int         { return 1 }

func (p AppAssetPayload) Validate() error {
	if strings.TrimSpace(p.AssetKind) == "" {
		return errors.New("app asset kind is required")
	}
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("app asset external_id is required")
	}
	return nil
}

func (p AppAssetPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["asset_kind"] = strings.TrimSpace(p.AssetKind)
	attrs["parent_external_id"] = strings.TrimSpace(p.ParentExternalID)
	attrs["status"] = strings.TrimSpace(p.Status)
	if !p.CreatedAtSource.IsZero() {
		attrs["created_at_source"] = p.CreatedAtSource.UTC()
	}
	if !p.UpdatedAtSource.IsZero() {
		attrs["updated_at_source"] = p.UpdatedAtSource.UTC()
	}
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.DisplayName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

type AppAssetOwnerPayload struct {
	AssetKind        string
	AssetExternalID  string
	OwnerKind        string
	OwnerExternalID  string
	OwnerDisplayName string
	OwnerEmail       string
	ProviderAttrs    map[string]any
	Raw              map[string]any
}

func (p AppAssetOwnerPayload) ResourceName() ResourceName { return ResourceAppAssetOwner }
func (p AppAssetOwnerPayload) SchemaVersion() int         { return 1 }

func (p AppAssetOwnerPayload) Validate() error {
	if strings.TrimSpace(p.AssetKind) == "" || strings.TrimSpace(p.AssetExternalID) == "" {
		return errors.New("app asset owner asset ref is required")
	}
	if strings.TrimSpace(p.OwnerKind) == "" || strings.TrimSpace(p.OwnerExternalID) == "" {
		return errors.New("app asset owner ref is required")
	}
	return nil
}

func (p AppAssetOwnerPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["asset_kind"] = strings.TrimSpace(p.AssetKind)
	attrs["asset_external_id"] = strings.TrimSpace(p.AssetExternalID)
	attrs["owner_kind"] = strings.TrimSpace(p.OwnerKind)
	attrs["owner_external_id"] = strings.TrimSpace(p.OwnerExternalID)
	attrs["owner_email"] = strings.TrimSpace(p.OwnerEmail)
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.AssetKind) + ":" + strings.TrimSpace(p.AssetExternalID) + ":" + strings.TrimSpace(p.OwnerKind) + ":" + strings.TrimSpace(p.OwnerExternalID),
		DisplayName:   strings.TrimSpace(p.OwnerDisplayName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

type PrincipalRef struct {
	Kind        string `json:"kind,omitempty"`
	ExternalID  string `json:"external_id,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

type CredentialPayload struct {
	AssetRefKind       string
	AssetRefExternalID string
	CredentialKind     string
	ExternalID         string
	DisplayName        string
	Fingerprint        string
	ScopeJSON          []byte
	Status             string
	CreatedAtSource    time.Time
	ExpiresAtSource    time.Time
	LastUsedAtSource   time.Time
	CreatedBy          PrincipalRef
	ApprovedBy         PrincipalRef
	ProviderAttrs      map[string]any
	Raw                map[string]any
}

func (p CredentialPayload) ResourceName() ResourceName { return ResourceCredential }
func (p CredentialPayload) SchemaVersion() int         { return 1 }

func (p CredentialPayload) Validate() error {
	if strings.TrimSpace(p.AssetRefKind) == "" || strings.TrimSpace(p.AssetRefExternalID) == "" {
		return errors.New("credential asset ref is required")
	}
	if strings.TrimSpace(p.CredentialKind) == "" {
		return errors.New("credential kind is required")
	}
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("credential external_id is required")
	}
	return nil
}

func (p CredentialPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["asset_ref_kind"] = strings.TrimSpace(p.AssetRefKind)
	attrs["asset_ref_external_id"] = strings.TrimSpace(p.AssetRefExternalID)
	attrs["credential_kind"] = strings.TrimSpace(p.CredentialKind)
	attrs["fingerprint"] = strings.TrimSpace(p.Fingerprint)
	attrs["status"] = strings.TrimSpace(p.Status)
	if len(p.ScopeJSON) > 0 {
		attrs["scope"] = json.RawMessage(p.ScopeJSON)
	}
	if !p.CreatedAtSource.IsZero() {
		attrs["created_at_source"] = p.CreatedAtSource.UTC()
	}
	if !p.ExpiresAtSource.IsZero() {
		attrs["expires_at_source"] = p.ExpiresAtSource.UTC()
	}
	if !p.LastUsedAtSource.IsZero() {
		attrs["last_used_at_source"] = p.LastUsedAtSource.UTC()
	}
	if p.CreatedBy != (PrincipalRef{}) {
		attrs["created_by"] = p.CreatedBy
	}
	if p.ApprovedBy != (PrincipalRef{}) {
		attrs["approved_by"] = p.ApprovedBy
	}
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.DisplayName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

type CredentialAuditEventPayload struct {
	EventExternalID      string
	EventType            string
	EventTime            time.Time
	ActorKind            string
	ActorExternalID      string
	ActorDisplayName     string
	TargetKind           string
	TargetExternalID     string
	TargetDisplayName    string
	CredentialKind       string
	CredentialExternalID string
	ProviderAttrs        map[string]any
	Raw                  map[string]any
}

func (p CredentialAuditEventPayload) ResourceName() ResourceName { return ResourceAuditEvent }
func (p CredentialAuditEventPayload) SchemaVersion() int         { return 1 }

func (p CredentialAuditEventPayload) Validate() error {
	if strings.TrimSpace(p.EventExternalID) == "" {
		return errors.New("credential audit event external_id is required")
	}
	if p.EventTime.IsZero() {
		return errors.New("credential audit event time is required")
	}
	return nil
}

func (p CredentialAuditEventPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["event_type"] = strings.TrimSpace(p.EventType)
	attrs["event_time"] = p.EventTime.UTC()
	attrs["actor_kind"] = strings.TrimSpace(p.ActorKind)
	attrs["actor_external_id"] = strings.TrimSpace(p.ActorExternalID)
	attrs["actor_display_name"] = strings.TrimSpace(p.ActorDisplayName)
	attrs["target_kind"] = strings.TrimSpace(p.TargetKind)
	attrs["target_external_id"] = strings.TrimSpace(p.TargetExternalID)
	attrs["target_display_name"] = strings.TrimSpace(p.TargetDisplayName)
	attrs["credential_kind"] = strings.TrimSpace(p.CredentialKind)
	attrs["credential_external_id"] = strings.TrimSpace(p.CredentialExternalID)
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.EventExternalID),
		DisplayName:   strings.TrimSpace(p.EventType),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

const (
	EntitlementKindOktaGroupMembership    = "okta_group_membership"
	EntitlementKindOktaAppUserAssignment  = "okta_app_user_assignment"
	EntitlementKindOktaAppGroupAssignment = "okta_app_group_assignment"
)

type EntitlementPayload struct {
	ExternalID    string
	Kind          string
	Subject       ResourceRef
	Target        ResourceRef
	Permission    string
	Scope         string
	Priority      int
	Profile       map[string]any
	ProviderAttrs map[string]any
	Raw           map[string]any
}

func (p EntitlementPayload) ResourceName() ResourceName { return ResourceEntitlement }
func (p EntitlementPayload) SchemaVersion() int         { return 1 }

func (p EntitlementPayload) Validate() error {
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("entitlement external_id is required")
	}
	if strings.TrimSpace(p.Kind) == "" {
		return errors.New("entitlement kind is required")
	}
	if strings.TrimSpace(string(p.Subject.Resource)) == "" || strings.TrimSpace(p.Subject.ExternalID) == "" {
		return errors.New("entitlement subject is required")
	}
	if strings.TrimSpace(string(p.Target.Resource)) == "" || strings.TrimSpace(p.Target.ExternalID) == "" {
		return errors.New("entitlement target is required")
	}
	return nil
}

func (p EntitlementPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["kind"] = strings.TrimSpace(p.Kind)
	attrs["subject"] = p.Subject
	attrs["target"] = p.Target
	attrs["permission"] = strings.TrimSpace(p.Permission)
	attrs["scope"] = strings.TrimSpace(p.Scope)
	attrs["priority"] = p.Priority
	if len(p.Profile) > 0 {
		attrs["profile"] = copyMap(p.Profile)
	}
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.ExternalID),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

const (
	DiscoveryEvidenceKindSource = "source"
	DiscoveryEvidenceKindEvent  = "event"
)

type DiscoveryEvidencePayload struct {
	ExternalID       string
	Kind             string
	CanonicalKey     string
	SignalKind       string
	EventExternalID  string
	SourceAppID      string
	SourceAppName    string
	SourceAppDomain  string
	SourceVendorName string
	SourceCategory   string
	ActorExternalID  string
	ActorEmail       string
	ActorDisplayName string
	ObservedAt       time.Time
	Scopes           []string
	ProviderAttrs    map[string]any
	Raw              map[string]any
}

func (p DiscoveryEvidencePayload) ResourceName() ResourceName { return ResourceDiscoveryEvidence }
func (p DiscoveryEvidencePayload) SchemaVersion() int         { return 1 }

func (p DiscoveryEvidencePayload) Validate() error {
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("discovery evidence external_id is required")
	}
	switch strings.TrimSpace(p.Kind) {
	case DiscoveryEvidenceKindSource:
		if strings.TrimSpace(p.SourceAppID) == "" {
			return errors.New("discovery source evidence source_app_id is required")
		}
	case DiscoveryEvidenceKindEvent:
		if strings.TrimSpace(p.EventExternalID) == "" {
			return errors.New("discovery event evidence event_external_id is required")
		}
		if strings.TrimSpace(p.SignalKind) == "" {
			return errors.New("discovery event evidence signal_kind is required")
		}
	default:
		return errors.New("discovery evidence kind is required")
	}
	return nil
}

func (p DiscoveryEvidencePayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["kind"] = strings.TrimSpace(p.Kind)
	attrs["canonical_key"] = strings.TrimSpace(p.CanonicalKey)
	attrs["signal_kind"] = strings.TrimSpace(p.SignalKind)
	attrs["event_external_id"] = strings.TrimSpace(p.EventExternalID)
	attrs["source_app_id"] = strings.TrimSpace(p.SourceAppID)
	attrs["source_app_name"] = strings.TrimSpace(p.SourceAppName)
	attrs["source_app_domain"] = strings.TrimSpace(p.SourceAppDomain)
	attrs["source_vendor_name"] = strings.TrimSpace(p.SourceVendorName)
	attrs["source_category"] = strings.TrimSpace(p.SourceCategory)
	attrs["actor_external_id"] = strings.TrimSpace(p.ActorExternalID)
	attrs["actor_email"] = strings.TrimSpace(p.ActorEmail)
	attrs["actor_display_name"] = strings.TrimSpace(p.ActorDisplayName)
	if !p.ObservedAt.IsZero() {
		attrs["observed_at"] = p.ObservedAt.UTC()
	}
	if len(p.Scopes) > 0 {
		attrs["scopes"] = p.Scopes
	}
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.SourceAppName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

func copyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out[k] = v
	}
	return out
}

func MapFromJSON(raw []byte) map[string]any {
	out := map[string]any{}
	if len(raw) == 0 {
		return out
	}
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}
