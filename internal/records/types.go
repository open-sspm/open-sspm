package records

import (
	"errors"
	"strings"
	"time"
)

type ResourceName string

const (
	ResourceIdentity          ResourceName = "identity"
	ResourceGroup             ResourceName = "group"
	ResourceApplication       ResourceName = "application"
	ResourceServicePrincipal  ResourceName = "service_principal"
	ResourceAppAsset          ResourceName = "app_asset"
	ResourceAppAssetOwner     ResourceName = "app_asset_owner"
	ResourceCredential        ResourceName = "credential"
	ResourceEntitlement       ResourceName = "entitlement"
	ResourceDiscoveryEvidence ResourceName = "discovery_evidence"
	ResourcePolicy            ResourceName = "policy"
	ResourceAuditEvent        ResourceName = "audit_event"
)

type RecordKind string

const (
	RecordKindEvent            RecordKind = "event"
	RecordKindStateUpsert      RecordKind = "state_upsert"
	RecordKindStateDelete      RecordKind = "state_delete"
	RecordKindSnapshotBoundary RecordKind = "snapshot_boundary"
)

type SourceRef struct {
	Kind string
	ID   *int64
	Name string
}

type InboundRecord interface {
	SourceKind() string
	SourceName() string
	DedupeKey() string
	RecordKind() RecordKind
}

type ActorRef struct {
	Kind        string
	ID          string
	Email       string
	DisplayName string
	Envelope    map[string]any
}

type TargetRef struct {
	Kind       string
	ID         string
	Name       string
	Email      string
	Role       string
	IdentityID *int64
	SaaSAppID  *int64
	Envelope   map[string]any
}

type Outcome string

const (
	OutcomeUnknown Outcome = ""
	OutcomeSuccess Outcome = "success"
	OutcomeFailure Outcome = "failure"
	OutcomeSkipped Outcome = "skipped"
)

type ClientRef struct {
	IP        string
	UserAgent string
	Envelope  map[string]any
}

type EventRecord struct {
	Source          SourceRef
	Channel         string
	ProviderEventID string
	DedupeKeyValue  string
	EventType       string
	Category        string
	Action          string
	Severity        int16
	OccurredAt      time.Time
	ObservedAt      time.Time
	Actor           ActorRef
	PrimaryTarget   *TargetRef
	Targets         []TargetRef
	Outcome         Outcome
	Client          ClientRef
	IdentityID      *int64
	SaaSAppID       *int64
	Envelope        map[string]any
	Raw             map[string]any
	TraceID         string
}

func (r EventRecord) SourceKind() string { return normalizeLower(r.Source.Kind) }
func (r EventRecord) SourceName() string { return strings.TrimSpace(r.Source.Name) }
func (r EventRecord) DedupeKey() string  { return strings.TrimSpace(r.DedupeKeyValue) }
func (r EventRecord) RecordKind() RecordKind {
	return RecordKindEvent
}

type StateUpsert struct {
	Source         SourceRef
	Resource       ResourceName
	Key            string
	ProviderID     string
	Payload        ResourcePayload
	ObservedAt     time.Time
	DedupeKeyValue string
}

func (r StateUpsert) SourceKind() string { return normalizeLower(r.Source.Kind) }
func (r StateUpsert) SourceName() string { return strings.TrimSpace(r.Source.Name) }
func (r StateUpsert) DedupeKey() string  { return strings.TrimSpace(r.DedupeKeyValue) }
func (r StateUpsert) RecordKind() RecordKind {
	return RecordKindStateUpsert
}

func (r StateUpsert) Validate() error {
	if strings.TrimSpace(string(r.Resource)) == "" {
		return errors.New("state upsert resource is required")
	}
	if strings.TrimSpace(r.Key) == "" {
		return errors.New("state upsert key is required")
	}
	if r.Payload == nil {
		return errors.New("state upsert payload is required")
	}
	if r.Payload.ResourceName() != r.Resource {
		return errors.New("state upsert payload resource does not match record resource")
	}
	return r.Payload.Validate()
}

type DeleteReason string

const (
	DeleteReasonExplicit       DeleteReason = "explicit_delete"
	DeleteReasonStaleAbsent    DeleteReason = "stale_absent"
	DeleteReasonProviderRemove DeleteReason = "provider_removed"
)

type StateDelete struct {
	Source         SourceRef
	Resource       ResourceName
	Key            string
	ProviderID     string
	DeletedAt      time.Time
	Reason         DeleteReason
	DedupeKeyValue string
}

func (r StateDelete) SourceKind() string { return normalizeLower(r.Source.Kind) }
func (r StateDelete) SourceName() string { return strings.TrimSpace(r.Source.Name) }
func (r StateDelete) DedupeKey() string  { return strings.TrimSpace(r.DedupeKeyValue) }
func (r StateDelete) RecordKind() RecordKind {
	return RecordKindStateDelete
}

type FullScope struct {
	Resource ResourceName
	Key      string
	Envelope map[string]any
}

type SnapshotBegin struct {
	Source         SourceRef
	Resource       ResourceName
	Scope          FullScope
	Complete       bool
	StartedAt      time.Time
	DedupeKeyValue string
}

func (r SnapshotBegin) SourceKind() string { return normalizeLower(r.Source.Kind) }
func (r SnapshotBegin) SourceName() string { return strings.TrimSpace(r.Source.Name) }
func (r SnapshotBegin) DedupeKey() string  { return strings.TrimSpace(r.DedupeKeyValue) }
func (r SnapshotBegin) RecordKind() RecordKind {
	return RecordKindSnapshotBoundary
}

type SnapshotComplete struct {
	Source         SourceRef
	Resource       ResourceName
	Scope          FullScope
	Complete       bool
	ExpireAbsent   bool
	FinishedAt     time.Time
	DedupeKeyValue string
}

func (r SnapshotComplete) SourceKind() string { return normalizeLower(r.Source.Kind) }
func (r SnapshotComplete) SourceName() string { return strings.TrimSpace(r.Source.Name) }
func (r SnapshotComplete) DedupeKey() string  { return strings.TrimSpace(r.DedupeKeyValue) }
func (r SnapshotComplete) RecordKind() RecordKind {
	return RecordKindSnapshotBoundary
}

func normalizeLower(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
