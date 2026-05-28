package capabilities

import (
	"context"
	"net/http"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

type Integration interface {
	Descriptor() Descriptor
	Capabilities() Capabilities
}

type Descriptor struct {
	Kind        string
	DisplayName string
	Role        registry.IntegrationRole
	Resources   []records.ResourceName
	EventTypes  []string
}

type Capabilities struct {
	Push      *PushCapability
	Tail      *TailCapability
	Full      *FullCapability
	Discovery *DiscoveryCapability
}

type PushCapability struct {
	Channels             []PushChannel
	DeliverySemantics    DeliverySemantics
	RequiresSubscription bool
	SubscriptionTTL      time.Duration
	Register             func(context.Context, SubscriptionContext) error
	Renew                func(context.Context, SubscriptionContext) error
	VerifyHandshake      func(context.Context, *http.Request, []byte) (*HandshakeResponse, error)
	VerifyAndDecode      func(context.Context, string, *http.Request, []byte) (*PushDecodeResult, error)
}

type DeliverySemantics string

const (
	DeliveryBestEffort  DeliverySemantics = "best_effort"
	DeliveryAtLeastOnce DeliverySemantics = "at_least_once"
)

type PushKind string

const (
	PushKindEventPayload PushKind = "event_payload"
	PushKindChangeHint   PushKind = "change_hint"
	PushKindExternalBus  PushKind = "external_bus"
	PushKindPreview      PushKind = "preview_push"
)

type PushChannel struct {
	Name         string
	Kind         PushKind
	BodyLimit    int64
	RequiresHMAC bool
}

type PushDecodeResult struct {
	SourceRef    records.SourceRef
	ExternalID   string
	DedupeKey    string
	Records      []records.InboundRecord
	WakeTail     []TailWakeup
	IgnoreReason string
}

type SubscriptionContext struct {
	Source      records.SourceRef
	CallbackURL string
	SecretRef   string
	Now         time.Time
}

type HandshakeResponse struct {
	StatusCode  int
	ContentType string
	Body        []byte
}

type TailCapability struct {
	Resources          []TailResource
	TargetLatency      time.Duration
	MinPollInterval    time.Duration
	CursorKind         CursorKind
	CursorExpiresAfter time.Duration
	MaxBatchSize       int
	Poll               func(context.Context, TailContext) (*TailResult, error)
}

type CursorKind string

const (
	CursorKindDeltaToken       CursorKind = "delta_token"
	CursorKindLogCursor        CursorKind = "log_cursor"
	CursorKindWatermarkOverlap CursorKind = "watermark_overlap"
	CursorKindPageCursor       CursorKind = "page_cursor"
)

type TailResource struct {
	Name               records.ResourceName
	ProviderResource   string
	SupportsDeletes    bool
	SupportsCheckpoint bool
}

type TailWakeup struct {
	Source   records.SourceRef
	Resource records.ResourceName
	Reason   string
	Priority int
	RunAfter time.Time
	Payload  map[string]any
}

type TailResult struct {
	Records    []records.InboundRecord
	NextCursor CursorState
	HasMore    bool
}

type CursorState struct {
	Kind                CursorKind
	Cursor              map[string]any
	Watermark           time.Time
	ExpiresAt           time.Time
	LastProviderEventID string
	NeedsFullResync     bool
}

type TailContext interface {
	RecordEmitter
	Source() records.SourceRef
	Resource() records.ResourceName
	Cursor() CursorState
	SetCursor(context.Context, CursorState) error
	WakeReason() string
	Deadline() time.Time
}

type FullCapability struct {
	Resources               []FullResource
	RecommendedInterval     time.Duration
	SupportsScopedReconcile bool
	Run                     func(context.Context, FullContext) (*FullResult, error)
}

type FullResource struct {
	Name                 records.ResourceName
	SnapshotCompleteness SnapshotCompleteness
	ExpireAbsentAllowed  bool
}

type SnapshotCompleteness string

const (
	SnapshotComplete   SnapshotCompleteness = "complete_snapshot"
	SnapshotScoped     SnapshotCompleteness = "scoped_snapshot"
	SnapshotBestEffort SnapshotCompleteness = "best_effort_snapshot"
	SnapshotBootstrap  SnapshotCompleteness = "bootstrap_snapshot"
)

type FullContext interface {
	RecordEmitter
	Source() records.SourceRef
	RunID() string
	Scope() records.FullScope
	Deadline() time.Time
}

type FullResult struct {
	Records  []records.InboundRecord
	Complete bool
}

type DiscoveryCapability struct {
	Resources           []DiscoveryResource
	RecommendedInterval time.Duration
	Incremental         bool
}

type DiscoveryResource struct {
	Name        records.ResourceName
	SignalKinds []string
}

type RecordEmitter interface {
	EmitEvent(context.Context, records.EventRecord) error
	UpsertState(context.Context, records.StateUpsert) error
	DeleteState(context.Context, records.StateDelete) error
	BeginSnapshot(context.Context, records.SnapshotBegin) error
	CompleteSnapshot(context.Context, records.SnapshotComplete) error
	EmitInternalEvent(context.Context, records.EventRecord) error
}
