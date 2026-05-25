package okta

import (
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

func (i *OktaIntegration) Descriptor() capabilities.Descriptor {
	name := ""
	if i != nil {
		name = strings.TrimSpace(i.sourceName)
	}
	return capabilities.Descriptor{
		Kind:        configstore.KindOkta,
		DisplayName: name,
		Role:        registry.RoleIdP,
		Resources: []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourceApplication,
			records.ResourceEntitlement,
			records.ResourceDiscoveryEvidence,
			records.ResourceAuditEvent,
		},
		EventTypes: []string{
			"user.authentication.sso",
			"app.oauth2.signon",
			"application.user_membership.add",
			"application.user_membership.remove",
			"application.user_membership.update",
			"group.user_membership.add",
			"group.user_membership.remove",
			"group.user_membership.update",
			"user.lifecycle.*",
			"user.account.*",
			"group.lifecycle.*",
			"application.lifecycle.*",
			"app.lifecycle.*",
		},
	}
}

func (i *OktaIntegration) Capabilities() capabilities.Capabilities {
	caps := capabilities.Capabilities{
		Push: &capabilities.PushCapability{
			Channels: []capabilities.PushChannel{
				{
					Name:      "event_hook",
					Kind:      capabilities.PushKindEventPayload,
					BodyLimit: 2 << 20,
				},
				{
					Name:      "eventbridge",
					Kind:      capabilities.PushKindExternalBus,
					BodyLimit: 2 << 20,
				},
			},
			DeliverySemantics: capabilities.DeliveryAtLeastOnce,
		},
	}
	if i == nil {
		return caps
	}
	if i.client != nil || i.systemLogLister != nil {
		caps.Tail = &capabilities.TailCapability{
			Resources: []capabilities.TailResource{
				{
					Name:               records.ResourceAuditEvent,
					ProviderResource:   SystemLogTailResource,
					SupportsCheckpoint: true,
				},
			},
			TargetLatency:   5 * time.Minute,
			MinPollInterval: time.Minute,
			CursorKind:      capabilities.CursorKindWatermarkOverlap,
			MaxBatchSize:    1000,
		}
	}
	if i.client != nil {
		caps.Full = &capabilities.FullCapability{
			Resources: []capabilities.FullResource{
				{Name: records.ResourceIdentity, SnapshotCompleteness: capabilities.SnapshotComplete, ExpireAbsentAllowed: true},
				{Name: records.ResourceGroup, SnapshotCompleteness: capabilities.SnapshotComplete, ExpireAbsentAllowed: true},
				{Name: records.ResourceApplication, SnapshotCompleteness: capabilities.SnapshotComplete, ExpireAbsentAllowed: true},
				{Name: records.ResourceEntitlement, SnapshotCompleteness: capabilities.SnapshotComplete, ExpireAbsentAllowed: true},
			},
			RecommendedInterval:     time.Hour,
			SupportsScopedReconcile: false,
		}
	}
	return caps
}
