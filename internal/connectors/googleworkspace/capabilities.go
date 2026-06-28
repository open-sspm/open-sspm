package googleworkspace

import (
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/records"
)

func (i *GoogleWorkspaceIntegration) Descriptor() capabilities.Descriptor {
	name := ""
	if i != nil {
		name = strings.TrimSpace(i.customerID)
	}
	return capabilities.Descriptor{
		Kind:        configstore.KindGoogleWorkspace,
		DisplayName: name,
		Role:        registry.RoleApp,
		Resources: []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourceApplication,
			records.ResourceAppAsset,
			records.ResourceCredential,
			records.ResourceEntitlement,
			records.ResourceDiscoveryEvidence,
			records.ResourceAuditEvent,
		},
		EventTypes: []string{
			"google_workspace.login.*",
			"google_workspace.token.*",
		},
	}
}

func (i *GoogleWorkspaceIntegration) Capabilities() capabilities.Capabilities {
	caps := capabilities.Capabilities{}
	if i == nil {
		return caps
	}
	if i.client != nil && i.discoveryEnabled {
		caps.Discovery = &capabilities.DiscoveryCapability{
			Resources: []capabilities.DiscoveryResource{
				{
					Name: records.ResourceDiscoveryEvidence,
					SignalKinds: []string{
						discovery.SignalKindIDPSSO,
						discovery.SignalKindOAuth,
					},
				},
			},
			RecommendedInterval: 15 * time.Minute,
			Incremental:         true,
		}
	}
	if i.client != nil || i.reportsActivityLister != nil {
		caps.Tail = &capabilities.TailCapability{
			Resources: []capabilities.TailResource{
				{
					Name:               records.ResourceAuditEvent,
					ProviderResource:   GoogleWorkspaceReportsTailResource,
					SupportsCheckpoint: true,
				},
			},
			TargetLatency:   10 * time.Minute,
			MinPollInterval: 5 * time.Minute,
			CursorKind:      capabilities.CursorKindWatermarkOverlap,
			MaxBatchSize:    2000,
		}
	}
	if i.client != nil {
		caps.Full = &capabilities.FullCapability{
			Resources: []capabilities.FullResource{
				{Name: records.ResourceIdentity, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceGroup, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceEntitlement, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceAppAsset, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceCredential, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceAuditEvent, SnapshotCompleteness: capabilities.SnapshotBestEffort},
			},
			RecommendedInterval: time.Hour,
		}
	}
	return caps
}
