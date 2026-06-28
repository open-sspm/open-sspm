package aws

import (
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

func (i *AWSIntegration) Descriptor() capabilities.Descriptor {
	name := ""
	if i != nil {
		name = strings.TrimSpace(i.sourceName)
	}
	return capabilities.Descriptor{
		Kind:        "aws",
		DisplayName: name,
		Role:        registry.RoleApp,
		Resources: []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourceEntitlement,
		},
		EventTypes: []string{
			"aws.cloudtrail.*",
		},
	}
}

func (i *AWSIntegration) Capabilities() capabilities.Capabilities {
	if i == nil || i.client == nil {
		return capabilities.Capabilities{}
	}
	caps := capabilities.Capabilities{
		Full: &capabilities.FullCapability{
			Resources: []capabilities.FullResource{
				{Name: records.ResourceIdentity, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceGroup, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceEntitlement, SnapshotCompleteness: capabilities.SnapshotBestEffort},
			},
			RecommendedInterval: 4 * time.Hour,
		},
	}
	if i.client.cloudtrail != nil {
		caps.Tail = &capabilities.TailCapability{
			Resources: []capabilities.TailResource{
				{
					Name:               records.ResourceAuditEvent,
					ProviderResource:   AWSCloudTrailTailResource,
					SupportsCheckpoint: true,
				},
			},
			TargetLatency:   15 * time.Minute,
			MinPollInterval: 5 * time.Minute,
			CursorKind:      capabilities.CursorKindWatermarkOverlap,
			MaxBatchSize:    50,
		}
	}
	return caps
}
