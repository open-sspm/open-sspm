package datadog

import (
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

func (i *DatadogIntegration) Descriptor() capabilities.Descriptor {
	name := ""
	if i != nil {
		name = strings.TrimSpace(i.site)
	}
	return capabilities.Descriptor{
		Kind:        configstore.KindDatadog,
		DisplayName: name,
		Role:        registry.RoleApp,
		Resources: []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourceEntitlement,
		},
		EventTypes: []string{
			"datadog.audit.*",
		},
	}
}

func (i *DatadogIntegration) Capabilities() capabilities.Capabilities {
	if i == nil || i.adapter == nil {
		return capabilities.Capabilities{}
	}
	return capabilities.Capabilities{
		Tail: &capabilities.TailCapability{
			Resources: []capabilities.TailResource{
				{
					Name:               records.ResourceAuditEvent,
					ProviderResource:   DatadogAuditTailResource,
					SupportsCheckpoint: true,
				},
			},
			TargetLatency:   10 * time.Minute,
			MinPollInterval: 5 * time.Minute,
			CursorKind:      capabilities.CursorKindWatermarkOverlap,
			MaxBatchSize:    1000,
		},
		Full: &capabilities.FullCapability{
			Resources: []capabilities.FullResource{
				{Name: records.ResourceIdentity, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceGroup, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceEntitlement, SnapshotCompleteness: capabilities.SnapshotBestEffort},
			},
			RecommendedInterval:     time.Hour,
			SupportsScopedReconcile: false,
		},
	}
}
