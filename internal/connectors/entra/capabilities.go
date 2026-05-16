package entra

import (
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

func (i *EntraIntegration) Descriptor() capabilities.Descriptor {
	name := ""
	if i != nil {
		name = strings.TrimSpace(i.tenantID)
	}
	return capabilities.Descriptor{
		Kind:        configstore.KindEntra,
		DisplayName: name,
		Role:        registry.RoleApp,
		Resources: []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourceApplication,
			records.ResourceServicePrincipal,
			records.ResourceAppAsset,
			records.ResourceCredential,
			records.ResourceEntitlement,
			records.ResourceAuditEvent,
		},
		EventTypes: []string{
			"microsoft.graph.directory_audit.*",
			"microsoft.graph.signin.*",
		},
	}
}

func (i *EntraIntegration) Capabilities() capabilities.Capabilities {
	if i == nil || i.client == nil {
		return capabilities.Capabilities{}
	}
	return capabilities.Capabilities{
		Full: &capabilities.FullCapability{
			Resources: []capabilities.FullResource{
				{Name: records.ResourceIdentity, SnapshotCompleteness: capabilities.SnapshotBootstrap},
				{Name: records.ResourceGroup, SnapshotCompleteness: capabilities.SnapshotBootstrap},
				{Name: records.ResourceApplication, SnapshotCompleteness: capabilities.SnapshotBootstrap},
				{Name: records.ResourceServicePrincipal, SnapshotCompleteness: capabilities.SnapshotBootstrap},
				{Name: records.ResourceAppAsset, SnapshotCompleteness: capabilities.SnapshotBootstrap},
				{Name: records.ResourceCredential, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceEntitlement, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceAuditEvent, SnapshotCompleteness: capabilities.SnapshotBestEffort},
			},
			RecommendedInterval:     time.Hour,
			SupportsScopedReconcile: false,
		},
	}
}
