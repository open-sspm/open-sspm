package vault

import (
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

func (i *VaultIntegration) Descriptor() capabilities.Descriptor {
	name := ""
	if i != nil {
		name = strings.TrimSpace(i.sourceName)
	}
	return capabilities.Descriptor{
		Kind:        configstore.KindVault,
		DisplayName: name,
		Role:        registry.RoleApp,
		Resources: []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourcePolicy,
			records.ResourceAppAsset,
			records.ResourceEntitlement,
		},
		EventTypes: []string{
			"vault.audit.*",
		},
	}
}

func (i *VaultIntegration) Capabilities() capabilities.Capabilities {
	if i == nil || i.client == nil {
		return capabilities.Capabilities{}
	}
	return capabilities.Capabilities{
		Full: &capabilities.FullCapability{
			Resources: []capabilities.FullResource{
				{Name: records.ResourceIdentity, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceGroup, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourcePolicy, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceAppAsset, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceEntitlement, SnapshotCompleteness: capabilities.SnapshotBestEffort},
			},
			RecommendedInterval: 4 * time.Hour,
		},
	}
}
