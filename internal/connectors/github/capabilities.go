package github

import (
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

func (i *GitHubIntegration) Descriptor() capabilities.Descriptor {
	name := ""
	if i != nil {
		name = strings.TrimSpace(i.org)
	}
	return capabilities.Descriptor{
		Kind:        configstore.KindGitHub,
		DisplayName: name,
		Role:        registry.RoleApp,
		Resources: []records.ResourceName{
			records.ResourceIdentity,
			records.ResourceGroup,
			records.ResourceApplication,
			records.ResourceAppAsset,
			records.ResourceCredential,
			records.ResourceEntitlement,
			records.ResourceAuditEvent,
		},
		EventTypes: []string{
			"github.org.*",
			"github.member.*",
			"github.team.*",
			"github.audit.*",
		},
	}
}

func (i *GitHubIntegration) Capabilities() capabilities.Capabilities {
	if i == nil || i.client == nil {
		return capabilities.Capabilities{}
	}
	return capabilities.Capabilities{
		Full: &capabilities.FullCapability{
			Resources: []capabilities.FullResource{
				{Name: records.ResourceIdentity, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceGroup, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceApplication, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceAppAsset, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceCredential, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceEntitlement, SnapshotCompleteness: capabilities.SnapshotBestEffort},
				{Name: records.ResourceAuditEvent, SnapshotCompleteness: capabilities.SnapshotBestEffort},
			},
			RecommendedInterval:     time.Hour,
			SupportsScopedReconcile: false,
		},
	}
}
