package handlers

import (
	"path"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/accessgraph"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleResourceShow(c *echo.Context) error {
	sourceKind := strings.TrimSpace(c.Param("sourceKind"))
	sourceName := strings.TrimSpace(c.Param("sourceName"))
	resourceKind := strings.TrimSpace(c.Param("resourceKind"))

	if !IsKnownConnectorKind(sourceKind) || sourceName == "" || resourceKind == "" {
		return RenderNotFound(c)
	}

	rawExternalID := strings.Trim(c.Param("*"), "/")
	if rawExternalID == "" {
		return RenderNotFound(c)
	}
	externalID := strings.TrimPrefix(path.Clean("/"+rawExternalID), "/")
	if externalID == "" || externalID == "." {
		return RenderNotFound(c)
	}
	for seg := range strings.SplitSeq(externalID, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return RenderNotFound(c)
		}
	}

	resourceKind = strings.ToLower(strings.TrimSpace(resourceKind))
	resourceRef := resourceKind + ":" + externalID

	ctx := c.Request().Context()
	rows, err := h.Q.ListEntitlementAccessBySourceAndResourceRef(ctx, gen.ListEntitlementAccessBySourceAndResourceRefParams{
		SourceKind:  sourceKind,
		SourceName:  sourceName,
		ResourceRef: resourceRef,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	displayName := externalID
	if len(rows) > 0 {
		displayName = accessgraph.DisplayResourceLabel(resourceRef, rows[0].EntitlementRawJson)
	}

	title := ConnectorDisplayName(sourceKind) + " resource"
	if strings.TrimSpace(title) == "resource" {
		title = "Resource"
	}
	layout, _, err := h.LayoutData(ctx, c, title)
	if err != nil {
		return h.RenderError(c, err)
	}

	resourceKindLabel := humanizeResourceKind(resourceKind)
	if resourceKindLabel == "" {
		resourceKindLabel = resourceKind
	}

	seenAccounts := make(map[int64]struct{})
	seenIdentities := make(map[int64]struct{})
	accessRows := make([]viewmodels.ResourceAccessRow, 0, len(rows))

	for _, row := range rows {
		seenAccounts[row.AccountID] = struct{}{}

		identityID := int64(0)
		identityHref := ""
		identityEmail := ""
		identityName := ""
		identityStatus := ""
		if row.IdentityID.Valid {
			identityID = row.IdentityID.Int64
			identityHref = "/identities/" + strconv.FormatInt(identityID, 10)
			seenIdentities[identityID] = struct{}{}
		}
		if row.IdentityEmail.Valid {
			identityEmail = strings.TrimSpace(row.IdentityEmail.String)
		}
		if row.IdentityDisplayName.Valid {
			identityName = strings.TrimSpace(row.IdentityDisplayName.String)
		}
		if row.IdentityStatus.Valid {
			identityStatus = strings.TrimSpace(row.IdentityStatus.String)
		}

		linkReason := ""
		if row.LinkReason.Valid {
			linkReason = strings.TrimSpace(row.LinkReason.String)
		}

		accessRows = append(accessRows, viewmodels.ResourceAccessRow{
			IdentityID:            identityID,
			IdentityHref:          identityHref,
			IdentityEmail:         identityEmail,
			IdentityDisplayName:   identityName,
			IdentityStatus:        identityStatus,
			AccountExternalID:     strings.TrimSpace(row.AccountExternalID),
			AccountEmail:          strings.TrimSpace(row.AccountEmail),
			AccountDisplayName:    strings.TrimSpace(row.AccountDisplayName),
			EntitlementKind:       strings.TrimSpace(row.EntitlementKind),
			EntitlementPermission: strings.TrimSpace(row.EntitlementPermission),
			LinkReason:            linkReason,
		})
	}

	data := viewmodels.ResourceShowViewData{
		Layout:              layout,
		SourceKind:          strings.ToLower(strings.TrimSpace(sourceKind)),
		SourceName:          sourceName,
		SourceLabel:         ConnectorDisplayName(sourceKind),
		SourceHref:          IntegratedAppHref(sourceKind),
		ResourceKind:        resourceKind,
		ResourceKindLabel:   resourceKindLabel,
		ExternalID:          externalID,
		DisplayName:         displayName,
		ExternalConsoleHref: accessgraph.ExternalConsoleHref(sourceKind, sourceName, resourceKind, externalID),
		EntitlementCount:    len(rows),
		AccountCount:        len(seenAccounts),
		LinkedIdentityCount: len(seenIdentities),
		Rows:                accessRows,
		HasRows:             len(accessRows) > 0,
	}

	return h.RenderComponent(c, views.ResourceShowPage(data))
}

func humanizeResourceKind(resourceKind string) string {
	switch strings.ToLower(strings.TrimSpace(resourceKind)) {
	case accessgraph.ResourceKindGitHubOrg:
		return "Organization"
	case accessgraph.ResourceKindGitHubTeam:
		return "Team"
	case accessgraph.ResourceKindGitHubRepo:
		return "Repository"
	case accessgraph.ResourceKindDatadogRole:
		return "Role"
	case accessgraph.ResourceKindAWSAccount:
		return "AWS account"
	case accessgraph.ResourceKindVaultPolicy:
		return "Vault policy"
	case accessgraph.ResourceKindVaultGroup:
		return "Vault group"
	case accessgraph.ResourceKindVaultAuthMount:
		return "Vault auth mount"
	case accessgraph.ResourceKindVaultSecretsMount:
		return "Vault secrets mount"
	default:
		return ""
	}
}
