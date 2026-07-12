package handlers

import (
	"path"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	identitydomain "github.com/open-sspm/open-sspm/internal/identity"
)

func (h *Handlers) HandleResourceShow(c *echo.Context) error {
	sourceKind := strings.TrimSpace(c.Param("sourceKind"))
	sourceName := strings.TrimSpace(c.Param("sourceName"))
	resourceKind := strings.TrimSpace(c.Param("resourceKind"))

	if !IsKnownConnectorKind(sourceKind) || sourceName == "" || resourceKind == "" {
		return h.RenderPageNotFound(c)
	}

	rawExternalID := strings.Trim(c.Param("*"), "/")
	if rawExternalID == "" {
		return h.RenderPageNotFound(c)
	}
	externalID := strings.TrimPrefix(path.Clean("/"+rawExternalID), "/")
	if externalID == "" || externalID == "." {
		return h.RenderPageNotFound(c)
	}
	for seg := range strings.SplitSeq(externalID, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return h.RenderPageNotFound(c)
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
		displayName = identitydomain.DisplayResourceLabel(resourceRef, rows[0].EntitlementRawJson)
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
			EntitlementPermission: identitydomain.DisplayEntitlementPermission(row.EntitlementKind, row.EntitlementPermission, row.EntitlementRawJson),
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
		ExternalConsoleHref: identitydomain.ExternalConsoleHref(sourceKind, sourceName, resourceKind, externalID),
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
	case identitydomain.ResourceKindGitHubOrg:
		return "Organization"
	case identitydomain.ResourceKindGitHubTeam:
		return "Team"
	case identitydomain.ResourceKindGitHubRepo:
		return "Repository"
	case identitydomain.ResourceKindDatadogRole:
		return "Role"
	case identitydomain.ResourceKindAWSAccount:
		return "AWS account"
	case identitydomain.ResourceKindEntraServicePrincipal:
		return "Enterprise app"
	case identitydomain.ResourceKindEntraDirectoryRole:
		return "Directory role"
	case identitydomain.ResourceKindVaultPolicy:
		return "Vault policy"
	case identitydomain.ResourceKindVaultGroup:
		return "Vault group"
	case identitydomain.ResourceKindVaultAuthMount:
		return "Vault auth mount"
	case identitydomain.ResourceKindVaultSecretsMount:
		return "Vault secrets mount"
	default:
		return ""
	}
}
