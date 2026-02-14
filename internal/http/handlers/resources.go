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

	seenAppUsers := make(map[int64]struct{})
	seenIdpUsers := make(map[int64]struct{})
	accessRows := make([]viewmodels.ResourceAccessRow, 0, len(rows))

	for _, row := range rows {
		seenAppUsers[row.AppUserID] = struct{}{}

		idpUserID := int64(0)
		idpUserHref := ""
		idpUserEmail := ""
		idpUserName := ""
		idpUserStatus := ""
		if row.IdpUserID.Valid {
			idpUserID = row.IdpUserID.Int64
			idpUserHref = "/idp-users/" + strconv.FormatInt(idpUserID, 10)
			seenIdpUsers[idpUserID] = struct{}{}
		}
		if row.IdpUserEmail.Valid {
			idpUserEmail = strings.TrimSpace(row.IdpUserEmail.String)
		}
		if row.IdpUserDisplayName.Valid {
			idpUserName = strings.TrimSpace(row.IdpUserDisplayName.String)
		}
		if row.IdpUserStatus.Valid {
			idpUserStatus = strings.TrimSpace(row.IdpUserStatus.String)
		}

		linkReason := ""
		if row.LinkReason.Valid {
			linkReason = strings.TrimSpace(row.LinkReason.String)
		}

		accessRows = append(accessRows, viewmodels.ResourceAccessRow{
			IdpUserID:             idpUserID,
			IdpUserHref:           idpUserHref,
			IdpUserEmail:          idpUserEmail,
			IdpUserDisplayName:    idpUserName,
			IdpUserStatus:         idpUserStatus,
			AppUserExternalID:     strings.TrimSpace(row.AppUserExternalID),
			AppUserEmail:          strings.TrimSpace(row.AppUserEmail),
			AppUserDisplayName:    strings.TrimSpace(row.AppUserDisplayName),
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
		AppAccountCount:     len(seenAppUsers),
		LinkedIdpUserCount:  len(seenIdpUsers),
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
	default:
		return ""
	}
}
