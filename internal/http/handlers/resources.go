package handlers

import (
	"path"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	identitydomain "github.com/open-sspm/open-sspm/internal/identity"
)

func (h *Handlers) HandleResourceShow(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")
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
	query := querystate.ParseResourceAccessQuery(c.Request().URL.EscapedPath(), c.QueryParams())

	ctx := c.Request().Context()
	renderFrame := isHX(c) && isHXTarget(c, "resource-access-frame")
	renderResults := isHX(c) && isHXTarget(c, "resource-access-results")

	// The summary only feeds the page header, which partial swaps never re-render.
	var summary gen.GetEntitlementAccessResourceSummaryRow
	displayName := externalID
	if !renderFrame && !renderResults {
		var err error
		summary, err = h.Q.GetEntitlementAccessResourceSummary(ctx, gen.GetEntitlementAccessResourceSummaryParams{
			SourceKind:  sourceKind,
			SourceName:  sourceName,
			ResourceRef: resourceRef,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		if summary.EntitlementCount > 0 {
			displayName = identitydomain.DisplayResourceLabel(resourceRef, summary.SampleEntitlementRawJson)
		}
	}

	filterParams := gen.CountEntitlementAccessBySourceAndResourceRefQueryParams{
		SourceKind:   sourceKind,
		SourceName:   sourceName,
		ResourceRef:  resourceRef,
		Query:        query.Q,
		Permission:   query.Permission,
		Kind:         query.Kind,
		Link:         query.Link,
		IdentityKind: query.IdentityKind,
	}
	matchingRows, err := h.Q.CountEntitlementAccessBySourceAndResourceRefQuery(ctx, filterParams)
	if err != nil {
		return h.RenderError(c, err)
	}
	const perPage = 25
	page, totalPages, offset := paginate(matchingRows, query.Page, perPage)
	query.Page = page
	rows, err := h.Q.ListEntitlementAccessBySourceAndResourceRefPage(ctx, gen.ListEntitlementAccessBySourceAndResourceRefPageParams{
		SourceKind:   sourceKind,
		SourceName:   sourceName,
		ResourceRef:  resourceRef,
		Query:        query.Q,
		Permission:   query.Permission,
		Kind:         query.Kind,
		Link:         query.Link,
		IdentityKind: query.IdentityKind,
		SortBy:       query.SortBy,
		SortDir:      query.SortDir,
		PageLimit:    perPage,
		PageOffset:   int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
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

	accessRows := make([]viewmodels.ResourceAccessRow, 0, len(rows))

	for _, row := range rows {
		identityID := int64(0)
		identityHref := ""
		identityEmail := ""
		identityName := ""
		identityStatus := ""
		if row.IdentityID.Valid {
			identityID = row.IdentityID.Int64
			identityHref = "/identities/" + strconv.FormatInt(identityID, 10)
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
		EntitlementCount:    summary.EntitlementCount,
		AccountCount:        summary.AccountCount,
		LinkedIdentityCount: summary.LinkedIdentityCount,
		Query:               query,
		Page:                page,
		TotalPages:          totalPages,
		MatchingRows:        matchingRows,
		Rows:                accessRows,
		HasRows:             len(accessRows) > 0,
	}
	data.ShowingFrom, data.ShowingTo = showingRange(matchingRows, offset, len(accessRows))

	if renderFrame {
		c.Response().Header().Set("HX-Push-Url", query.Href())
		return h.RenderComponent(c, views.ResourceAccessFrame(data))
	}
	if renderResults {
		c.Response().Header().Set("HX-Push-Url", query.Href())
		return h.RenderComponent(c, views.ResourceAccessResults(data))
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
