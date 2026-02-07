package handlers

import (
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleAWSUsers(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "AWS Identity Center Users")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)

	sourceName := strings.TrimSpace(snap.AWSIdentityCenter.Name)
	if sourceName == "" {
		sourceName = strings.TrimSpace(snap.AWSIdentityCenter.Region)
	}

	if !snap.AWSIdentityCenterConfigured || !snap.AWSIdentityCenterEnabled {
		message := "AWS Identity Center is not configured yet. Add settings in Connectors."
		if snap.AWSIdentityCenterConfigured && !snap.AWSIdentityCenterEnabled {
			message = "AWS Identity Center sync is disabled. Enable it in Connectors."
		}
		totalPages := 1
		data := viewmodels.AWSUsersViewData{
			Layout:         layout,
			Users:          nil,
			Query:          query,
			ShowingCount:   0,
			ShowingFrom:    0,
			ShowingTo:      0,
			TotalCount:     0,
			Page:           1,
			PerPage:        perPage,
			TotalPages:     totalPages,
			HasUsers:       false,
			EmptyStateMsg:  message,
			EmptyStateHref: "/settings/connectors?open=aws_identity_center",
		}
		return h.RenderComponent(c, views.AWSUsersPage(data))
	}

	totalCount, err := h.Q.CountAppUsersWithLinkBySourceAndQuery(ctx, gen.CountAppUsersWithLinkBySourceAndQueryParams{
		SourceKind: "aws",
		SourceName: sourceName,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	users, err := h.Q.ListAppUsersWithLinkPageBySourceAndQuery(ctx, gen.ListAppUsersWithLinkPageBySourceAndQueryParams{
		SourceKind: "aws",
		SourceName: sourceName,
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	showingCount := len(users)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	appUserIDs := make([]int64, 0, len(users))
	for _, user := range users {
		appUserIDs = append(appUserIDs, user.ID)
	}

	accountCounts := make(map[int64]int)
	assignmentCounts := make(map[int64]int)
	if len(appUserIDs) > 0 {
		rows, err := h.Q.ListEntitlementResourcesByAppUserIDsAndKind(ctx, gen.ListEntitlementResourcesByAppUserIDsAndKindParams{
			AppUserIds: appUserIDs,
			EntKind:    "aws_permission_set",
		})
		if err != nil {
			return h.RenderError(c, err)
		}

		var lastUserID int64
		lastResource := ""
		for _, row := range rows {
			resource := strings.TrimSpace(row.Resource)
			if resource == "" {
				continue
			}
			assignmentCounts[row.AppUserID]++

			if row.AppUserID != lastUserID {
				lastUserID = row.AppUserID
				lastResource = ""
			}
			if resource == lastResource {
				continue
			}
			lastResource = resource
			accountCounts[row.AppUserID]++
		}
	}

	items := make([]viewmodels.AWSUserListItem, 0, len(users))
	for _, user := range users {
		userName := strings.TrimSpace(user.DisplayName)
		if userName == "" {
			userName = strings.TrimSpace(user.Email)
		}
		if userName == "" {
			userName = strings.TrimSpace(user.ExternalID)
		}

		items = append(items, viewmodels.AWSUserListItem{
			ID:              user.ID,
			ExternalID:      strings.TrimSpace(user.ExternalID),
			Email:           strings.TrimSpace(user.Email),
			DisplayName:     userName,
			IdpUserID:       user.IdpUserID,
			AccountCount:    accountCounts[user.ID],
			AssignmentCount: assignmentCounts[user.ID],
		})
	}

	emptyState := "No AWS Identity Center users synced yet."
	if query != "" {
		emptyState = "No AWS Identity Center users match the current search."
	}

	data := viewmodels.AWSUsersViewData{
		Layout:         layout,
		Users:          items,
		Query:          query,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     totalCount,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		HasUsers:       showingCount > 0,
		EmptyStateMsg:  emptyState,
		EmptyStateHref: "/settings/connectors?open=aws_identity_center",
	}

	return h.RenderComponent(c, views.AWSUsersPage(data))
}

func (h *Handlers) HandleUnmatchedAWS(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, snap, err := h.LayoutData(ctx, c, "Unmanaged AWS Identity Center Users")
	if err != nil {
		return h.RenderError(c, err)
	}

	const perPage = 20
	query := strings.TrimSpace(c.QueryParam("q"))
	page := parsePageParam(c)

	sourceName := strings.TrimSpace(snap.AWSIdentityCenter.Name)
	if sourceName == "" {
		sourceName = strings.TrimSpace(snap.AWSIdentityCenter.Region)
	}

	if !snap.AWSIdentityCenterConfigured || !snap.AWSIdentityCenterEnabled {
		message := "AWS Identity Center is not configured yet. Add settings in Connectors."
		if snap.AWSIdentityCenterConfigured && !snap.AWSIdentityCenterEnabled {
			message = "AWS Identity Center sync is disabled. Enable it in Connectors."
		}
		data := viewmodels.UnmatchedAWSViewData{
			Layout:         layout,
			Users:          nil,
			Query:          query,
			ShowingCount:   0,
			ShowingFrom:    0,
			ShowingTo:      0,
			TotalCount:     0,
			Page:           1,
			PerPage:        perPage,
			TotalPages:     1,
			HasUsers:       false,
			EmptyStateMsg:  message,
			EmptyStateHref: "/settings/connectors?open=aws_identity_center",
		}
		return h.RenderComponent(c, views.UnmatchedAWSPage(data))
	}

	totalCount, err := h.Q.CountUnmatchedAppUsersBySourceAndQuery(ctx, gen.CountUnmatchedAppUsersBySourceAndQueryParams{
		SourceKind: "aws",
		SourceName: sourceName,
		Query:      query,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	page, totalPages, offset := paginate(totalCount, page, perPage)
	users, err := h.Q.ListUnmatchedAppUsersPageBySourceAndQuery(ctx, gen.ListUnmatchedAppUsersPageBySourceAndQueryParams{
		SourceKind: "aws",
		SourceName: sourceName,
		Query:      query,
		PageLimit:  int32(perPage),
		PageOffset: int32(offset),
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	showingCount := len(users)
	showingFrom, showingTo := showingRange(totalCount, offset, showingCount)

	emptyState := "No unmanaged AWS Identity Center users."
	if query != "" {
		emptyState = "No unmanaged AWS Identity Center users match the current search."
	}

	data := viewmodels.UnmatchedAWSViewData{
		Layout:         layout,
		Users:          users,
		Query:          query,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     totalCount,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		HasUsers:       showingCount > 0,
		EmptyStateMsg:  emptyState,
		EmptyStateHref: "/settings/connectors?open=aws_identity_center",
	}

	return h.RenderComponent(c, views.UnmatchedAWSPage(data))
}
