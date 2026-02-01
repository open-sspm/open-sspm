package handlers

import (
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func parsePageParam(c *echo.Context) int {
	page := 1
	if rawPage := strings.TrimSpace(c.QueryParam("page")); rawPage != "" {
		if parsed, err := strconv.Atoi(rawPage); err == nil && parsed > 0 {
			page = parsed
		}
	}
	return page
}

func paginate(totalCount int64, page, perPage int) (int, int, int) {
	if perPage < 1 {
		perPage = 1
	}
	if page < 1 {
		page = 1
	}
	denom := int64(perPage)
	totalPages := int((totalCount + denom - 1) / denom)
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}
	offset := (page - 1) * perPage
	return page, totalPages, offset
}

func showingRange(totalCount int64, offset, showingCount int) (int, int) {
	if totalCount <= 0 || showingCount <= 0 {
		return 0, 0
	}
	showingFrom := offset + 1
	showingTo := offset + showingCount
	if int64(showingTo) > totalCount {
		showingTo = int(totalCount)
	}
	return showingFrom, showingTo
}
