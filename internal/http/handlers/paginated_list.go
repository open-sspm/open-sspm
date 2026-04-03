package handlers

import "github.com/open-sspm/open-sspm/internal/http/viewmodels"

type paginatedListState struct {
	totalCount int64
	perPage    int
	page       int
	totalPages int
	offset     int
}

func newPaginatedListState(totalCount int64, requestedPage, perPage int) paginatedListState {
	page, totalPages, offset := paginate(totalCount, requestedPage, perPage)
	return paginatedListState{
		totalCount: totalCount,
		perPage:    perPage,
		page:       page,
		totalPages: totalPages,
		offset:     offset,
	}
}

func (s paginatedListState) Offset() int {
	return s.offset
}

func (s paginatedListState) PageData(layout viewmodels.LayoutData, showingCount int, emptyStateMsg, emptyStateHref string) viewmodels.PaginatedListPageData {
	showingFrom, showingTo := showingRange(s.totalCount, s.offset, showingCount)
	return viewmodels.PaginatedListPageData{
		Layout:         layout,
		ShowingCount:   showingCount,
		ShowingFrom:    showingFrom,
		ShowingTo:      showingTo,
		TotalCount:     s.totalCount,
		Page:           s.page,
		PerPage:        s.perPage,
		TotalPages:     s.totalPages,
		EmptyStateMsg:  emptyStateMsg,
		EmptyStateHref: emptyStateHref,
	}
}
