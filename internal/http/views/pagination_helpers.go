package views

import "github.com/open-sspm/open-sspm/internal/http/querystate"

func BasicListPageHref(query querystate.BasicListQuery) func(int) string {
	return func(page int) string {
		return query.WithPage(page).Href()
	}
}
