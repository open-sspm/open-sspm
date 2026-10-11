package views

import "github.com/open-sspm/open-sspm/internal/http/querystate"

func BasicListPageHref(query querystate.BasicListQuery) func(int) string {
	return func(page int) string {
		return query.WithPage(page).Href()
	}
}

func ResourceAccessPageHref(query querystate.ResourceAccessQuery) func(int) string {
	return func(page int) string {
		return query.WithPage(page).Href()
	}
}

func AppAssetsPageHref(query querystate.AppAssetsQuery) func(int) string {
	return func(page int) string { return query.WithPage(page).Href() }
}

func NonHumanIdentitiesPageHref(query querystate.NonHumanIdentitiesQuery) func(int) string {
	return func(page int) string { return query.WithPage(page).Href() }
}
