package querystate

import (
	"net/url"
	"strings"
)

type BasicListOptions struct {
	StateAliases   []string
	NormalizeState func(string) string
}

type BasicListQuery struct {
	BasePath string
	Q        string
	State    string
	Page     int
}

func ParseBasicListQuery(basePath string, values url.Values, opts BasicListOptions) BasicListQuery {
	state := values.Get("state")
	if strings.TrimSpace(state) == "" {
		for _, alias := range opts.StateAliases {
			state = values.Get(alias)
			if strings.TrimSpace(state) != "" {
				break
			}
		}
	}
	if opts.NormalizeState != nil {
		state = opts.NormalizeState(state)
	} else {
		state = strings.TrimSpace(state)
	}

	return BasicListQuery{
		BasePath: strings.TrimSpace(basePath),
		Q:        strings.TrimSpace(values.Get("q")),
		State:    state,
		Page:     parsePage(values.Get("page")),
	}
}

func (q BasicListQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "state", q.State)
	setIfPage(values, q.Page)
	return values
}

func (q BasicListQuery) Href() string {
	return encodeURL(q.BasePath, q.Values())
}

func (q BasicListQuery) WithPage(page int) BasicListQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q BasicListQuery) ClearQuery() BasicListQuery {
	q.Q = ""
	q.Page = 1
	return q
}

func (q BasicListQuery) HasFilters() bool {
	return strings.TrimSpace(q.Q) != "" || strings.TrimSpace(q.State) != ""
}
