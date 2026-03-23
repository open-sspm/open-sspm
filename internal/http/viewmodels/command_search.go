package viewmodels

type CommandBadgeView struct {
	Label string
	Class string
}

type CommandItemView struct {
	ID           string
	Kind         string
	Href         string
	Primary      string
	Secondary    string
	FilterText   string
	Keywords     string
	Badges       []CommandBadgeView
	Disabled     bool
	ForceVisible bool
}

type CommandActionView struct {
	ID           string
	Label        string
	Href         string
	ForceVisible bool
}

type CommandSectionView struct {
	Key   string
	Title string
	Items []CommandItemView
}

type CommandSearchViewData struct {
	Query       string
	Placeholder string
	AriaLabel   string
	Notices     []CommandItemView
	Sections    []CommandSectionView
}
