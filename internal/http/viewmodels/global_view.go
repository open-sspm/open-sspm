package viewmodels

type GlobalViewData struct {
	Layout LayoutData
	Cards  []GlobalViewAppCard
}

type GlobalViewAppCard struct {
	Kind           string
	Name           string
	Subtitle       string
	StatusLabel    string
	StatusClass    string
	Score          int
	ScoreLabel     string
	Metrics        []GlobalViewKV
	Highlights     []GlobalViewKV
	PrimaryHref    string
	PrimaryLabel   string
	SecondaryHref  string
	SecondaryLabel string
}

type GlobalViewKV struct {
	Label string
	Value string
}
