package viewmodels

type GlobalViewData struct {
	Layout LayoutData
	Cards  []GlobalViewAppCard
}

type GlobalViewAppCard struct {
	Kind           string
	Name           string
	CategoryLabel  string
	Subtitle       string
	StatusLabel    string
	StatusClass    string
	IsActive       bool // configured + enabled + no config error
	ShowScore      bool // whether to render the score bar and metrics
	ScoreLabel     string
	ScoreValue     int
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
