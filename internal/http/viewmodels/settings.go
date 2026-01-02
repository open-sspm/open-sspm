package viewmodels

type ResyncBanner struct {
	Class   string
	Title   string
	Message string
}

type SettingsViewData struct {
	Layout        LayoutData
	SyncInterval  string
	ResyncEnabled bool
	ResyncBanner  *ResyncBanner
}
