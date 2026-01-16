package viewmodels

type ConnectorHealthViewData struct {
	Layout             LayoutData
	LookbackLabel      string
	SummaryLabel       string
	WarningMessage     string
	WarningDestructive bool
	ShowWarning        bool
	Items              []ConnectorHealthItem
}

type ConnectorHealthItem struct {
	Kind             string
	Name             string
	StatusLabel      string
	StatusClass      string
	LastSuccessLabel string
	LastRunLabel     string
	SuccessRate7d    string
	AvgDuration7d    string
}
