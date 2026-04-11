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
	SourceKind       string
	SourceName       string
	StatusLabel      string
	StatusClass      string
	LastSuccessLabel string
	LastRunLabel     string
	SuccessRate7d    string
	AvgDuration7d    string
	DetailsURL       string
	CanViewDetails   bool
	CanTriggerSync   bool
	Lanes            []ConnectorHealthItemLane
}

// ConnectorHealthItemLane holds per-lane values for multi-lane connectors.
type ConnectorHealthItemLane struct {
	Label       string
	LastSuccess string
	LastRun     string
	SuccessRate string
	AvgDuration string
}

type ConnectorHealthErrorDetailsDialogViewData struct {
	DialogID      string
	ConnectorName string
	SourceKind    string
	SourceName    string
	Rows          []ConnectorHealthErrorDetailsRow
	HasRows       bool
}

type ConnectorHealthErrorDetailsRow struct {
	RowID             string
	RunID             int64
	LaneLabel         string
	StatusLabel       string
	StatusClass       string
	FinishedAt        TimeDisplay
	ErrorKind         string
	MessagePreview    string
	MessageFull       string
	PreviewTruncated  bool
	FullTextTruncated bool
	HasMessage        bool
	ExpandControlID   string
	ExpandContentID   string
}
