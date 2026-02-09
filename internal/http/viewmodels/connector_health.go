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
	StatusLabel       string
	StatusClass       string
	FinishedAtLabel   string
	FinishedAtTitle   string
	ErrorKind         string
	MessagePreview    string
	MessageFull       string
	PreviewTruncated  bool
	FullTextTruncated bool
	HasMessage        bool
	ExpandControlID   string
	ExpandContentID   string
}
