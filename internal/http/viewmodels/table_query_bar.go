package viewmodels

type TableFilterFieldKind string

const (
	TableFilterFieldKindSingleSelect TableFilterFieldKind = "single_select"
	TableFilterFieldKindBoolean      TableFilterFieldKind = "boolean"
	TableFilterFieldKindState        TableFilterFieldKind = "state"
	TableFilterFieldKindSort         TableFilterFieldKind = "sort"
	TableFilterFieldKindPreset       TableFilterFieldKind = "preset"
)

type TableFilterControl struct {
	Name  string
	Value string
}

type TableFilterOption struct {
	Value    string
	Label    string
	Controls []TableFilterControl
}

type TableFilterField struct {
	ID            string
	Label         string
	Kind          TableFilterFieldKind
	InputNames    []string
	Options       []TableFilterOption
	ActiveValue   string
	PickerVisible bool
}

type TableActiveChip struct {
	FieldID string
	Label   string
}

type TableQuerySearch struct {
	Name        string
	Value       string
	Placeholder string
}

type TableQueryBarData struct {
	Search         TableQuerySearch
	Fields         []TableFilterField
	ActiveChips    []TableActiveChip
	HiddenControls []TableFilterControl
}

func (d TableQueryBarData) HasSearch() bool {
	return d.Search.Name != ""
}

func (d TableQueryBarData) HasAddFilter() bool {
	for _, field := range d.Fields {
		if field.PickerVisible {
			return true
		}
	}
	return false
}

func (d TableQueryBarData) HasActiveFilters() bool {
	return len(d.ActiveChips) > 0
}
