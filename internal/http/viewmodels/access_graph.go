package viewmodels

type AccessTreeNode struct {
	ID          string
	Label       string
	SubLabel    string
	Badges      []string
	HasChildren bool
	Href        string
}
