package views

// ListMetaData describes the meta strip rendered above a list. Count and noun
// drive the left-side summary; SubLabel is an optional second-line breadcrumb
// (e.g. "grouped by Asset") that callers set when the table beneath is grouped.
type ListMetaData struct {
	Count    int64
	Noun     string
	SubLabel string
}

// ListMetaCountLabel formats the count + noun for the left-hand summary. We
// keep pluralization here so callers pass a singular noun and the helper picks
// the right suffix — e.g. "1 credential", "12 credentials".
func ListMetaCountLabel(data ListMetaData) string {
	noun := data.Noun
	if noun == "" {
		noun = "result"
	}
	if data.Count == 1 {
		return FormatInt64(data.Count) + " " + noun
	}
	return FormatInt64(data.Count) + " " + pluralize(noun)
}

func pluralize(noun string) string {
	switch noun {
	case "credential":
		return "credentials"
	case "identity":
		return "identities"
	case "app":
		return "apps"
	case "finding":
		return "findings"
	case "result":
		return "results"
	}
	return noun + "s"
}
