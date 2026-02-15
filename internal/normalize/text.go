package normalize

import "strings"

func Trim(value string) string {
	return strings.TrimSpace(value)
}

func Lower(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func EqualFoldTrimmed(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}
