package handlers

// defangCSVCell neutralizes spreadsheet formula injection by prefixing
// cells that begin with a character a spreadsheet program would interpret
// as a formula. Excel, Numbers, and Calc all stop parsing the cell as a
// formula when it begins with a leading apostrophe.
//
// See: https://owasp.org/www-community/attacks/CSV_Injection
func defangCSVCell(s string) string {
	if s == "" {
		return s
	}
	switch s[0] {
	case '=', '+', '-', '@', '\t', '\r', '\n':
		return "'" + s
	}
	return s
}
