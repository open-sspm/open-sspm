package viewmodels

import "strings"

// HumanLinkReason maps the machine-readable identity_accounts.link_reason
// values to an operator-friendly label suitable for badges and table cells.
// Unknown values are returned verbatim with snake_case rewritten to spaces so
// new reasons surface intelligibly until a real label is wired up.
func HumanLinkReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case "":
		return ""
	case "manual":
		return "Manual"
	case "auto_email":
		return "Email match"
	case "auto_provisional_identity":
		return "Provisional"
	case "auto_provisional_ambiguous_email":
		return "Ambiguous email"
	case "seed_migration":
		return "Seeded"
	case "seed_orphan":
		return "Seeded (orphan)"
	}
	// Render unknown reasons as a humanized fallback so new link_reason
	// values introduced by a migration are at least readable until this
	// switch is updated.
	return strings.ReplaceAll(strings.TrimSpace(reason), "_", " ")
}
