package handlers

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
)

func sourcePrimaryLabel(kind string) string {
	switch NormalizeConnectorKind(kind) {
	case configstore.KindEntra:
		return "Microsoft Entra"
	}

	if label := strings.TrimSpace(ConnectorDisplayName(kind)); label != "" {
		return label
	}

	kind = strings.TrimSpace(kind)
	if kind == "" {
		return "—"
	}
	return kind
}

func sourceDiagnosticLabel(kind, sourceName string) string {
	primary := strings.TrimSpace(sourcePrimaryLabel(kind))
	if primary == "—" {
		primary = ""
	}
	sourceName = strings.TrimSpace(sourceName)

	switch {
	case primary != "" && sourceName != "":
		return primary + " (" + sourceName + ")"
	case primary != "":
		return primary
	case sourceName != "":
		return sourceName
	default:
		return "—"
	}
}
