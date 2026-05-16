package connectordisplay

import (
	"strings"
	"unicode"
)

func KindLabel(kind string) string {
	switch normalized(kind) {
	case "okta":
		return "Okta"
	case "google_workspace":
		return "Google Workspace"
	case "github":
		return "GitHub"
	case "datadog":
		return "Datadog"
	case "aws", "aws_identity_center":
		return "AWS Identity Center"
	case "entra":
		return "Microsoft Entra"
	case "vault", "hashicorp_vault":
		return "Vault"
	case "slack":
		return "Slack"
	case "snowflake":
		return "Snowflake"
	case "pagerduty":
		return "PagerDuty"
	default:
		return fallbackHumanized(kind)
	}
}

func ScopeLabel(kind string) string {
	switch normalized(kind) {
	case "okta":
		return "Org URL"
	case "google_workspace":
		return "Customer"
	case "github":
		return "Org"
	case "datadog":
		return "Site"
	case "aws", "aws_identity_center":
		return "Instance"
	case "entra":
		return "Tenant"
	case "vault", "hashicorp_vault":
		return "Vault"
	case "slack":
		return "Workspace"
	case "snowflake":
		return "Account"
	case "pagerduty":
		return "Subdomain"
	default:
		return "Source"
	}
}

func normalized(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func fallbackHumanized(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "—"
	}
	parts := strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
		return r == '_' || r == ':' || r == '-'
	})
	for idx, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		if len(runes) == 0 {
			continue
		}
		runes[0] = unicode.ToUpper(runes[0])
		parts[idx] = string(runes)
	}
	if len(parts) == 0 {
		return value
	}
	return strings.Join(parts, " ")
}
