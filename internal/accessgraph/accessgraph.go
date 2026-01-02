package accessgraph

import (
	"encoding/json"
	"net/url"
	"strings"
)

const (
	ResourceKindGitHubOrg  = "github_org"
	ResourceKindGitHubTeam = "github_team"
	ResourceKindGitHubRepo = "github_repo"

	ResourceKindDatadogRole = "datadog_role"

	ResourceKindAWSAccount = "aws_account"
)

func ParseCanonicalResourceRef(ref string) (resourceKind, externalID string, ok bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", "", false
	}
	parts := strings.SplitN(ref, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	resourceKind = strings.ToLower(strings.TrimSpace(parts[0]))
	externalID = strings.TrimSpace(parts[1])
	if resourceKind == "" || externalID == "" {
		return "", "", false
	}
	return resourceKind, externalID, true
}

func EscapePathPreservingSlashes(value string) string {
	value = strings.Trim(value, "/")
	if value == "" {
		return ""
	}
	parts := strings.Split(value, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func BuildResourceHref(sourceKind, sourceName, resourceKind, externalID string) string {
	sourceKind = strings.TrimSpace(sourceKind)
	sourceName = strings.TrimSpace(sourceName)
	resourceKind = strings.TrimSpace(resourceKind)
	externalID = strings.TrimSpace(externalID)
	if sourceKind == "" || sourceName == "" || resourceKind == "" || externalID == "" {
		return ""
	}
	return "/resources/" + url.PathEscape(sourceKind) + "/" + url.PathEscape(sourceName) + "/" + url.PathEscape(resourceKind) + "/" + EscapePathPreservingSlashes(externalID)
}

func BuildResourceHrefFromResourceRef(sourceKind, sourceName, resourceRef string) string {
	resourceKind, externalID, ok := ParseCanonicalResourceRef(resourceRef)
	if !ok {
		return ""
	}
	return BuildResourceHref(sourceKind, sourceName, resourceKind, externalID)
}

func DisplayResourceLabel(resourceRef string, rawJSON []byte) string {
	resourceKind, externalID, ok := ParseCanonicalResourceRef(resourceRef)
	if !ok {
		return strings.TrimSpace(resourceRef)
	}

	if resourceKind == ResourceKindDatadogRole && len(rawJSON) > 0 {
		var payload struct {
			RoleID   string `json:"role_id"`
			RoleName string `json:"role_name"`
		}
		if err := json.Unmarshal(rawJSON, &payload); err == nil {
			if name := strings.TrimSpace(payload.RoleName); name != "" {
				return name
			}
			if id := strings.TrimSpace(payload.RoleID); id != "" {
				return id
			}
		}
	}

	return externalID
}

func ExternalConsoleHref(sourceKind, sourceName, resourceKind, externalID string) string {
	sourceKind = strings.ToLower(strings.TrimSpace(sourceKind))
	sourceName = strings.TrimSpace(sourceName)
	resourceKind = strings.ToLower(strings.TrimSpace(resourceKind))
	externalID = strings.Trim(externalID, "/")

	switch sourceKind {
	case "github":
		switch resourceKind {
		case ResourceKindGitHubRepo:
			if externalID != "" {
				return "https://github.com/" + externalID
			}
		case ResourceKindGitHubOrg:
			if externalID != "" {
				return "https://github.com/orgs/" + externalID
			}
		case ResourceKindGitHubTeam:
			teamSlug := externalID
			if _, slug, ok := strings.Cut(externalID, "/"); ok {
				teamSlug = slug
			}
			if teamSlug != "" && sourceName != "" {
				return "https://github.com/orgs/" + url.PathEscape(sourceName) + "/teams/" + url.PathEscape(teamSlug)
			}
		}
	case "datadog":
		if sourceName != "" {
			return "https://app." + sourceName + "/organization-settings/roles"
		}
	}
	return ""
}
