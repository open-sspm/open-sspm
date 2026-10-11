package views

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

// ConnectorListItem is the single description of a connector that drives both
// the desktop row and the mobile card on the connectors page.
type ConnectorListItem struct {
	Kind                 string
	Name                 string
	Description          string
	InboxStatus          string
	Configured           bool
	Enabled              bool
	AuthoritativeCapable bool
	Authoritative        bool
}

func ConnectorListItems(data viewmodels.ConnectorsViewData) []ConnectorListItem {
	inboxStatus := ""
	if data.Okta.EventInboxStatusVisible {
		inboxStatus = data.Okta.EventInboxStatusLabel
	}
	return []ConnectorListItem{
		{Kind: configstore.KindOkta, Name: "Okta", Description: "Identity provider source for users and groups.", InboxStatus: inboxStatus, Configured: data.Okta.Configured, Enabled: data.Okta.Enabled, AuthoritativeCapable: true, Authoritative: data.Okta.Authoritative},
		{Kind: configstore.KindGoogleWorkspace, Name: "Google Workspace", Description: "Users, groups, OAuth grants, and token audits.", Configured: data.GoogleWorkspace.Configured, Enabled: data.GoogleWorkspace.Enabled},
		{Kind: configstore.KindEntra, Name: "Microsoft Entra ID", Description: "Users and access via Microsoft Graph.", Configured: data.Entra.Configured, Enabled: data.Entra.Enabled, AuthoritativeCapable: true, Authoritative: data.Entra.Authoritative},
		{Kind: configstore.KindGitHub, Name: "GitHub", Description: "Organization membership, teams, and repo permissions.", Configured: data.GitHub.Configured, Enabled: data.GitHub.Enabled},
		{Kind: configstore.KindDatadog, Name: "Datadog", Description: "Datadog users and roles for entitlement visibility.", Configured: data.Datadog.Configured, Enabled: data.Datadog.Enabled},
		{Kind: configstore.KindAWSIdentityCenter, Name: "AWS Identity Center", Description: "AWS SSO users and account assignments.", Configured: data.AWSIdentityCenter.Configured, Enabled: data.AWSIdentityCenter.Enabled},
		{Kind: configstore.KindVault, Name: "Vault", Description: "Identity entities, policies, mounts, and auth roles.", Configured: data.Vault.Configured, Enabled: data.Vault.Enabled},
	}
}

func ConnectorListItemForKind(data viewmodels.ConnectorsViewData, kind string) (ConnectorListItem, bool) {
	for _, item := range ConnectorListItems(data) {
		if item.Kind == kind {
			return item, true
		}
	}
	return ConnectorListItem{}, false
}

func (item ConnectorListItem) domKey() string {
	return strings.ReplaceAll(item.Kind, "_", "-")
}

func (item ConnectorListItem) RowID() string {
	return "connector-row-" + item.domKey()
}

func (item ConnectorListItem) CardID() string {
	return "connector-card-" + item.domKey()
}
