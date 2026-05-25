package views

import (
	"net/url"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
)

func ConnectorDialogHref(href string) string {
	u, err := url.Parse(href)
	if err != nil || u.Path != "/settings/connectors" {
		return ""
	}

	kind := u.Query().Get("open")
	switch kind {
	case configstore.KindOkta,
		configstore.KindGoogleWorkspace,
		configstore.KindEntra,
		configstore.KindGitHub,
		configstore.KindDatadog,
		configstore.KindAWSIdentityCenter,
		configstore.KindVault:
		return "/settings/connectors/" + kind + "/dialog"
	default:
		return ""
	}
}
