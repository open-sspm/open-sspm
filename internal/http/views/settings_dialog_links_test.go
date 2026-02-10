package views

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestSettingsUsersPageUsesQueryParamModalOpenLink(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, SettingsUsersPage(viewmodels.SettingsUsersViewData{
		Layout: viewmodels.LayoutData{
			CSRFToken: "csrf-token-123",
		},
	}))

	assertContains(t, html, `id="settings-users-add-trigger" class="btn-sm-primary" href="/settings/users?open=add"`)
	assertContains(t, html, `href="/settings/users?open=add"`)
}

func TestConnectorsPageUsesQueryParamModalOpenLinks(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, ConnectorsPage(viewmodels.ConnectorsViewData{
		Layout: viewmodels.LayoutData{
			CSRFToken: "csrf-token-123",
		},
	}))

	assertContains(t, html, `id="connector-okta-configure" href="/settings/connectors?open=okta"`)
	assertContains(t, html, `id="connector-entra-configure" href="/settings/connectors?open=entra"`)
	assertContains(t, html, `id="connector-aws-configure" href="/settings/connectors?open=aws_identity_center"`)
	assertContains(t, html, `id="connector-github-configure" href="/settings/connectors?open=github"`)
	assertContains(t, html, `id="connector-datadog-configure" href="/settings/connectors?open=datadog"`)
}
