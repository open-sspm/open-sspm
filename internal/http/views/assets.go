package views

import (
	"encoding/json"
	"net/url"

	"github.com/a-h/templ"
)

// FrontendAssetVersion forces existing sessions to load the current mutable
// application assets. Static responses also require revalidation, so this only
// needs to change when introducing versioning to an already-cached URL.
const FrontendAssetVersion = "20260713-askbar"

// versionedAppModules were cached before static responses required
// revalidation. The import map pins every way of importing them to one
// versioned URL so browsers never load a stale or duplicate module instance.
var versionedAppModules = []string{
	"htmx.js",
	"components/askbar.js",
	"components/registry.js",
}

func StaticAssetURL(path string) string {
	return path + "?v=" + url.QueryEscape(FrontendAssetVersion)
}

func AppImportMapScript() templ.Component {
	return templ.Raw(`<script type="importmap">` + appImportMap() + `</script>`)
}

// appImportMap relies on json.Marshal escaping <, >, and & so the payload
// cannot terminate the surrounding script element.
func appImportMap() string {
	imports := map[string]string{"open-sspm-app/": "/static/app/"}
	for _, module := range versionedAppModules {
		versioned := StaticAssetURL("/static/app/" + module)
		imports["open-sspm-app/"+module] = versioned
		imports["/static/app/"+module] = versioned
	}
	b, err := json.Marshal(map[string]any{"imports": imports})
	if err != nil {
		return `{"imports":{"open-sspm-app/":"/static/app/"}}`
	}
	return string(b)
}
