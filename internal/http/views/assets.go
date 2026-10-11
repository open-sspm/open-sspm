package views

import "net/url"

// FrontendAssetVersion forces existing sessions to load the current mutable
// application assets. Static responses also require revalidation, so this only
// needs to change when introducing versioning to an already-cached URL.
const FrontendAssetVersion = "20260713-askbar"

func StaticAssetURL(path string) string {
	return path + "?v=" + url.QueryEscape(FrontendAssetVersion)
}
