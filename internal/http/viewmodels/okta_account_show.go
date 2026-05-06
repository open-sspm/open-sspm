package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

// SourceAccountShowViewData drives the per-source-account inspector page.
//
// The page is keyed on a single accounts row and renders only data intrinsic
// to that source account (profile, status, raw assignments). Cross-source
// concerns — linked accounts and entitlements — live on /identities/:id.
type SourceAccountShowViewData struct {
	Layout         LayoutData
	Account        gen.Account
	LastLoginAt    TimeDisplay
	LastObservedAt TimeDisplay
	IdentityHref   string
	IdentityName   string
	// OktaSection is nil when this inspector is reused for non-Okta source accounts.
	OktaSection *OktaInspectorSection
}

// OktaInspectorSection holds the Okta-specific lower portion of the inspector
// page: groups the user belongs to and the app assignments granted to them.
type OktaInspectorSection struct {
	Groups          []OktaGroupBadge
	Assignments     []OktaAssignmentView
	AssignmentCount int
}

type OktaGroupBadge struct {
	Name       string
	ExternalID string
}
