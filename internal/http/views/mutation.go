package views

import (
	"strings"

	"github.com/a-h/templ"
)

// MutationFormAttrs prevents repeat submissions from the same form while its
// request is in flight. The selector deliberately excludes controls that were
// already disabled so HTMX does not enable them during request cleanup.
func MutationFormAttrs() templ.Attributes {
	return templ.Attributes{
		"hx-disabled-elt": "find button[type='submit']:not(:disabled), find input[type='submit']:not(:disabled)",
		"hx-sync":         "this:drop",
	}
}

// ScopedMutationAttrs coordinates mutation controls that live in separate
// forms but update one shared UI resource, such as a connector table row.
func ScopedMutationAttrs(scopeID, controlSelector string) templ.Attributes {
	scopeID = strings.TrimSpace(strings.TrimPrefix(scopeID, "#"))
	controlSelector = strings.TrimSpace(controlSelector)
	if scopeID == "" || controlSelector == "" {
		panic("views: scoped mutation requires a scope id and control selector")
	}

	scope := "#" + scopeID
	return templ.Attributes{
		"hx-disabled-elt": scope + " " + controlSelector + ":not(:disabled)",
		"hx-sync":         scope + ":drop",
	}
}
