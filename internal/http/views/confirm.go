package views

import (
	"strings"

	"github.com/a-h/templ"
)

// ConfirmTone controls the emphasis of the dialog's accept action.
type ConfirmTone string

const (
	ConfirmToneDefault ConfirmTone = ""
	ConfirmToneWarning ConfirmTone = "warning"
	ConfirmToneDanger  ConfirmTone = "danger"
)

// ConfirmConfig describes a confirmation prompt. The dialog and HTMX's native
// fallback deliberately share the same title and body so either path tells the
// user what will happen.
type ConfirmConfig struct {
	Title       string
	Body        string
	AcceptLabel string
	Tone        ConfirmTone
	ReasonLabel string
	ReasonName  string
}

// ConfirmAttrs returns the attributes consumed by confirm.js and HTMX. It
// derives hx-confirm from the visible title and body, which makes a blank
// native fallback a programming error instead of a per-template concern.
func ConfirmAttrs(config ConfirmConfig) templ.Attributes {
	title := strings.TrimSpace(config.Title)
	body := strings.TrimSpace(config.Body)
	fallback := strings.TrimSpace(strings.Join([]string{title, body}, " "))
	if fallback == "" {
		panic("views: confirmation requires a title or body")
	}

	attrs := templ.Attributes{
		"hx-confirm": fallback,
	}
	if title != "" {
		attrs["data-osspm-confirm-title"] = title
	}
	if body != "" {
		attrs["data-osspm-confirm-body"] = body
	}
	if acceptLabel := strings.TrimSpace(config.AcceptLabel); acceptLabel != "" {
		attrs["data-osspm-confirm-accept-label"] = acceptLabel
	}
	if config.Tone != ConfirmToneDefault {
		attrs["data-osspm-confirm-tone"] = string(config.Tone)
	}
	if reasonLabel := strings.TrimSpace(config.ReasonLabel); reasonLabel != "" {
		attrs["data-osspm-confirm-reason"] = reasonLabel
	}
	if reasonName := strings.TrimSpace(config.ReasonName); reasonName != "" {
		attrs["data-osspm-confirm-reason-name"] = reasonName
	}
	return attrs
}
