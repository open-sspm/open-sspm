package views

import "testing"

func TestConfirmAttrsUsesDialogCopyForNativeFallback(t *testing.T) {
	attrs := ConfirmAttrs(ConfirmConfig{
		Title:       "Revoke grant?",
		Body:        "Revoking removes this credential's access to the app.",
		AcceptLabel: "Revoke",
		Tone:        ConfirmToneDanger,
	})

	if got := attrs["hx-confirm"]; got != "Revoke grant? Revoking removes this credential's access to the app." {
		t.Errorf("hx-confirm = %q", got)
	}
	if got := attrs["data-osspm-confirm-tone"]; got != "danger" {
		t.Errorf("confirmation tone = %q", got)
	}
}

func TestConfirmAttrsRequiresVisibleCopy(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("ConfirmAttrs should reject an empty confirmation")
		}
	}()

	ConfirmAttrs(ConfirmConfig{})
}
