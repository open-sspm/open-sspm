package handlers

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSizeConnectorHealthErrorMessage(t *testing.T) {
	t.Run("empty message", func(t *testing.T) {
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage("   ")
		if preview != "" || full != "" {
			t.Fatalf("expected empty preview/full, got preview=%q full=%q", preview, full)
		}
		if previewTruncated || fullTruncated {
			t.Fatalf("expected no truncation flags, got preview=%v full=%v", previewTruncated, fullTruncated)
		}
	})

	t.Run("preview truncation only", func(t *testing.T) {
		message := strings.Repeat("a", connectorHealthErrorPreviewRunes+5)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if utf8.RuneCountInString(preview) != connectorHealthErrorPreviewRunes {
			t.Fatalf("preview rune count = %d, want %d", utf8.RuneCountInString(preview), connectorHealthErrorPreviewRunes)
		}
		if utf8.RuneCountInString(full) != connectorHealthErrorPreviewRunes+5 {
			t.Fatalf("full rune count = %d, want %d", utf8.RuneCountInString(full), connectorHealthErrorPreviewRunes+5)
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if fullTruncated {
			t.Fatalf("fullTruncated = true, want false")
		}
	})

	t.Run("full truncation", func(t *testing.T) {
		message := strings.Repeat("b", connectorHealthErrorFullRunes+17)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if utf8.RuneCountInString(full) != connectorHealthErrorFullRunes {
			t.Fatalf("full rune count = %d, want %d", utf8.RuneCountInString(full), connectorHealthErrorFullRunes)
		}
		if utf8.RuneCountInString(preview) != connectorHealthErrorPreviewRunes {
			t.Fatalf("preview rune count = %d, want %d", utf8.RuneCountInString(preview), connectorHealthErrorPreviewRunes)
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if !fullTruncated {
			t.Fatalf("fullTruncated = false, want true")
		}
	})

	t.Run("preserves utf8 and line breaks", func(t *testing.T) {
		message := "line 1\n" + strings.Repeat("界", connectorHealthErrorPreviewRunes+1)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if !utf8.ValidString(preview) {
			t.Fatalf("preview is not valid UTF-8")
		}
		if !utf8.ValidString(full) {
			t.Fatalf("full is not valid UTF-8")
		}
		if !strings.Contains(full, "\n") {
			t.Fatalf("expected line break to be preserved in full message")
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if fullTruncated {
			t.Fatalf("fullTruncated = true, want false")
		}
	})
}

func TestConnectorHealthErrorDetailsURL(t *testing.T) {
	url := connectorHealthErrorDetailsURL("github", "acme org", "GitHub")
	if !strings.HasPrefix(url, "/settings/connector-health/errors?") {
		t.Fatalf("unexpected url prefix: %q", url)
	}
	if !strings.Contains(url, "source_kind=github") {
		t.Fatalf("url missing source_kind: %q", url)
	}
	if !strings.Contains(url, "source_name=acme+org") {
		t.Fatalf("url missing encoded source_name: %q", url)
	}
	if !strings.Contains(url, "connector_name=GitHub") {
		t.Fatalf("url missing connector_name: %q", url)
	}
}
