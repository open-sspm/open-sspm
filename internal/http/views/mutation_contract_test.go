package views

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestTemplatesDoNotDisableNonInteractiveContainers(t *testing.T) {
	paths, err := filepath.Glob("*.templ")
	if err != nil {
		t.Fatalf("glob templates: %v", err)
	}

	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		source := string(body)
		if strings.Contains(source, `hx-disabled-elt="closest tr"`) {
			t.Errorf("%s disables a table row; disable its interactive controls and add a shared hx-sync scope", path)
		}
		if regexp.MustCompile(`(?s)<form\b[^>]*hx-disabled-elt="this"`).MatchString(source) {
			t.Errorf("%s disables a form; disable its submit controls and add hx-sync=\"this:drop\"", path)
		}
	}
}
