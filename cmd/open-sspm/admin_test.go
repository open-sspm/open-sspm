package main

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestAdminCommandOwnsMaintenanceCommands(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"sync",
		"migrate",
		"seed-rules",
		"validate-rules",
		"validate-policy-packs",
		"spec-version",
		"users",
	} {
		if !commandHasChild(adminCmd, name) {
			t.Fatalf("admin command is missing %q", name)
		}
	}
}

func commandHasChild(parent *cobra.Command, name string) bool {
	for _, cmd := range parent.Commands() {
		if cmd.Name() == name {
			return true
		}
	}
	return false
}
