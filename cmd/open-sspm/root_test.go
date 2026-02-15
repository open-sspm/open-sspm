package main

import "testing"

func TestRootCommand_RegistersDiscoveryCommands(t *testing.T) {
	t.Parallel()

	if cmd, _, err := rootCmd.Find([]string{"worker-discovery"}); err != nil || cmd == nil || cmd.Name() != "worker-discovery" {
		t.Fatalf("worker-discovery command not registered: cmd=%v err=%v", cmd, err)
	}
	if cmd, _, err := rootCmd.Find([]string{"sync-discovery"}); err != nil || cmd == nil || cmd.Name() != "sync-discovery" {
		t.Fatalf("sync-discovery command not registered: cmd=%v err=%v", cmd, err)
	}
}

func TestCommandUsesStructuredLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "serve", args: []string{"serve"}, want: true},
		{name: "worker", args: []string{"worker"}, want: true},
		{name: "worker-discovery", args: []string{"worker-discovery"}, want: true},
		{name: "sync", args: []string{"sync"}, want: true},
		{name: "sync-discovery", args: []string{"sync-discovery"}, want: true},
		{name: "migrate", args: []string{"migrate"}, want: true},
		{name: "seed-rules", args: []string{"seed-rules"}, want: true},
		{name: "validate-rules", args: []string{"validate-rules"}, want: true},
		{name: "users bootstrap-admin", args: []string{"users", "bootstrap-admin"}, want: false},
		{name: "spec-version", args: []string{"spec-version"}, want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cmd, _, err := rootCmd.Find(tc.args)
			if err != nil {
				t.Fatalf("Find(%v) error = %v", tc.args, err)
			}
			if cmd == nil {
				t.Fatalf("Find(%v) returned nil command", tc.args)
			}

			if got := commandUsesStructuredLogging(cmd); got != tc.want {
				t.Fatalf("commandUsesStructuredLogging(%q) = %v, want %v", cmd.CommandPath(), got, tc.want)
			}
		})
	}
}
