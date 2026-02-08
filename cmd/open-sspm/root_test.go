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
