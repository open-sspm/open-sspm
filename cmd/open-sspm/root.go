package main

import (
	"strings"
	"sync"

	"github.com/open-sspm/open-sspm/internal/logging"
	"github.com/spf13/cobra"
)

var structuredLoggingCommandNames = map[string]struct{}{
	"serve":            {},
	"worker":           {},
	"worker-discovery": {},
	"sync":             {},
	"sync-discovery":   {},
	"migrate":          {},
	"seed-rules":       {},
	"validate-rules":   {},
}

type commandExecutionContext struct {
	CommandPath       string
	UsesStructuredLog bool
}

var (
	commandContextMu sync.RWMutex
	commandContext   commandExecutionContext
)

var rootCmd = &cobra.Command{
	Use:           "open-sspm",
	Short:         "Open-SSPM is a tiny who-has-access-to-what service.",
	SilenceErrors: true,
	SilenceUsage:  true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		path := strings.TrimSpace(cmd.CommandPath())
		usesStructured := commandUsesStructuredLogging(cmd)
		setCommandExecutionContext(commandExecutionContext{
			CommandPath:       path,
			UsesStructuredLog: usesStructured,
		})
		if !usesStructured {
			return nil
		}
		if _, err := logging.BootstrapFromEnv(logging.BootstrapOptions{
			Command: path,
		}); err != nil {
			return err
		}
		return nil
	},
}

func Execute() error {
	resetCommandExecutionContext()
	return rootCmd.Execute()
}

func commandUsesStructuredLogging(cmd *cobra.Command) bool {
	name := topLevelCommandName(cmd)
	if name == "" {
		return false
	}
	_, ok := structuredLoggingCommandNames[name]
	return ok
}

func topLevelCommandName(cmd *cobra.Command) string {
	if cmd == nil {
		return ""
	}
	parts := strings.Fields(strings.TrimSpace(cmd.CommandPath()))
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

func currentCommandExecutionContext() commandExecutionContext {
	commandContextMu.RLock()
	defer commandContextMu.RUnlock()
	return commandContext
}

func resetCommandExecutionContext() {
	commandContextMu.Lock()
	defer commandContextMu.Unlock()
	commandContext = commandExecutionContext{}
}

func setCommandExecutionContext(ctx commandExecutionContext) {
	commandContextMu.Lock()
	defer commandContextMu.Unlock()
	commandContext = ctx
}

func init() {
	rootCmd.AddCommand(
		serveCmd,
		workerCmd,
		workerDiscoveryCmd,
		syncCmd,
		syncDiscoveryCmd,
		migrateCmd,
		seedRulesCmd,
		validateRulesCmd,
		specVersionCmd,
		usersCmd,
	)
}
