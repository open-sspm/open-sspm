package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:           "open-sspm",
	Short:         "Open-SSPM is a tiny who-has-access-to-what service.",
	SilenceErrors: true,
	SilenceUsage:  true,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(serveCmd, workerCmd, syncCmd, migrateCmd, seedRulesCmd, validateRulesCmd, specVersionCmd, usersCmd)
}
