package main

import "github.com/spf13/cobra"

var adminCmd = &cobra.Command{
	Use:   "admin",
	Short: "Run administrative and maintenance commands.",
}

func init() {
	adminCmd.AddCommand(
		syncCmd,
		migrateCmd,
		seedRulesCmd,
		validateRulesCmd,
		validatePolicyPacksCmd,
		specVersionCmd,
		usersCmd,
	)
}
