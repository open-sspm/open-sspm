package main

import "github.com/spf13/cobra"

var workerIngestCmd = &cobra.Command{
	Use:   "worker-ingest",
	Short: "Run background ingest queue processors.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorkerIngest()
	},
}

func runWorkerIngest() error {
	return runWorkerHost(oktaPushIngestLane{})
}
