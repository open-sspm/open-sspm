package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var workerLaneFlag string

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run a background worker lane.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorker(workerLaneFlag)
	},
}

func init() {
	workerCmd.Flags().StringVar(&workerLaneFlag, "lane", "full", "worker lane to run: full, discovery, event-inbox, tail, evaluator")
}

func runWorker(laneName string) error {
	lane, err := workerLaneByName(laneName)
	if err != nil {
		return err
	}
	return runWorkerHost(lane)
}

func workerLaneByName(name string) (workerLane, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "full":
		return fullSyncLane(), nil
	case "discovery":
		return discoverySyncLane(), nil
	case "event-inbox":
		return eventInboxLane{}, nil
	case "tail":
		return tailSyncLane(), nil
	case "evaluator":
		return eventEvaluatorLane{}, nil
	default:
		return nil, fmt.Errorf("unknown worker lane %q", name)
	}
}
