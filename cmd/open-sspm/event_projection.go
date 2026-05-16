package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/projectors"
	"github.com/spf13/cobra"
)

var eventProjectionOptions struct {
	sourceKind string
	sourceName string
	since      string
	until      string
	limit      int32
	diffOnly   bool
}

var eventProjectionCmd = &cobra.Command{
	Use:   "event-projection",
	Short: "Run shadow canonical event projection and baseline parity diffing.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runEventProjection()
	},
}

func init() {
	eventProjectionCmd.Flags().StringVar(&eventProjectionOptions.sourceKind, "source-kind", "okta", "Connector source kind to project.")
	eventProjectionCmd.Flags().StringVar(&eventProjectionOptions.sourceName, "source-name", "", "Connector source name to project.")
	eventProjectionCmd.Flags().StringVar(&eventProjectionOptions.since, "since", "", "Optional RFC3339 projection window start.")
	eventProjectionCmd.Flags().StringVar(&eventProjectionOptions.until, "until", "", "Optional RFC3339 projection window end.")
	eventProjectionCmd.Flags().Int32Var(&eventProjectionOptions.limit, "limit", 1000, "Maximum canonical events to project.")
	eventProjectionCmd.Flags().BoolVar(&eventProjectionOptions.diffOnly, "diff-only", false, "Only diff existing shadow rows against baseline rows.")
}

func runEventProjection() error {
	sourceKind := strings.ToLower(strings.TrimSpace(eventProjectionOptions.sourceKind))
	sourceName := strings.TrimSpace(eventProjectionOptions.sourceName)
	if sourceKind == "" || sourceName == "" {
		return errors.New("--source-kind and --source-name are required")
	}
	windowStart, err := parseOptionalRFC3339(eventProjectionOptions.since)
	if err != nil {
		return fmt.Errorf("parse --since: %w", err)
	}
	windowEnd, err := parseOptionalRFC3339(eventProjectionOptions.until)
	if err != nil {
		return fmt.Errorf("parse --until: %w", err)
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runtimeDeps, err := openRuntimeDependencies(ctx, cfg)
	if err != nil {
		return err
	}
	defer runtimeDeps.pool.Close()

	projector := projectors.NewEventProjector(runtimeDeps.queries)
	params := projectors.DiscoveryProjectionParams{
		SourceKind:           sourceKind,
		SourceName:           sourceName,
		WindowStart:          windowStart,
		WindowEnd:            windowEnd,
		Limit:                eventProjectionOptions.limit,
		ResumeFromCheckpoint: eventProjectionOptions.since == "",
	}
	if !eventProjectionOptions.diffOnly {
		result, err := projector.ProjectDiscovery(ctx, params)
		if err != nil {
			return err
		}
		fmt.Printf("projected=%d projection=%s source=%s/%s resumed_from=%s last_received_at=%s\n",
			result.Projected,
			result.ProjectionName,
			result.SourceKind,
			result.SourceName,
			formatCLIOptionalTime(result.ResumedFrom),
			formatCLIOptionalTime(result.LastReceivedAt),
		)
	}

	diff, err := projector.DiffDiscovery(ctx, params)
	if err != nil {
		return err
	}
	fmt.Printf("baseline=%d projected=%d matching=%d missing_in_projection=%d missing_in_baseline=%d\n",
		diff.BaselineCount,
		diff.ProjectedCount,
		diff.MatchingCount,
		len(diff.MissingInProjection),
		len(diff.MissingInBaseline),
	)
	return nil
}

func parseOptionalRFC3339(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, value)
}

func formatCLIOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}
