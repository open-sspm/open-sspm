package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/open-sspm/open-sspm/internal/logging"
)

func main() {
	code := runMain(Execute, os.Stderr)
	if code != 0 {
		os.Exit(code)
	}
}

func runMain(execute func() error, stderr io.Writer) int {
	if err := execute(); err != nil {
		return exitCodeForError(err, stderr)
	}
	return 0
}

func exitCodeForError(err error, stderr io.Writer) int {
	var ee *exitError
	if errors.As(err, &ee) {
		if !ee.silent {
			emitCommandError(resolveErrorForExitError(ee, err), "command failed", ee.code, stderr)
		}
		return ee.code
	}

	if errors.Is(err, context.Canceled) {
		emitCommandError(err, "command canceled", 130, stderr)
		return 130
	}

	emitCommandError(err, "command failed", 1, stderr)
	return 1
}

func emitCommandError(err error, message string, exitCode int, stderr io.Writer) {
	ctx := currentCommandExecutionContext()
	if !ctx.UsesStructuredLog {
		if exitCode == 130 {
			fmt.Fprintln(stderr, "canceled")
			return
		}
		fmt.Fprintln(stderr, err)
		return
	}

	logger := loggerForFatalPath(ctx, stderr)
	logger.Error(message, "exit_code", exitCode, "error", err)
}

func loggerForFatalPath(ctx commandExecutionContext, stderr io.Writer) *slog.Logger {
	cfg, err := logging.LoadConfigFromEnv()
	if err != nil {
		cfg = logging.DefaultConfig()
	}
	return logging.NewLogger(cfg, stderr, ctx.CommandPath)
}

func resolveErrorForExitError(ee *exitError, fallback error) error {
	if ee != nil && ee.err != nil {
		return ee.err
	}
	return fallback
}
