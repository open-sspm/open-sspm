package main

import (
	"context"
	"errors"
	"fmt"
	"os"
)

func main() {
	if err := Execute(); err != nil {
		var ee *exitError
		if errors.As(err, &ee) {
			if !ee.silent {
				if ee.err != nil {
					fmt.Fprintln(os.Stderr, ee.err)
				} else {
					fmt.Fprintln(os.Stderr, err)
				}
			}
			os.Exit(ee.code)
		}

		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "canceled")
			os.Exit(130)
		}

		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
