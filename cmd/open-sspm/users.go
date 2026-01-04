package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var usersCmd = &cobra.Command{
	Use:   "users",
	Short: "Manage Open-SSPM UI users.",
}

var (
	bootstrapAdminEmail          string
	bootstrapAdminPassword       string
	bootstrapAdminPasswordStdin  bool
	bootstrapAdminGeneratePasswd bool
)

var bootstrapAdminCmd = &cobra.Command{
	Use:   "bootstrap-admin",
	Short: "Create the first admin user (idempotent if an admin already exists).",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		email := auth.NormalizeEmail(bootstrapAdminEmail)
		if email == "" {
			return errors.New("--email is required")
		}

		password, generated, err := resolveBootstrapPassword(cmd)
		if err != nil {
			return err
		}

		cfg, err := config.Load()
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()

		q := gen.New(pool)

		adminCount, err := q.CountAuthAdmins(ctx)
		if err != nil {
			return err
		}
		if adminCount > 0 {
			cmd.Println("admin user already exists; nothing to do")
			return nil
		}

		if _, err := q.GetAuthUserByEmail(ctx, email); err == nil {
			return fmt.Errorf("user already exists: %s", email)
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		hash, err := auth.HashPassword(password)
		if err != nil {
			return err
		}

		_, err = q.CreateAuthUser(ctx, gen.CreateAuthUserParams{
			Email:        email,
			PasswordHash: hash,
			Role:         auth.RoleAdmin,
			IsActive:     true,
		})
		if err != nil {
			return err
		}

		cmd.Printf("created admin user: %s\n", email)
		if generated {
			cmd.Printf("generated password: %s\n", password)
		}
		return nil
	},
}

func resolveBootstrapPassword(cmd *cobra.Command) (string, bool, error) {
	if bootstrapAdminPasswordStdin && bootstrapAdminGeneratePasswd {
		return "", false, errors.New("--password-stdin and --generate-password are mutually exclusive")
	}
	if bootstrapAdminPasswordStdin && bootstrapAdminPassword != "" {
		return "", false, errors.New("--password-stdin and --password are mutually exclusive")
	}
	if bootstrapAdminGeneratePasswd && bootstrapAdminPassword != "" {
		return "", false, errors.New("--generate-password and --password are mutually exclusive")
	}

	if bootstrapAdminPasswordStdin {
		raw, err := ioReadAllStdin()
		if err != nil {
			return "", false, err
		}
		password := strings.TrimRight(raw, "\r\n")
		if password == "" {
			return "", false, errors.New("password is empty")
		}
		return password, false, nil
	}

	if bootstrapAdminGeneratePasswd {
		password, err := generatePassword(24)
		if err != nil {
			return "", false, err
		}
		return password, true, nil
	}

	if bootstrapAdminPassword != "" {
		return bootstrapAdminPassword, false, nil
	}

	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return "", false, errors.New("no password provided (use --password, --password-stdin, or --generate-password)")
	}

	cmd.Print("Password: ")
	pass1, err := term.ReadPassword(int(os.Stdin.Fd()))
	cmd.Println()
	if err != nil {
		return "", false, err
	}
	if len(pass1) == 0 {
		return "", false, errors.New("password is empty")
	}

	cmd.Print("Confirm password: ")
	pass2, err := term.ReadPassword(int(os.Stdin.Fd()))
	cmd.Println()
	if err != nil {
		return "", false, err
	}

	if string(pass1) != string(pass2) {
		return "", false, errors.New("passwords do not match")
	}

	return string(pass1), false, nil
}

func ioReadAllStdin() (string, error) {
	in, err := os.Stdin.Stat()
	if err != nil {
		return "", err
	}
	if in.Mode()&os.ModeCharDevice != 0 {
		return "", errors.New("stdin is a terminal; use --password or omit to prompt")
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", err
		}
		return "", nil
	}
	return scanner.Text(), nil
}

func generatePassword(length int) (string, error) {
	if length < 16 {
		return "", errors.New("password length too short")
	}
	const alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	const alphabetLen = byte(len(alphabet))
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	for i := range b {
		b[i] = alphabet[b[i]%alphabetLen]
	}
	return string(b), nil
}

func init() {
	usersCmd.AddCommand(bootstrapAdminCmd)
	bootstrapAdminCmd.Flags().StringVar(&bootstrapAdminEmail, "email", "", "Email address for the admin user")
	bootstrapAdminCmd.Flags().StringVar(&bootstrapAdminPassword, "password", "", "Password for the admin user (discouraged; prefer --password-stdin)")
	bootstrapAdminCmd.Flags().BoolVar(&bootstrapAdminPasswordStdin, "password-stdin", false, "Read the password from stdin")
	bootstrapAdminCmd.Flags().BoolVar(&bootstrapAdminGeneratePasswd, "generate-password", false, "Generate a random password and print it")
	_ = bootstrapAdminCmd.MarkFlagRequired("email")
}
