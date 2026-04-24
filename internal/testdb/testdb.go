package testdb

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	DatabaseURLVars []string
	NamePrefix      string
	Timeout         time.Duration
}

func WithDatabase(t *testing.T, opts Options, fn func(context.Context, *pgxpool.Pool, *migrate.Migrate)) {
	t.Helper()

	baseURL := strings.TrimSpace(databaseURLFromEnv(t, opts.DatabaseURLVars))
	if baseURL == "" {
		return
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	adminURL, err := databaseURLWithName(baseURL, "postgres")
	if err != nil {
		t.Fatalf("test database admin URL: %v", err)
	}

	adminConn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgx.Connect(admin) err = %v", err)
	}
	defer adminConn.Close(ctx)

	dbName := sanitizedNamePrefix(opts.NamePrefix) + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminConn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatalf("CREATE DATABASE %s: %v", dbName, err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		_, _ = adminConn.Exec(dropCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	}()

	testURL, err := databaseURLWithName(baseURL, dbName)
	if err != nil {
		t.Fatalf("test database URL: %v", err)
	}

	migrator, err := migrate.New("file://"+MigrationsDir(t), testURL)
	if err != nil {
		t.Fatalf("migrate.New() err = %v", err)
	}
	defer func() {
		_, _ = migrator.Close()
	}()

	pool, err := pgxpool.New(ctx, testURL)
	if err != nil {
		t.Fatalf("pgxpool.New() err = %v", err)
	}
	defer pool.Close()

	fn(ctx, pool, migrator)
}

func MigrateUp(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
}

func MigrationsDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "db", "migrations")
}

func databaseURLFromEnv(t *testing.T, envVars []string) string {
	t.Helper()

	if len(envVars) == 0 {
		envVars = []string{"OPENSSPM_TEST_DATABASE_URL"}
	}
	for _, envVar := range envVars {
		if value := strings.TrimSpace(os.Getenv(envVar)); value != "" {
			return value
		}
	}
	t.Skipf("%s is not set", strings.Join(envVars, " or "))
	return ""
}

func databaseURLWithName(raw, dbName string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + dbName
	return parsed.String(), nil
}

func sanitizedNamePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "opensspm_test_"
	}
	if !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}
	return prefix
}
