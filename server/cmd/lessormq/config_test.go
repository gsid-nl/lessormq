package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lessormq.conf")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// unset clears the variables a test sets, and puts them back afterwards.
func unset(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
}

func TestConfigFileSetsTheEnvironment(t *testing.T) {
	unset(t, "DATABASE_QUEUE_URL", "LISTEN", "CORS_ALLOW_ORIGIN", "LOG_LEVEL")
	path := writeConfig(t, `# LessorMQ
DATABASE_QUEUE_URL=mysql://lessormq:p=ss#1@127.0.0.1:3306/lessormq?serverVersion=mariadb-10.11.0

  LISTEN = "127.0.0.1:8080"
CORS_ALLOW_ORIGIN='^https://admin\.example\.com$'
LOG_LEVEL="warn'
`)
	args, err := configure([]string{"--config", path, "serve"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(args, []string{"serve"}) {
		t.Errorf("args: %q", args)
	}
	for name, want := range map[string]string{
		"DATABASE_QUEUE_URL": "mysql://lessormq:p=ss#1@127.0.0.1:3306/lessormq?serverVersion=mariadb-10.11.0",
		"LISTEN":             "127.0.0.1:8080",
		"CORS_ALLOW_ORIGIN":  `^https://admin\.example\.com$`,
		"LOG_LEVEL":          `"warn'`,
	} {
		if got := os.Getenv(name); got != want {
			t.Errorf("%s: got %q, want %q", name, got, want)
		}
	}
}

func TestTheEnvironmentOverridesTheFile(t *testing.T) {
	t.Setenv("LISTEN", ":9090")
	unset(t, "LOG_LEVEL")
	path := writeConfig(t, "LISTEN=127.0.0.1:8080\nLOG_LEVEL=debug\n")
	args, err := configure([]string{"purge-old-messages", "--config=" + path, "--hours=1"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(args, []string{"purge-old-messages", "--hours=1"}) {
		t.Errorf("args: %q", args)
	}
	if got := os.Getenv("LISTEN"); got != ":9090" {
		t.Errorf("LISTEN: %q", got)
	}
	if got := os.Getenv("LOG_LEVEL"); got != "debug" {
		t.Errorf("LOG_LEVEL: %q", got)
	}
}

func TestConfigFileErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.conf")
	for name, c := range map[string]struct {
		args []string
		want string
	}{
		"a missing file asked for": {[]string{"--config", missing}, "no such file"},
		"no file after --config":   {[]string{"serve", "--config"}, "--config needs a file"},
		"an unknown setting":       {[]string{"--config", writeConfig(t, "LISTEN=:8080\nDATABASE_URL=x\n")}, ":2: unknown setting DATABASE_URL"},
		"a line without a value":   {[]string{"--config", writeConfig(t, "LISTEN\n")}, ":1: expected NAME=value"},
		"export, as in a shell":    {[]string{"--config", writeConfig(t, "export LISTEN=:8080\n")}, "unknown setting export LISTEN"},
		"a line without a name":    {[]string{"--config", writeConfig(t, "=:8080\n")}, ":1: expected NAME=value"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := configure(c.args)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("got %v, want an error with %q", err, c.want)
			}
		})
	}
}

func TestNoConfigFileIsFine(t *testing.T) {
	if _, err := os.Stat(defaultConfig); err == nil {
		t.Skip(defaultConfig + " exists on this machine")
	}
	args, err := configure([]string{"migrate"})
	if err != nil || !slices.Equal(args, []string{"migrate"}) {
		t.Errorf("got %q, %v", args, err)
	}
}
