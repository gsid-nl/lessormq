package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
)

// defaultConfig is read when it exists and no --config is given: where the systemd units
// and docs/self-hosting.md put it on a host without containers.
const defaultConfig = "/etc/lessormq/lessormq.conf"

// settings are the names a configuration file may set: the environment variables the
// server reads.
var settings = []string{
	"DATABASE_QUEUE_URL",
	"REDIS_URL",
	"LISTEN",
	"LEASE_SWEEP_SECONDS",
	"MAX_WAITING_CLAIMS",
	"CORS_ALLOW_ORIGIN",
	"DB_MAX_CONNECTIONS",
	"MIGRATE",
	"LOG_LEVEL",
}

// configure takes --config FILE (or --config=FILE) out of the arguments and applies that
// file, or defaultConfig when there is one, to the environment. A variable that is set in
// the environment already keeps its value: the environment overrides the file.
func configure(args []string) ([]string, error) {
	path, explicit := defaultConfig, false
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		name, value, hasValue := strings.Cut(args[i], "=")
		if name != "--config" && name != "-config" {
			rest = append(rest, args[i])
			continue
		}
		if !hasValue {
			if i+1 == len(args) {
				return nil, errors.New("--config needs a file")
			}
			i++
			value = args[i]
		}
		path, explicit = value, true
	}
	values, err := readConfig(path)
	if errors.Is(err, fs.ErrNotExist) && !explicit {
		return rest, nil
	}
	if err != nil {
		return nil, err
	}
	for _, kv := range values {
		if _, set := os.LookupEnv(kv[0]); !set {
			os.Setenv(kv[0], kv[1])
		}
	}
	return rest, nil
}

// readConfig reads lines of NAME=value. Blank lines and lines that start with # are
// skipped; a value in matching double or single quotes loses them, and nothing else in it
// is special.
func readConfig(path string) ([][2]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var values [][2]string
	scanner := bufio.NewScanner(f)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if !ok || name == "" {
			return nil, fmt.Errorf("%s:%d: expected NAME=value", path, n)
		}
		if !slices.Contains(settings, name) {
			return nil, fmt.Errorf("%s:%d: unknown setting %s", path, n, name)
		}
		if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		values = append(values, [2]string{name, value})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return values, nil
}
