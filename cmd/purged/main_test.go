package main

import (
	"errors"
	"testing"

	"github.com/redmadrobot/bee/purged/internal/config"
	"github.com/stretchr/testify/require"
)

func envFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestRunConfigOnly_HappyPath(t *testing.T) {
	t.Parallel()
	err := runConfigOnly(nil, envFrom(map[string]string{
		"PURGE_DSN":     "postgres://user:pass@localhost:5432/db",
		"PURGE_TARGETS": "events:public.events:created_at",
	}))
	require.NoError(t, err)
}

func TestRunConfigOnly_MissingDSN(t *testing.T) {
	t.Parallel()
	err := runConfigOnly(nil, envFrom(map[string]string{
		"PURGE_TARGETS": "events:public.events:created_at",
	}))
	require.Error(t, err)
	require.True(t, errors.Is(err, config.ErrEmptyDSN))
}

func TestRunConfigOnly_MissingTargets(t *testing.T) {
	t.Parallel()
	err := runConfigOnly(nil, envFrom(map[string]string{
		"PURGE_DSN": "postgres://x",
	}))
	require.Error(t, err)
	require.True(t, errors.Is(err, config.ErrEmptyTargets))
}

func TestRunReturnsOnBadDSN(t *testing.T) {
	t.Parallel()
	// Невалидный DSN: ParseConfig должен упасть, run возвращает ошибку.
	err := run(nil, envFrom(map[string]string{
		"PURGE_DSN":     "::not a dsn::",
		"PURGE_TARGETS": "events:public.events:created_at",
		"PURGE_LISTEN":  "127.0.0.1:0",
	}))
	require.Error(t, err)
}
