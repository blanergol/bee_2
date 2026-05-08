package config

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func envFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestParseTargets(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		want    map[string]Target
		wantErr error
	}{
		{
			name: "single",
			in:   "events:public.events:created_at",
			want: map[string]Target{"events": {Alias: "events", Schema: "public", Table: "events", TSColumn: "created_at"}},
		},
		{
			name: "two with spaces",
			in:   " events : public.events : created_at , logs : audit.app_logs : ts ",
			want: map[string]Target{
				"events": {Alias: "events", Schema: "public", Table: "events", TSColumn: "created_at"},
				"logs":   {Alias: "logs", Schema: "audit", Table: "app_logs", TSColumn: "ts"},
			},
		},
		{name: "empty string", in: "", want: map[string]Target{}},
		{name: "missing schema", in: "events:events:created_at", wantErr: ErrInvalidTargets},
		{name: "duplicate", in: "a:s.t:c,a:s.u:c", wantErr: ErrDuplicateAlias},
		{name: "wrong parts", in: "events:public.events", wantErr: ErrInvalidTargets},
		{name: "empty component", in: "events::created_at", wantErr: ErrInvalidTargets},
		{name: "non-identifier alias", in: "1bad:public.t:c", wantErr: ErrInvalidTargets},
		{name: "non-identifier schema", in: "ok:public-bad.t:c", wantErr: ErrInvalidTargets},
		{name: "non-identifier table", in: "ok:public.t!:c", wantErr: ErrInvalidTargets},
		{name: "trailing dot", in: "ok:public.:c", wantErr: ErrInvalidTargets},
		{name: "leading dot", in: "ok:.t:c", wantErr: ErrInvalidTargets},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseTargets(tc.in)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.wantErr), "want %v, got %v", tc.wantErr, err)
				return
			}
			require.NoError(t, err)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("ParseTargets diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoad_EnvAndFlags(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		args    []string
		env     map[string]string
		want    Config
		wantErr error
	}{
		{
			name: "env only",
			args: nil,
			env: map[string]string{
				"PURGE_DSN":     "postgres://x",
				"PURGE_TARGETS": "events:public.events:created_at",
			},
			want: Config{
				DSN: "postgres://x", Listen: DefaultListen,
				ChunkSize: DefaultChunkSize, MaxChunkSize: DefaultMaxChunkSize,
				Pause: DefaultPause, ChunkTimeout: DefaultChunkTimeout,
				ShutdownTimeout: DefaultShutdownTimeout,
				RetainCompleted: DefaultRetainCompleted,
				Targets: map[string]Target{
					"events": {Alias: "events", Schema: "public", Table: "events", TSColumn: "created_at"},
				},
			},
		},
		{
			name: "flag overrides env",
			args: []string{"-listen=:9090", "-chunk-size=5000"},
			env: map[string]string{
				"PURGE_DSN":     "postgres://x",
				"PURGE_LISTEN":  ":7000",
				"PURGE_TARGETS": "events:public.events:ts",
			},
			want: Config{
				DSN: "postgres://x", Listen: ":9090",
				ChunkSize: 5000, MaxChunkSize: DefaultMaxChunkSize,
				Pause: DefaultPause, ChunkTimeout: DefaultChunkTimeout,
				ShutdownTimeout: DefaultShutdownTimeout,
				RetainCompleted: DefaultRetainCompleted,
				Targets: map[string]Target{
					"events": {Alias: "events", Schema: "public", Table: "events", TSColumn: "ts"},
				},
			},
		},
		{
			name: "missing dsn",
			args: nil,
			env: map[string]string{
				"PURGE_TARGETS": "events:public.events:ts",
			},
			wantErr: ErrEmptyDSN,
		},
		{
			name:    "missing targets",
			args:    nil,
			env:     map[string]string{"PURGE_DSN": "postgres://x"},
			wantErr: ErrEmptyTargets,
		},
		{
			name: "negative chunk via env",
			args: nil,
			env: map[string]string{
				"PURGE_DSN":        "postgres://x",
				"PURGE_TARGETS":    "events:public.events:ts",
				"PURGE_CHUNK_SIZE": "-1",
			},
			wantErr: ErrNonPositive,
		},
		{
			name: "chunk over max",
			args: []string{"-chunk-size=300000"},
			env: map[string]string{
				"PURGE_DSN":     "postgres://x",
				"PURGE_TARGETS": "events:public.events:ts",
			},
			wantErr: ErrChunkTooBig,
		},
		{
			name: "duration parsed from env",
			args: nil,
			env: map[string]string{
				"PURGE_DSN":           "postgres://x",
				"PURGE_TARGETS":       "events:public.events:ts",
				"PURGE_CHUNK_TIMEOUT": "5s",
				"PURGE_PAUSE_MS":      "100",
			},
			want: Config{
				DSN: "postgres://x", Listen: DefaultListen,
				ChunkSize: DefaultChunkSize, MaxChunkSize: DefaultMaxChunkSize,
				Pause: 100 * time.Millisecond, ChunkTimeout: 5 * time.Second,
				ShutdownTimeout: DefaultShutdownTimeout,
				RetainCompleted: DefaultRetainCompleted,
				Targets: map[string]Target{
					"events": {Alias: "events", Schema: "public", Table: "events", TSColumn: "ts"},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Load(tc.args, envFrom(tc.env), io.Discard)
			if tc.wantErr != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.wantErr), "want %v, got %v", tc.wantErr, err)
				return
			}
			require.NoError(t, err)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("Load diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	base := Config{
		DSN: "postgres://x", Listen: ":1", ChunkSize: 100, MaxChunkSize: 1000,
		Pause: 10 * time.Millisecond, ChunkTimeout: time.Second, ShutdownTimeout: time.Second,
		Targets: map[string]Target{"a": {Alias: "a", Schema: "s", Table: "t", TSColumn: "c"}},
	}
	require.NoError(t, base.Validate())

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{name: "no dsn", mutate: func(c *Config) { c.DSN = "" }, wantErr: ErrEmptyDSN},
		{name: "no listen", mutate: func(c *Config) { c.Listen = "" }, wantErr: ErrNonPositive},
		{name: "zero chunk", mutate: func(c *Config) { c.ChunkSize = 0 }, wantErr: ErrNonPositive},
		{name: "zero max chunk", mutate: func(c *Config) { c.MaxChunkSize = 0 }, wantErr: ErrNonPositive},
		{name: "chunk over max", mutate: func(c *Config) { c.ChunkSize = c.MaxChunkSize + 1 }, wantErr: ErrChunkTooBig},
		{name: "negative pause", mutate: func(c *Config) { c.Pause = -1 }, wantErr: ErrNonPositive},
		{name: "zero chunk timeout", mutate: func(c *Config) { c.ChunkTimeout = 0 }, wantErr: ErrNonPositive},
		{name: "zero shutdown timeout", mutate: func(c *Config) { c.ShutdownTimeout = 0 }, wantErr: ErrNonPositive},
		{name: "no targets", mutate: func(c *Config) { c.Targets = nil }, wantErr: ErrEmptyTargets},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := base
			c.Targets = map[string]Target{"a": {Alias: "a", Schema: "s", Table: "t", TSColumn: "c"}}
			tc.mutate(&c)
			err := c.Validate()
			require.Error(t, err)
			require.True(t, errors.Is(err, tc.wantErr), "want %v, got %v", tc.wantErr, err)
		})
	}
}

func FuzzParseTargets(f *testing.F) {
	for _, s := range []string{
		"",
		"events:public.events:ts",
		"a:b.c:d,e:f.g:h",
		"::",
		"alias:schema.table:",
		"a:b:c",
		"events:public.events:ts,",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ParseTargets(s)
	})
}
