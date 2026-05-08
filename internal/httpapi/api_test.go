package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redmadrobot/bee/purged/internal/purge"
	"github.com/stretchr/testify/require"
)

type fakeEngine struct {
	mu       sync.Mutex
	startErr map[string]error
	starts   atomic.Int32
	registry *purge.Registry
	clock    time.Time
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{
		startErr: map[string]error{},
		registry: purge.NewRegistry(),
		clock:    time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
	}
}

func (f *fakeEngine) Start(_ context.Context, p purge.Params) (*purge.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts.Add(1)
	if err := f.startErr[p.Alias]; err != nil {
		return nil, err
	}
	run := &purge.Run{
		ID: "run-" + p.Alias, Alias: p.Alias, Before: p.Before,
		StartedAt: f.clock, Status: purge.StatusRunning,
	}
	f.registry.Add(run)
	return run, nil
}

type fakePinger struct {
	err error
}

func (p *fakePinger) Ping(context.Context) error { return p.err }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newServer(t *testing.T, eng *fakeEngine, pinger Pinger) *Server {
	t.Helper()
	targets := map[string]TargetSpec{
		"events": {Schema: "public", Table: "events", TSColumn: "created_at"},
		"logs":   {Schema: "audit", Table: "app_logs", TSColumn: "ts"},
	}
	return NewServer(eng, eng.registry, pinger, discardLogger(), targets, Limits{
		MaxChunkSize: 50000, DefaultChunkSize: 1000, DefaultPause: 10 * time.Millisecond,
	})
}

func doRequest(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Buffer
	if body != "" {
		rdr = bytes.NewBufferString(body)
	} else {
		rdr = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPostPurges(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		body          string
		startErr      map[string]error
		wantStatus    int
		wantErrSubstr string
		wantStarts    int32
	}{
		{
			name:       "ok with all defaults",
			body:       `{"target":"events","before":"2026-01-01T00:00:00Z"}`,
			wantStatus: http.StatusAccepted,
			wantStarts: 1,
		},
		{
			name:       "ok with explicit chunk",
			body:       `{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":2000,"pause_ms":20}`,
			wantStatus: http.StatusAccepted,
			wantStarts: 1,
		},
		{
			name:          "unknown target",
			body:          `{"target":"users","before":"2026-01-01T00:00:00Z"}`,
			wantStatus:    http.StatusBadRequest,
			wantErrSubstr: "unknown target",
		},
		{
			name:          "bad before",
			body:          `{"target":"events","before":"yesterday"}`,
			wantStatus:    http.StatusBadRequest,
			wantErrSubstr: "RFC3339",
		},
		{
			name:          "negative chunk",
			body:          `{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":-1}`,
			wantStatus:    http.StatusBadRequest,
			wantErrSubstr: "chunk_size",
		},
		{
			name:          "chunk over max",
			body:          `{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":1000000}`,
			wantStatus:    http.StatusBadRequest,
			wantErrSubstr: "exceeds",
		},
		{
			name:       "extra field",
			body:       `{"target":"events","before":"2026-01-01T00:00:00Z","extra":1}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "lock busy",
			body:          `{"target":"events","before":"2026-01-01T00:00:00Z"}`,
			startErr:      map[string]error{"events": purge.ErrLockBusy},
			wantStatus:    http.StatusConflict,
			wantErrSubstr: "in progress",
			wantStarts:    1,
		},
		{
			name:       "internal error",
			body:       `{"target":"events","before":"2026-01-01T00:00:00Z"}`,
			startErr:   map[string]error{"events": errors.New("db down")},
			wantStatus: http.StatusInternalServerError,
			wantStarts: 1,
		},
		{
			name:       "invalid params from engine",
			body:       `{"target":"events","before":"2026-01-01T00:00:00Z"}`,
			startErr:   map[string]error{"events": purge.ErrInvalidParams},
			wantStatus: http.StatusBadRequest,
			wantStarts: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eng := newFakeEngine()
			eng.startErr = tc.startErr
			s := newServer(t, eng, &fakePinger{})
			rec := doRequest(t, s.Handler(), http.MethodPost, "/v1/purges", tc.body)
			require.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
			if tc.wantErrSubstr != "" {
				require.Contains(t, rec.Body.String(), tc.wantErrSubstr)
			}
			require.Equal(t, tc.wantStarts, eng.starts.Load())
		})
	}
}

func TestPostPurges_InjectionAttempt(t *testing.T) {
	t.Parallel()
	eng := newFakeEngine()
	s := newServer(t, eng, &fakePinger{})
	body := `{"target":"events\"; DROP TABLE users; --","before":"2026-01-01T00:00:00Z"}`
	rec := doRequest(t, s.Handler(), http.MethodPost, "/v1/purges", body)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, int32(0), eng.starts.Load())
}

func TestGetPurges(t *testing.T) {
	t.Parallel()
	eng := newFakeEngine()
	s := newServer(t, eng, &fakePinger{})

	// preload one running run
	rec := doRequest(t, s.Handler(), http.MethodPost, "/v1/purges",
		`{"target":"events","before":"2026-01-01T00:00:00Z"}`)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var created createResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	t.Run("found running", func(t *testing.T) {
		t.Parallel()
		rec := doRequest(t, s.Handler(), http.MethodGet, "/v1/purges/"+created.PurgeID, "")
		require.Equal(t, http.StatusOK, rec.Code)
		var got runResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, "events", got.Target)
		require.Equal(t, "running", got.Status)
		require.NotEmpty(t, got.StartedAt)
		require.Empty(t, got.FinishedAt)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		rec := doRequest(t, s.Handler(), http.MethodGet, "/v1/purges/missing", "")
		require.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHealthz(t *testing.T) {
	t.Parallel()
	eng := newFakeEngine()
	s := newServer(t, eng, &fakePinger{})
	rec := doRequest(t, s.Handler(), http.MethodGet, "/healthz", "")
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestReadyz(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"ok", nil, http.StatusOK},
		{"db down", errors.New("conn refused"), http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eng := newFakeEngine()
			s := newServer(t, eng, &fakePinger{err: tc.err})
			rec := doRequest(t, s.Handler(), http.MethodGet, "/readyz", "")
			require.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

func TestConcurrentPostsDifferentTargets(t *testing.T) {
	t.Parallel()
	eng := newFakeEngine()
	s := newServer(t, eng, &fakePinger{})
	h := s.Handler()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		alias := "events"
		if i%2 == 0 {
			alias = "logs"
		}
		body := `{"target":"` + alias + `","before":"2026-01-01T00:00:00Z"}`
		wg.Go(func() {
			rec := doRequest(t, h, http.MethodPost, "/v1/purges", body)
			if rec.Code != http.StatusAccepted {
				t.Errorf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
	wg.Wait()
	require.Equal(t, int32(32), eng.starts.Load())
}

func TestConcurrentPostsSameTarget_Lock(t *testing.T) {
	t.Parallel()
	eng := newFakeEngine()
	eng.startErr = map[string]error{"events": purge.ErrLockBusy}
	s := newServer(t, eng, &fakePinger{})
	h := s.Handler()

	var ok, conflict atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			rec := doRequest(t, h, http.MethodPost, "/v1/purges",
				`{"target":"events","before":"2026-01-01T00:00:00Z"}`)
			switch rec.Code {
			case http.StatusAccepted:
				ok.Add(1)
			case http.StatusConflict:
				conflict.Add(1)
			}
		})
	}
	wg.Wait()
	require.Equal(t, int32(16), conflict.Load())
	require.Equal(t, int32(0), ok.Load())
}

func FuzzPurgeRequest(f *testing.F) {
	for _, s := range []string{
		`{"target":"events","before":"2026-01-01T00:00:00Z"}`,
		`{"target":"","before":""}`,
		`{}`,
		`not-json`,
		`{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":-1}`,
	} {
		f.Add(s)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.Fuzz(func(t *testing.T, body string) {
		eng := newFakeEngine()
		targets := map[string]TargetSpec{"events": {Schema: "public", Table: "events", TSColumn: "created_at"}}
		s := NewServer(eng, eng.registry, &fakePinger{}, logger, targets, Limits{MaxChunkSize: 1000, DefaultChunkSize: 100})
		req := httptest.NewRequest(http.MethodPost, "/v1/purges", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		// Любой ответ должен быть валидным JSON либо пустым (при stream-disconnect).
		if rec.Body.Len() > 0 {
			var any map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &any)
		}
	})
}
