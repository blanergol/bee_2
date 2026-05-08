package purge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// fakeStore — потокобезопасный фейковый Deleter.
type fakeStore struct {
	mu        sync.Mutex
	chunks    []int64 // последовательность результатов DeleteChunk
	chunkErr  []error // последовательность ошибок (по индексу)
	calls     int
	lockedSet map[string]bool
	lockBusy  map[string]bool
	lockErr   error
	delay     time.Duration
}

func (s *fakeStore) AcquireTableLock(_ context.Context, key string) (func(), bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lockErr != nil {
		return nil, false, s.lockErr
	}
	if s.lockBusy[key] {
		return nil, false, nil
	}
	if s.lockedSet == nil {
		s.lockedSet = map[string]bool{}
	}
	s.lockedSet[key] = true
	return func() {
		s.mu.Lock()
		delete(s.lockedSet, key)
		s.mu.Unlock()
	}, true, nil
}

func (s *fakeStore) DeleteChunk(ctx context.Context, _, _, _ string, _ time.Time, _ int) (int64, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	s.mu.Lock()
	idx := s.calls
	s.calls++
	s.mu.Unlock()
	if idx < len(s.chunkErr) && s.chunkErr[idx] != nil {
		return 0, s.chunkErr[idx]
	}
	if idx < len(s.chunks) {
		return s.chunks[idx], nil
	}
	return 0, nil
}

func defaultParams() Params {
	return Params{
		Alias: "events", Schema: "public", Table: "events", TSColumn: "created_at",
		Before:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ChunkSize: 100, Pause: 0,
	}
}

func waitForStatus(t *testing.T, run *Run, want Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if run.Snapshot().Status == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected status %q, got %q", want, run.Snapshot().Status)
}

func TestParams_Validate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(*Params)
		wantErr error
	}{
		{name: "ok", mutate: func(p *Params) {}},
		{name: "empty alias", mutate: func(p *Params) { p.Alias = "" }, wantErr: ErrInvalidParams},
		{name: "empty schema", mutate: func(p *Params) { p.Schema = "" }, wantErr: ErrInvalidParams},
		{name: "empty table", mutate: func(p *Params) { p.Table = "" }, wantErr: ErrInvalidParams},
		{name: "empty ts col", mutate: func(p *Params) { p.TSColumn = "" }, wantErr: ErrInvalidParams},
		{name: "zero before", mutate: func(p *Params) { p.Before = time.Time{} }, wantErr: ErrInvalidParams},
		{name: "zero chunk", mutate: func(p *Params) { p.ChunkSize = 0 }, wantErr: ErrInvalidParams},
		{name: "negative max chunks", mutate: func(p *Params) { p.MaxChunks = -1 }, wantErr: ErrInvalidParams},
		{name: "negative pause", mutate: func(p *Params) { p.Pause = -1 }, wantErr: ErrInvalidParams},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := defaultParams()
			tc.mutate(&p)
			err := p.Validate()
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.True(t, errors.Is(err, tc.wantErr))
		})
	}
}

func TestEngine_HappyPath(t *testing.T) {
	t.Parallel()
	store := &fakeStore{chunks: []int64{100, 100, 50, 0}}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, discardLogger())

	run, err := eng.Start(context.Background(), defaultParams())
	require.NoError(t, err)
	require.Equal(t, StatusRunning, run.Snapshot().Status)

	eng.Wait()

	snap := run.Snapshot()
	require.Equal(t, StatusDone, snap.Status)
	require.Equal(t, int64(250), snap.Deleted)
	require.Equal(t, 4, snap.ChunksDone)
	require.NotZero(t, snap.FinishedAt)
}

func TestEngine_MaxChunks(t *testing.T) {
	t.Parallel()
	store := &fakeStore{chunks: []int64{10, 10, 10, 10, 10, 10}}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, discardLogger())

	p := defaultParams()
	p.MaxChunks = 3
	run, err := eng.Start(context.Background(), p)
	require.NoError(t, err)
	eng.Wait()

	snap := run.Snapshot()
	require.Equal(t, StatusDone, snap.Status)
	require.Equal(t, 3, snap.ChunksDone)
	require.Equal(t, int64(30), snap.Deleted)
}

func TestEngine_LockBusy(t *testing.T) {
	t.Parallel()
	store := &fakeStore{lockBusy: map[string]bool{"events": true}}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, discardLogger())

	_, err := eng.Start(context.Background(), defaultParams())
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrLockBusy))
}

func TestEngine_FailureNonRetryable(t *testing.T) {
	t.Parallel()
	want := errors.New("boom: 42P01 relation does not exist")
	store := &fakeStore{chunks: []int64{}, chunkErr: []error{want}}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, discardLogger())

	run, err := eng.Start(context.Background(), defaultParams())
	require.NoError(t, err)
	eng.Wait()

	snap := run.Snapshot()
	require.Equal(t, StatusFailed, snap.Status)
	require.Contains(t, snap.Err, "boom")
}

func TestEngine_RetryThenSuccess(t *testing.T) {
	t.Parallel()
	store := &fakeStore{
		chunks:   []int64{0, 100, 0},
		chunkErr: []error{errors.New("ERROR: 55P03 lock_not_available"), nil, nil},
	}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, discardLogger())

	run, err := eng.Start(context.Background(), defaultParams())
	require.NoError(t, err)
	eng.Wait()

	snap := run.Snapshot()
	require.Equal(t, StatusDone, snap.Status)
	require.Equal(t, int64(100), snap.Deleted)
}

func TestEngine_CancelViaBaseCtx(t *testing.T) {
	t.Parallel()
	store := &fakeStore{
		chunks: []int64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100},
		delay:  20 * time.Millisecond,
	}
	reg := NewRegistry()
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	eng := NewEngine(baseCtx, store, reg, discardLogger())

	run, err := eng.Start(context.Background(), defaultParams())
	require.NoError(t, err)

	time.Sleep(40 * time.Millisecond)
	baseCancel()
	eng.Wait()

	require.Equal(t, StatusAborted, run.Snapshot().Status)
}

// TestEngine_AcquireCtxCancelDoesNotAbort фиксирует регрессию: отмена
// HTTP-контекста ПОСЛЕ возврата Start не должна прерывать прогон.
func TestEngine_AcquireCtxCancelDoesNotAbort(t *testing.T) {
	t.Parallel()
	store := &fakeStore{
		chunks: []int64{100, 100, 0},
		delay:  20 * time.Millisecond,
	}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, discardLogger())

	acquireCtx, cancelAcquire := context.WithCancel(context.Background())
	run, err := eng.Start(acquireCtx, defaultParams())
	require.NoError(t, err)
	cancelAcquire() // как будто HTTP-запрос завершился сразу после 202

	eng.Wait()
	require.Equal(t, StatusDone, run.Snapshot().Status)
	require.Equal(t, int64(200), run.Snapshot().Deleted)
}

func TestEngine_PauseRespected(t *testing.T) {
	t.Parallel()
	store := &fakeStore{chunks: []int64{10, 0}}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, discardLogger())

	p := defaultParams()
	p.Pause = 50 * time.Millisecond

	start := time.Now()
	_, err := eng.Start(context.Background(), p)
	require.NoError(t, err)
	eng.Wait()
	dur := time.Since(start)
	require.GreaterOrEqual(t, dur, 50*time.Millisecond, "pause must be applied between chunks")
}

func TestRegistry_GetMissing(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	_, err := reg.Get("nope")
	require.True(t, errors.Is(err, ErrNotFound))
}

func TestRegistry_RaceAddGet(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Go(func() {
			id := strconv.Itoa(i)
			run := &Run{ID: id, Alias: "a", Status: StatusRunning}
			reg.Add(run)
		})
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		_, err := reg.Get(strconv.Itoa(i))
		require.NoError(t, err)
	}
}

func TestEngine_ConcurrentDifferentTables(t *testing.T) {
	t.Parallel()
	store := &fakeStore{chunks: []int64{1, 0, 1, 0}}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, discardLogger())

	p1 := defaultParams()
	p1.Alias, p1.Table = "events", "events"
	p2 := defaultParams()
	p2.Alias, p2.Table = "logs", "app_logs"

	r1, err := eng.Start(context.Background(), p1)
	require.NoError(t, err)
	r2, err := eng.Start(context.Background(), p2)
	require.NoError(t, err)
	eng.Wait()

	require.Equal(t, StatusDone, r1.Snapshot().Status)
	require.Equal(t, StatusDone, r2.Snapshot().Status)
}

func TestRegistry_Sweep(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	old := &Run{ID: "old", Status: StatusDone, FinishedAt: now.Add(-2 * time.Hour)}
	fresh := &Run{ID: "fresh", Status: StatusDone, FinishedAt: now.Add(-10 * time.Minute)}
	running := &Run{ID: "run", Status: StatusRunning}
	failedOld := &Run{ID: "failedOld", Status: StatusFailed, FinishedAt: now.Add(-3 * time.Hour)}
	reg.Add(old)
	reg.Add(fresh)
	reg.Add(running)
	reg.Add(failedOld)

	removed := reg.Sweep(now.Add(-1 * time.Hour))
	require.Equal(t, 2, removed)
	require.Equal(t, 2, reg.Len())
	_, err := reg.Get("old")
	require.True(t, errors.Is(err, ErrNotFound))
	_, err = reg.Get("failedOld")
	require.True(t, errors.Is(err, ErrNotFound))
	_, err = reg.Get("run")
	require.NoError(t, err)
	_, err = reg.Get("fresh")
	require.NoError(t, err)
}

func TestRegistry_CancelAll(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	var canceled atomic.Int32
	for i := 0; i < 5; i++ {
		_, c := context.WithCancel(context.Background())
		stop := func() { canceled.Add(1); c() }
		run := &Run{ID: strconv.Itoa(i), Status: StatusRunning, cancel: stop}
		reg.Add(run)
	}
	reg.CancelAll()
	require.Equal(t, int32(5), canceled.Load())
}

func TestEngine_LogsStartedAndCompleted(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store := &fakeStore{chunks: []int64{10, 0}}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, logger)

	_, err := eng.Start(context.Background(), defaultParams())
	require.NoError(t, err)
	eng.Wait()

	// Каждая строка лога — отдельный JSON.
	lines := splitLines(buf.String())
	require.NotEmpty(t, lines)

	var sawStart, sawCompleted bool
	for _, l := range lines {
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &rec))
		require.Equal(t, "events", rec["target"])
		require.NotEmpty(t, rec["purge_id"])
		switch rec["msg"] {
		case "purge.started":
			sawStart = true
			require.Equal(t, "INFO", rec["level"])
		case "purge.completed":
			sawCompleted = true
			require.Equal(t, "INFO", rec["level"])
			require.Equal(t, float64(10), rec["deleted_total"])
		}
	}
	require.True(t, sawStart, "purge.started must be logged")
	require.True(t, sawCompleted, "purge.completed must be logged")
}

func TestEngine_LogsFailedOnError(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	store := &fakeStore{chunkErr: []error{errors.New("ERROR: 42P01 relation does not exist")}}
	reg := NewRegistry()
	eng := NewEngine(context.Background(), store, reg, logger)

	_, err := eng.Start(context.Background(), defaultParams())
	require.NoError(t, err)
	eng.Wait()

	var sawFailed bool
	for _, l := range splitLines(buf.String()) {
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &rec))
		if rec["msg"] == "purge.failed" {
			sawFailed = true
			require.Equal(t, "ERROR", rec["level"])
			require.Contains(t, rec["error"], "42P01")
		}
	}
	require.True(t, sawFailed)
}

func splitLines(s string) []string {
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func BenchmarkEngine_Loop(b *testing.B) {
	chunks := make([]int64, 1024)
	for i := range chunks {
		chunks[i] = 100
	}
	chunks[len(chunks)-1] = 0
	for i := 0; i < b.N; i++ {
		store := &fakeStore{chunks: chunks}
		reg := NewRegistry()
		eng := NewEngine(context.Background(), store, reg, discardLogger())
		_, err := eng.Start(context.Background(), defaultParams())
		if err != nil {
			b.Fatal(err)
		}
		eng.Wait()
	}
}
