package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redmadrobot/bee/purged/internal/purge"
	"github.com/stretchr/testify/require"
)

// realStore — потокобезопасный фейк Deleter, моделирующий чанковое удаление
// и advisory-lock по alias. Используется в "интеграционных" тестах HTTP-слоя
// с настоящим purge.Engine.
type realStore struct {
	mu          sync.Mutex
	rowsPerCall int64
	chunks      int
	chunksLeft  int
	called      atomic.Int32
	delay       time.Duration
	locked      map[string]bool
}

func newRealStore(rowsPerCall int64, chunks int, delay time.Duration) *realStore {
	return &realStore{
		rowsPerCall: rowsPerCall,
		chunks:      chunks,
		chunksLeft:  chunks,
		delay:       delay,
		locked:      map[string]bool{},
	}
}

func (s *realStore) AcquireTableLock(_ context.Context, key string) (func(), bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locked[key] {
		return nil, false, nil
	}
	s.locked[key] = true
	return func() {
		s.mu.Lock()
		delete(s.locked, key)
		s.mu.Unlock()
	}, true, nil
}

func (s *realStore) DeleteChunk(ctx context.Context, _, _, _ string, _ time.Time, _ int) (int64, error) {
	s.called.Add(1)
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chunksLeft <= 0 {
		return 0, nil
	}
	s.chunksLeft--
	return s.rowsPerCall, nil
}

func discardJSON() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func realServer(t *testing.T, baseCtx context.Context, st *realStore) (*httptest.Server, *purge.Registry, *purge.Engine) {
	t.Helper()
	reg := purge.NewRegistry()
	eng := purge.NewEngine(baseCtx, st, reg, discardJSON())
	srv := NewServer(eng, reg, nil, discardJSON(),
		map[string]TargetSpec{
			"events": {Schema: "public", Table: "events", TSColumn: "created_at"},
			"logs":   {Schema: "audit", Table: "app_logs", TSColumn: "ts"},
		},
		Limits{MaxChunkSize: 50000, DefaultChunkSize: 1000, DefaultPause: 0},
	)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	t.Cleanup(eng.Wait)
	return ts, reg, eng
}

func postPurge(t *testing.T, base, body string) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Post(base+"/v1/purges", "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp, b
}

// TestRealEngine_PurgeCompletesAfterHTTPReturns доказывает, что прогон НЕ
// прерывается отменой контекста HTTP-запроса (это была регрессия: r.Context()
// в качестве родителя для runCtx убивал прогон сразу после возврата 202).
func TestRealEngine_PurgeCompletesAfterHTTPReturns(t *testing.T) {
	t.Parallel()
	st := newRealStore(50, 4, 30*time.Millisecond)
	ts, reg, eng := realServer(t, context.Background(), st)

	resp, body := postPurge(t, ts.URL,
		`{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":50}`)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "body: %s", body)

	var created createResponse
	require.NoError(t, json.Unmarshal(body, &created))

	// HTTP-запрос уже завершился — но прогон должен продолжаться.
	eng.Wait()

	run, err := reg.Get(created.PurgeID)
	require.NoError(t, err)
	snap := run.Snapshot()
	require.Equal(t, purge.StatusDone, snap.Status, "err=%q", snap.Err)
	require.Equal(t, int64(200), snap.Deleted, "must delete 4 chunks * 50 rows")
	require.GreaterOrEqual(t, st.called.Load(), int32(4))
}

// TestRealEngine_SameTargetLock — реалистичный кейс «один accepted, остальные
// 409, после завершения снова accepted».
func TestRealEngine_SameTargetLock(t *testing.T) {
	t.Parallel()
	st := newRealStore(10, 2, 80*time.Millisecond)
	ts, _, eng := realServer(t, context.Background(), st)

	// 1) первый POST принят
	resp1, _ := postPurge(t, ts.URL,
		`{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":10}`)
	require.Equal(t, http.StatusAccepted, resp1.StatusCode)

	// 2) второй и третий, пока первый ещё работает, должны получить 409
	var conflicts atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Go(func() {
			resp, _ := postPurge(t, ts.URL,
				`{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":10}`)
			if resp.StatusCode == http.StatusConflict {
				conflicts.Add(1)
			}
		})
	}
	wg.Wait()
	require.Equal(t, int32(5), conflicts.Load(), "пока прогон активен, все остальные должны быть 409")

	eng.Wait()

	// 3) после завершения первого прогона — снова можно
	resp4, _ := postPurge(t, ts.URL,
		`{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":10}`)
	require.Equal(t, http.StatusAccepted, resp4.StatusCode)
	eng.Wait()
}

// TestRealEngine_GetReachesDoneStatus — проверяем, что GET в итоге показывает done.
func TestRealEngine_GetReachesDoneStatus(t *testing.T) {
	t.Parallel()
	st := newRealStore(5, 3, 20*time.Millisecond)
	ts, _, eng := realServer(t, context.Background(), st)

	resp, body := postPurge(t, ts.URL,
		`{"target":"events","before":"2026-01-01T00:00:00Z","chunk_size":5}`)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var created createResponse
	require.NoError(t, json.Unmarshal(body, &created))

	eng.Wait()

	r, err := http.Get(ts.URL + "/v1/purges/" + created.PurgeID)
	require.NoError(t, err)
	defer func() { _ = r.Body.Close() }()
	require.Equal(t, http.StatusOK, r.StatusCode)

	var got runResponse
	require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
	require.Equal(t, "done", got.Status)
	require.Equal(t, int64(15), got.DeletedTotal)
	require.NotEmpty(t, got.FinishedAt)
}
