// Package purge реализует движок чанкового удаления старых строк по
// timestamp-колонке: интерфейс Deleter (на стороне потребителя), реестр
// активных прогонов и сам Engine.
package purge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Sentinel-ошибки движка.
var (
	// ErrLockBusy сигнализирует, что advisory-lock для целевой таблицы занят.
	ErrLockBusy = errors.New("purge: lock is busy")
	// ErrNotFound сигнализирует, что прогон с указанным id не найден в реестре.
	ErrNotFound = errors.New("purge: run not found")
	// ErrInvalidParams сигнализирует о невалидных параметрах прогона.
	ErrInvalidParams = errors.New("purge: invalid params")
)

// Status — финальное или промежуточное состояние прогона.
type Status string

const (
	// StatusRunning — прогон выполняется.
	StatusRunning Status = "running"
	// StatusDone — прогон завершён успешно (нет строк к удалению или достигнут MaxChunks).
	StatusDone Status = "done"
	// StatusFailed — прогон завершён с ошибкой.
	StatusFailed Status = "failed"
	// StatusAborted — прогон отменён внешним контекстом (shutdown, отзыв и т.п.).
	StatusAborted Status = "aborted"
)

// Deleter — узкий интерфейс на стороне потребителя для слоя БД.
// Реализуется *store.PgxStore.
type Deleter interface {
	// AcquireTableLock пытается взять advisory-lock по ключу. ok=false без err — лок занят.
	AcquireTableLock(ctx context.Context, key string) (release func(), ok bool, err error)
	// DeleteChunk удаляет до chunkSize строк, где tsCol < before.
	DeleteChunk(ctx context.Context, schema, table, tsCol string, before time.Time, chunkSize int) (int64, error)
}

// Params — параметры одного прогона удаления.
type Params struct {
	// Alias — короткое имя таблицы (используется как ключ advisory-lock и в логах).
	Alias string
	// Schema, Table, TSColumn — реальные идентификаторы PostgreSQL.
	Schema, Table, TSColumn string
	// Before — верхняя граница времени (исключительно): удаляются строки с TS < Before.
	Before time.Time
	// ChunkSize — размер пакета удаления (>0).
	ChunkSize int
	// MaxChunks — мягкий предел числа пакетов; 0 — без предела.
	MaxChunks int
	// Pause — пауза между пакетами (>=0).
	Pause time.Duration
}

// Validate проверяет корректность параметров.
func (p Params) Validate() error {
	if p.Alias == "" || p.Schema == "" || p.Table == "" || p.TSColumn == "" {
		return fmt.Errorf("%w: empty identifier", ErrInvalidParams)
	}
	if p.Before.IsZero() {
		return fmt.Errorf("%w: zero before", ErrInvalidParams)
	}
	if p.ChunkSize <= 0 {
		return fmt.Errorf("%w: chunk_size must be positive", ErrInvalidParams)
	}
	if p.MaxChunks < 0 {
		return fmt.Errorf("%w: max_chunks must be non-negative", ErrInvalidParams)
	}
	if p.Pause < 0 {
		return fmt.Errorf("%w: pause must be non-negative", ErrInvalidParams)
	}
	return nil
}

// Run — запись о прогоне (статус + метрики). Все поля защищены mu.
type Run struct {
	ID         string
	Alias      string
	Before     time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	Status     Status
	ChunksDone int
	Deleted    int64
	Err        string

	mu     sync.RWMutex
	cancel context.CancelFunc
}

// Snapshot возвращает безопасную копию Run для сериализации.
func (r *Run) Snapshot() Run {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return Run{
		ID: r.ID, Alias: r.Alias, Before: r.Before,
		StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
		Status: r.Status, ChunksDone: r.ChunksDone, Deleted: r.Deleted, Err: r.Err,
	}
}

// Cancel отменяет прогон, если он ещё активен.
func (r *Run) Cancel() {
	r.mu.RLock()
	c := r.cancel
	r.mu.RUnlock()
	if c != nil {
		c()
	}
}

func (r *Run) update(fn func(*Run)) {
	r.mu.Lock()
	fn(r)
	r.mu.Unlock()
}

// Registry — потокобезопасный реестр активных и завершённых прогонов.
type Registry struct {
	mu   sync.RWMutex
	runs map[string]*Run
}

// NewRegistry создаёт пустой реестр.
func NewRegistry() *Registry {
	return &Registry{runs: make(map[string]*Run)}
}

// Add регистрирует новый прогон.
func (r *Registry) Add(run *Run) {
	r.mu.Lock()
	r.runs[run.ID] = run
	r.mu.Unlock()
}

// Get возвращает прогон по id или ErrNotFound.
func (r *Registry) Get(id string) (*Run, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	run, ok := r.runs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return run, nil
}

// Active возвращает копию слайса активных прогонов (status=running).
func (r *Registry) Active() []*Run {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Run, 0)
	for _, run := range r.runs {
		if run.Snapshot().Status == StatusRunning {
			out = append(out, run)
		}
	}
	return out
}

// CancelAll отменяет все активные прогоны.
func (r *Registry) CancelAll() {
	for _, run := range r.Active() {
		run.Cancel()
	}
}

// Sweep удаляет из реестра завершённые прогоны, у которых FinishedAt
// старше cutoff. Возвращает число удалённых записей. Активные прогоны
// (status=running) не удаляются.
func (r *Registry) Sweep(cutoff time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := 0
	for id, run := range r.runs {
		snap := run.Snapshot()
		if snap.Status == StatusRunning {
			continue
		}
		if !snap.FinishedAt.IsZero() && snap.FinishedAt.Before(cutoff) {
			delete(r.runs, id)
			removed++
		}
	}
	return removed
}

// Len возвращает текущее число записей в реестре.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.runs)
}

// Engine выполняет прогон удаления чанками.
//
// Engine владеет baseCtx — долгоживущим контекстом сервиса (обычно от
// signal.NotifyContext в main). Каждый прогон запускается на runCtx, который
// производен от baseCtx и не от HTTP-запроса; иначе после ответа `202 Accepted`
// контекст HTTP отменился бы и прогон сразу попал в StatusAborted.
type Engine struct {
	baseCtx  context.Context
	store    Deleter
	logger   *slog.Logger
	registry *Registry
	wg       sync.WaitGroup
	idGen    func() string
	clock    func() time.Time
}

// EngineOption — функциональный опционал конструктора.
type EngineOption func(*Engine)

// WithIDGen позволяет подменить генератор id (для тестов).
func WithIDGen(fn func() string) EngineOption { return func(e *Engine) { e.idGen = fn } }

// WithClock позволяет подменить источник времени (для тестов).
func WithClock(fn func() time.Time) EngineOption { return func(e *Engine) { e.clock = fn } }

// NewEngine создаёт движок. baseCtx задаёт верхнюю границу жизни всех прогонов
// (обычно rootCtx сервиса). logger обязателен; nil заменяется на slog.Default().
func NewEngine(baseCtx context.Context, store Deleter, registry *Registry, logger *slog.Logger, opts ...EngineOption) *Engine {
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}
	e := &Engine{
		baseCtx: baseCtx,
		store:   store, logger: logger, registry: registry,
		idGen: newRandomID, clock: time.Now,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Wait блокируется до завершения всех запущенных прогонов.
func (e *Engine) Wait() { e.wg.Wait() }

// Start пытается запустить прогон. При успехе возвращает зарегистрированный *Run
// в статусе running и nil; при занятом advisory-lock — nil и ErrLockBusy.
//
// acquireCtx используется только для синхронной фазы: взятия advisory-lock'а.
// Жизнь самого прогона привязана к baseCtx из NewEngine; поэтому Start можно
// безопасно вызывать из HTTP-хендлера, передавая r.Context() в acquireCtx.
func (e *Engine) Start(acquireCtx context.Context, p Params) (*Run, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	release, ok, err := e.store.AcquireTableLock(acquireCtx, p.Alias)
	if err != nil {
		return nil, fmt.Errorf("acquire lock: %w", err)
	}
	if !ok {
		return nil, ErrLockBusy
	}

	runCtx, cancel := context.WithCancel(e.baseCtx)
	run := &Run{
		ID:        e.idGen(),
		Alias:     p.Alias,
		Before:    p.Before,
		StartedAt: e.clock(),
		Status:    StatusRunning,
		cancel:    cancel,
	}
	e.registry.Add(run)

	e.wg.Go(func() {
		defer release()
		defer cancel()
		e.loop(runCtx, run, p)
	})
	return run, nil
}

func (e *Engine) loop(ctx context.Context, run *Run, p Params) {
	log := e.logger.With(
		slog.String("purge_id", run.ID),
		slog.String("target", p.Alias),
		slog.Time("before", p.Before),
	)
	log.Info("purge.started",
		slog.Int("chunk_size", p.ChunkSize),
		slog.Int("max_chunks", p.MaxChunks),
		slog.Duration("pause", p.Pause),
	)

	for chunkIdx := 0; ; chunkIdx++ {
		if err := ctx.Err(); err != nil {
			e.finalize(run, StatusAborted, err)
			log.Warn("purge.aborted",
				slog.String("error", err.Error()),
				slog.Int("chunks_done", run.Snapshot().ChunksDone),
				slog.Int64("deleted_total", run.Snapshot().Deleted),
			)
			return
		}
		if p.MaxChunks > 0 && chunkIdx >= p.MaxChunks {
			e.finalizeOK(run, log)
			return
		}

		start := e.clock()
		rows, err := e.deleteChunkWithRetry(ctx, p, log)
		dur := e.clock().Sub(start)
		if err != nil {
			if ctx.Err() != nil {
				e.finalize(run, StatusAborted, ctx.Err())
				log.Warn("purge.aborted", slog.String("error", ctx.Err().Error()))
				return
			}
			e.finalize(run, StatusFailed, err)
			log.Error("purge.failed", slog.String("error", err.Error()),
				slog.Int("chunks_done", run.Snapshot().ChunksDone),
				slog.Int64("deleted_total", run.Snapshot().Deleted),
			)
			return
		}

		run.update(func(r *Run) {
			r.ChunksDone++
			r.Deleted += rows
		})
		log.Info("purge.chunk",
			slog.Int64("rows_deleted", rows),
			slog.Duration("duration", dur),
			slog.Int("chunk_idx", chunkIdx),
		)

		if rows == 0 {
			e.finalizeOK(run, log)
			return
		}
		if p.Pause > 0 {
			if !sleepCtx(ctx, p.Pause) {
				e.finalize(run, StatusAborted, ctx.Err())
				log.Warn("purge.aborted", slog.String("error", "context canceled during pause"))
				return
			}
		}
	}
}

func (e *Engine) deleteChunkWithRetry(ctx context.Context, p Params, log *slog.Logger) (int64, error) {
	const maxRetries = 1
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		rows, err := e.store.DeleteChunk(ctx, p.Schema, p.Table, p.TSColumn, p.Before, p.ChunkSize)
		if err == nil {
			return rows, nil
		}
		lastErr = err
		if !isRetryable(err) || ctx.Err() != nil {
			return 0, err
		}
		// Экспоненциальный backoff: attempt=0 → 100ms, attempt=1 → 200ms.
		backoff := time.Duration(1<<attempt) * 100 * time.Millisecond
		log.Warn("purge.chunk.retry",
			slog.String("error", err.Error()),
			slog.Int("attempt", attempt),
			slog.Duration("backoff", backoff),
		)
		if !sleepCtx(ctx, backoff) {
			return 0, ctx.Err()
		}
	}
	return 0, lastErr
}

func (e *Engine) finalize(run *Run, status Status, err error) {
	run.update(func(r *Run) {
		r.Status = status
		r.FinishedAt = e.clock()
		if err != nil {
			r.Err = err.Error()
		}
	})
}

func (e *Engine) finalizeOK(run *Run, log *slog.Logger) {
	e.finalize(run, StatusDone, nil)
	snap := run.Snapshot()
	log.Info("purge.completed",
		slog.Int64("deleted_total", snap.Deleted),
		slog.Int("chunks_done", snap.ChunksDone),
		slog.Duration("duration", snap.FinishedAt.Sub(snap.StartedAt)),
		slog.String("status", string(snap.Status)),
	)
}

// sleepCtx ждёт d или ctx.Done. Возвращает true, если выждал полностью.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// isRetryable распознаёт временные ошибки PostgreSQL по тексту:
// lock_timeout (55P03), serialization_failure (40001), deadlock_detected (40P01).
// Распознавание по подстроке — компромисс ради независимости от внешнего пакета,
// конкретные SQLSTATE'ы пробрасываются pgx в текст ошибки.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, mark := range []string{
		"55P03",               // lock_not_available / lock_timeout
		"40001",               // serialization_failure
		"40P01",               // deadlock_detected
		"canceling statement", // statement_timeout
	} {
		if strings.Contains(msg, mark) {
			return true
		}
	}
	return false
}

// newRandomID возвращает 16-байтовый идентификатор в hex (32 символа).
// Использует crypto/rand; ошибки не ожидаются на платформе с /dev/urandom
// или CryptGenRandom — в случае невозможной ошибки паникуем (это процесс уровня).
func newRandomID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand на современных ОС не должен возвращать ошибку;
		// если возвращает — продолжать работу нельзя.
		panic(fmt.Errorf("crypto/rand: %w", err))
	}
	return hex.EncodeToString(buf[:])
}
