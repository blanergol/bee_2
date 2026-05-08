// Package httpapi реализует HTTP-контракт сервиса purged: запуск, наблюдение,
// liveness и readiness проверки.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redmadrobot/bee/purged/internal/purge"
)

// Pinger — узкий интерфейс для readiness-проверки БД.
type Pinger interface {
	Ping(ctx context.Context) error
}

// TargetSpec — описание разрешённой таблицы из whitelist'а.
type TargetSpec struct {
	Schema   string
	Table    string
	TSColumn string
}

// Engine — узкий интерфейс на стороне HTTP-хендлеров.
type Engine interface {
	Start(ctx context.Context, p purge.Params) (*purge.Run, error)
}

// Registry — узкий интерфейс реестра прогонов на стороне HTTP-хендлеров.
type Registry interface {
	Get(id string) (*purge.Run, error)
}

// Limits задаёт верхние границы клиентских параметров.
type Limits struct {
	// MaxChunkSize — верхняя граница chunk_size, переданного в запросе.
	MaxChunkSize int
	// DefaultChunkSize — значение, подставляемое при отсутствии в запросе.
	DefaultChunkSize int
	// DefaultPause — значение, подставляемое при отсутствии в запросе.
	DefaultPause time.Duration
}

// Server агрегирует зависимости HTTP-хендлеров.
type Server struct {
	engine   Engine
	registry Registry
	pinger   Pinger
	logger   *slog.Logger
	targets  map[string]TargetSpec
	limits   Limits
	readyCtx func(parent context.Context) (context.Context, context.CancelFunc)
}

// NewServer создаёт Server. logger обязателен, nil заменяется на slog.Default().
func NewServer(engine Engine, registry Registry, pinger Pinger, logger *slog.Logger, targets map[string]TargetSpec, limits Limits) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if limits.MaxChunkSize <= 0 {
		limits.MaxChunkSize = 200000
	}
	if limits.DefaultChunkSize <= 0 {
		limits.DefaultChunkSize = 10000
	}
	if limits.DefaultPause < 0 {
		limits.DefaultPause = 0
	}
	return &Server{
		engine: engine, registry: registry, pinger: pinger, logger: logger,
		targets: targets, limits: limits,
		readyCtx: func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(parent, 2*time.Second)
		},
	}
}

// Handler возвращает router'а с зарегистрированными эндпоинтами.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/purges", s.handleCreate)
	mux.HandleFunc("GET /v1/purges/{id}", s.handleGet)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	return mux
}

type createRequest struct {
	Target    string `json:"target"`
	Before    string `json:"before"`
	ChunkSize *int   `json:"chunk_size,omitempty"`
	PauseMS   *int   `json:"pause_ms,omitempty"`
	MaxChunks *int   `json:"max_chunks,omitempty"`
}

type createResponse struct {
	PurgeID string `json:"purge_id"`
	Status  string `json:"status"`
}

type runResponse struct {
	PurgeID      string `json:"purge_id"`
	Target       string `json:"target"`
	Status       string `json:"status"`
	DeletedTotal int64  `json:"deleted_total"`
	ChunksDone   int    `json:"chunks_done"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at,omitempty"`
	Error        string `json:"error,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<14))
	dec.DisallowUnknownFields()
	var req createRequest
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("decode: %s", err)})
		return
	}
	spec, ok := s.targets[req.Target]
	if !ok {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "unknown target"})
		return
	}
	before, err := time.Parse(time.RFC3339, req.Before)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "before must be RFC3339"})
		return
	}

	chunkSize := s.limits.DefaultChunkSize
	if req.ChunkSize != nil {
		if *req.ChunkSize <= 0 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "chunk_size must be positive"})
			return
		}
		if *req.ChunkSize > s.limits.MaxChunkSize {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "chunk_size exceeds max"})
			return
		}
		chunkSize = *req.ChunkSize
	}
	pause := s.limits.DefaultPause
	if req.PauseMS != nil {
		if *req.PauseMS < 0 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "pause_ms must be non-negative"})
			return
		}
		pause = time.Duration(*req.PauseMS) * time.Millisecond
	}
	maxChunks := 0
	if req.MaxChunks != nil {
		if *req.MaxChunks < 0 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "max_chunks must be non-negative"})
			return
		}
		maxChunks = *req.MaxChunks
	}

	params := purge.Params{
		Alias: req.Target, Schema: spec.Schema, Table: spec.Table, TSColumn: spec.TSColumn,
		Before: before, ChunkSize: chunkSize, Pause: pause, MaxChunks: maxChunks,
	}

	run, err := s.engine.Start(r.Context(), params)
	switch {
	case errors.Is(err, purge.ErrLockBusy):
		writeJSON(w, http.StatusConflict, errorResponse{Error: "another purge for this target is in progress"})
		return
	case errors.Is(err, purge.ErrInvalidParams):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	case err != nil:
		s.logger.Error("purge.start.failed", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal error"})
		return
	}

	writeJSON(w, http.StatusAccepted, createResponse{PurgeID: run.ID, Status: string(purge.StatusRunning)})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.registry.Get(id)
	if errors.Is(err, purge.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "not found"})
		return
	}
	if err != nil {
		s.logger.Error("registry.get.failed", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal error"})
		return
	}
	snap := run.Snapshot()
	resp := runResponse{
		PurgeID: snap.ID, Target: snap.Alias, Status: string(snap.Status),
		DeletedTotal: snap.Deleted, ChunksDone: snap.ChunksDone,
		StartedAt: snap.StartedAt.UTC().Format(time.RFC3339Nano),
		Error:     snap.Err,
	}
	if !snap.FinishedAt.IsZero() {
		resp.FinishedAt = snap.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.pinger == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	ctx, cancel := s.readyCtx(r.Context())
	defer cancel()
	if err := s.pinger.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// Логировать здесь нечем — клиент уже получил статус-код. Игнорируем,
		// но не молча: возвращать ошибку через panic в http-handler нельзя.
		_ = err
	}
}
