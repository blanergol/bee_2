// Команда purged — HTTP-сервис чанкового удаления старых строк по
// timestamp-колонке из крупных PostgreSQL-таблиц.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/redmadrobot/bee/purged/internal/config"
	"github.com/redmadrobot/bee/purged/internal/httpapi"
	"github.com/redmadrobot/bee/purged/internal/obs"
	"github.com/redmadrobot/bee/purged/internal/purge"
	"github.com/redmadrobot/bee/purged/internal/store"
)

func main() {
	if err := run(os.Args[1:], os.LookupEnv); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// runE — вспомогательный entry-point для smoke-тестов: возвращает ошибку
// конфигурации без попытки коннекта к БД, если useDB=false.
func runConfigOnly(args []string, lookupEnv func(string) (string, bool)) error {
	_, err := config.Load(args, lookupEnv, os.Stderr)
	return err
}

func run(args []string, lookupEnv func(string) (string, bool)) error {
	cfg, err := config.Load(args, lookupEnv, os.Stderr)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := obs.NewLogger(os.Stdout, envOr(lookupEnv, "PURGE_LOG_LEVEL", "info"))
	logger.Info("purged.starting",
		slog.String("listen", cfg.Listen),
		slog.Int("targets", len(cfg.Targets)),
	)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(rootCtx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer pool.Close()

	// Стартовый ping не блокирует запуск (fail-open): pgxpool коннектится
	// лениво и сетевые/перезагрузочные сбои бывают временными. Состояние БД
	// далее наблюдается через GET /readyz.
	pingCtx, pingCancel := context.WithTimeout(rootCtx, 5*time.Second)
	if err := pool.Ping(pingCtx); err != nil {
		logger.Warn("startup.ping.failed",
			slog.String("error", err.Error()),
			slog.String("note", "service starts; readyz will report 503 until db becomes available"),
		)
	} else {
		logger.Info("startup.ping.ok")
	}
	pingCancel()

	st := store.NewPgxStore(pool, cfg.ChunkTimeout, cfg.ChunkTimeout/2)
	registry := purge.NewRegistry()
	engine := purge.NewEngine(rootCtx, st, registry, logger)

	// Периодически чистим завершённые прогоны старше PURGE_RETAIN.
	retain := cfg.RetainCompleted
	go runSweeper(rootCtx, registry, retain, logger)

	srv := httpapi.NewServer(engine, registry, st, logger, toTargetSpecs(cfg.Targets), httpapi.Limits{
		MaxChunkSize: cfg.MaxChunkSize, DefaultChunkSize: cfg.ChunkSize, DefaultPause: cfg.Pause,
	})

	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http.listen", slog.String("addr", cfg.Listen))
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown.signal")
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("http: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http.shutdown", slog.String("error", err.Error()))
	}
	registry.CancelAll()
	engine.Wait()
	logger.Info("purged.stopped")
	return nil
}

func toTargetSpecs(targets map[string]config.Target) map[string]httpapi.TargetSpec {
	out := make(map[string]httpapi.TargetSpec, len(targets))
	for k, v := range targets {
		out[k] = httpapi.TargetSpec{Schema: v.Schema, Table: v.Table, TSColumn: v.TSColumn}
	}
	return out
}

func envOr(lookupEnv func(string) (string, bool), key, def string) string {
	if v, ok := lookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// runSweeper периодически удаляет из реестра завершённые прогоны старше retain.
// retain<=0 отключает уборку. Goroutine завершается при ctx.Done().
func runSweeper(ctx context.Context, registry *purge.Registry, retain time.Duration, logger *slog.Logger) {
	if retain <= 0 {
		return
	}
	// Тикер не чаще, чем раз в минуту, и не реже retain.
	interval := retain / 4
	if interval < time.Minute {
		interval = time.Minute
	}
	if interval > retain {
		interval = retain
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			n := registry.Sweep(now.Add(-retain))
			if n > 0 {
				logger.Info("registry.swept", slog.Int("removed", n), slog.Int("remaining", registry.Len()))
			}
		}
	}
}
