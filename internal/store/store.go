// Package store содержит реализацию доступа к PostgreSQL для сервиса purged:
// взятие table-level advisory-lock'а, чанковое удаление по timestamp-колонке
// и проверку доступности БД.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel-ошибки слоя store.
var (
	// ErrLockBusy сигнализирует, что advisory-lock для целевой таблицы уже удерживается.
	ErrLockBusy = errors.New("store: advisory lock is busy")
	// ErrInvalidIdentifier сообщает, что переданное имя таблицы/колонки не прошло базовую проверку.
	ErrInvalidIdentifier = errors.New("store: invalid identifier")
)

// PgxStore — конкретная реализация хранилища поверх pgxpool.Pool.
type PgxStore struct {
	pool         *pgxpool.Pool
	chunkTimeout time.Duration
	lockTimeout  time.Duration
}

// NewPgxStore создаёт PgxStore. Параметр chunkTimeout задаёт statement_timeout
// одной чанковой транзакции; lockTimeout — lock_timeout внутри неё (рекомендуется
// chunkTimeout/2). Оба значения должны быть положительными.
func NewPgxStore(pool *pgxpool.Pool, chunkTimeout, lockTimeout time.Duration) *PgxStore {
	return &PgxStore{pool: pool, chunkTimeout: chunkTimeout, lockTimeout: lockTimeout}
}

// Ping проверяет доступность БД.
func (s *PgxStore) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}

// AcquireTableLock пытается взять session-level advisory-lock по ключу,
// удерживая выделенный коннект из пула. Возвращает функцию-release, которая
// освобождает лок и коннект; ok=false без ошибки означает «лок занят».
// Ключ — произвольная строка (как правило, alias таблицы); внутри хешируется
// в bigint через hashtext.
func (s *PgxStore) AcquireTableLock(ctx context.Context, key string) (release func(), ok bool, err error) {
	if key == "" {
		return nil, false, fmt.Errorf("%w: empty lock key", ErrInvalidIdentifier)
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire conn: %w", err)
	}
	var got bool
	row := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1)::bigint)`, key)
	if err := row.Scan(&got); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("try advisory lock: %w", err)
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	release = func() {
		// Освобождение ошибок не возвращает — конец сессии всё равно снимет lock.
		// Логирование делается на уровне выше.
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtext($1)::bigint)`, key)
		conn.Release()
	}
	return release, true, nil
}

// DeleteChunk выполняет одно чанковое удаление в собственной короткой транзакции.
// Возвращает фактическое число удалённых строк. Идентификаторы schema/table/tsCol
// экранируются через pgx.Identifier.Sanitize.
func (s *PgxStore) DeleteChunk(ctx context.Context, schema, table, tsCol string, before time.Time, chunkSize int) (int64, error) {
	if !isSafeIdent(schema) || !isSafeIdent(table) || !isSafeIdent(tsCol) {
		return 0, fmt.Errorf("%w: %q.%q.%q", ErrInvalidIdentifier, schema, table, tsCol)
	}
	if chunkSize <= 0 {
		return 0, fmt.Errorf("%w: chunk size must be positive", ErrInvalidIdentifier)
	}
	sql := buildDeleteChunkSQL(schema, table, tsCol)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() {
		// Если коммит уже прошёл, Rollback вернёт ErrTxClosed — это норма.
		_ = tx.Rollback(context.Background())
	}()

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = %d", s.chunkTimeout.Milliseconds())); err != nil {
		return 0, fmt.Errorf("set statement_timeout: %w", err)
	}
	if s.lockTimeout > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", s.lockTimeout.Milliseconds())); err != nil {
			return 0, fmt.Errorf("set lock_timeout: %w", err)
		}
	}

	tag, err := tx.Exec(ctx, sql, before, chunkSize)
	if err != nil {
		return 0, fmt.Errorf("delete chunk: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return tag.RowsAffected(), nil
}

// buildDeleteChunkSQL формирует SQL чанкового удаления с позиционными параметрами:
// $1 — верхняя граница времени (исключительно), $2 — размер пакета.
// Идентификаторы экранируются pgx.Identifier.Sanitize.
func buildDeleteChunkSQL(schema, table, tsCol string) string {
	qTable := pgx.Identifier{schema, table}.Sanitize()
	qTs := pgx.Identifier{tsCol}.Sanitize()
	return fmt.Sprintf(
		"DELETE FROM %s WHERE ctid IN (SELECT ctid FROM %s WHERE %s < $1 ORDER BY %s LIMIT $2)",
		qTable, qTable, qTs, qTs,
	)
}

// isSafeIdent дублирует базовую проверку идентификатора (защита в глубину):
// первая линия — whitelist в config; здесь — повторная проверка перед SQL.
func isSafeIdent(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
