//go:build integration

// Файл собирается только под build-tag `integration` и требует реального
// PostgreSQL: укажите DSN в переменной окружения PURGE_TEST_DSN
// и запускайте `go test -tags=integration ./internal/store`.
package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func mustPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PURGE_TEST_DSN")
	if dsn == "" {
		t.Skip("PURGE_TEST_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	return pool
}

func TestIntegration_DeleteAndAdvisoryLock(t *testing.T) {
	pool := mustPool(t)
	defer pool.Close()

	ctx := context.Background()
	tableName := fmt.Sprintf("purged_it_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf(`CREATE TABLE public.%s (
		id   bigserial PRIMARY KEY,
		ts   timestamptz NOT NULL,
		body text)`, tableName)
	_, err := pool.Exec(ctx, createSQL)
	require.NoError(t, err)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public."+tableName)
	}()
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE INDEX ON public.%s (ts)`, tableName))
	require.NoError(t, err)

	const total = 1000
	for i := 0; i < total; i++ {
		_, err := pool.Exec(ctx,
			fmt.Sprintf(`INSERT INTO public.%s(ts, body) VALUES (now() - make_interval(secs => $1), 'x')`, tableName),
			float64((total-i)*10))
		require.NoError(t, err)
	}

	s := NewPgxStore(pool, 5*time.Second, 1*time.Second)

	cutoff := time.Now().Add(-30 * time.Second)
	totalDeleted := int64(0)
	for i := 0; i < 100; i++ {
		n, err := s.DeleteChunk(ctx, "public", tableName, "ts", cutoff, 100)
		require.NoError(t, err)
		totalDeleted += n
		if n == 0 {
			break
		}
	}
	require.Greater(t, totalDeleted, int64(0))

	rel1, ok1, err := s.AcquireTableLock(ctx, "events_test")
	require.NoError(t, err)
	require.True(t, ok1)
	defer rel1()

	_, ok2, err := s.AcquireTableLock(ctx, "events_test")
	require.NoError(t, err)
	require.False(t, ok2, "second lock for same key must be busy")
}
