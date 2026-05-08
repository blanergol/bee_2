package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildDeleteChunkSQL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		schema, table string
		ts            string
		wantContains  []string
	}{
		{
			name:   "simple identifiers",
			schema: "public", table: "events", ts: "created_at",
			wantContains: []string{
				`DELETE FROM "public"."events"`,
				`SELECT ctid FROM "public"."events"`,
				`"created_at" < $1`,
				`ORDER BY "created_at" LIMIT $2`,
			},
		},
		{
			name:   "uppercase identifiers escaped",
			schema: "AuditX", table: "AppLogs", ts: "TS",
			wantContains: []string{
				`"AuditX"."AppLogs"`,
				`"TS" < $1`,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := buildDeleteChunkSQL(tc.schema, tc.table, tc.ts)
			for _, sub := range tc.wantContains {
				require.Contains(t, got, sub)
			}
		})
	}
}

func TestDeleteChunk_RejectsBadIdentifiers(t *testing.T) {
	t.Parallel()
	s := &PgxStore{} // pool не используется — выпадаем по валидации идентификатора
	cases := []struct {
		name                 string
		schema, table, tsCol string
		chunkSize            int
	}{
		{name: "schema with quote", schema: `pub"lic`, table: "t", tsCol: "c", chunkSize: 1},
		{name: "table empty", schema: "p", table: "", tsCol: "c", chunkSize: 1},
		{name: "ts with semicolon", schema: "p", table: "t", tsCol: "c;DROP", chunkSize: 1},
		{name: "non-positive chunk", schema: "p", table: "t", tsCol: "c", chunkSize: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := s.DeleteChunk(context.Background(), tc.schema, tc.table, tc.tsCol, time.Now(), tc.chunkSize)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrInvalidIdentifier))
		})
	}
}

func TestAcquireTableLock_EmptyKey(t *testing.T) {
	t.Parallel()
	s := &PgxStore{}
	_, ok, err := s.AcquireTableLock(context.Background(), "")
	require.Error(t, err)
	require.False(t, ok)
	require.True(t, errors.Is(err, ErrInvalidIdentifier))
}

func TestIsSafeIdent(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"":         false,
		"a":        true,
		"a1":       true,
		"_x":       true,
		"1bad":     false,
		"good_one": true,
		"bad-one":  false,
		"bad one":  false,
		`bad"x`:    false,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, want, isSafeIdent(in))
		})
	}
}
