//go:build integration

package parter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool returns a connected pgxpool.Pool from PARTER_TEST_DSN, skipping the
// test when the env var is empty.
//
// CONTRACT §13.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PARTER_TEST_DSN")
	if dsn == "" {
		t.Skip("PARTER_TEST_DSN not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("pool.Ping: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// uniqueSchema returns a random, unused schema name and registers a cleanup
// that drops it CASCADE. The schema itself is NOT created here; callers create
// it (e.g. via createSchema) so tests can also exercise creation idempotency.
//
// CONTRACT §13.
func uniqueSchema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	name := "parter_test_" + hex.EncodeToString(b[:])
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", name))
	})
	return name
}
