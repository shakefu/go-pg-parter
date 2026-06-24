//go:build integration

package parter

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// columnExists reports whether a column exists on <schema>.managed_partitions.
func columnExists(t *testing.T, schema, col string) bool {
	t.Helper()
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = 'managed_partitions' AND column_name = $2
		)`, schema, col).Scan(&exists)
	if err != nil {
		t.Fatalf("columnExists query: %v", err)
	}
	return exists
}

// columnCount returns the number of columns on <schema>.managed_partitions.
func columnCount(t *testing.T, schema string) int {
	t.Helper()
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'managed_partitions'`, schema).Scan(&n)
	if err != nil {
		t.Fatalf("columnCount query: %v", err)
	}
	return n
}

// metadata.yass.yaml parter.ManagedPartitions SIDE-EFFECT: CREATE SCHEMA and
// CREATE TABLE succeed and are idempotent (run twice).
func TestIntegrationCreateSchemaTableIdempotent(t *testing.T) {
	pool := testPool(t)
	schema := uniqueSchema(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 2; i++ {
		if err := createSchema(ctx, pool, schema); err != nil {
			t.Fatalf("createSchema (run %d): %v", i, err)
		}
		if err := createTable(ctx, pool, schema); err != nil {
			t.Fatalf("createTable (run %d): %v", i, err)
		}
	}

	// Exactly these columns are present after a fresh create — no more (the
	// retention redesign removed the crash-recovery column) and no fewer.
	wantCols := []string{
		"table_name", "partition_name", "range_start", "range_end",
		"status", "created_at", "fail_count",
	}
	for _, col := range wantCols {
		if !columnExists(t, schema, col) {
			t.Errorf("column %q missing after createTable", col)
		}
	}
	if got := columnCount(t, schema); got != len(wantCols) {
		t.Errorf("fresh table has %d columns, want exactly %d (no removed recovery column)", got, len(wantCols))
	}
}

// metadata.yass.yaml parter.SchemaVersioning SIDE-EFFECT: applyMigrations is a
// no-op on a freshly created table (all columns already present).
func TestIntegrationMigrationsNoOpOnFreshTable(t *testing.T) {
	pool := testPool(t)
	schema := uniqueSchema(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := createSchema(ctx, pool, schema); err != nil {
		t.Fatalf("createSchema: %v", err)
	}
	if err := createTable(ctx, pool, schema); err != nil {
		t.Fatalf("createTable: %v", err)
	}

	// Run migrations (twice) on a fresh table; must be a clean no-op.
	for i := 0; i < 2; i++ {
		if err := applyMigrations(ctx, pool, schema); err != nil {
			t.Fatalf("applyMigrations (run %d): %v", i, err)
		}
	}

	for _, col := range []string{"fail_count"} {
		if !columnExists(t, schema, col) {
			t.Errorf("column %q missing after migrations on fresh table", col)
		}
	}
}

// metadata.yass.yaml parter.SchemaVersioning SIDE-EFFECT: applyMigrations
// upgrades an old table that was created missing fail_count, adding the column
// in place. (The crash-recovery column no longer exists after the retention
// redesign, so fail_count is the only migration.)
func TestIntegrationMigrationsUpgradeOldTable(t *testing.T) {
	pool := testPool(t)
	schema := uniqueSchema(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := createSchema(ctx, pool, schema); err != nil {
		t.Fatalf("createSchema: %v", err)
	}

	// Simulate an old table: create managed_partitions WITHOUT the later-added
	// fail_count column.
	oldDDL := fmt.Sprintf(`CREATE TABLE %s.managed_partitions (
        table_name TEXT NOT NULL,
        partition_name TEXT NOT NULL,
        range_start TIMESTAMPTZ NOT NULL,
        range_end TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TIMESTAMPTZ,
        PRIMARY KEY (table_name, partition_name)
    )`, schema)
	if _, err := pool.Exec(ctx, oldDDL); err != nil {
		t.Fatalf("create old table: %v", err)
	}

	// Precondition: fail_count is absent.
	if columnExists(t, schema, "fail_count") {
		t.Fatal("precondition failed: fail_count already present on old table")
	}

	if err := applyMigrations(ctx, pool, schema); err != nil {
		t.Fatalf("applyMigrations: %v", err)
	}

	// Postcondition: fail_count now exists.
	if !columnExists(t, schema, "fail_count") {
		t.Error("fail_count missing after migration of old table")
	}

	// fail_count default is 0 NOT NULL: an insert without it succeeds and
	// reads back 0 (confirms the ADD COLUMN default applied).
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.managed_partitions (table_name, partition_name, range_start, range_end)
		 VALUES ('t', 't.p', $1, $2)`, schema),
		time.Now().UTC(), time.Now().UTC().Add(time.Hour),
	); err != nil {
		t.Fatalf("insert after migration: %v", err)
	}
	var failCount int
	if err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT fail_count FROM %s.managed_partitions WHERE partition_name = 't.p'`, schema),
	).Scan(&failCount); err != nil {
		t.Fatalf("read fail_count: %v", err)
	}
	if failCount != 0 {
		t.Errorf("fail_count = %d after migration, want 0 (default)", failCount)
	}
}
