package parter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// hasDefaultPartition reports whether the parent partitioned table tableName
// has a DEFAULT partition defined. It runs the catalog EXISTS query verbatim
// with $1 = tableName and scans the boolean result. It MUST NOT create the
// DEFAULT — its presence is the operator's responsibility.
//
// This is the shared DEFAULT-partition precondition check defined here per
// CONTRACT §10; Start (a later file) REUSES it.
//
// ensure-ready.yass.yaml parter.EnsureReady ERROR (verbatim catalog query).
func hasDefaultPartition(ctx context.Context, pool *pgxpool.Pool, tableName string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_partitioned_table WHERE partrelid = $1::regclass AND partdefid <> 0)",
		tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// EnsureReady is the synchronous startup hook that prepares the metadata schema,
// the managed_partitions table and its additive migrations, verifies the
// DEFAULT-partition precondition for every registered table, runs the
// declarative reconciliation, and executes a synchronous create pass. It is
// idempotent and safe to call repeatedly.
//
// It executes in EXACTLY this order (CONTRACT §10 / ensure-ready.yass.yaml
// RETURN):
//  1. ValidateConfig(cfg) — return the error if non-nil, BEFORE any DB work.
//  2. CREATE SCHEMA IF NOT EXISTS <schema>.
//  3. CREATE TABLE IF NOT EXISTS <schema>.managed_partitions (...).
//  4. apply idempotent additive column migrations.
//  5. for each table: verify a DEFAULT partition exists; error if absent
//     (does NOT create the DEFAULT).
//  6. for each table: reconcile (seed pending rows for desired periods).
//  7. for each table: createPass synchronously (creating overdue partitions).
//
// reconcile and createPass are the SAME shared code paths used by the
// background maintenance loop, so a deterministic create failure here advances
// the circuit breaker (fail_count) durably. createPass returns nil on
// transient/deterministic-handled outcomes and a non-nil error only on context
// cancellation/deadline or an unexpected condition.
//
// INVARIANT (ensure-ready.yass.yaml): on return EnsureReady guarantees only
// that all UNCONTESTED pending rows are drained — the create pass uses
// FOR UPDATE SKIP LOCKED, so a row currently locked by a concurrent instance's
// goroutine is intentionally skipped, not an error.
//
// ensure-ready.yass.yaml parter.EnsureReady (INPUT/RETURN/ERROR/SIDE-EFFECT/
// INVARIANT).
func EnsureReady(ctx context.Context, cfg Config) error {
	// 1. Validate the Config in memory FIRST, before any DB work.
	if err := ValidateConfig(cfg); err != nil {
		return err
	}

	schema := cfg.schema()

	// 2. Create the configured metadata schema.
	if err := createSchema(ctx, cfg.Pool, schema); err != nil {
		return fmt.Errorf("parter: EnsureReady: create schema %q: %w", schema, err)
	}

	// 3. Create the managed_partitions table.
	if err := createTable(ctx, cfg.Pool, schema); err != nil {
		return fmt.Errorf("parter: EnsureReady: create managed_partitions in %q: %w", schema, err)
	}

	// 4. Apply idempotent additive column migrations.
	if err := applyMigrations(ctx, cfg.Pool, schema); err != nil {
		return fmt.Errorf("parter: EnsureReady: apply migrations in %q: %w", schema, err)
	}

	// 5. Verify the DEFAULT-partition precondition for every registered table.
	// MUST NOT create the DEFAULT.
	for _, t := range cfg.Tables {
		exists, err := hasDefaultPartition(ctx, cfg.Pool, t.Name)
		if err != nil {
			return fmt.Errorf("parter: EnsureReady: check DEFAULT partition for %q: %w", t.Name, err)
		}
		if !exists {
			return fmt.Errorf("parter: EnsureReady: table %q has no DEFAULT partition", t.Name)
		}
	}

	// 6/7. Obtain the shared state (reuse a running State if Start already set
	// one; otherwise build a fresh State for counters + the no-op tracer).
	st := core.Global.Load()
	if st == nil {
		st = newState(cfg)
	}

	// For each table in order: reconcile (seed pending rows) then run the
	// synchronous create pass.
	for _, t := range cfg.Tables {
		if err := reconcile(ctx, cfg, t); err != nil {
			return fmt.Errorf("parter: EnsureReady: reconcile %q: %w", t.Name, err)
		}
		if err := createPass(ctx, cfg, t, st); err != nil {
			return fmt.Errorf("parter: EnsureReady: create pass %q: %w", t.Name, err)
		}
	}

	return nil
}
