package parter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// boundaryLayout renders a timestamptz literal for embedding in a CREATE TABLE
// ... PARTITION OF ... FOR VALUES bound. The transaction first runs
// SET LOCAL TIME ZONE 'UTC' so the value is interpreted deterministically; the
// literal therefore carries an explicit +00:00 offset, e.g.
// "2026-06-01 00:00:00+00:00".
//
// CONTRACT §7 / create.yass.yaml parter.CreatePass SIDE-EFFECT.
const boundaryLayout = "2006-01-02 15:04:05-07:00"

// errNotAttached is the deterministic sentinel for a create whose CREATE TABLE
// IF NOT EXISTS appeared to succeed but the relation is NOT actually attached
// to the parent (the standalone previously-detached relation hazard). It is a
// plain error, NOT a *pgconn.PgError, so classifyCreateError treats it as
// deterministic.
//
// create.yass.yaml parter.CreatePass SIDE-EFFECT: verify attachment because
// CREATE TABLE IF NOT EXISTS succeeds even when a standalone relation of that
// name already exists.
var errNotAttached = errors.New("parter: partition not attached to parent after create (pg_inherits verify found no row)")

// errBoundaryMismatch is the deterministic sentinel for a create whose attached
// partition bound text does not match the canonical expected bound. Like
// errNotAttached it is a plain error so it classifies deterministic.
//
// create.yass.yaml parter.CreatePass SIDE-EFFECT: treat a boundary-mismatched
// verification result as a create FAILURE.
var errBoundaryMismatch = errors.New("parter: partition attached with mismatched boundaries")

// createPass drains one registered table: it repeatedly selects a single
// pending row with FOR UPDATE SKIP LOCKED, creates and verifies the partition,
// and commits the status flip atomically with the DDL. A poison row is attempted
// at most once per tick via an in-memory attempted set, so it neither livelocks
// the pass nor starves newer rows behind it in range_start order. Deterministic
// failures advance the circuit breaker in a SEPARATE committed transaction;
// transient failures stop the pass without advancing the breaker; context
// cancellation/deadline rolls back and exits leaving the row untouched.
//
// create.yass.yaml parter.CreatePass (INPUT/RETURN/SIDE-EFFECT/ERROR).
func createPass(ctx context.Context, cfg Config, t Table, st *core.State) error {
	schema := cfg.schema()
	counters := st.Counters.Table(t.Name)

	// attempted holds the partition_name values tried this tick. table_name is
	// constant for the whole pass, so the spec's
	// (table_name, partition_name) <> ALL(attempted) reduces to comparing
	// partition_name alone: AND partition_name <> ALL($2::text[]).
	attempted := make([]string, 0)

	selectSQL := fmt.Sprintf(
		"SELECT partition_name, range_start, range_end "+
			"FROM %s.managed_partitions "+
			"WHERE table_name=$1 AND status='pending' AND partition_name <> ALL($2::text[]) "+
			"ORDER BY range_start FOR UPDATE SKIP LOCKED LIMIT 1",
		schema)

	for {
		err := createOne(ctx, cfg, t, st, schema, counters, selectSQL, &attempted)
		switch {
		case err == nil:
			// One row was processed successfully; continue draining.
			continue
		case errors.Is(err, errDeterministicContinue):
			// A deterministic failure already recorded the error counter/span
			// and advanced the breaker; the row is in attempted, so continue.
			continue
		case errors.Is(err, errPassDone):
			// The SELECT found no actionable pending row: the pass is drained.
			return nil
		case errors.Is(err, errTransientStop):
			// A transient create failure already recorded the error counter and
			// span; stop the pass without advancing the breaker (next tick
			// retries).
			return nil
		case isCtxError(err):
			// Context cancelled / deadline exceeded: leave the row pending, do
			// not touch fail_count, do not increment create_errors_total.
			return err
		default:
			// Defensive: an unexpected error type stops the pass.
			return err
		}
	}
}

// Internal control-flow sentinels returned by createOne to drive createPass's
// loop. They never surface to callers of createPass.
var (
	errPassDone              = errors.New("parter: create pass done (no pending row)")
	errTransientStop         = errors.New("parter: transient create failure; stop pass")
	errDeterministicContinue = errors.New("parter: deterministic create failure; continue pass")
)

// createOne runs ONE create iteration in its own transaction: BEGIN, SELECT one
// pending row FOR UPDATE SKIP LOCKED, CREATE + verify, and on success COMMIT the
// status flip. It returns a control sentinel describing how createPass should
// proceed. On a deterministic failure it advances the breaker in a separate
// committed transaction AFTER this transaction has rolled back.
func createOne(
	ctx context.Context,
	cfg Config,
	t Table,
	st *core.State,
	schema string,
	counters *core.TableCounters,
	selectSQL string,
	attempted *[]string,
) error {
	tx, err := cfg.Pool.Begin(ctx)
	if err != nil {
		if isCtxError(err) {
			return err
		}
		// Could not even BEGIN (e.g. connection loss): transient, stop the pass.
		return errTransientStop
	}
	// Rollback is safe to call after Commit (no-op once closed).
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		partitionName string
		rangeStart    time.Time
		rangeEnd      time.Time
	)
	err = tx.QueryRow(ctx, selectSQL, t.Name, *attempted).
		Scan(&partitionName, &rangeStart, &rangeEnd)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No actionable pending row: COMMIT (releases any locks) and exit.
			_ = tx.Commit(ctx)
			return errPassDone
		}
		if isCtxError(err) {
			return err
		}
		// A failed SELECT before we have a row is not attributable to any
		// partition; treat as transient and stop the pass.
		return errTransientStop
	}

	// Mark attempted BEFORE acting: at most one attempt per tick.
	*attempted = append(*attempted, partitionName)

	createErr := attemptCreate(ctx, tx, t, schema, counters, partitionName, rangeStart, rangeEnd)
	if createErr == nil {
		if err := tx.Commit(ctx); err != nil {
			if isCtxError(err) {
				return err
			}
			// Commit failed: the create did not durably land. Record an error
			// and treat by classification (transient COMMIT failures are common
			// under contention; do not advance breaker).
			counters.CreateErrorsTotal.Add(1)
			st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
				Table: t.Name, PartitionName: partitionName,
				RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "error",
			})
			if classifyCreateError(err) {
				return errTransientStop
			}
			return advanceAndContinue(ctx, cfg, t, schema, partitionName)
		}
		counters.CreatedTotal.Add(1)
		st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
			Table: t.Name, PartitionName: partitionName,
			RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "created",
		})
		return nil // continue the loop
	}

	// Create or verify failed.
	if isCtxError(createErr) {
		// Roll back (deferred), leave pending, do NOT touch fail_count or the
		// create_errors_total counter.
		return createErr
	}

	// Roll back the create tx before recording / advancing the breaker.
	_ = tx.Rollback(ctx)

	counters.CreateErrorsTotal.Add(1)
	st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
		Table: t.Name, PartitionName: partitionName,
		RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "error",
	})

	if classifyCreateError(createErr) {
		// Transient: do NOT advance breaker; stop the pass (next tick retries).
		return errTransientStop
	}
	return advanceAndContinue(ctx, cfg, t, schema, partitionName)
}

// advanceAndContinue advances the circuit breaker for a deterministic failure
// and signals createPass to continue the loop. A context error while advancing
// surfaces as a cancellation (exit pass) rather than a breaker stop.
func advanceAndContinue(ctx context.Context, cfg Config, t Table, schema, partitionName string) error {
	if err := advanceBreaker(ctx, cfg, schema, t.Name, partitionName); err != nil {
		if isCtxError(err) {
			return err
		}
		// The breaker bump failed transiently (e.g. lock contention). The row
		// is already in attempted this tick; continue rather than spin.
		return errDeterministicContinue
	}
	return errDeterministicContinue
}

// attemptCreate runs, in the SAME transaction tx: SET LOCAL TIME ZONE 'UTC',
// the CREATE TABLE ... PARTITION OF ... FOR VALUES, the pg_inherits attachment
// verification, the canonical boundary comparison, and on success the
// status='created' UPDATE. It returns nil on success and a (possibly sentinel)
// error on any failure. It does NOT commit or roll back tx.
//
// create.yass.yaml parter.CreatePass SIDE-EFFECT.
func attemptCreate(
	ctx context.Context,
	tx pgx.Tx,
	t Table,
	schema string,
	counters *core.TableCounters,
	partitionName string,
	rangeStart, rangeEnd time.Time,
) error {
	// Scope the timezone so bound rendering and the canonical comparison below
	// are deterministic regardless of the session/server timezone.
	if _, err := tx.Exec(ctx, "SET LOCAL TIME ZONE 'UTC'"); err != nil {
		return err
	}

	fromLit := rangeStart.UTC().Format(boundaryLayout)
	toLit := rangeEnd.UTC().Format(boundaryLayout)
	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
		partitionName, t.Name, fromLit, toLit)
	if _, err := tx.Exec(ctx, createSQL); err != nil {
		return err
	}

	// VERIFY attachment + bounds. CREATE TABLE IF NOT EXISTS succeeds even when
	// a standalone same-named relation exists, so a successful CREATE is NOT
	// proof of attachment. The pg_inherits join is the PRIMARY guarantee.
	var actualBound string
	err := tx.QueryRow(ctx, `
		SELECT pg_get_expr(c.relpartbound, c.oid)
		FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid
		WHERE c.oid = $1::regclass AND i.inhparent = $2::regclass`,
		partitionName, t.Name).Scan(&actualBound)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No row from the pg_inherits join: NOT attached.
			return errNotAttached
		}
		return err
	}

	// Compute the canonical expected bound under the SAME UTC transaction so the
	// textual comparison is robust against rendering differences.
	var expectedBound string
	if err := tx.QueryRow(ctx,
		"SELECT format('FOR VALUES FROM (%L) TO (%L)', $1::timestamptz, $2::timestamptz)",
		rangeStart, rangeEnd).Scan(&expectedBound); err != nil {
		return err
	}
	if actualBound != expectedBound {
		return fmt.Errorf("%w: got %q, want %q", errBoundaryMismatch, actualBound, expectedBound)
	}

	// Success: flip status in the SAME transaction as the DDL (PostgreSQL DDL is
	// transactional), so the create and the metadata update commit atomically.
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"UPDATE %s.managed_partitions SET status='created', created_at=now() "+
			"WHERE (table_name, partition_name)=($1,$2)", schema),
		t.Name, partitionName); err != nil {
		return err
	}
	return nil
}

// advanceBreaker advances fail_count for a deterministically-poison row in a
// SEPARATE committed transaction, run AFTER the failed create transaction has
// rolled back — an in-transaction bump would be discarded with the rollback and
// the breaker could never trip. fail_count is a running total; the row parks as
// 'failed' once fail_count reaches 5, after which it is never selected (the
// SELECT filters status='pending') and so is never retried. The same code path
// is shared with EnsureReady, so a deterministic failure during startup parks a
// genuinely poison row durably.
//
// create.yass.yaml parter.CircuitBreaker SIDE-EFFECT (verbatim UPDATE).
func advanceBreaker(ctx context.Context, cfg Config, schema, tableName, partitionName string) error {
	tx, err := cfg.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"UPDATE %s.managed_partitions "+
			"SET fail_count = fail_count + 1, "+
			"status = CASE WHEN fail_count + 1 >= 5 THEN 'failed' ELSE status END "+
			"WHERE (table_name, partition_name) = ($1,$2) AND status = 'pending'",
		schema), tableName, partitionName); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// classifyCreateError reports whether err is TRANSIENT (true) — connection
// loss, lock_not_available (SQLSTATE 55P03), or serialization_failure (40001) —
// versus DETERMINISTIC (false). Deterministic failures (invalid range, missing
// or non-partitioned parent, name conflict, the attach/boundary sentinels, and
// check_violation 23514 from a non-empty DEFAULT) advance the circuit breaker;
// transient failures do not.
//
// create.yass.yaml parter.CreatePass ERROR / parter.CircuitBreaker RETURN.
func classifyCreateError(err error) (transient bool) {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "55P03", // lock_not_available
			"40001": // serialization_failure
			return true
		default:
			// Everything else with a SQLSTATE (23514 check_violation, 42P01
			// undefined_table, 42P17 invalid range, etc.) is deterministic.
			return false
		}
	}
	// Connection-loss errors carry no SQLSTATE. pgconn.SafeToRetry is true only
	// for errors raised before the query was sent (a genuinely transient, retry-
	// safe condition such as a broken connection on acquire).
	if pgconn.SafeToRetry(err) {
		return true
	}
	// Plain sentinels (errNotAttached, errBoundaryMismatch) and any other
	// non-PgError are deterministic.
	return false
}

// isCtxError reports whether err is (or wraps) a context cancellation or
// deadline-exceeded error. Such errors are NOT the row's fault: createPass rolls
// back, leaves the row pending, and does not touch fail_count or the error
// counter.
//
// create.yass.yaml parter.CreatePass ERROR (WHEN context is cancelled or the
// deadline is exceeded).
func isCtxError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
