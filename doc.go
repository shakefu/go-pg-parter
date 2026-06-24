// Package parter is a PostgreSQL RANGE partition-maintenance library.
//
// It maintains time-ranged partitions on parent tables declaratively: a
// background goroutine reconciles a desired set of partitions each tick,
// creates pending partitions, and (optionally) detaches and drops partitions
// past their retention window. All coordination is lock-free across instances
// via FOR UPDATE SKIP LOCKED on a metadata table, so multiple replicas may run
// concurrently against one database safely.
//
// The single source of truth is the managed_partitions metadata table, which
// lives in the configured Config.Schema (default "parter"). Each managed
// partition is exactly one row in exactly one of four statuses: pending,
// created, detached, or failed.
//
// # Circuit breaker and operator reset
//
// A partition that fails to create for a deterministic reason — an invalid
// range, a missing or non-partitioned parent, a name conflict, an
// attach/boundary mismatch, or a non-empty DEFAULT partition already holding
// rows in the new partition's range (SQLSTATE 23514, check_violation) — has its
// fail_count advanced in a separate committed transaction. After five failed
// attempts the row is parked with status 'failed' and is never retried
// automatically.
//
// To recover a parked row, an operator must reset it manually. The reset
// clears both status and fail_count (clearing only one re-trips the breaker
// after a single attempt). The reset statement, rendered against the
// configured schema, is documented as [OperatorResetSQL]:
//
//	UPDATE <schema>.managed_partitions
//	  SET status = 'pending', fail_count = 0
//	  WHERE (table_name, partition_name) = (...)
//	    AND status = 'failed'
//
// For a row parked because the DEFAULT partition holds rows in the target
// range, the operator must relocate those rows out of the DEFAULT before
// resetting the parked row.
package parter

// OperatorResetSQL documents the manual reset an operator runs to un-park a
// row that the circuit breaker has set to 'failed'. It clears both status and
// fail_count; clearing only one re-trips the breaker after a single attempt.
// <schema> is the configured Config.Schema (default "parter") and the
// (table_name, partition_name) pair identifies the parked row. It is
// documentation only — the library never executes it.
//
// create.yass.yaml parter.CircuitBreaker ERROR.
const OperatorResetSQL = `UPDATE <schema>.managed_partitions SET status = 'pending', fail_count = 0 WHERE (table_name, partition_name) = (...) AND status = 'failed'`
