package parter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// retentionInterval converts an integer Retention count and a RangeStrategy into
// the calendar interval text used inline in retention SQL filters, e.g.
// "now() - interval '3 months'". The unit follows the strategy: Monthly ->
// "months", Weekly -> "weeks", Daily -> "days". For Retention=3 it returns
// "3 months", "3 weeks", or "3 days" respectively.
//
// The value is an integer count plus a fixed unit keyword, so it is injection-
// safe to inline directly into a SQL interval literal.
//
// retention.yass.yaml parter.RetentionPass RETURN (WHEN Retention > 0) /
// CONTRACT §8.
func retentionInterval(r int, s RangeStrategy) string {
	var unit string
	switch s {
	case Weekly:
		unit = "weeks"
	case Daily:
		unit = "days"
	default: // Monthly
		unit = "months"
	}
	return fmt.Sprintf("%d %s", r, unit)
}

// retentionPass runs the retention maintenance for one table. When Retention is
// zero the pass is skipped ENTIRELY: a zero retention interval would render the
// Step 1 filter as "range_end < now() - interval '0'", matching ALL historical
// partitions and dropping everything, so the guard returns nil before any work.
//
// Otherwise the two steps run in the mandatory in-tick order detach -> drop:
//  1. Step 1 (retentionDetach): detach newly-expired 'created' partitions with a
//     plain, transactional ALTER TABLE ... DETACH PARTITION, flipping the row to
//     'detached' in the SAME transaction (atomic). Drains fully.
//  2. Step 2 (retentionDrop): drop everything that has reached 'detached',
//     draining fully, in a separate transaction so the slower DROP never holds
//     the parent's lock. A row Step 1 just flipped to 'detached' is dropped here
//     in the SAME pass.
//
// Each step attempts each eligible row AT MOST ONCE per tick via an in-memory
// attempted set, so a persistently failing row neither livelocks the pass nor
// starves the rest and is retried on the next tick.
//
// retention.yass.yaml parter.RetentionPass / CONTRACT §8.
func retentionPass(ctx context.Context, cfg Config, t Table, st *core.State) error {
	if t.Retention == 0 {
		// Skip entirely: never drop, and a zero interval would match all history.
		return nil
	}

	// Step 1: detach newly-expired partitions.
	if err := retentionDetach(ctx, cfg, t, st); err != nil {
		return err
	}
	// Step 2: drop everything that reached detached.
	if err := retentionDrop(ctx, cfg, t, st); err != nil {
		return err
	}
	return nil
}

// retentionDetach is Step 1: it drains every 'created' row of the table whose
// range_end has aged past the retention window, detaching each partition off the
// parent. For each eligible row it, in ONE transaction:
//  1. BEGIN, SELECT one eligible row FOR UPDATE SKIP LOCKED LIMIT 1.
//  2. Run a PLAIN "ALTER TABLE <table_name> DETACH PARTITION <partition_name>"
//     (no concurrent variant): plain DETACH is catalog-only, takes a brief
//     ACCESS EXCLUSIVE on the parent, runs inside the transaction, and is
//     compatible with the parent's required DEFAULT partition (the concurrent
//     variant would be rejected with SQLSTATE 55000 while a DEFAULT exists).
//  3. UPDATE the row to 'detached' and COMMIT, so the detach and the status flip
//     are atomic.
//
// On any error the transaction ROLLs back wholesale (the row stays 'created' and
// the partition stays attached — no partially-detached limbo), increments
// parter.detach_errors_total, and the row is retried on a later tick.
//
// The pass maintains an in-memory attempted set keyed on partition_name
// (table_name is constant for the pass, so the spec's composite
// (table_name, partition_name) <> ALL(attempted) reduces to comparing
// partition_name alone), so a deterministically-failing row is attempted at most
// once per tick and the pass drains promptly rather than livelocking.
//
// retention.yass.yaml parter.RetentionDetach / CONTRACT §8.
func retentionDetach(ctx context.Context, cfg Config, t Table, st *core.State) error {
	schema := cfg.schema()
	counters := st.Counters.Table(t.Name)
	interval := retentionInterval(t.Retention, t.Strategy)

	// attempted holds the partition_name values tried this pass so a row whose
	// DETACH fails deterministically is attempted at most once per tick and
	// retried on the next tick rather than re-selected in a tight loop.
	attempted := make([]string, 0)

	// The interval is an integer + fixed unit keyword, injection-safe to inline.
	selectSQL := fmt.Sprintf(
		"SELECT partition_name, range_start, range_end "+
			"FROM %s.managed_partitions "+
			"WHERE table_name=$1 AND status='created' "+
			"AND range_end < now() - interval '%s' "+
			"AND partition_name <> ALL($2::text[]) "+
			"ORDER BY range_start FOR UPDATE SKIP LOCKED LIMIT 1",
		schema, interval)

	for {
		var (
			partitionName string
			rangeStart    time.Time
			rangeEnd      time.Time
		)

		tx, err := cfg.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, selectSQL, t.Name, attempted).Scan(&partitionName, &rangeStart, &rangeEnd)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// No eligible 'created' row: COMMIT and exit the pass.
				_ = tx.Commit(ctx)
				return nil
			}
			_ = tx.Rollback(ctx)
			return err
		}

		// Attempt this row at most once per pass.
		attempted = append(attempted, partitionName)

		detachErr := detachOne(ctx, tx, schema, t.Name, partitionName)
		if detachErr != nil {
			// ROLLBACK leaves status='created' and the partition still attached.
			_ = tx.Rollback(ctx)
			if isCtxError(detachErr) {
				return detachErr
			}
			counters.DetachErrorsTotal.Add(1)
			st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
				Table: t.Name, PartitionName: partitionName,
				RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "error",
			})
			continue
		}

		if err := tx.Commit(ctx); err != nil {
			if isCtxError(err) {
				return err
			}
			counters.DetachErrorsTotal.Add(1)
			st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
				Table: t.Name, PartitionName: partitionName,
				RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "error",
			})
			continue
		}

		// COMMIT succeeded: the detach and the status flip are atomic.
		st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
			Table: t.Name, PartitionName: partitionName,
			RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "detached",
		})
	}
}

// detachOne runs the plain DETACH PARTITION + metadata status flip inside tx. It
// does NOT commit or roll back tx. A non-nil return signals the caller to roll
// back and record a detach error. The DETACH is PLAIN (no concurrent variant) so
// it is transactional and compatible with the parent's required DEFAULT
// partition.
func detachOne(ctx context.Context, tx pgx.Tx, schema, tableName, partitionName string) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s DETACH PARTITION %s", tableName, partitionName)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"UPDATE %s.managed_partitions SET status='detached' "+
			"WHERE (table_name, partition_name)=($1,$2)", schema),
		tableName, partitionName); err != nil {
		return err
	}
	return nil
}

// retentionDrop is Step 2: it drains every 'detached' row of the table, dropping
// the standalone partition table and deleting its metadata row in ONE
// transaction (separate from the detach so the slower DROP never holds the
// parent's lock). For each row it:
//  1. BEGIN, SELECT one 'detached' row FOR UPDATE SKIP LOCKED LIMIT 1.
//  2. DROP TABLE IF EXISTS <partition_name>, DELETE the metadata row, COMMIT;
//     then increment parter.dropped_total.
//  3. On a drop error, ROLLBACK, increment parter.drop_errors_total, and leave
//     the row 'detached' to retry next tick.
//
// retention.yass.yaml parter.RetentionDrop / CONTRACT §8.
func retentionDrop(ctx context.Context, cfg Config, t Table, st *core.State) error {
	schema := cfg.schema()
	counters := st.Counters.Table(t.Name)

	// attempted holds the partition_name values tried this pass so a row whose
	// DROP fails deterministically (e.g. a dependent object) is attempted at most
	// once per tick and "retried on the next tick" rather than re-selected in a
	// tight loop. table_name is constant for the pass, so the spec's
	// (table_name, partition_name) <> ALL(attempted) reduces to comparing
	// partition_name alone: AND partition_name <> ALL($2::text[]).
	attempted := make([]string, 0)

	selectSQL := fmt.Sprintf(
		"SELECT partition_name, range_start, range_end "+
			"FROM %s.managed_partitions "+
			"WHERE table_name=$1 AND status='detached' "+
			"AND partition_name <> ALL($2::text[]) "+
			"ORDER BY range_start FOR UPDATE SKIP LOCKED LIMIT 1",
		schema)

	for {
		var (
			partitionName string
			rangeStart    time.Time
			rangeEnd      time.Time
		)

		tx, err := cfg.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, selectSQL, t.Name, attempted).Scan(&partitionName, &rangeStart, &rangeEnd)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				_ = tx.Commit(ctx)
				return nil
			}
			_ = tx.Rollback(ctx)
			return err
		}

		// Attempt this row at most once per pass.
		attempted = append(attempted, partitionName)

		dropErr := dropOne(ctx, tx, schema, t.Name, partitionName)
		if dropErr != nil {
			_ = tx.Rollback(ctx)
			if isCtxError(dropErr) {
				return dropErr
			}
			// Drop failed: leave the row 'detached' and retry next tick.
			counters.DropErrorsTotal.Add(1)
			st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
				Table: t.Name, PartitionName: partitionName,
				RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "error",
			})
			continue
		}

		if err := tx.Commit(ctx); err != nil {
			if isCtxError(err) {
				return err
			}
			counters.DropErrorsTotal.Add(1)
			st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
				Table: t.Name, PartitionName: partitionName,
				RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "error",
			})
			continue
		}

		counters.DroppedTotal.Add(1)
		st.Tracer().PartitionSpan(ctx, core.PartitionAttrs{
			Table: t.Name, PartitionName: partitionName,
			RangeStart: rangeStart, RangeEnd: rangeEnd, Outcome: "dropped",
		})
	}
}

// dropOne runs the DROP TABLE + metadata DELETE inside tx. It does NOT commit or
// roll back tx. A non-nil return signals the caller to roll back and record a
// drop error.
func dropOne(ctx context.Context, tx pgx.Tx, schema, tableName, partitionName string) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"DROP TABLE IF EXISTS %s", partitionName)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s.managed_partitions WHERE (table_name, partition_name)=($1,$2)", schema),
		tableName, partitionName); err != nil {
		return err
	}
	return nil
}
