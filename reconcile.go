package parter

import (
	"context"
	"fmt"
	"time"
)

// Period is one contiguous half-open partition range [Start, End) computed by
// desiredPeriods. All boundaries are in UTC.
//
// reconcile.yass.yaml parter.Reconcile RETURN.
type Period struct {
	Start, End time.Time
}

// truncatePeriod returns the UTC start of the period containing now for the
// given strategy. All boundary math is performed in UTC so every instance
// agrees on boundaries regardless of its local timezone.
//
// reconcile.yass.yaml parter.Reconcile RETURN: truncate now() to the start of
// the current month, ISO week, or day per Strategy, computing ALL boundaries in
// UTC; MUST-NOT compute any boundary in the local timezone.
func truncatePeriod(now time.Time, s RangeStrategy) time.Time {
	u := now.UTC()
	switch s {
	case Monthly:
		return time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC)
	case Weekly:
		// ISO week, Monday start: shift back to the preceding Monday.
		wd := (int(u.Weekday()) + 6) % 7
		dayStart := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
		return dayStart.AddDate(0, 0, -wd)
	default: // Daily
		return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
	}
}

// advancePeriod returns the boundary one strategy-unit after t.
//
// reconcile.yass.yaml parter.Reconcile RETURN: advance each subsequent period
// boundary one strategy-unit from the previous boundary.
func advancePeriod(t time.Time, s RangeStrategy) time.Time {
	switch s {
	case Monthly:
		return t.AddDate(0, 1, 0)
	case Weekly:
		return t.AddDate(0, 0, 7)
	default: // Daily
		return t.AddDate(0, 0, 1)
	}
}

// desiredPeriods returns the Lookahead+1 contiguous periods that must exist:
// the current period plus Lookahead periods into the future. period[i].Start =
// b_i and period[i].End = b_{i+1}, with b_0 = truncatePeriod(now, s) and
// b_{i+1} = advancePeriod(b_i, s). The set deliberately begins at the current
// period boundary and never at now()-retention.
//
// reconcile.yass.yaml parter.Reconcile RETURN: compute the desired set from the
// current period boundary through Lookahead periods ahead; treat Lookahead as
// an integer count of strategy-unit periods (Lookahead=3 Monthly = current
// month plus 3 ahead, 4 partitions total).
func desiredPeriods(now time.Time, s RangeStrategy, lookahead int) []Period {
	periods := make([]Period, 0, lookahead+1)
	b := truncatePeriod(now, s)
	for i := 0; i <= lookahead; i++ {
		next := advancePeriod(b, s)
		periods = append(periods, Period{Start: b, End: next})
		b = next
	}
	return periods
}

// reconcile computes the desired partition set for t and seeds a pending
// metadata row for any desired period that has no row yet. It reads the current
// time as time.Now().UTC(), derives the desired set via desiredPeriods, queries
// existing partition_name values for the table, and INSERTs the missing ones
// with ON CONFLICT (table_name, partition_name) DO NOTHING so concurrent passes
// from multiple instances are safe.
//
// The desired set starts at the CURRENT period boundary and never includes past
// periods, so reconcile never resurrects rows the retention pass has dropped.
// This is the single shared seeding code path used by both EnsureReady and the
// background maintenance loop.
//
// reconcile.yass.yaml parter.Reconcile (INPUT/RETURN/SIDE-EFFECT/INVARIANT).
func reconcile(ctx context.Context, cfg Config, t Table) error {
	now := time.Now().UTC()
	desired := desiredPeriods(now, t.Strategy, t.Lookahead)

	schema := cfg.schema()
	pSchema, _ := parentSchema(t.Name)

	// Existing partition names for this table.
	rows, err := cfg.Pool.Query(ctx, fmt.Sprintf(
		"SELECT partition_name FROM %s.managed_partitions WHERE table_name=$1", schema),
		t.Name)
	if err != nil {
		return fmt.Errorf("parter: reconcile: query existing partitions for %q: %w", t.Name, err)
	}
	existing := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("parter: reconcile: scan partition_name for %q: %w", t.Name, err)
		}
		existing[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("parter: reconcile: iterate existing partitions for %q: %w", t.Name, err)
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s.managed_partitions (table_name, partition_name, range_start, range_end, status) "+
			"VALUES ($1,$2,$3,$4,'pending') ON CONFLICT (table_name, partition_name) DO NOTHING",
		schema)

	for _, p := range desired {
		partitionName := pSchema + "." + t.Namer(p.Start)
		if existing[partitionName] {
			continue
		}
		if _, err := cfg.Pool.Exec(ctx, insertSQL, t.Name, partitionName, p.Start, p.End); err != nil {
			return fmt.Errorf("parter: reconcile: insert pending row %q for %q: %w", partitionName, t.Name, err)
		}
	}
	return nil
}
