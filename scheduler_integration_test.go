//go:build integration

package parter

import (
	"context"
	"testing"
	"time"
)

// schedulerCreateEnv reuses the retention env builder (unique metadata schema +
// managed_partitions table, unique parent schema, a RANGE-partitioned parent
// with a DEFAULT partition, and a core.State carrying the per-table counters).
// runMaintenanceTick exercises reconcile -> create -> retention against it.

// createdRowCount returns the number of 'created' managed_partitions rows for
// the env's table.
func (e retentionTestEnv) createdRowCount(t *testing.T, ctx context.Context) int64 {
	t.Helper()
	var n int64
	if err := e.cfg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM `+e.cfg.schema()+`.managed_partitions
		   WHERE table_name=$1 AND status='created'`,
		e.tableName).Scan(&n); err != nil {
		t.Fatalf("createdRowCount: %v", err)
	}
	return n
}

func (e retentionTestEnv) createdTotal() int64 {
	return e.st.Counters.Table(e.tableName).CreatedTotal.Load()
}

// scheduler.yass.yaml parter.MaintenanceLoop SIDE-EFFECT (reconcile pending rows
// then drain the create pass per table): reconcile seeds pending rows for the
// current+lookahead periods, then ONE runMaintenanceTick creates and physically
// attaches the partitions and flips the rows to 'created'. Asserts the rows are
// 'created', the partitions are physically attached (pg_inherits), and
// parter.created_total advanced by >= 1.
func TestIntegrationRunMaintenanceTickCreates(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(0) // create-only table (no retention)
	e.cfg.Tables = []Table{tbl}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Seed pending rows for the current + lookahead periods.
	if err := reconcile(ctx, e.cfg, tbl); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// The current period's partition_name (range_start = current month start).
	curStart, _ := freshMonthlyRange()
	curName := e.partitionName(tbl, curStart)

	createdBefore := e.createdTotal()

	runMaintenanceTick(ctx, e.cfg, 30*time.Second, e.st)

	// The current-period row is now 'created'.
	status, exists := e.rowStatus(t, pool, curName)
	if !exists {
		t.Fatalf("current-period row %q missing after tick", curName)
	}
	if status != "created" {
		t.Errorf("status = %q, want \"created\" after the create pass", status)
	}
	// The partition is physically attached to the parent.
	if !e.isAttached(t, pool, curName) {
		t.Errorf("partition %q not physically attached after the tick", curName)
	}
	// At least one create was counted.
	if got := e.createdTotal() - createdBefore; got < 1 {
		t.Errorf("CreatedTotal delta = %d, want >= 1", got)
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop SIDE-EFFECT (WHEN Retention>0: run
// the retention pass after the create pass): a 'created' row whose range_end is
// older than the retention window, with a real attached partition, is detached
// then dropped within the SAME tick. Asserts the row is gone (drop deletes it)
// and parter.dropped_total advanced by >= 1.
func TestIntegrationRunMaintenanceTickRetention(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(3) // Retention>0
	e.cfg.Tables = []Table{tbl}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// An expired, physically-attached 'created' partition.
	expStart, expEnd := expiredMonthlyRange(3)
	expName := e.createAttachedPartition(t, pool, tbl, expStart, expEnd)
	e.seedRow(t, pool, expName, expStart, expEnd, "created")

	droppedBefore := e.dropped()

	runMaintenanceTick(ctx, e.cfg, 30*time.Second, e.st)

	// Detach (Step1) then drop (Step2) ran in-tick: row gone, table gone.
	if _, exists := e.rowStatus(t, pool, expName); exists {
		t.Errorf("row for %q still exists, want detached then dropped within the tick", expName)
	}
	if e.tableExists(t, pool, expName) {
		t.Errorf("table %q still exists, want dropped within the tick", expName)
	}
	if got := e.dropped() - droppedBefore; got < 1 {
		t.Errorf("DroppedTotal delta = %d, want >= 1", got)
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop SIDE-EFFECT (run reconcile, then
// create, then retention in order, per table): with BOTH a pending current-period
// row (seeded by reconcile) AND an expired 'created' row for the same retention
// table, ONE tick creates the new partition AND drops the expired one — proving
// reconcile -> create -> retention all ran in order in the single tick.
func TestIntegrationRunMaintenanceTickOrdering(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(3) // Retention>0
	e.cfg.Tables = []Table{tbl}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Expired created partition that retention must drop.
	expStart, expEnd := expiredMonthlyRange(3)
	expName := e.createAttachedPartition(t, pool, tbl, expStart, expEnd)
	e.seedRow(t, pool, expName, expStart, expEnd, "created")

	// reconcile seeds pending rows for the current + lookahead periods, which the
	// create pass then materializes.
	if err := reconcile(ctx, e.cfg, tbl); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	curStart, _ := freshMonthlyRange()
	curName := e.partitionName(tbl, curStart)

	createdBefore := e.createdTotal()
	droppedBefore := e.dropped()

	runMaintenanceTick(ctx, e.cfg, 30*time.Second, e.st)

	// Create pass materialized the current-period partition.
	status, exists := e.rowStatus(t, pool, curName)
	if !exists || status != "created" {
		t.Errorf("current-period row %q status=%q exists=%v, want created", curName, status, exists)
	}
	if !e.isAttached(t, pool, curName) {
		t.Errorf("current-period partition %q not attached after the tick", curName)
	}
	// Retention pass dropped the expired partition in the same tick.
	if _, exists := e.rowStatus(t, pool, expName); exists {
		t.Errorf("expired row %q still exists, want dropped in the same tick", expName)
	}
	if e.tableExists(t, pool, expName) {
		t.Errorf("expired table %q still exists, want dropped in the same tick", expName)
	}
	if got := e.createdTotal() - createdBefore; got < 1 {
		t.Errorf("CreatedTotal delta = %d, want >= 1", got)
	}
	if got := e.dropped() - droppedBefore; got < 1 {
		t.Errorf("DroppedTotal delta = %d, want >= 1", got)
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop ERROR (WHEN the context expires:
// cancel and roll back the in-progress transaction and proceed): calling
// runMaintenanceTick with an already-cancelled ctx short-circuits — it returns
// promptly, creates nothing, and does not panic. Pending rows stay pending; the
// CreatedTotal counter does not move.
func TestIntegrationRunMaintenanceTickCancelledCtx(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(0)
	e.cfg.Tables = []Table{tbl}

	// Seed pending rows that WOULD be created if the tick ran.
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer seedCancel()
	if err := reconcile(seedCtx, e.cfg, tbl); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	createdBefore := e.createdTotal()
	createdRowsBefore := e.createdRowCount(t, seedCtx)

	// Already-cancelled context: the derived deadline context is already done,
	// so the create pass short-circuits without creating anything.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		runMaintenanceTick(ctx, e.cfg, 30*time.Second, e.st)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("runMaintenanceTick did not return promptly with a cancelled ctx")
	}

	// Nothing was created.
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer checkCancel()
	if got := e.createdRowCount(t, checkCtx) - createdRowsBefore; got != 0 {
		t.Errorf("created-row delta = %d, want 0 (cancelled ctx must create nothing)", got)
	}
	if got := e.createdTotal() - createdBefore; got != 0 {
		t.Errorf("CreatedTotal delta = %d, want 0 (cancelled ctx must create nothing)", got)
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop RETURN (sleep until next_tick, or
// return when ctx is Done): MaintenanceLoop launched in a goroutine returns
// within a few seconds once ctx is cancelled — it honors cancellation without
// hanging or busy-looping. The first scheduled tick can be up to ~60s out, so
// this verifies clean shutdown only (the per-tick body is covered by the
// runMaintenanceTick tests above).
func TestIntegrationMaintenanceLoopCleanShutdown(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(3)
	e.cfg.Tables = []Table{tbl}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		MaintenanceLoop(ctx, e.cfg)
		close(done)
	}()

	// Let the loop reach its initial sleep, then cancel.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("MaintenanceLoop did not return within 5s of ctx cancel")
	}
}
