//go:build integration

package parter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// retentionTestEnv bundles everything a retention integration test needs: a
// Config whose metadata schema is unique and created, a unique parent schema, a
// RANGE-partitioned parent table WITH a DEFAULT partition, the fully-qualified
// parent name, and a fresh core.State carrying the per-table counters retention
// increments.
type retentionTestEnv struct {
	cfg       Config
	parent    string // parent schema name
	tableName string // schema-qualified parent table
	rel       string // unqualified parent relation
	st        *core.State
}

// newRetentionEnv builds the metadata schema/table, a unique parent schema, the
// RANGE-partitioned parent table, AND a DEFAULT partition on the parent.
// retentionPass needs a real parent so DETACH/DROP have something to act on.
//
// The DEFAULT partition is deliberately present: every managed table is required
// to have a DEFAULT, and retention now uses a PLAIN (transactional)
// "ALTER TABLE ... DETACH PARTITION" which is compatible with a DEFAULT. Its
// presence is the regression proof for the resolved PG18 conflict — the
// concurrent detach variant would be rejected with SQLSTATE 55000 ("cannot
// detach partitions concurrently when a default partition exists"), but plain
// DETACH is not.
func newRetentionEnv(t *testing.T, pool *pgxpool.Pool) retentionTestEnv {
	t.Helper()
	metaSchema := uniqueSchema(t, pool)
	parentSchemaName := uniqueSchema(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := createSchema(ctx, pool, metaSchema); err != nil {
		t.Fatalf("createSchema(meta): %v", err)
	}
	if err := createTable(ctx, pool, metaSchema); err != nil {
		t.Fatalf("createTable(meta): %v", err)
	}
	if err := createSchema(ctx, pool, parentSchemaName); err != nil {
		t.Fatalf("createSchema(parent): %v", err)
	}

	rel := "events"
	tableName := parentSchemaName + "." + rel
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint, ts timestamptz NOT NULL) PARTITION BY RANGE (ts)`,
		tableName)); err != nil {
		t.Fatalf("create parent table: %v", err)
	}
	// Attach a DEFAULT partition. Plain DETACH is compatible with it; its
	// presence is the regression proof that the SQLSTATE 55000 concurrent-detach
	// conflict is resolved by using a plain transactional DETACH.
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s_default PARTITION OF %s DEFAULT`, tableName, tableName)); err != nil {
		t.Fatalf("create default partition: %v", err)
	}

	cfg := Config{Pool: pool, Schema: metaSchema}
	st := &core.State{
		Schema:   metaSchema,
		Tables:   []core.TableInfo{{Name: tableName, HasRetention: true}},
		Counters: core.NewCounters([]string{tableName}),
	}
	return retentionTestEnv{cfg: cfg, parent: parentSchemaName, tableName: tableName, rel: rel, st: st}
}

// monthlyTable returns a Table for the parent with a Monthly Namer and the given
// retention (in months).
func (e retentionTestEnv) monthlyTable(retention int) Table {
	return Table{
		Name:      e.tableName,
		Strategy:  Monthly,
		Lookahead: 1,
		Retention: retention,
		Namer: func(s time.Time) string {
			return fmt.Sprintf("%s_%s", e.rel, s.Format("2006_01"))
		},
	}
}

// partitionName returns the schema-qualified partition_name for a range start.
func (e retentionTestEnv) partitionName(tbl Table, start time.Time) string {
	return e.parent + "." + tbl.Namer(start)
}

// createAttachedPartition physically CREATEs a real partition attached to the
// parent for [start,end) and returns its schema-qualified name.
func (e retentionTestEnv) createAttachedPartition(t *testing.T, pool *pgxpool.Pool, tbl Table, start, end time.Time) string {
	t.Helper()
	pName := e.partitionName(tbl, start)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fromLit := start.UTC().Format(boundaryLayout)
	toLit := end.UTC().Format(boundaryLayout)
	ddl := fmt.Sprintf(
		`CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
		pName, e.tableName, fromLit, toLit)
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("create attached partition %q: %v", pName, err)
	}
	return pName
}

// createStandaloneTable creates a standalone (NOT attached) table named pName,
// used by Step2 drop tests.
func (e retentionTestEnv) createStandaloneTable(t *testing.T, pool *pgxpool.Pool, pName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint, ts timestamptz NOT NULL)`, pName)); err != nil {
		t.Fatalf("create standalone table %q: %v", pName, err)
	}
}

// seedRow inserts a managed_partitions row with the given status.
func (e retentionTestEnv) seedRow(t *testing.T, pool *pgxpool.Pool, pName string, start, end time.Time, status string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.managed_partitions
		   (table_name, partition_name, range_start, range_end, status)
		 VALUES ($1,$2,$3,$4,$5)`, e.cfg.schema()),
		e.tableName, pName, start, end, status); err != nil {
		t.Fatalf("seedRow(%s, %s): %v", pName, status, err)
	}
}

// rowStatus reads status for a partition row. exists is false when no row is
// present (e.g. after a drop deletes it).
func (e retentionTestEnv) rowStatus(t *testing.T, pool *pgxpool.Pool, pName string) (status string, exists bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT status FROM %s.managed_partitions
		   WHERE table_name=$1 AND partition_name=$2`, e.cfg.schema()),
		e.tableName, pName).Scan(&status)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", false
		}
		t.Fatalf("rowStatus(%s): %v", pName, err)
	}
	return status, true
}

// isAttached reports whether pName is physically attached to the parent.
func (e retentionTestEnv) isAttached(t *testing.T, pool *pgxpool.Pool, pName string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var attached bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pg_class c
			JOIN pg_inherits i ON i.inhrelid = c.oid
			WHERE c.oid = $1::regclass AND i.inhparent = $2::regclass)`,
		pName, e.tableName).Scan(&attached); err != nil {
		t.Fatalf("isAttached(%s): %v", pName, err)
	}
	return attached
}

// tableExists reports whether the relation pName exists at all (to_regclass).
func (e retentionTestEnv) tableExists(t *testing.T, pool *pgxpool.Pool, pName string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var oid *uint32
	if err := pool.QueryRow(ctx, `SELECT to_regclass($1)::oid`, pName).Scan(&oid); err != nil {
		t.Fatalf("tableExists(%s): %v", pName, err)
	}
	return oid != nil && *oid != 0
}

func (e retentionTestEnv) detachErrors() int64 {
	return e.st.Counters.Table(e.tableName).DetachErrorsTotal.Load()
}
func (e retentionTestEnv) dropErrors() int64 {
	return e.st.Counters.Table(e.tableName).DropErrorsTotal.Load()
}
func (e retentionTestEnv) dropped() int64 {
	return e.st.Counters.Table(e.tableName).DroppedTotal.Load()
}

// expiredMonthlyRange returns a [start,end) range whose end is comfortably older
// than now() minus the given Monthly retention, so it satisfies
// range_end < now() - interval '<retention> months'.
func expiredMonthlyRange(retention int) (start, end time.Time) {
	now := time.Now().UTC()
	// Pick a period that ended well before the retention cutoff: go back
	// retention+3 months for the end, with the start a month earlier.
	end = truncatePeriod(now, Monthly).AddDate(0, -(retention + 3), 0)
	start = end.AddDate(0, -1, 0)
	return start, end
}

// freshMonthlyRange returns a current-period range whose end is NOT older than
// the retention cutoff (so it must be left alone by Step1).
func freshMonthlyRange() (start, end time.Time) {
	now := time.Now().UTC()
	start = truncatePeriod(now, Monthly)
	end = advancePeriod(start, Monthly)
	return start, end
}

// retention.yass.yaml parter.RetentionPass RETURN (WHEN Retention == 0): a zero
// retention interval would make "range_end < now() - interval '0'" match all
// history and drop everything, so the pass is skipped ENTIRELY. An expired
// 'created' row present -> retentionPass with Retention=0 leaves the row
// untouched: nothing detached, nothing dropped, no counters move.
func TestIntegrationRetentionPassSkipsWhenZero(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(0) // Retention == 0

	start, end := expiredMonthlyRange(3)
	pName := e.createAttachedPartition(t, pool, tbl, start, end)
	e.seedRow(t, pool, pName, start, end, "created")

	detachBefore := e.detachErrors()
	droppedBefore := e.dropped()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := retentionPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("retentionPass: %v", err)
	}

	status, exists := e.rowStatus(t, pool, pName)
	if !exists {
		t.Fatalf("row was dropped, want untouched (Retention==0 skips the pass)")
	}
	if status != "created" {
		t.Errorf("status = %q, want \"created\" (Retention==0 must not touch the row)", status)
	}
	if !e.isAttached(t, pool, pName) {
		t.Errorf("partition was detached, want still attached (Retention==0 skips the pass)")
	}
	if got := e.dropped() - droppedBefore; got != 0 {
		t.Errorf("DroppedTotal delta = %d, want 0", got)
	}
	if got := e.detachErrors() - detachBefore; got != 0 {
		t.Errorf("DetachErrorsTotal delta = %d, want 0", got)
	}
}

// retention.yass.yaml parter.RetentionDetach RETURN/SIDE-EFFECT/INVARIANT (plain
// DETACH compatible with a DEFAULT): an attached partition whose range_end is
// older than now() - retention_interval and whose row is 'created' is selected
// and DETACHed off the parent with a PLAIN (transactional) DETACH PARTITION,
// then flipped to 'detached' in the SAME transaction (atomic). The parent has a
// DEFAULT partition the whole time, proving the plain DETACH succeeds where the
// concurrent detach variant would be rejected (SQLSTATE 55000). A NON-expired
// created row in the same table is left alone. DetachErrorsTotal stays 0.
func TestIntegrationRetentionDetachWithDefault(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool) // env has a DEFAULT partition on the parent
	tbl := e.monthlyTable(3)

	// Sanity: the parent really does have a DEFAULT partition (the regression
	// precondition that previously rejected a concurrent detach). A plain DETACH
	// must still succeed.
	if !e.hasDefaultPartition(t, pool) {
		t.Fatalf("precondition: parent must have a DEFAULT partition for the regression test")
	}

	// Expired created row: must be detached.
	expStart, expEnd := expiredMonthlyRange(3)
	expName := e.createAttachedPartition(t, pool, tbl, expStart, expEnd)
	e.seedRow(t, pool, expName, expStart, expEnd, "created")

	// Fresh created row: must be left alone.
	freshStart, freshEnd := freshMonthlyRange()
	freshName := e.createAttachedPartition(t, pool, tbl, freshStart, freshEnd)
	e.seedRow(t, pool, freshName, freshStart, freshEnd, "created")

	detachBefore := e.detachErrors()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := retentionDetach(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("retentionDetach: %v", err)
	}

	// Expired row -> detached, physically detached from parent (no pg_inherits
	// row to the parent).
	status, exists := e.rowStatus(t, pool, expName)
	if !exists {
		t.Fatalf("expired row missing after detach")
	}
	if status != "detached" {
		t.Errorf("expired status = %q, want \"detached\"", status)
	}
	if e.isAttached(t, pool, expName) {
		t.Errorf("expired partition %q still attached, want physically detached", expName)
	}
	if got := e.detachErrors() - detachBefore; got != 0 {
		t.Errorf("DetachErrorsTotal delta = %d, want 0 (plain DETACH works despite the DEFAULT)", got)
	}

	// Fresh row -> untouched (still created, still attached).
	fStatus, _ := e.rowStatus(t, pool, freshName)
	if fStatus != "created" {
		t.Errorf("fresh status = %q, want \"created\" (non-expired left alone)", fStatus)
	}
	if !e.isAttached(t, pool, freshName) {
		t.Errorf("fresh partition %q detached, want still attached", freshName)
	}
}

// retention.yass.yaml parter.RetentionDetach ERROR / INVARIANT (crash-safe by
// atomicity, no-livelock attempted set): a 'created' row whose partition_name is
// NOT an actually-attached partition (it was never created) forces a
// deterministic DETACH failure. retentionDetach: the transaction ROLLs back so
// the row stays 'created' (no partially-detached limbo), parter.detach_errors_total
// increments by exactly 1, and the pass RETURNS promptly (the attempted set means
// exactly one attempt — no livelock to the deadline).
func TestIntegrationRetentionDetachErrorLeavesCreated(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(3)

	start, end := expiredMonthlyRange(3)
	// A 'created' row whose partition was never created/attached. The DETACH of a
	// non-existent partition fails deterministically (relation does not exist).
	pName := e.partitionName(tbl, start)
	if e.tableExists(t, pool, pName) {
		t.Fatalf("precondition: partition %q must not exist", pName)
	}
	e.seedRow(t, pool, pName, start, end, "created")

	detachErrBefore := e.detachErrors()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The pass must RETURN (not livelock to the deadline): the row enters the
	// attempted set after its single failed attempt.
	if err := retentionDetach(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("retentionDetach returned error (should be a recorded detach failure): %v", err)
	}

	// Row stays 'created' (rolled back wholesale — no 'detached', no limbo).
	status, exists := e.rowStatus(t, pool, pName)
	if !exists {
		t.Fatalf("row missing, want kept 'created' after failed detach")
	}
	if status != "created" {
		t.Errorf("status = %q, want \"created\" (failed detach rolls back)", status)
	}
	// Exactly one error recorded (one attempt, attempted set prevents a livelock).
	if got := e.detachErrors() - detachErrBefore; got != 1 {
		t.Errorf("DetachErrorsTotal delta = %d, want exactly 1 (one attempt, no livelock)", got)
	}
}

// retention.yass.yaml parter.RetentionDrop RETURN/SIDE-EFFECT: a 'detached' row
// plus its standalone table -> retentionDrop DROPs the table, DELETEs the
// managed_partitions row, and increments parter.dropped_total by 1.
func TestIntegrationRetentionDrop(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(3)

	start, end := expiredMonthlyRange(3)
	pName := e.partitionName(tbl, start)
	// A standalone (already-detached) table awaiting DROP, with a 'detached' row.
	e.createStandaloneTable(t, pool, pName)
	e.seedRow(t, pool, pName, start, end, "detached")

	droppedBefore := e.dropped()
	dropErrBefore := e.dropErrors()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := retentionDrop(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("retentionDrop: %v", err)
	}

	if e.tableExists(t, pool, pName) {
		t.Errorf("table %q still exists after drop", pName)
	}
	if _, exists := e.rowStatus(t, pool, pName); exists {
		t.Errorf("managed_partitions row for %q still exists after drop", pName)
	}
	if got := e.dropped() - droppedBefore; got != 1 {
		t.Errorf("DroppedTotal delta = %d, want 1", got)
	}
	if got := e.dropErrors() - dropErrBefore; got != 0 {
		t.Errorf("DropErrorsTotal delta = %d, want 0", got)
	}
}

// retention.yass.yaml parter.RetentionDrop ERROR: when the DROP errors the
// transaction ROLLs back, parter.drop_errors_total increments, and the row stays
// 'detached' for retry next tick. We force a deterministic drop error by making
// another table DEPEND on the partition's table via a foreign key, so
// DROP TABLE (without CASCADE) fails with a dependency error.
func TestIntegrationRetentionDropErrorLeavesDetached(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(3)

	start, end := expiredMonthlyRange(3)
	pName := e.partitionName(tbl, start)
	e.createStandaloneTable(t, pool, pName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Give the standalone table a primary key, then create a dependent table with
	// a foreign key referencing it. DROP TABLE <pName> (no CASCADE) then fails
	// with a dependency error (2BP01 dependent_objects_still_exist) -> the drop
	// txn ROLLs back. This is a deterministic, repeatable drop failure.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD PRIMARY KEY (id)`, pName)); err != nil {
		t.Fatalf("add pk to standalone: %v", err)
	}
	depTable := e.parent + ".dep_blocker"
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint, ref bigint REFERENCES %s(id))`, depTable, pName)); err != nil {
		t.Fatalf("create dependent table: %v", err)
	}

	e.seedRow(t, pool, pName, start, end, "detached")

	droppedBefore := e.dropped()
	dropErrBefore := e.dropErrors()

	if err := retentionDrop(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("retentionDrop returned error (should be a recorded drop failure): %v", err)
	}

	if !e.tableExists(t, pool, pName) {
		t.Errorf("table %q was dropped, want preserved (drop should have failed)", pName)
	}
	status, exists := e.rowStatus(t, pool, pName)
	if !exists {
		t.Fatalf("row deleted, want kept after drop failure")
	}
	if status != "detached" {
		t.Errorf("status = %q, want \"detached\" (failed drop leaves the row)", status)
	}
	if got := e.dropErrors() - dropErrBefore; got != 1 {
		t.Errorf("DropErrorsTotal delta = %d, want 1", got)
	}
	if got := e.dropped() - droppedBefore; got != 0 {
		t.Errorf("DroppedTotal delta = %d, want 0", got)
	}
}

// retention.yass.yaml parter.RetentionPass SIDE-EFFECT (step order detach -> drop;
// a freshly-detached row reaches 'detached' via Step1's own in-transaction UPDATE
// then is dropped by Step2 in a separate transaction the same pass): an expired
// created partition -> ONE retentionPass detaches it (Step1) AND drops it (Step2)
// in the same pass: DroppedTotal=1, the row is gone, the table is gone. A
// pre-existing 'detached' row is also dropped, so both eligible rows are
// processed in the one pass.
func TestIntegrationRetentionPassDetachThenDrop(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(3)

	// Subject A: an expired attached 'created' row -> Step1 detaches, Step2 drops.
	aStart, aEnd := expiredMonthlyRange(3)
	aName := e.createAttachedPartition(t, pool, tbl, aStart, aEnd)
	e.seedRow(t, pool, aName, aStart, aEnd, "created")

	// Subject B: a pre-existing 'detached' standalone table (another month) ->
	// Step2 drops it in the same pass.
	bStart := aStart.AddDate(0, -2, 0)
	bEnd := bStart.AddDate(0, 1, 0)
	bName := e.partitionName(tbl, bStart)
	e.createStandaloneTable(t, pool, bName)
	e.seedRow(t, pool, bName, bStart, bEnd, "detached")

	droppedBefore := e.dropped()
	detachErrBefore := e.detachErrors()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := retentionPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("retentionPass: %v", err)
	}

	// Subject A: detached (Step1) then dropped (Step2) -> row and table gone.
	if _, exists := e.rowStatus(t, pool, aName); exists {
		t.Errorf("row for %q still exists, want detached then dropped in the same pass", aName)
	}
	if e.tableExists(t, pool, aName) {
		t.Errorf("table %q still exists, want dropped", aName)
	}
	// Subject B: dropped by Step2.
	if _, exists := e.rowStatus(t, pool, bName); exists {
		t.Errorf("row for %q still exists, want dropped", bName)
	}
	if e.tableExists(t, pool, bName) {
		t.Errorf("table %q still exists, want dropped", bName)
	}
	// Both eligible rows reached drop this pass.
	if got := e.dropped() - droppedBefore; got != 2 {
		t.Errorf("DroppedTotal delta = %d, want 2 (Step1's detached row + the pre-existing detached row)", got)
	}
	if got := e.detachErrors() - detachErrBefore; got != 0 {
		t.Errorf("DetachErrorsTotal delta = %d, want 0", got)
	}
}

// retention.yass.yaml parter.RetentionPass SIDE-EFFECT (drain AT MOST ONCE per
// tick via the attempted set; a persistently failing row neither livelocks nor
// starves the rest) / parter.RetentionDetach ERROR + parter.RetentionDrop ERROR:
// a persistently-failing detach row AND a persistently-failing drop row are each
// attempted at most once per pass. retentionPass returns promptly (does not loop
// to the deadline), recording exactly one detach error and one drop error.
func TestIntegrationRetentionPassNoLivelock(t *testing.T) {
	pool := testPool(t)
	e := newRetentionEnv(t, pool)
	tbl := e.monthlyTable(3)

	// Detach subject: a 'created' row whose partition was never created -> DETACH
	// fails deterministically every time.
	dStart, dEnd := expiredMonthlyRange(3)
	dName := e.partitionName(tbl, dStart)
	e.seedRow(t, pool, dName, dStart, dEnd, "created")

	// Drop subject: a 'detached' row whose table cannot be dropped (FK dependency).
	rStart := dStart.AddDate(0, -2, 0)
	rEnd := rStart.AddDate(0, 1, 0)
	rName := e.partitionName(tbl, rStart)
	e.createStandaloneTable(t, pool, rName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD PRIMARY KEY (id)`, rName)); err != nil {
		t.Fatalf("add pk: %v", err)
	}
	depTable := e.parent + ".dep_blocker"
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint, ref bigint REFERENCES %s(id))`, depTable, rName)); err != nil {
		t.Fatalf("create dependent table: %v", err)
	}
	e.seedRow(t, pool, rName, rStart, rEnd, "detached")

	detachErrBefore := e.detachErrors()
	dropErrBefore := e.dropErrors()
	droppedBefore := e.dropped()

	done := make(chan error, 1)
	go func() { done <- retentionPass(ctx, e.cfg, tbl, e.st) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("retentionPass returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("retentionPass did not return promptly: livelock on a persistently-failing row")
	}

	if got := e.detachErrors() - detachErrBefore; got != 1 {
		t.Errorf("DetachErrorsTotal delta = %d, want exactly 1 (one attempt, no livelock)", got)
	}
	if got := e.dropErrors() - dropErrBefore; got != 1 {
		t.Errorf("DropErrorsTotal delta = %d, want exactly 1 (one attempt, no livelock)", got)
	}
	if got := e.dropped() - droppedBefore; got != 0 {
		t.Errorf("DroppedTotal delta = %d, want 0 (both subjects fail)", got)
	}
	// The failing detach row stays 'created'; the failing drop row stays 'detached'.
	if status, _ := e.rowStatus(t, pool, dName); status != "created" {
		t.Errorf("detach subject status = %q, want \"created\"", status)
	}
	if status, _ := e.rowStatus(t, pool, rName); status != "detached" {
		t.Errorf("drop subject status = %q, want \"detached\"", status)
	}
}

// hasDefaultPartition reports whether the parent has a DEFAULT partition.
func (e retentionTestEnv) hasDefaultPartition(t *testing.T, pool *pgxpool.Pool) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_partitioned_table WHERE partrelid = $1::regclass AND partdefid <> 0)`,
		e.tableName).Scan(&exists); err != nil {
		t.Fatalf("hasDefaultPartition: %v", err)
	}
	return exists
}
