//go:build integration

package parter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// createTestEnv bundles everything an integration createPass test needs: a
// Config whose metadata schema is unique and created, a unique parent schema,
// the fully-qualified parent table name, and a fresh core.State carrying the
// per-table counters createPass increments.
type createTestEnv struct {
	cfg       Config
	parent    string // parent schema name
	tableName string // schema-qualified parent table
	rel       string // unqualified parent relation
	st        *core.State
}

// newCreateEnv builds the metadata schema/table and a unique parent schema. It
// does NOT create the parent table; helpers do so per scenario.
func newCreateEnv(t *testing.T, pool *pgxpool.Pool) createTestEnv {
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
	// The parent schema must exist for parent/partition relations to live in it.
	if err := createSchema(ctx, pool, parentSchemaName); err != nil {
		t.Fatalf("createSchema(parent): %v", err)
	}

	rel := "events"
	tableName := parentSchemaName + "." + rel
	cfg := Config{Pool: pool, Schema: metaSchema}
	st := &core.State{
		Schema:   metaSchema,
		Tables:   []core.TableInfo{{Name: tableName}},
		Counters: core.NewCounters([]string{tableName}),
	}
	return createTestEnv{cfg: cfg, parent: parentSchemaName, tableName: tableName, rel: rel, st: st}
}

// createParentTable creates the RANGE-partitioned parent table.
func (e createTestEnv) createParentTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ddl := fmt.Sprintf(
		`CREATE TABLE %s (id bigint, ts timestamptz NOT NULL) PARTITION BY RANGE (ts)`,
		e.tableName)
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("create parent table: %v", err)
	}
}

// createDefaultPartition attaches a DEFAULT partition to the parent.
func (e createTestEnv) createDefaultPartition(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ddl := fmt.Sprintf(
		`CREATE TABLE %s_default PARTITION OF %s DEFAULT`, e.tableName, e.tableName)
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("create default partition: %v", err)
	}
}

// monthlyTable returns a Table for the parent with a unique Monthly Namer.
func (e createTestEnv) monthlyTable() Table {
	return Table{
		Name:      e.tableName,
		Strategy:  Monthly,
		Lookahead: 1,
		Namer: func(s time.Time) string {
			return fmt.Sprintf("%s_%s", e.rel, s.Format("2006_01"))
		},
	}
}

// seedPending inserts a single pending metadata row for [start,end) and returns
// the schema-qualified partition_name.
func (e createTestEnv) seedPending(t *testing.T, pool *pgxpool.Pool, tbl Table, start, end time.Time) string {
	t.Helper()
	pName := e.parent + "." + tbl.Namer(start)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.managed_partitions (table_name, partition_name, range_start, range_end, status)
		 VALUES ($1,$2,$3,$4,'pending')`, e.cfg.schema()),
		e.tableName, pName, start, end); err != nil {
		t.Fatalf("seedPending: %v", err)
	}
	return pName
}

// rowState reads status, fail_count and created_at for a partition row.
func (e createTestEnv) rowState(t *testing.T, pool *pgxpool.Pool, pName string) (status string, failCount int, createdAt *time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT status, fail_count, created_at FROM %s.managed_partitions
		   WHERE table_name=$1 AND partition_name=$2`, e.cfg.schema()),
		e.tableName, pName).Scan(&status, &failCount, &createdAt)
	if err != nil {
		t.Fatalf("rowState: %v", err)
	}
	return status, failCount, createdAt
}

// isAttached reports whether pName is physically attached to the parent as a
// partition (via pg_inherits), the PRIMARY attachment guarantee.
func (e createTestEnv) isAttached(t *testing.T, pool *pgxpool.Pool, pName string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var attached bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pg_class c
			JOIN pg_inherits i ON i.inhrelid = c.oid
			WHERE c.oid = $1::regclass AND i.inhparent = $2::regclass)`,
		pName, e.tableName).Scan(&attached)
	if err != nil {
		t.Fatalf("isAttached: %v", err)
	}
	return attached
}

func (e createTestEnv) createdTotal() int64 {
	return e.st.Counters.Table(e.tableName).CreatedTotal.Load()
}
func (e createTestEnv) createErrorsTotal() int64 {
	return e.st.Counters.Table(e.tableName).CreateErrorsTotal.Load()
}

// create.yass.yaml parter.CreatePass SIDE-EFFECT (WHEN create+verify succeed):
// a pending row whose period has begun -> createPass physically creates and
// ATTACHES the partition (verified via pg_inherits), flips the row to 'created'
// with created_at set, and increments parter.created_total by 1.
func TestIntegrationCreatePassSuccess(t *testing.T) {
	pool := testPool(t)
	e := newCreateEnv(t, pool)
	e.createParentTable(t, pool)
	tbl := e.monthlyTable()

	now := time.Now().UTC()
	start := truncatePeriod(now, Monthly)
	end := advancePeriod(start, Monthly)
	pName := e.seedPending(t, pool, tbl, start, end)

	before := e.createdTotal()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("createPass: %v", err)
	}

	if !e.isAttached(t, pool, pName) {
		t.Errorf("partition %q is not attached to parent after createPass", pName)
	}
	status, failCount, createdAt := e.rowState(t, pool, pName)
	if status != "created" {
		t.Errorf("status = %q, want \"created\"", status)
	}
	if createdAt == nil {
		t.Errorf("created_at is NULL, want set")
	}
	if failCount != 0 {
		t.Errorf("fail_count = %d, want 0 on success", failCount)
	}
	if got := e.createdTotal() - before; got != 1 {
		t.Errorf("CreatedTotal delta = %d, want 1", got)
	}
}

// create.yass.yaml parter.CreatePass SIDE-EFFECT (attach-verify hazard): a
// STANDALONE relation already exists with the partition_name (not attached).
// CREATE TABLE IF NOT EXISTS no-ops, so success is NOT proof of attachment; the
// pg_inherits verify detects not-attached -> deterministic FAILURE: the row
// stays pending, fail_count advances to 1, parter.create_errors_total +1,
// parter.created_total unchanged.
func TestIntegrationCreatePassAttachVerifyHazard(t *testing.T) {
	pool := testPool(t)
	e := newCreateEnv(t, pool)
	e.createParentTable(t, pool)
	tbl := e.monthlyTable()

	now := time.Now().UTC()
	start := truncatePeriod(now, Monthly)
	end := advancePeriod(start, Monthly)
	pName := e.seedPending(t, pool, tbl, start, end)

	// Pre-create a STANDALONE relation with the partition_name (NOT attached).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint, ts timestamptz NOT NULL)`, pName)); err != nil {
		t.Fatalf("create standalone relation: %v", err)
	}

	createdBefore := e.createdTotal()
	errsBefore := e.createErrorsTotal()
	if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("createPass returned error (should be a recorded create failure, not a returned error): %v", err)
	}

	if e.isAttached(t, pool, pName) {
		t.Errorf("standalone relation %q must NOT be attached", pName)
	}
	status, failCount, _ := e.rowState(t, pool, pName)
	if status != "pending" {
		t.Errorf("status = %q, want \"pending\" (attach-verify failure leaves row pending)", status)
	}
	if failCount != 1 {
		t.Errorf("fail_count = %d, want 1 (deterministic attach failure advances breaker)", failCount)
	}
	if got := e.createErrorsTotal() - errsBefore; got != 1 {
		t.Errorf("CreateErrorsTotal delta = %d, want 1", got)
	}
	if got := e.createdTotal() - createdBefore; got != 0 {
		t.Errorf("CreatedTotal delta = %d, want 0", got)
	}
}

// create.yass.yaml parter.CircuitBreaker SIDE-EFFECT / RETURN: a
// deterministically failing row (parent table dropped) parks after 5 attempts.
// Running createPass 5 times advances fail_count to 5 and flips status to
// 'failed'; a 6th run does NOT retry it (still 'failed', fail_count stays 5).
// The breaker bump SURVIVES even though the create txn rolled back, proving the
// separate committed txn.
func TestIntegrationCreatePassPoisonParksAfter5(t *testing.T) {
	pool := testPool(t)
	e := newCreateEnv(t, pool)
	// Deliberately do NOT create the parent table: CREATE ... PARTITION OF a
	// nonexistent parent is a deterministic failure.
	tbl := e.monthlyTable()

	now := time.Now().UTC()
	start := truncatePeriod(now, Monthly)
	end := advancePeriod(start, Monthly)
	pName := e.seedPending(t, pool, tbl, start, end)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 1; i <= 5; i++ {
		if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
			t.Fatalf("createPass run %d: unexpected returned error: %v", i, err)
		}
		status, failCount, _ := e.rowState(t, pool, pName)
		if i < 5 {
			if status != "pending" {
				t.Errorf("after run %d: status = %q, want \"pending\"", i, status)
			}
			if failCount != i {
				t.Errorf("after run %d: fail_count = %d, want %d", i, failCount, i)
			}
		} else {
			if status != "failed" {
				t.Errorf("after run 5: status = %q, want \"failed\"", status)
			}
			if failCount != 5 {
				t.Errorf("after run 5: fail_count = %d, want 5", failCount)
			}
		}
	}

	// 6th run: the row is 'failed' (not 'pending') so it is NOT selected and
	// not retried.
	if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("createPass run 6: unexpected returned error: %v", err)
	}
	status, failCount, _ := e.rowState(t, pool, pName)
	if status != "failed" {
		t.Errorf("after run 6: status = %q, want \"failed\" (no auto-retry)", status)
	}
	if failCount != 5 {
		t.Errorf("after run 6: fail_count = %d, want 5 (parked, not retried)", failCount)
	}
}

// create.yass.yaml parter.CircuitBreaker ERROR (23514): a parent with a DEFAULT
// partition holding a row whose ts falls in a target partition's range. The
// CREATE ... PARTITION OF FOR VALUES raises 23514 (check_violation) because the
// DEFAULT holds conflicting rows -> deterministic -> fail_count advances; after
// 5 the row parks as 'failed'. Recovery: relocate the rows out of DEFAULT, then
// reset.
func TestIntegrationCreatePassDefaultConflict23514(t *testing.T) {
	pool := testPool(t)
	e := newCreateEnv(t, pool)
	e.createParentTable(t, pool)
	e.createDefaultPartition(t, pool)
	tbl := e.monthlyTable()

	now := time.Now().UTC()
	start := truncatePeriod(now, Monthly)
	end := advancePeriod(start, Monthly)
	pName := e.seedPending(t, pool, tbl, start, end)

	// Insert a row into the DEFAULT whose ts falls in [start,end), so creating
	// the partition for that range would orphan it -> PG raises 23514.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conflictTS := start.Add(12 * time.Hour)
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, ts) VALUES (1, $1)`, e.tableName), conflictTS); err != nil {
		t.Fatalf("insert conflicting row into default: %v", err)
	}

	// First attempt -> 23514 -> deterministic -> fail_count=1, still pending.
	errsBefore := e.createErrorsTotal()
	if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("createPass: unexpected returned error: %v", err)
	}
	status, failCount, _ := e.rowState(t, pool, pName)
	if status != "pending" {
		t.Errorf("after 1 attempt: status = %q, want \"pending\"", status)
	}
	if failCount != 1 {
		t.Errorf("after 1 attempt: fail_count = %d, want 1 (23514 is deterministic)", failCount)
	}
	if got := e.createErrorsTotal() - errsBefore; got != 1 {
		t.Errorf("CreateErrorsTotal delta = %d, want 1", got)
	}

	// Run to 5 total -> parks.
	for i := 2; i <= 5; i++ {
		if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
			t.Fatalf("createPass run %d: %v", i, err)
		}
	}
	status, failCount, _ = e.rowState(t, pool, pName)
	if status != "failed" || failCount != 5 {
		t.Errorf("after 5 attempts: status=%q fail_count=%d, want failed/5", status, failCount)
	}
}

// create.yass.yaml parter.CreatePass INPUT (attempted set / no starvation): two
// pending rows where the EARLIER (smaller range_start) is poison and the later
// is good. ONE createPass attempts the poison once (fail_count=1, still
// pending) AND creates the good row in the SAME tick. Proves a poison row
// neither livelocks nor starves newer rows behind it in range_start order.
func TestIntegrationCreatePassAttemptedNoStarvation(t *testing.T) {
	pool := testPool(t)
	e := newCreateEnv(t, pool)
	e.createParentTable(t, pool)
	tbl := e.monthlyTable()

	now := time.Now().UTC()
	goodStart := truncatePeriod(now, Monthly)
	goodEnd := advancePeriod(goodStart, Monthly)
	// The poison row is the EARLIER period (previous month) so it sorts first
	// by range_start. It is poison because we pre-create a standalone relation
	// with its name (attach-verify failure), which is deterministic but leaves
	// the row pending after one attempt.
	poisonStart := goodStart.AddDate(0, -1, 0)
	poisonEnd := goodStart

	goodName := e.seedPending(t, pool, tbl, goodStart, goodEnd)
	poisonName := e.seedPending(t, pool, tbl, poisonStart, poisonEnd)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint, ts timestamptz NOT NULL)`, poisonName)); err != nil {
		t.Fatalf("create standalone poison relation: %v", err)
	}

	createdBefore := e.createdTotal()
	if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("createPass: %v", err)
	}

	// Poison row: attempted exactly once, still pending, fail_count=1.
	pStatus, pFail, _ := e.rowState(t, pool, poisonName)
	if pStatus != "pending" {
		t.Errorf("poison status = %q, want \"pending\"", pStatus)
	}
	if pFail != 1 {
		t.Errorf("poison fail_count = %d, want 1 (attempted exactly once this tick)", pFail)
	}
	// Good row: created in the same tick despite the poison sorting first.
	gStatus, _, gCreatedAt := e.rowState(t, pool, goodName)
	if gStatus != "created" {
		t.Errorf("good status = %q, want \"created\" (not starved by poison)", gStatus)
	}
	if gCreatedAt == nil {
		t.Errorf("good created_at is NULL, want set")
	}
	if !e.isAttached(t, pool, goodName) {
		t.Errorf("good partition %q not attached", goodName)
	}
	if got := e.createdTotal() - createdBefore; got != 1 {
		t.Errorf("CreatedTotal delta = %d, want 1 (only the good row)", got)
	}
}

// create.yass.yaml parter.CreatePass ERROR (ctx cancel): an already-cancelled
// context -> createPass returns the ctx error, the row stays pending,
// fail_count is unchanged, parter.create_errors_total is unchanged (it is not
// the row's fault).
func TestIntegrationCreatePassCtxCancelled(t *testing.T) {
	pool := testPool(t)
	e := newCreateEnv(t, pool)
	e.createParentTable(t, pool)
	tbl := e.monthlyTable()

	now := time.Now().UTC()
	start := truncatePeriod(now, Monthly)
	end := advancePeriod(start, Monthly)
	pName := e.seedPending(t, pool, tbl, start, end)

	errsBefore := e.createErrorsTotal()
	createdBefore := e.createdTotal()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := createPass(ctx, e.cfg, tbl, e.st)
	if err == nil {
		t.Fatalf("createPass with cancelled ctx returned nil, want a ctx error")
	}
	if !isCtxError(err) {
		t.Errorf("createPass returned %v, want a context error", err)
	}

	status, failCount, _ := e.rowState(t, pool, pName)
	if status != "pending" {
		t.Errorf("status = %q, want \"pending\" after ctx cancel", status)
	}
	if failCount != 0 {
		t.Errorf("fail_count = %d, want 0 (ctx cancel must not touch fail_count)", failCount)
	}
	if got := e.createErrorsTotal() - errsBefore; got != 0 {
		t.Errorf("CreateErrorsTotal delta = %d, want 0 (ctx cancel is not a create error)", got)
	}
	if got := e.createdTotal() - createdBefore; got != 0 {
		t.Errorf("CreatedTotal delta = %d, want 0", got)
	}
}

// create.yass.yaml parter.CircuitBreaker ERROR (operator reset): a 'failed' row
// is un-parked by the documented reset SQL (status='pending', fail_count=0).
// After fixing the underlying cause (creating the parent), createPass succeeds.
// Asserts fail_count MUST be cleared too: a reset that clears only status
// re-trips the breaker after one attempt.
func TestIntegrationCreatePassOperatorReset(t *testing.T) {
	pool := testPool(t)
	e := newCreateEnv(t, pool)
	// No parent yet -> deterministic failures park the row.
	tbl := e.monthlyTable()

	now := time.Now().UTC()
	start := truncatePeriod(now, Monthly)
	end := advancePeriod(start, Monthly)
	pName := e.seedPending(t, pool, tbl, start, end)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
			t.Fatalf("createPass run %d: %v", i, err)
		}
	}
	if status, fc, _ := e.rowState(t, pool, pName); status != "failed" || fc != 5 {
		t.Fatalf("precondition: row not parked (status=%q fail_count=%d)", status, fc)
	}

	// Fix the underlying cause.
	e.createParentTable(t, pool)

	// A reset that clears ONLY status (leaving fail_count=5) re-trips after one
	// attempt: the create succeeds but if we had not cleared fail_count, a
	// subsequent deterministic failure would immediately re-park. Demonstrate
	// the required behavior by clearing BOTH (the documented reset).
	resetSQL := fmt.Sprintf(
		`UPDATE %s.managed_partitions SET status='pending', fail_count=0
		   WHERE (table_name, partition_name)=($1,$2) AND status='failed'`, e.cfg.schema())
	if _, err := pool.Exec(ctx, resetSQL, e.tableName, pName); err != nil {
		t.Fatalf("operator reset: %v", err)
	}

	if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("createPass after reset: %v", err)
	}
	status, failCount, createdAt := e.rowState(t, pool, pName)
	if status != "created" {
		t.Errorf("after reset+fix: status = %q, want \"created\"", status)
	}
	if createdAt == nil {
		t.Errorf("after reset+fix: created_at is NULL, want set")
	}
	if failCount != 0 {
		t.Errorf("after reset+fix: fail_count = %d, want 0", failCount)
	}
	if !e.isAttached(t, pool, pName) {
		t.Errorf("after reset+fix: partition %q not attached", pName)
	}
}

// create.yass.yaml parter.CircuitBreaker ERROR: a reset that clears ONLY status
// (NOT fail_count) re-trips the breaker after a SINGLE failed attempt, proving
// fail_count must be cleared too. We park a row, reset status only (leaving
// fail_count=5), and run ONE createPass against the still-broken cause: the row
// immediately re-parks (fail_count 6, status 'failed') because 5+1 >= 5.
func TestIntegrationCreatePassResetStatusOnlyRetrips(t *testing.T) {
	pool := testPool(t)
	e := newCreateEnv(t, pool)
	// No parent -> stays poison.
	tbl := e.monthlyTable()

	now := time.Now().UTC()
	start := truncatePeriod(now, Monthly)
	end := advancePeriod(start, Monthly)
	pName := e.seedPending(t, pool, tbl, start, end)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
			t.Fatalf("createPass run %d: %v", i, err)
		}
	}

	// Reset ONLY status, leaving fail_count=5 (the WRONG reset).
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s.managed_partitions SET status='pending'
		   WHERE (table_name, partition_name)=($1,$2) AND status='failed'`, e.cfg.schema()),
		e.tableName, pName); err != nil {
		t.Fatalf("status-only reset: %v", err)
	}

	// A single attempt re-parks because fail_count+1 = 6 >= 5.
	if err := createPass(ctx, e.cfg, tbl, e.st); err != nil {
		t.Fatalf("createPass after status-only reset: %v", err)
	}
	status, failCount, _ := e.rowState(t, pool, pName)
	if status != "failed" {
		t.Errorf("status = %q, want \"failed\" (status-only reset re-trips after one attempt)", status)
	}
	if failCount != 6 {
		t.Errorf("fail_count = %d, want 6 (breaker continued from 5)", failCount)
	}
}
