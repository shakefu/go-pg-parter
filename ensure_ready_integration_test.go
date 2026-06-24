//go:build integration

package parter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ensureReadyEnv bundles what an EnsureReady integration test needs: a Config
// whose metadata schema is UNIQUE but deliberately NOT pre-created (EnsureReady
// step 2 must create it), a unique parent schema, the RANGE-partitioned parent
// table, and the unqualified relation name for the Namer.
type ensureReadyEnv struct {
	cfg        Config
	parent     string // parent schema name
	metaSchema string // metadata schema (uncreated; EnsureReady creates it)
	tableName  string // schema-qualified parent table
	rel        string // unqualified parent relation
}

// newEnsureReadyEnv builds a unique parent schema and a RANGE-partitioned parent
// table in it, and reserves (without creating) a unique metadata schema. The
// metadata schema being uncreated is intentional: EnsureReady step 2 must create
// it. withDefault controls whether the parent gets a DEFAULT partition.
func newEnsureReadyEnv(t *testing.T, pool *pgxpool.Pool, withDefault bool) ensureReadyEnv {
	t.Helper()
	metaSchema := uniqueSchema(t, pool) // NOT created here — EnsureReady must create it.
	parentSchemaName := uniqueSchema(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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
	if withDefault {
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s_default PARTITION OF %s DEFAULT`, tableName, tableName)); err != nil {
			t.Fatalf("create default partition: %v", err)
		}
	}

	cfg := Config{Pool: pool, Schema: metaSchema}
	return ensureReadyEnv{
		cfg:        cfg,
		parent:     parentSchemaName,
		metaSchema: metaSchema,
		tableName:  tableName,
		rel:        rel,
	}
}

// monthlyTable returns a Monthly Table for the parent with Lookahead 1 and the
// given retention. The Namer renders e.g. "events_2026_06".
func (e ensureReadyEnv) monthlyTable(retention int) Table {
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
func (e ensureReadyEnv) partitionName(tbl Table, start time.Time) string {
	return e.parent + "." + tbl.Namer(start)
}

// schemaExists reports whether a schema exists in the catalog.
func schemaExists(t *testing.T, pool *pgxpool.Pool, schema string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schema).Scan(&exists); err != nil {
		t.Fatalf("schemaExists(%s): %v", schema, err)
	}
	return exists
}

// managedTableExists reports whether <schema>.managed_partitions exists.
func managedTableExists(t *testing.T, pool *pgxpool.Pool, schema string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = 'managed_partitions')`,
		schema).Scan(&exists); err != nil {
		t.Fatalf("managedTableExists(%s): %v", schema, err)
	}
	return exists
}

// rowCount returns the number of managed_partitions rows for the parent table.
func (e ensureReadyEnv) rowCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s.managed_partitions WHERE table_name=$1`, e.metaSchema),
		e.tableName).Scan(&n); err != nil {
		t.Fatalf("rowCount: %v", err)
	}
	return n
}

// rowStatusAndFail reads (status, fail_count) for a partition row; exists is
// false when no row is present.
func (e ensureReadyEnv) rowStatusAndFail(t *testing.T, pool *pgxpool.Pool, pName string) (status string, failCount int, exists bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT status, fail_count FROM %s.managed_partitions
		   WHERE table_name=$1 AND partition_name=$2`, e.metaSchema),
		e.tableName, pName).Scan(&status, &failCount)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return "", 0, false
		}
		t.Fatalf("rowStatusAndFail(%s): %v", pName, err)
	}
	return status, failCount, true
}

// relationExists reports whether the relation pName exists at all. It uses
// to_regclass (which returns NULL rather than erroring on a missing relation)
// so it is safe to call when a deterministic create failure left no relation.
func relationExists(t *testing.T, pool *pgxpool.Pool, pName string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var oid *uint32
	if err := pool.QueryRow(ctx, `SELECT to_regclass($1)::oid`, pName).Scan(&oid); err != nil {
		t.Fatalf("relationExists(%s): %v", pName, err)
	}
	return oid != nil && *oid != 0
}

// isAttached reports whether pName is physically attached to the parent. When
// the relation does not exist (e.g. a deterministic create failure left
// nothing behind), it returns false rather than erroring on the regclass cast.
func (e ensureReadyEnv) isAttached(t *testing.T, pool *pgxpool.Pool, pName string) bool {
	t.Helper()
	if !relationExists(t, pool, pName) {
		return false
	}
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

// ensure-ready.yass.yaml parter.EnsureReady RETURN (the ordered steps 1-6),
// SIDE-EFFECT (create the schema, managed_partitions table, pending rows, and
// overdue partitions), and RETURN "be idempotent". Parent has a DEFAULT;
// metadata schema is uncreated. After EnsureReady: the schema and table exist,
// pending rows are seeded for the desired periods, and the current-period
// partition is 'created' and physically attached. A second call is a clean
// no-op (no error, no duplicate rows).
func TestIntegrationEnsureReadyHappyPathIdempotent(t *testing.T) {
	pool := testPool(t)
	e := newEnsureReadyEnv(t, pool, true)
	tbl := e.monthlyTable(0)
	e.cfg.Tables = []Table{tbl}

	// Precondition: the metadata schema does NOT yet exist (EnsureReady step 2
	// must create it).
	if schemaExists(t, pool, e.metaSchema) {
		t.Fatalf("precondition: metadata schema %q already exists", e.metaSchema)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := EnsureReady(ctx, e.cfg); err != nil {
		t.Fatalf("EnsureReady (first call): %v", err)
	}

	// Steps 2-3: schema + table now exist.
	if !schemaExists(t, pool, e.metaSchema) {
		t.Errorf("metadata schema %q was not created", e.metaSchema)
	}
	if !managedTableExists(t, pool, e.metaSchema) {
		t.Errorf("managed_partitions table was not created in %q", e.metaSchema)
	}

	// Step 5: reconcile seeded Lookahead+1 = 2 pending-or-created rows for the
	// desired periods.
	now := time.Now().UTC()
	desired := desiredPeriods(now, tbl.Strategy, tbl.Lookahead)
	if got, want := e.rowCount(t, pool), len(desired); got != want {
		t.Errorf("row count after first EnsureReady = %d, want %d (Lookahead+1)", got, want)
	}

	// Step 6: the current-period partition was created synchronously and is
	// physically attached.
	curStart := truncatePeriod(now, Monthly)
	curName := e.partitionName(tbl, curStart)
	status, _, exists := e.rowStatusAndFail(t, pool, curName)
	if !exists {
		t.Fatalf("current-period row %q missing after EnsureReady", curName)
	}
	if status != string(StatusCreated) {
		t.Errorf("current-period row %q status = %q, want %q", curName, status, StatusCreated)
	}
	if !e.isAttached(t, pool, curName) {
		t.Errorf("current-period partition %q is not physically attached to parent", curName)
	}

	rowsAfterFirst := e.rowCount(t, pool)

	// Idempotency: a second EnsureReady returns nil and adds no rows.
	if err := EnsureReady(ctx, e.cfg); err != nil {
		t.Fatalf("EnsureReady (second call): %v", err)
	}
	if got := e.rowCount(t, pool); got != rowsAfterFirst {
		t.Errorf("row count changed across idempotent calls: %d -> %d", rowsAfterFirst, got)
	}
	status2, _, _ := e.rowStatusAndFail(t, pool, curName)
	if status2 != string(StatusCreated) {
		t.Errorf("current-period row status after second call = %q, want %q", status2, StatusCreated)
	}
}

// ensure-ready.yass.yaml parter.EnsureReady ERROR (verify a DEFAULT partition
// exists; return an error if it is absent) + MUST-NOT create the DEFAULT.
// Parent has NO DEFAULT partition. EnsureReady returns a non-nil error naming
// the table, after steps 1-4 created the schema + managed_partitions table, and
// NO default partition is created (the catalog check still returns false).
func TestIntegrationEnsureReadyDefaultAbsentErrors(t *testing.T) {
	pool := testPool(t)
	e := newEnsureReadyEnv(t, pool, false) // NO default partition
	tbl := e.monthlyTable(0)
	e.cfg.Tables = []Table{tbl}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := EnsureReady(ctx, e.cfg)
	if err == nil {
		t.Fatal("EnsureReady returned nil for a parent without a DEFAULT partition, want an error")
	}
	// The error mentions the table whose DEFAULT is missing.
	if want := e.tableName; !contains(err.Error(), want) {
		t.Errorf("error %q does not mention the table %q", err.Error(), want)
	}

	// Steps 1-4 ran: the schema + managed_partitions table were created.
	if !schemaExists(t, pool, e.metaSchema) {
		t.Errorf("metadata schema %q was not created before the DEFAULT check failed", e.metaSchema)
	}
	if !managedTableExists(t, pool, e.metaSchema) {
		t.Errorf("managed_partitions table was not created before the DEFAULT check failed")
	}

	// MUST-NOT create the DEFAULT: the catalog check still returns false.
	exists, qerr := hasDefaultPartition(ctx, pool, e.tableName)
	if qerr != nil {
		t.Fatalf("hasDefaultPartition: %v", qerr)
	}
	if exists {
		t.Error("EnsureReady created a DEFAULT partition; it MUST NOT")
	}
}

// ensure-ready.yass.yaml parter.EnsureReady SIDE-EFFECT ("advance fail_count
// durably when a deterministic failure occurs during its create pass, because
// that create pass shares the circuit breaker"). The parent has a DEFAULT; a
// row whose ts falls in the CURRENT period is inserted into the DEFAULT
// partition, so creating the current-period partition fails with
// check_violation (23514) — a deterministic failure handled inside createPass.
// EnsureReady returns nil; the current-period metadata row has fail_count = 1
// and status 'pending' (breaker advanced durably, still < 5 so not parked).
func TestIntegrationEnsureReadySharedCircuitBreaker(t *testing.T) {
	pool := testPool(t)
	e := newEnsureReadyEnv(t, pool, true)
	tbl := e.monthlyTable(0)
	e.cfg.Tables = []Table{tbl}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Insert a row whose ts is inside the CURRENT calendar month. With no
	// current-period partition attached yet, it lands in the DEFAULT partition.
	// Creating the current-period partition later then fails with 23514 because a
	// DEFAULT row already falls in the new partition's range.
	now := time.Now().UTC()
	curStart := truncatePeriod(now, Monthly)
	tsInCurrent := curStart.Add(36 * time.Hour) // safely within the current month
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, ts) VALUES (1, $1)`, e.tableName), tsInCurrent); err != nil {
		t.Fatalf("insert row into DEFAULT partition: %v", err)
	}

	// Deterministic create failures are handled INSIDE createPass and not
	// propagated, so EnsureReady returns nil.
	if err := EnsureReady(ctx, e.cfg); err != nil {
		t.Fatalf("EnsureReady returned an error for a deterministic create failure: %v", err)
	}

	// The breaker advanced durably for the current-period row.
	curName := e.partitionName(tbl, curStart)
	status, failCount, exists := e.rowStatusAndFail(t, pool, curName)
	if !exists {
		t.Fatalf("current-period row %q missing after EnsureReady", curName)
	}
	if failCount != 1 {
		t.Errorf("current-period row fail_count = %d, want 1 (breaker advanced once)", failCount)
	}
	if status != string(StatusPending) {
		t.Errorf("current-period row status = %q, want %q (< 5 failures, not parked)", status, StatusPending)
	}

	// The current-period partition was NOT created/attached (the create failed).
	if e.isAttached(t, pool, curName) {
		t.Errorf("current-period partition %q is attached, but its create should have failed", curName)
	}
}

// contains is a tiny substring helper to avoid importing strings just for the
// error-message assertion.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return len(sub) == 0
}
