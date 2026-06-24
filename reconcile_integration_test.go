//go:build integration

package parter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// reconcileTestConfig builds a Config whose metadata schema is unique and
// creates the metadata schema + table. It returns the Config and the unique
// PARENT schema name to embed in Table.Name. reconcile only writes metadata
// rows, so no real partitioned parent table is required.
func reconcileTestConfig(t *testing.T, pool *pgxpool.Pool) (Config, string) {
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
	cfg := Config{Pool: pool, Schema: metaSchema}
	return cfg, parentSchemaName
}

type metaRow struct {
	tableName     string
	partitionName string
	rangeStart    time.Time
	rangeEnd      time.Time
	status        string
}

// fetchRows returns all managed_partitions rows for tableName, ordered by
// range_start.
func fetchRows(t *testing.T, pool *pgxpool.Pool, schema, tableName string) []metaRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := pool.Query(ctx, fmt.Sprintf(
		`SELECT table_name, partition_name, range_start, range_end, status
		   FROM %s.managed_partitions WHERE table_name = $1 ORDER BY range_start`, schema),
		tableName)
	if err != nil {
		t.Fatalf("fetchRows query: %v", err)
	}
	defer rows.Close()
	var out []metaRow
	for rows.Next() {
		var r metaRow
		if err := rows.Scan(&r.tableName, &r.partitionName, &r.rangeStart, &r.rangeEnd, &r.status); err != nil {
			t.Fatalf("fetchRows scan: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("fetchRows rows.Err: %v", err)
	}
	return out
}

// monthNamer returns a Namer producing unique year-month names.
func monthNamer(rel string) func(time.Time) string {
	return func(start time.Time) string {
		return fmt.Sprintf("%s_%s", rel, start.Format("2006_01"))
	}
}

// reconcile.yass.yaml SIDE-EFFECT: a fresh table -> reconcile inserts exactly
// Lookahead+1 pending rows with correct table_name, schema-qualified
// partition_name, range_start/range_end, status='pending'.
func TestIntegrationReconcileSeeding(t *testing.T) {
	pool := testPool(t)
	cfg, parent := reconcileTestConfig(t, pool)
	rel := "events"
	tableName := parent + "." + rel
	lookahead := 3
	tbl := Table{Name: tableName, Strategy: Monthly, Lookahead: lookahead, Namer: monthNamer(rel)}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := reconcile(ctx, cfg, tbl); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	rows := fetchRows(t, pool, cfg.schema(), tableName)
	if len(rows) != lookahead+1 {
		t.Fatalf("seeded %d rows, want %d", len(rows), lookahead+1)
	}

	now := time.Now().UTC()
	want := desiredPeriods(now, Monthly, lookahead)
	for i, r := range rows {
		if r.tableName != tableName {
			t.Errorf("row[%d].table_name = %q, want %q", i, r.tableName, tableName)
		}
		wantPart := parent + "." + tbl.Namer(want[i].Start)
		if r.partitionName != wantPart {
			t.Errorf("row[%d].partition_name = %q, want %q", i, r.partitionName, wantPart)
		}
		if !r.rangeStart.UTC().Equal(want[i].Start) {
			t.Errorf("row[%d].range_start = %v, want %v", i, r.rangeStart.UTC(), want[i].Start)
		}
		if !r.rangeEnd.UTC().Equal(want[i].End) {
			t.Errorf("row[%d].range_end = %v, want %v", i, r.rangeEnd.UTC(), want[i].End)
		}
		if r.status != "pending" {
			t.Errorf("row[%d].status = %q, want \"pending\"", i, r.status)
		}
	}
}

// reconcile.yass.yaml SIDE-EFFECT / coordination INVARIANT (idempotent):
// running reconcile twice leaves exactly Lookahead+1 rows (ON CONFLICT DO
// NOTHING; no duplicates, no errors).
func TestIntegrationReconcileIdempotent(t *testing.T) {
	pool := testPool(t)
	cfg, parent := reconcileTestConfig(t, pool)
	rel := "events"
	tableName := parent + "." + rel
	lookahead := 4
	tbl := Table{Name: tableName, Strategy: Daily, Lookahead: lookahead, Namer: func(s time.Time) string {
		return fmt.Sprintf("%s_%s", rel, s.Format("2006_01_02"))
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 2; i++ {
		if err := reconcile(ctx, cfg, tbl); err != nil {
			t.Fatalf("reconcile (run %d): %v", i, err)
		}
	}
	rows := fetchRows(t, pool, cfg.schema(), tableName)
	if len(rows) != lookahead+1 {
		t.Fatalf("after two reconciles: %d rows, want %d", len(rows), lookahead+1)
	}
}

// reconcile.yass.yaml INVARIANT (self-healing) / coordination INVARIANT:
// deleting a current/future desired row and reconciling re-inserts it.
func TestIntegrationReconcileSelfHealing(t *testing.T) {
	pool := testPool(t)
	cfg, parent := reconcileTestConfig(t, pool)
	rel := "events"
	tableName := parent + "." + rel
	lookahead := 3
	tbl := Table{Name: tableName, Strategy: Monthly, Lookahead: lookahead, Namer: monthNamer(rel)}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := reconcile(ctx, cfg, tbl); err != nil {
		t.Fatalf("reconcile (seed): %v", err)
	}

	// Delete a FUTURE desired row (the last one).
	rows := fetchRows(t, pool, cfg.schema(), tableName)
	victim := rows[len(rows)-1]
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s.managed_partitions WHERE table_name=$1 AND partition_name=$2`, cfg.schema()),
		victim.tableName, victim.partitionName); err != nil {
		t.Fatalf("delete victim: %v", err)
	}
	if got := fetchRows(t, pool, cfg.schema(), tableName); len(got) != lookahead {
		t.Fatalf("after delete: %d rows, want %d", len(got), lookahead)
	}

	// Reconcile re-inserts the deleted current/future row.
	if err := reconcile(ctx, cfg, tbl); err != nil {
		t.Fatalf("reconcile (heal): %v", err)
	}
	healed := fetchRows(t, pool, cfg.schema(), tableName)
	if len(healed) != lookahead+1 {
		t.Fatalf("after heal: %d rows, want %d", len(healed), lookahead+1)
	}
	found := false
	for _, r := range healed {
		if r.partitionName == victim.partitionName {
			found = true
		}
	}
	if !found {
		t.Errorf("self-heal did not re-insert %q", victim.partitionName)
	}
}

// reconcile.yass.yaml INVARIANT (MUST-NOT re-insert past periods) +
// retention SEE: a manually inserted past-period row that is then deleted is
// NOT re-inserted, and reconcile never creates any row with range_start before
// the current period boundary.
func TestIntegrationReconcileNoPastResurrection(t *testing.T) {
	pool := testPool(t)
	cfg, parent := reconcileTestConfig(t, pool)
	rel := "events"
	tableName := parent + "." + rel
	lookahead := 3
	tbl := Table{Name: tableName, Strategy: Monthly, Lookahead: lookahead, Namer: monthNamer(rel)}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Now().UTC()
	boundary := truncatePeriod(now, Monthly)
	// A past period: the month before the current boundary.
	pastStart := boundary.AddDate(0, -1, 0)
	pastEnd := boundary
	pastName := parent + "." + tbl.Namer(pastStart)

	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.managed_partitions (table_name, partition_name, range_start, range_end, status)
		 VALUES ($1,$2,$3,$4,'pending')`, cfg.schema()),
		tableName, pastName, pastStart, pastEnd); err != nil {
		t.Fatalf("insert past row: %v", err)
	}
	// Delete it (simulating a row the retention pass already dropped).
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s.managed_partitions WHERE table_name=$1 AND partition_name=$2`, cfg.schema()),
		tableName, pastName); err != nil {
		t.Fatalf("delete past row: %v", err)
	}

	if err := reconcile(ctx, cfg, tbl); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	rows := fetchRows(t, pool, cfg.schema(), tableName)
	// Exactly Lookahead+1 desired rows, none resurrected.
	if len(rows) != lookahead+1 {
		t.Fatalf("after reconcile: %d rows, want %d", len(rows), lookahead+1)
	}
	for _, r := range rows {
		if r.partitionName == pastName {
			t.Errorf("past period %q was resurrected", pastName)
		}
		if r.rangeStart.UTC().Before(boundary) {
			t.Errorf("row %q has range_start %v before current boundary %v",
				r.partitionName, r.rangeStart.UTC(), boundary)
		}
	}
}

// reconcile.yass.yaml INVARIANT (MUST-NOT recover from Namer collisions) /
// config.yass.yaml parter.Table INVARIANT: a Namer that returns the SAME name
// for two distinct periods causes reconcile to insert only ONE row for that
// name; the colliding period gets no row (ON CONFLICT silently drops the
// duplicate). This is operator error, only fixable by fixing the Namer.
func TestIntegrationReconcileNamerCollision(t *testing.T) {
	pool := testPool(t)
	cfg, parent := reconcileTestConfig(t, pool)
	rel := "events"
	tableName := parent + "." + rel
	lookahead := 3

	// A deliberately broken Namer: it omits the month, so every Monthly period
	// in the same year collides to one name.
	collidingNamer := func(s time.Time) string {
		return fmt.Sprintf("%s_%s", rel, s.Format("2006"))
	}
	tbl := Table{Name: tableName, Strategy: Monthly, Lookahead: lookahead, Namer: collidingNamer}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := reconcile(ctx, cfg, tbl); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	rows := fetchRows(t, pool, cfg.schema(), tableName)

	// Determine how many distinct partition names the desired set maps to in
	// this year; only that many rows can exist (the rest collide away).
	now := time.Now().UTC()
	desired := desiredPeriods(now, Monthly, lookahead)
	distinct := map[string]bool{}
	for _, p := range desired {
		distinct[parent+"."+collidingNamer(p.Start)] = true
	}
	if len(rows) != len(distinct) {
		t.Fatalf("Namer collision: got %d rows, want %d distinct names (lookahead+1=%d desired periods)",
			len(rows), len(distinct), lookahead+1)
	}
	if len(rows) >= lookahead+1 {
		t.Fatalf("Namer collision did not drop any duplicate: %d rows for %d desired periods",
			len(rows), lookahead+1)
	}
}

// reconcile.yass.yaml SIDE-EFFECT (ON CONFLICT DO NOTHING) / coordination
// INVARIANT (multi-instance safe): concurrent reconcile calls produce no error
// and no duplicates.
func TestIntegrationReconcileConcurrent(t *testing.T) {
	pool := testPool(t)
	cfg, parent := reconcileTestConfig(t, pool)
	rel := "events"
	tableName := parent + "." + rel
	lookahead := 3
	tbl := Table{Name: tableName, Strategy: Monthly, Lookahead: lookahead, Namer: monthNamer(rel)}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const n = 4
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = reconcile(ctx, cfg, tbl)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent reconcile[%d]: %v", i, err)
		}
	}
	rows := fetchRows(t, pool, cfg.schema(), tableName)
	if len(rows) != lookahead+1 {
		t.Fatalf("after %d concurrent reconciles: %d rows, want %d", n, len(rows), lookahead+1)
	}
}
