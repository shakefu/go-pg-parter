//go:build integration

package parter

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// obsTestEnv bundles a unique metadata schema/table plus a unique parent schema
// and a RANGE-partitioned parent WITH a DEFAULT partition, and a fresh
// core.State carrying per-table counters. computeGauges only reads the metadata
// table, but the parent + DEFAULT mirror the real environment Start sets up.
type obsTestEnv struct {
	cfg        Config
	parent     string // parent schema name
	tableRet   string // schema-qualified parent table WITH retention
	relRet     string
	tableNoRet string // schema-qualified parent table WITHOUT retention
	relNoRet   string
	st         *core.State
}

// newObsEnv builds the metadata schema/table and two unique parent tables (one
// with retention, one without), each RANGE-partitioned with a DEFAULT.
func newObsEnv(t *testing.T, pool *pgxpool.Pool) obsTestEnv {
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

	relRet := "events_ret"
	relNoRet := "events_noret"
	tableRet := parentSchemaName + "." + relRet
	tableNoRet := parentSchemaName + "." + relNoRet
	for _, tbl := range []string{tableRet, tableNoRet} {
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s (id bigint, ts timestamptz NOT NULL) PARTITION BY RANGE (ts)`, tbl)); err != nil {
			t.Fatalf("create parent table %s: %v", tbl, err)
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s_default PARTITION OF %s DEFAULT`, tbl, tbl)); err != nil {
			t.Fatalf("create default partition for %s: %v", tbl, err)
		}
	}

	cfg := Config{
		Pool:   pool,
		Schema: metaSchema,
		Tables: []Table{
			{Name: tableRet, Strategy: Monthly, Lookahead: 1, Retention: 3,
				Namer: func(s time.Time) string { return relRet + "_" + s.Format("2006_01") }},
			{Name: tableNoRet, Strategy: Monthly, Lookahead: 1, Retention: 0,
				Namer: func(s time.Time) string { return relNoRet + "_" + s.Format("2006_01") }},
		},
	}
	st := &core.State{
		Schema: metaSchema,
		Tables: []core.TableInfo{
			{Name: tableRet, HasRetention: true},
			{Name: tableNoRet, HasRetention: false},
		},
		Counters: core.NewCounters([]string{tableRet, tableNoRet}),
	}
	return obsTestEnv{
		cfg: cfg, parent: parentSchemaName,
		tableRet: tableRet, relRet: relRet,
		tableNoRet: tableNoRet, relNoRet: relNoRet,
		st: st,
	}
}

// seedRow inserts a managed_partitions row for the given table/status.
func (e obsTestEnv) seedRow(t *testing.T, pool *pgxpool.Pool, tableName, pName string, start, end time.Time, status string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.managed_partitions
		   (table_name, partition_name, range_start, range_end, status)
		 VALUES ($1,$2,$3,$4,$5)`, e.cfg.schema()),
		tableName, pName, start, end, status); err != nil {
		t.Fatalf("seedRow(%s, %s, %s): %v", tableName, pName, status, err)
	}
}

// observability.yass.yaml parter.ObservabilityGoroutine SIDE-EFFECT /
// parter.Metrics RETURN / CONTRACT §9: computeGauges snapshots every row of
// managed_partitions read-only and computes per-table Pending, Overdue,
// Created, Failed, RetentionOverdue (0 when Retention=0), and HeadroomSeconds.
func TestComputeGauges(t *testing.T) {
	pool := testPool(t)
	e := newObsEnv(t, pool)
	now := time.Now().UTC()

	// --- tableRet (Retention=3 months) rows ---
	// Two pending: one overdue (range_start in the past), one future.
	e.seedRow(t, pool, e.tableRet, e.parent+".ret_pending_overdue",
		now.Add(-2*time.Hour), now.Add(-1*time.Hour), "pending")
	e.seedRow(t, pool, e.tableRet, e.parent+".ret_pending_future",
		now.Add(48*time.Hour), now.Add(72*time.Hour), "pending")
	// One pending exactly at now() boundary counts as overdue (range_start <= now()).
	e.seedRow(t, pool, e.tableRet, e.parent+".ret_pending_now",
		now, now.Add(time.Hour), "pending")

	// Created rows: one with range_end well in the future (drives headroom),
	// one whose range_end is older than the 3-month retention cutoff (overdue),
	// one created but inside the retention window (not retention-overdue).
	headroomEnd := now.Add(240 * time.Hour) // +10 days
	e.seedRow(t, pool, e.tableRet, e.parent+".ret_created_future",
		now.Add(-time.Hour), headroomEnd, "created")
	e.seedRow(t, pool, e.tableRet, e.parent+".ret_created_old",
		now.AddDate(0, -5, 0), now.AddDate(0, -4, 0), "created") // range_end ~4mo ago < cutoff(3mo)
	e.seedRow(t, pool, e.tableRet, e.parent+".ret_created_recent",
		now.AddDate(0, -1, 0), now.Add(-time.Hour), "created") // range_end ~1h ago, within 3mo window

	// One failed row.
	e.seedRow(t, pool, e.tableRet, e.parent+".ret_failed",
		now.Add(-100*time.Hour), now.Add(-99*time.Hour), "failed")

	// --- tableNoRet (Retention=0) rows ---
	// One pending overdue, one created whose range_end is very old. With
	// Retention=0 the RetentionOverdue computation is skipped (must be 0).
	e.seedRow(t, pool, e.tableNoRet, e.parent+".noret_pending_overdue",
		now.Add(-3*time.Hour), now.Add(-2*time.Hour), "pending")
	e.seedRow(t, pool, e.tableNoRet, e.parent+".noret_created_old",
		now.AddDate(0, -10, 0), now.AddDate(0, -9, 0), "created")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	snap, err := computeGauges(ctx, e.cfg)
	if err != nil {
		t.Fatalf("computeGauges: %v", err)
	}
	if snap == nil || snap.PerTable == nil {
		t.Fatalf("computeGauges returned nil snapshot")
	}

	// Every cfg.Tables entry must have an entry.
	gRet, ok := snap.PerTable[e.tableRet]
	if !ok {
		t.Fatalf("snapshot missing entry for %s", e.tableRet)
	}
	gNoRet, ok := snap.PerTable[e.tableNoRet]
	if !ok {
		t.Fatalf("snapshot missing entry for %s", e.tableNoRet)
	}

	// tableRet: 3 pending; overdue = past + now boundary = 2.
	if gRet.Pending != 3 {
		t.Errorf("tableRet Pending = %d, want 3", gRet.Pending)
	}
	if gRet.Overdue != 2 {
		t.Errorf("tableRet Overdue = %d, want 2", gRet.Overdue)
	}
	// 3 created rows.
	if gRet.Created != 3 {
		t.Errorf("tableRet Created = %d, want 3", gRet.Created)
	}
	// 1 failed.
	if gRet.Failed != 1 {
		t.Errorf("tableRet Failed = %d, want 1", gRet.Failed)
	}
	// retention_overdue: only the ~4-month-old created row is past the 3-month cutoff.
	if gRet.RetentionOverdue != 1 {
		t.Errorf("tableRet RetentionOverdue = %d, want 1", gRet.RetentionOverdue)
	}
	// headroom = max(range_end over created) - now, clamped >= 0. The future
	// created row drives it to roughly +240h; assert it is positive and within
	// a small tolerance of the seeded value.
	wantHeadroom := headroomEnd.Sub(now).Seconds()
	if gRet.HeadroomSeconds <= 0 {
		t.Errorf("tableRet HeadroomSeconds = %v, want > 0", gRet.HeadroomSeconds)
	}
	if diff := gRet.HeadroomSeconds - wantHeadroom; diff > 60 || diff < -60 {
		t.Errorf("tableRet HeadroomSeconds = %v, want ~%v (within 60s)", gRet.HeadroomSeconds, wantHeadroom)
	}

	// tableNoRet: 1 pending overdue; 1 created. RetentionOverdue MUST be 0
	// because Retention=0 (computation skipped) even though the created row is
	// ancient.
	if gNoRet.Pending != 1 {
		t.Errorf("tableNoRet Pending = %d, want 1", gNoRet.Pending)
	}
	if gNoRet.Overdue != 1 {
		t.Errorf("tableNoRet Overdue = %d, want 1", gNoRet.Overdue)
	}
	if gNoRet.Created != 1 {
		t.Errorf("tableNoRet Created = %d, want 1", gNoRet.Created)
	}
	if gNoRet.RetentionOverdue != 0 {
		t.Errorf("tableNoRet RetentionOverdue = %d, want 0 (Retention=0)", gNoRet.RetentionOverdue)
	}
	// Its single created row's range_end is ~9 months in the past, so headroom
	// clamps at 0.
	if gNoRet.HeadroomSeconds != 0 {
		t.Errorf("tableNoRet HeadroomSeconds = %v, want 0 (clamped)", gNoRet.HeadroomSeconds)
	}
}

// observability.yass.yaml parter.ObservabilityGoroutine ERROR / CONTRACT §9:
// computeGauges returns a non-nil error when the metadata table does not exist.
func TestComputeGaugesErrorPath(t *testing.T) {
	pool := testPool(t)
	e := newObsEnv(t, pool)

	// Drop the metadata table so the SELECT fails.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		"DROP TABLE %s.managed_partitions", e.cfg.schema())); err != nil {
		t.Fatalf("drop managed_partitions: %v", err)
	}

	snap, err := computeGauges(ctx, e.cfg)
	if err == nil {
		t.Fatalf("computeGauges should return a non-nil error when managed_partitions is missing; got snap=%v", snap)
	}
}

// observability.yass.yaml parter.ObservabilityGoroutine INPUT/SIDE-EFFECT /
// parter.MetricsChan RETURN / CONTRACT §9: a single observability tick computes
// gauges, publishes them via SetGauges, and broadcasts a Prometheus payload to
// subscribers. Uses a fast-tick trick (maintOffset=1s => obsOffset 0,
// processStart ~300ms out) so the first tick fires while the test is blocked
// reading the channel — no 60s wait.
func TestObservabilityGoroutineSingleTick(t *testing.T) {
	pool := testPool(t)
	e := newObsEnv(t, pool)

	// Seed one created row so the snapshot has content.
	now := time.Now().UTC()
	e.seedRow(t, pool, e.tableRet, e.parent+".ret_created_future",
		now.Add(-time.Hour), now.Add(240*time.Hour), "created")

	prev := core.Global.Load()
	core.Global.Store(e.st)
	t.Cleanup(func() { core.Global.Store(prev) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe FIRST so the channel is registered before the tick broadcasts.
	ch := MetricsChan(ctx, e.cfg)

	// maintOffset = 1s => obsOffset = (1s - 1s + 60s) % 60s = 0. processStart
	// 300ms in the future so the first (and only needed) tick fires shortly.
	processStart := time.Now().Add(300 * time.Millisecond)
	go observabilityGoroutine(ctx, e.cfg, processStart, time.Second, e.st)

	select {
	case payload, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed before any payload")
		}
		// The payload is the underscore-name Prometheus exposition text.
		if len(payload) == 0 {
			t.Fatalf("received empty payload")
		}
		if want := "# TYPE parter_pending gauge"; !strings.Contains(payload, want) {
			t.Fatalf("payload missing %q:\n%s", want, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no payload received within 5s")
	}

	// Gauges were published.
	if e.st.Gauges() == nil {
		t.Fatalf("st.Gauges() is nil after a tick")
	}
	cancel()
}

// observability.yass.yaml parter.ObservabilityGoroutine ERROR / CONTRACT §9:
// when the metadata query fails each tick, the goroutine increments
// parter.observer_errors_total. Uses the same fast-tick trick.
func TestObservabilityGoroutineObserverErrors(t *testing.T) {
	pool := testPool(t)
	e := newObsEnv(t, pool)

	// Drop the metadata table so every tick's SELECT fails.
	dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if _, err := pool.Exec(dropCtx, fmt.Sprintf(
		"DROP TABLE %s.managed_partitions", e.cfg.schema())); err != nil {
		dropCancel()
		t.Fatalf("drop managed_partitions: %v", err)
	}
	dropCancel()

	prev := core.Global.Load()
	core.Global.Store(e.st)
	t.Cleanup(func() { core.Global.Store(prev) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processStart := time.Now().Add(300 * time.Millisecond)
	go observabilityGoroutine(ctx, e.cfg, processStart, time.Second, e.st)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e.st.Counters.ObserverErrorsTotal.Load() >= 1 {
			cancel()
			return
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("ctx cancelled before observer error observed")
		}
	}
	cancel()
	t.Fatalf("ObserverErrorsTotal did not reach >= 1 within 5s, got %d", e.st.Counters.ObserverErrorsTotal.Load())
}
