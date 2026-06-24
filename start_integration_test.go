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

// startEnv bundles a Config plus the parent metadata for a Start integration
// test. It mirrors newRetentionEnv but lets the caller decide whether to attach
// a DEFAULT partition (Start panics when one is absent).
type startEnv struct {
	cfg       Config
	parent    string // parent schema name
	tableName string // schema-qualified parent table
	rel       string // unqualified parent relation
}

// newStartEnv creates the metadata schema + managed_partitions table, a unique
// parent schema, and a RANGE-partitioned parent table. When withDefault is true
// it also attaches a DEFAULT partition. It returns a Config with a single
// Monthly table.
func newStartEnv(t *testing.T, pool *pgxpool.Pool, withDefault bool) startEnv {
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
	if withDefault {
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s_default PARTITION OF %s DEFAULT`, tableName, tableName)); err != nil {
			t.Fatalf("create default partition: %v", err)
		}
	}

	cfg := Config{
		Pool:   pool,
		Schema: metaSchema,
		Tables: []Table{{
			Name:      tableName,
			Strategy:  Monthly,
			Lookahead: 1,
			Retention: 0,
			Namer: func(s time.Time) string {
				return fmt.Sprintf("%s_%s", rel, s.Format("2006_01"))
			},
		}},
	}
	return startEnv{cfg: cfg, parent: parentSchemaName, tableName: tableName, rel: rel}
}

// saveGlobal snapshots the current core.Global value and registers a cleanup
// that restores it. Every test that calls Start mutates core.Global, so each
// must save/restore to avoid cross-test contamination.
func saveGlobal(t *testing.T) {
	t.Helper()
	prev := core.Global.Load()
	t.Cleanup(func() { core.Global.Store(prev) })
}

// start_integration_test happy path — start.yass.yaml parter.Start
// RETURN/SIDE-EFFECT: with a parent that has a DEFAULT and a pre-created
// metadata schema + managed_partitions table, Start must NOT panic, must return
// promptly, must build state, call SetStarted, store into core.Global, and
// launch the background goroutines. The cancelable ctx with deferred cancel
// stops the launched goroutines so they do not leak.
func TestStartHappyPath(t *testing.T) {
	pool := testPool(t)
	saveGlobal(t)

	env := newStartEnv(t, pool, true)

	// EnsureReady pre-creates everything Start needs and gives the background
	// goroutine a table to work with. It also verifies the DEFAULT precondition.
	prepCtx, prepCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer prepCancel()
	if err := EnsureReady(prepCtx, env.cfg); err != nil {
		t.Fatalf("EnsureReady: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // stop the launched goroutines on test exit

	done := make(chan struct{})
	go func() {
		defer close(done)
		Start(ctx, env.cfg) // MUST NOT panic
	}()
	select {
	case <-done:
		// returned promptly, as required
	case <-time.After(10 * time.Second):
		t.Fatalf("Start did not return promptly")
	}

	st := core.Global.Load()
	if st == nil {
		t.Fatalf("core.Global not stored after Start")
	}
	if !st.Started() {
		t.Fatalf("core.Global state Started() = false, want true")
	}
}

// start_integration_test DEFAULT absent — start.yass.yaml parter.Start ERROR
// ("any registered table lacks a DEFAULT partition" + "perform the DEFAULT
// partition check synchronously before launching the goroutine"): with a parent
// table that has NO DEFAULT, Start must panic, and because the synchronous
// DEFAULT check runs BEFORE state is built/stored, core.Global must remain nil.
func TestStartPanicsWhenDefaultAbsent(t *testing.T) {
	pool := testPool(t)
	saveGlobal(t)

	// Pre-create the metadata schema/table so the only failing precondition is
	// the missing DEFAULT, not a missing metadata table.
	env := newStartEnv(t, pool, false)

	// Establish the precondition for the assertion: Global is nil before Start.
	core.Global.Store(nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !didPanic(func() { Start(ctx, env.cfg) }) {
		t.Fatalf("Start did not panic when the registered table lacks a DEFAULT")
	}

	// The synchronous DEFAULT check must have panicked BEFORE state was stored.
	if got := core.Global.Load(); got != nil {
		t.Fatalf("core.Global was stored despite the DEFAULT-absent panic: %v", got)
	}
}
