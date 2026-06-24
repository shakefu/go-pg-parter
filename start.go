package parter

import (
	"context"
	"fmt"
	"time"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// Start validates the Config, verifies that every registered parent table has a
// DEFAULT partition, then launches the background maintenance and observability
// goroutines and returns. It has no error return: the two failure modes the
// spec mandates — an invalid Config and a registered table missing its DEFAULT
// partition — are reported by panicking, because neither precondition can be
// repaired by the library and both indicate operator misconfiguration. These
// are the ONLY panics the package emits.
//
// Order of operations (CONTRACT §11 / start.yass.yaml parter.Start):
//  1. ValidateConfig(cfg) — panic if invalid (same conditions as the in-memory
//     validation; CONFORMS ValidateConfig::ERROR). Runs first so no DB work
//     happens for an invalid Config.
//  2. For each table in cfg.Tables order, run the DEFAULT-partition check
//     SYNCHRONOUSLY and BEFORE building/storing state or launching any
//     goroutine. hasDefaultPartition runs the verbatim catalog EXISTS query. A
//     query error panics (cannot verify ⇒ cannot proceed); an absent DEFAULT
//     panics naming the offending table.
//  3. Build the shared State, mark it started, and store it into core.Global.
//  4. Compute processStart and the per-instance tick offset ONCE, then SHARE
//     that single offset with both goroutines.
//  5. Launch the maintenance goroutine UNCONDITIONALLY.
//  6. Launch the observability goroutine UNCONDITIONALLY (even if neither
//     MetricsChan nor ConfigureOtel is ever called).
//  7. Return. Both goroutines run until ctx is cancelled (tickLoop and
//     observabilityGoroutine return on ctx.Done).
//
// Start is safe to call from multiple replicas concurrently: the maintenance
// passes coordinate via FOR UPDATE SKIP LOCKED, so no extra coordination code
// is needed here. Start does NOT create the metadata schema/table or the parent
// DEFAULT — that is EnsureReady's and the operator's responsibility; Start only
// validates, DEFAULT-checks, and launches.
func Start(ctx context.Context, cfg Config) {
	// 1. Validate the Config in memory FIRST; panic on the same conditions as
	// the in-memory validation.
	if err := ValidateConfig(cfg); err != nil {
		panic(err)
	}

	// 2. Verify the DEFAULT-partition precondition for every registered table,
	// SYNCHRONOUSLY and BEFORE building state or launching any goroutine. A
	// verification error means we cannot prove the precondition, so we cannot
	// safely proceed.
	for _, t := range cfg.Tables {
		exists, err := hasDefaultPartition(ctx, cfg.Pool, t.Name)
		if err != nil {
			panic(fmt.Errorf("parter: Start: cannot verify DEFAULT partition for %q: %w", t.Name, err))
		}
		if !exists {
			panic(fmt.Errorf("parter: Start: table %q has no DEFAULT partition", t.Name))
		}
	}

	// 3. Build the shared State, mark it started, and publish it.
	st := newState(cfg)
	st.SetStarted()
	core.Global.Store(st)

	// 4. Compute the tick schedule ONCE and share the single offset with both
	// goroutines so their staggered cadence is derived from the same base.
	processStart := time.Now()
	secs := float64(processStart.Second()) + float64(processStart.Nanosecond())/1e9
	offset := TickOffset(secs, cfg.Instance)

	// 5. Launch the maintenance goroutine unconditionally.
	go tickLoop(ctx, processStart, offset, tickInterval, func(c context.Context) {
		runMaintenanceTick(c, cfg, TickDeadline, st)
	})

	// 6. Launch the observability goroutine unconditionally.
	go observabilityGoroutine(ctx, cfg, processStart, offset, st)

	// 7. Return; the goroutines live until ctx is cancelled.
}
