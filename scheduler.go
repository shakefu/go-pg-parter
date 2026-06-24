package parter

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// tickInterval is the fixed maintenance tick period.
//
// CONTRACT §5 / scheduler.yass.yaml.
const tickInterval = 60 * time.Second

// TickDeadline is the non-configurable per-tick deadline. The invariant
// TickDeadline < tickInterval guarantees the goroutine never misses more than
// one tick due to a single hung operation.
//
// scheduler.yass.yaml parter.MaintenanceLoop INVARIANT.
const TickDeadline = 55 * time.Second

// TickOffset computes the per-instance startup offset into the 60-second tick
// window. The base offset is (startupSecondsWithFraction * 23) mod 60; 23 is
// coprime to 60 so consecutive integer seconds map 23s apart and integer
// seconds cycle through all 60 positions. When instance is non-nil, the term
// (*instance * 23) is added before the modulo. The result is rounded to the
// nearest millisecond, ties rounding up, then wrapped within the 60-second
// window.
//
// scheduler.yass.yaml parter.TickOffset.
func TickOffset(startupSecondsWithFraction float64, instance *int) time.Duration {
	base := startupSecondsWithFraction * 23
	if instance != nil {
		base += float64(*instance) * 23
	}
	off := math.Mod(base, 60) // in [0,60) seconds (base is non-negative)
	ms := math.Floor(off*1000 + 0.5)
	msInt := int64(ms) % 60000
	return time.Duration(msInt) * time.Millisecond
}

// newState builds the process-wide shared core.State from a validated Config:
// the metadata Schema, one core.TableInfo per Config.Tables entry (in order,
// with HasRetention set iff Retention>0), and a pre-built per-table Counters
// map keyed by the schema-qualified table name. It does NOT call SetStarted and
// does NOT store into core.Global — that is Start's responsibility. Start and
// MaintenanceLoop both reuse this constructor.
//
// CONTRACT §5 / §11.
func newState(cfg Config) *core.State {
	tables := make([]core.TableInfo, 0, len(cfg.Tables))
	names := make([]string, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tables = append(tables, core.TableInfo{Name: t.Name, HasRetention: t.Retention > 0})
		names = append(names, t.Name)
	}
	return &core.State{
		Schema:   cfg.schema(),
		Tables:   tables,
		Counters: core.NewCounters(names),
	}
}

// runMaintenanceTick runs one maintenance tick: bounded by deadline, it opens
// the parter.tick span and, for each table in Config.Tables order, drains
// reconcile -> create -> (when Retention>0) retention, draining each table fully
// before moving to the next. A defer/recover logs any panic in the tick body and
// returns so the loop resumes on the NEXT tick rather than killing the
// goroutine. When the derived context expires (the deadline is hit), the
// in-flight transaction has already rolled back inside the pass and the tick
// returns early to proceed to the next tick. Non-context errors from a pass are
// logged and the next table proceeds.
//
// CONTRACT §5 / scheduler.yass.yaml parter.MaintenanceLoop SIDE-EFFECT/ERROR.
func runMaintenanceTick(ctx context.Context, cfg Config, deadline time.Duration, st *core.State) {
	tctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	// Recover from any panic in the tick body so the maintenance goroutine
	// survives and resumes on the next tick. Registered before the tick span's
	// End so defers unwind LIFO (span ends, then recover runs).
	defer func() {
		if r := recover(); r != nil {
			log.Printf("parter: maintenance tick recovered from panic: %v", r)
		}
	}()

	sctx, tickSpan := st.Tracer().StartTick(tctx)
	defer tickSpan.End()

	for _, t := range cfg.Tables {
		// 1. Reconcile pending rows before the create pass.
		if err := reconcile(sctx, cfg, t); err != nil {
			log.Printf("parter: reconcile %q: %v", t.Name, err)
			if isCtxError(err) {
				return
			}
		}

		// 2. Drain the create pass under a child span.
		cctx, cspan := st.Tracer().StartCreate(sctx)
		err := createPass(cctx, cfg, t, st)
		if err != nil {
			cspan.RecordError(err)
		}
		cspan.End()
		if err != nil {
			log.Printf("parter: create pass %q: %v", t.Name, err)
			if isCtxError(err) {
				return
			}
		}

		// 3. Run the retention pass ONLY when Retention>0, under a child span.
		if t.Retention > 0 {
			rctx, rspan := st.Tracer().StartRetention(sctx)
			err := retentionPass(rctx, cfg, t, st)
			if err != nil {
				rspan.RecordError(err)
			}
			rspan.End()
			if err != nil {
				log.Printf("parter: retention pass %q: %v", t.Name, err)
				if isCtxError(err) {
					return
				}
			}
		}
	}
}

// MaintenanceLoop runs the background maintenance loop until ctx is cancelled.
// It is independently callable: it accepts an already-validated Config, does NOT
// re-validate it, and does NOT store into core.Global. When launched under Start,
// core.Global is already set and the existing State (with its tracer and
// counters) is reused; an independent call without Start builds a fresh State so
// counters and the no-op tracer are still available. Each tick runs
// runMaintenanceTick with the fixed TickDeadline on a fixed-epoch schedule from
// the loop's own startup time and per-instance offset.
//
// CONTRACT §5 / scheduler.yass.yaml parter.MaintenanceLoop.
func MaintenanceLoop(ctx context.Context, cfg Config) {
	st := core.Global.Load()
	if st == nil {
		st = newState(cfg)
	}

	processStart := time.Now()
	secs := float64(processStart.Second()) + float64(processStart.Nanosecond())/1e9
	offset := TickOffset(secs, cfg.Instance)

	tickLoop(ctx, processStart, offset, tickInterval, func(c context.Context) {
		runMaintenanceTick(c, cfg, TickDeadline, st)
	})
}

// sleepUntil sleeps until t or until ctx is done. It returns true if it slept
// to completion, and false if ctx was cancelled first.
//
// CONTRACT §5.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		// Still honor cancellation even when the deadline is already past.
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// skipMissed advances n until epoch + n*interval is strictly in the future
// relative to now, returning the next future tick time and the advanced n. It
// is a pure helper that never busy-loops the scheduler.
//
// CONTRACT §5 / scheduler.yass.yaml parter.MaintenanceLoop RETURN.
func skipMissed(epoch time.Time, n int, now time.Time, interval time.Duration) (time.Time, int) {
	next := epoch.Add(time.Duration(n) * interval)
	for !next.After(now) {
		n++
		next = epoch.Add(time.Duration(n) * interval)
	}
	return next, n
}

// tickLoop runs work on a fixed-epoch schedule: epoch = start + offset, then
// next_tick = epoch + N*interval. It skips missed ticks by advancing N past
// any tick that is not in the future, and returns when ctx is cancelled. The
// interval parameter exists so tests can use a small cadence; production passes
// tickInterval.
//
// CONTRACT §5 / scheduler.yass.yaml parter.MaintenanceLoop.
func tickLoop(ctx context.Context, start time.Time, offset, interval time.Duration, work func(context.Context)) {
	epoch := start.Add(offset)
	if !sleepUntil(ctx, epoch) {
		return
	}
	for n := 1; ; n++ {
		work(ctx)
		next, advanced := skipMissed(epoch, n, time.Now(), interval)
		n = advanced
		if !sleepUntil(ctx, next) {
			return
		}
	}
}
