package parter

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

// CONTRACT §5 (newState) / scheduler.yass.yaml parter.MaintenanceLoop INPUT
// (the validated Config): newState builds the shared core.State from a Config —
// Schema = cfg.schema(), one TableInfo per Config.Tables entry in order with
// HasRetention set iff Retention>0, and a per-table Counters map keyed by table
// name (nil for an unknown name).
func TestNewState(t *testing.T) {
	withRet := func(s time.Time) string { return "p" }
	noRet := func(s time.Time) string { return "p" }
	cfg := Config{
		Pool:   nil, // newState does not touch the pool.
		Schema: "myschema",
		Tables: []Table{
			{Name: "a.events", Strategy: Monthly, Lookahead: 1, Retention: 3, Namer: withRet},
			{Name: "b.events", Strategy: Daily, Lookahead: 1, Retention: 0, Namer: noRet},
		},
	}

	st := newState(cfg)

	if st.Schema != cfg.schema() {
		t.Errorf("Schema = %q, want %q", st.Schema, cfg.schema())
	}
	if len(st.Tables) != 2 {
		t.Fatalf("len(Tables) = %d, want 2", len(st.Tables))
	}
	// Order preserved.
	if st.Tables[0].Name != "a.events" || st.Tables[1].Name != "b.events" {
		t.Errorf("table order not preserved: %q, %q", st.Tables[0].Name, st.Tables[1].Name)
	}
	// HasRetention maps correctly (Retention>0).
	if !st.Tables[0].HasRetention {
		t.Errorf("Tables[0].HasRetention = false, want true (Retention=3)")
	}
	if st.Tables[1].HasRetention {
		t.Errorf("Tables[1].HasRetention = true, want false (Retention=0)")
	}
	// Counters present for each configured table.
	if st.Counters.Table("a.events") == nil {
		t.Errorf("Counters.Table(%q) is nil, want non-nil", "a.events")
	}
	if st.Counters.Table("b.events") == nil {
		t.Errorf("Counters.Table(%q) is nil, want non-nil", "b.events")
	}
	// nil for an unknown name.
	if st.Counters.Table("unknown.table") != nil {
		t.Errorf("Counters.Table(unknown) is non-nil, want nil")
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop INVARIANT: TickDeadline < tickInterval.
func TestTickDeadlineLessThanInterval(t *testing.T) {
	if !(TickDeadline < tickInterval) {
		t.Fatalf("TickDeadline (%v) must be < tickInterval (%v)", TickDeadline, tickInterval)
	}
	if tickInterval != 60*time.Second {
		t.Errorf("tickInterval = %v, want 60s", tickInterval)
	}
	if TickDeadline != 55*time.Second {
		t.Errorf("TickDeadline = %v, want 55s", TickDeadline)
	}
}

// scheduler.yass.yaml parter.TickOffset RETURN: offset = (s*23) mod 60.
func TestTickOffsetFormula(t *testing.T) {
	cases := []struct {
		s float64
	}{
		{0}, {1}, {2}, {10}, {30}, {47.891234}, {59.5},
	}
	for _, c := range cases {
		want := math.Mod(c.s*23, 60)
		wantMs := int64(math.Floor(want*1000+0.5)) % 60000
		got := TickOffset(c.s, nil)
		if got != time.Duration(wantMs)*time.Millisecond {
			t.Errorf("TickOffset(%v) = %v, want %v", c.s, got, time.Duration(wantMs)*time.Millisecond)
		}
	}
}

// scheduler.yass.yaml parter.TickOffset RETURN: the 23-multiplier property —
// consecutive integer seconds map to offsets 23 seconds apart (mod 60).
func TestTickOffset23SecondsApart(t *testing.T) {
	for s := 0; s < 60; s++ {
		a := TickOffset(float64(s), nil)
		b := TickOffset(float64(s+1), nil)
		// b - a should be +23s mod 60s.
		diff := (b - a + tickInterval) % tickInterval
		if diff != 23*time.Second {
			t.Errorf("offset(%d)=%v offset(%d)=%v diff=%v, want 23s", s, a, s+1, b, diff)
		}
	}
}

// scheduler.yass.yaml parter.TickOffset INVARIANT: integer seconds cycle
// through all 60 positions before repeating (23 coprime to 60).
func TestTickOffsetCyclesAllPositions(t *testing.T) {
	seen := make(map[time.Duration]bool, 60)
	for s := 0; s < 60; s++ {
		seen[TickOffset(float64(s), nil)] = true
	}
	if len(seen) != 60 {
		t.Errorf("integer seconds produced %d distinct offsets, want 60", len(seen))
	}
}

// scheduler.yass.yaml parter.TickOffset WHEN Instance non-nil:
// offset = (s*23 + inst*23) mod 60.
func TestTickOffsetInstanceTerm(t *testing.T) {
	s := 10.0
	for _, inst := range []int{0, 1, 2, 5} {
		want := math.Mod(s*23+float64(inst)*23, 60)
		wantMs := int64(math.Floor(want*1000+0.5)) % 60000
		i := inst
		got := TickOffset(s, &i)
		if got != time.Duration(wantMs)*time.Millisecond {
			t.Errorf("TickOffset(%v, inst=%d) = %v, want %v", s, inst, got, time.Duration(wantMs)*time.Millisecond)
		}
	}
}

// scheduler.yass.yaml parter.TickOffset INVARIANT: instance spreads only mod 60
// — ordinals differing by a multiple of 60 contribute no extra separation.
func TestTickOffsetInstanceModulo(t *testing.T) {
	s := 3.25
	base := 1
	plus60 := base + 60
	a := TickOffset(s, &base)
	b := TickOffset(s, &plus60)
	if a != b {
		t.Errorf("instance %d and %d gave different offsets %v vs %v", base, plus60, a, b)
	}
}

// scheduler.yass.yaml parter.TickOffset RETURN: round to nearest ms, ties up.
func TestTickOffsetRoundTiesUp(t *testing.T) {
	// Choose s so that off*1000 lands exactly on a .5 boundary.
	// off = s*23 mod 60. Pick off = 0.0005s => 0.5ms => ties up to 1ms.
	// s*23 = 0.0005 => s = 0.0005/23.
	s := 0.0005 / 23.0
	got := TickOffset(s, nil)
	if got != 1*time.Millisecond {
		t.Errorf("ties-up rounding: TickOffset = %v, want 1ms", got)
	}
}

// scheduler.yass.yaml parter.TickOffset RETURN: wrap when rounding hits 60000ms.
func TestTickOffsetWrapAt60000ms(t *testing.T) {
	// off must round up to exactly 60000ms, which then wraps to 0.
	// Pick off = 59.9996s => off*1000+0.5 = 60000.1 => floor 60000 => wrap 0.
	// s*23 mod 60 = 59.9996 => choose s directly in [0,60/23) range:
	s := 59.9996 / 23.0
	if math.Mod(s*23, 60) < 59.999 {
		t.Fatalf("test setup wrong: off=%v", math.Mod(s*23, 60))
	}
	got := TickOffset(s, nil)
	if got != 0 {
		t.Errorf("wrap at 60000ms: TickOffset = %v, want 0", got)
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop RETURN: skipMissed advances past
// missed ticks to the next future tick without busy-looping.
func TestSkipMissed(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	interval := time.Second

	// now is 5.5s after epoch; n starts at 1. Ticks at 1,2,3,4,5 are missed;
	// next future tick is at epoch+6s with n=6.
	now := epoch.Add(5500 * time.Millisecond)
	next, n := skipMissed(epoch, 1, now, interval)
	if n != 6 {
		t.Errorf("skipMissed n = %d, want 6", n)
	}
	if !next.Equal(epoch.Add(6 * interval)) {
		t.Errorf("skipMissed next = %v, want %v", next, epoch.Add(6*interval))
	}
	if !next.After(now) {
		t.Error("skipMissed returned a non-future tick")
	}

	// When the current n is already in the future, n is unchanged.
	now2 := epoch.Add(500 * time.Millisecond)
	next2, n2 := skipMissed(epoch, 1, now2, interval)
	if n2 != 1 || !next2.Equal(epoch.Add(interval)) {
		t.Errorf("skipMissed (future) n=%d next=%v, want 1 and %v", n2, next2, epoch.Add(interval))
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop: tickLoop fires work on cadence.
func TestTickLoopFiresOnCadence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var count atomic.Int64
	interval := 20 * time.Millisecond

	done := make(chan struct{})
	go func() {
		tickLoop(ctx, time.Now(), 0, interval, func(context.Context) {
			count.Add(1)
		})
		close(done)
	}()

	// Allow several intervals to pass.
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	got := count.Load()
	if got < 3 {
		t.Errorf("tickLoop fired %d times in ~150ms at 20ms cadence, want >= 3", got)
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop RETURN: tickLoop returns promptly
// when ctx is cancelled.
func TestTickLoopReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		tickLoop(ctx, time.Now(), 50*time.Millisecond, 20*time.Millisecond, func(context.Context) {})
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tickLoop did not return promptly after ctx cancel")
	}
}

// scheduler.yass.yaml parter.MaintenanceLoop SIDE-EFFECT: tickLoop skips a
// missed tick when work blocks longer than the interval (fixed-epoch schedule,
// not from-finish).
func TestTickLoopSkipsMissedTickWhenWorkBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interval := 20 * time.Millisecond
	var ticks atomic.Int64

	done := make(chan struct{})
	go func() {
		tickLoop(ctx, time.Now(), 0, interval, func(context.Context) {
			n := ticks.Add(1)
			if n == 1 {
				// Block well past one interval so at least one tick is missed.
				time.Sleep(3 * interval)
			}
		})
		close(done)
	}()

	// Run for a bounded window then stop.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	got := ticks.Load()
	// Without skipping, a 60ms block would queue up catch-up ticks and we'd
	// see more fires. With skipping, over ~200ms at 20ms cadence minus the
	// 60ms block we expect a modest count; the key assertion is that the loop
	// kept running (>1) and did not busy-loop into a huge count.
	if got < 2 {
		t.Errorf("tickLoop fired %d times, want >= 2 (kept ticking after a blocked tick)", got)
	}
	if got > 12 {
		t.Errorf("tickLoop fired %d times, suspiciously high — missed ticks not skipped?", got)
	}
}
