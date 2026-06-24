package core

import (
	"context"
	"testing"
	"time"
)

// CONTRACT §2: Counters/TableCounters — NewCounters pre-builds the per-table
// map; Table(name) returns the pre-created struct (never nil for known tables);
// increments need no mutex.
func TestCountersIncrementAndRead(t *testing.T) {
	c := NewCounters([]string{"a.t1", "b.t2"})

	t1 := c.Table("a.t1")
	if t1 == nil {
		t.Fatal("Table returned nil for a known table")
	}
	t2 := c.Table("b.t2")
	if t2 == nil {
		t.Fatal("Table returned nil for a known table")
	}

	// Same pointer returned each call (read-only map after build).
	if c.Table("a.t1") != t1 {
		t.Fatal("Table did not return a stable pointer")
	}

	t1.CreatedTotal.Add(3)
	t1.DroppedTotal.Add(1)
	t1.CreateErrorsTotal.Add(2)
	t1.DetachErrorsTotal.Add(4)
	t1.DropErrorsTotal.Add(5)
	c.ObserverErrorsTotal.Add(7)

	if got := t1.CreatedTotal.Load(); got != 3 {
		t.Errorf("CreatedTotal = %d, want 3", got)
	}
	if got := t1.DroppedTotal.Load(); got != 1 {
		t.Errorf("DroppedTotal = %d, want 1", got)
	}
	if got := t1.CreateErrorsTotal.Load(); got != 2 {
		t.Errorf("CreateErrorsTotal = %d, want 2", got)
	}
	if got := t1.DetachErrorsTotal.Load(); got != 4 {
		t.Errorf("DetachErrorsTotal = %d, want 4", got)
	}
	if got := t1.DropErrorsTotal.Load(); got != 5 {
		t.Errorf("DropErrorsTotal = %d, want 5", got)
	}
	if got := c.ObserverErrorsTotal.Load(); got != 7 {
		t.Errorf("ObserverErrorsTotal = %d, want 7", got)
	}
	// Independent tables do not share counters.
	if got := t2.CreatedTotal.Load(); got != 0 {
		t.Errorf("t2.CreatedTotal = %d, want 0", got)
	}
}

// CONTRACT §2: Gauges() may be nil before first observe tick; SetGauges/Gauges
// round-trip a snapshot.
func TestGaugeSnapshotSetGet(t *testing.T) {
	st := &State{Schema: "parter", Counters: NewCounters(nil)}

	if g := st.Gauges(); g != nil {
		t.Fatalf("Gauges() = %v before first set, want nil", g)
	}

	snap := &GaugeSnapshot{PerTable: map[string]TableGauges{
		"a.t1": {Pending: 2, Overdue: 1, HeadroomSeconds: 12.5, Created: 3, RetentionOverdue: 0, Failed: 1},
	}}
	st.SetGauges(snap)

	got := st.Gauges()
	if got == nil {
		t.Fatal("Gauges() = nil after SetGauges")
	}
	if got != snap {
		t.Fatal("Gauges() did not return the stored snapshot pointer")
	}
	tg := got.PerTable["a.t1"]
	if tg.Pending != 2 || tg.Overdue != 1 || tg.HeadroomSeconds != 12.5 || tg.Created != 3 || tg.Failed != 1 {
		t.Errorf("snapshot round-trip mismatch: %+v", tg)
	}
}

// CONTRACT §2: Tracer() returns a noop when none set; SetTracer installs one.
func TestTracerNoopThenSet(t *testing.T) {
	st := &State{Counters: NewCounters(nil)}

	tr := st.Tracer()
	if tr == nil {
		t.Fatal("Tracer() returned nil; expected noop")
	}
	// noop must not panic and must produce a usable span.
	ctx, span := tr.StartTick(context.Background())
	if ctx == nil {
		t.Fatal("noop StartTick returned nil context")
	}
	span.RecordError(nil)
	span.End()
	tr.PartitionSpan(context.Background(), PartitionAttrs{Table: "a", Outcome: "created"})

	custom := &recordingTracer{}
	st.SetTracer(custom)
	if st.Tracer() != custom {
		t.Fatal("SetTracer did not install the tracer")
	}
}

// CONTRACT §2: MarkOtelConfigured is a CAS — returns true once, false after.
func TestMarkOtelConfiguredOnce(t *testing.T) {
	st := &State{Counters: NewCounters(nil)}
	if !st.MarkOtelConfigured() {
		t.Fatal("first MarkOtelConfigured = false, want true")
	}
	if st.MarkOtelConfigured() {
		t.Fatal("second MarkOtelConfigured = true, want false")
	}
	if st.MarkOtelConfigured() {
		t.Fatal("third MarkOtelConfigured = true, want false")
	}
}

// CONTRACT §2: Broadcast delivers to a reading subscriber.
func TestBroadcastDeliversToReader(t *testing.T) {
	st := &State{Counters: NewCounters(nil)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := st.Subscribe(ctx)

	done := make(chan string, 1)
	go func() {
		done <- <-ch
	}()
	// Give the reader a moment to block on the channel.
	time.Sleep(20 * time.Millisecond)
	st.Broadcast("payload-1")

	select {
	case got := <-done:
		if got != "payload-1" {
			t.Errorf("received %q, want %q", got, "payload-1")
		}
	case <-time.After(time.Second):
		t.Fatal("Broadcast did not deliver to a reading subscriber")
	}
}

// CONTRACT §2: Broadcast drops the payload when the subscriber is not reading
// (non-blocking select default).
func TestBroadcastDropsWhenNotReading(t *testing.T) {
	st := &State{Counters: NewCounters(nil)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = st.Subscribe(ctx) // never read

	done := make(chan struct{})
	go func() {
		// These must not block even though nobody is reading.
		st.Broadcast("a")
		st.Broadcast("b")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Broadcast blocked when subscriber was not reading")
	}
}

// CONTRACT §2: Subscribe channel closes promptly after ctx cancel (reaped under
// the list mutex).
func TestSubscribeClosesOnCancel(t *testing.T) {
	st := &State{Counters: NewCounters(nil)}
	ctx, cancel := context.WithCancel(context.Background())

	ch := st.Subscribe(ctx)
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe channel did not close promptly after ctx cancel")
	}

	// After close+reap, a subsequent Broadcast must not panic (never send on
	// a closed channel).
	st.Broadcast("after-close")
}

// recordingTracer is a minimal Tracer used to verify SetTracer/Tracer wiring.
type recordingTracer struct{}

func (recordingTracer) StartTick(ctx context.Context) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (recordingTracer) StartCreate(ctx context.Context) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (recordingTracer) StartRetention(ctx context.Context) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (recordingTracer) StartObserve(ctx context.Context) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (recordingTracer) PartitionSpan(ctx context.Context, a PartitionAttrs) {}
