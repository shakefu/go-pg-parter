package parter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// observability.yass.yaml parter.ObservabilityGoroutine SIDE-EFFECT /
// CONTRACT §9: retentionCutoff computes the calendar cutoff in Go — Monthly
// AddDate(0,-r,0); Weekly AddDate(0,0,-7*r); Daily AddDate(0,0,-r).
func TestRetentionCutoff(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 30, 0, 0, time.UTC)
	cases := []struct {
		name string
		r    int
		s    RangeStrategy
		want time.Time
	}{
		// Monthly: subtract r calendar months.
		{"monthly_1", 1, Monthly, time.Date(2026, 5, 23, 12, 30, 0, 0, time.UTC)},
		{"monthly_3", 3, Monthly, time.Date(2026, 3, 23, 12, 30, 0, 0, time.UTC)},
		// Weekly: subtract 7*r days.
		{"weekly_1", 1, Weekly, time.Date(2026, 6, 16, 12, 30, 0, 0, time.UTC)},
		{"weekly_2", 2, Weekly, time.Date(2026, 6, 9, 12, 30, 0, 0, time.UTC)},
		// Daily: subtract r days.
		{"daily_1", 1, Daily, time.Date(2026, 6, 22, 12, 30, 0, 0, time.UTC)},
		{"daily_30", 30, Daily, time.Date(2026, 5, 24, 12, 30, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := retentionCutoff(now, tc.r, tc.s)
			if !got.Equal(tc.want) {
				t.Errorf("retentionCutoff(%v, %d, %v) = %v, want %v", now, tc.r, tc.s, got, tc.want)
			}
		})
	}
}

// observability.yass.yaml parter.ObservabilityGoroutine RETURN (WHEN
// maintenance offset < 1s) / CONTRACT §9: obsOffset =
// (maintOffset - 1s + 60s) % 60s. Maintenance offset 0.3s yields 59.3s.
func TestObservabilityOffset(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		// Spec worked example: 0.3s maintenance offset -> 59.3s observability.
		{"sub_second_0.3s", 300 * time.Millisecond, 59300 * time.Millisecond},
		// 1s -> exactly 0 (wraps the full window).
		{"one_second", time.Second, 0},
		// 30s -> 29s.
		{"thirty_seconds", 30 * time.Second, 29 * time.Second},
		// 0 -> 59s (wrap below zero into the 60s window).
		{"zero", 0, 59 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := observabilityOffset(tc.in); got != tc.want {
				t.Errorf("observabilityOffset(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// observability.yass.yaml parter.Metrics RETURN / CONTRACT §9: formatPrometheus
// emits the full underscore-name exposition payload — one # HELP and # TYPE
// line per metric, one labeled sample line per table sorted ascending, EXCEPT
// parter_observer_errors_total which is unlabeled with a single line. Counters
// read from st.Counters atomics; gauges from the snapshot.
func TestFormatPrometheus(t *testing.T) {
	tableA := "public.aaa_events" // no retention
	tableB := "contacts.zzz_events"
	tables := []string{tableA, tableB}

	counters := core.NewCounters(tables)
	// Bump a spread of counters so each metric line has a distinct value.
	counters.Table(tableA).CreatedTotal.Add(5)
	counters.Table(tableA).CreateErrorsTotal.Add(2)
	counters.Table(tableB).DroppedTotal.Add(7)
	counters.Table(tableB).DetachErrorsTotal.Add(3)
	counters.Table(tableB).DropErrorsTotal.Add(1)
	counters.ObserverErrorsTotal.Add(4)

	snap := &core.GaugeSnapshot{PerTable: map[string]core.TableGauges{
		tableA: {Pending: 1, Overdue: 0, HeadroomSeconds: 0, Created: 3, RetentionOverdue: 0, Failed: 0},
		tableB: {Pending: 2, Overdue: 1, HeadroomSeconds: 1.5, Created: 4, RetentionOverdue: 2, Failed: 1},
	}}

	cfg := Config{Tables: []Table{
		{Name: tableA, Strategy: Daily, Lookahead: 1, Retention: 0, Namer: func(time.Time) string { return "" }},
		{Name: tableB, Strategy: Monthly, Lookahead: 1, Retention: 3, Namer: func(time.Time) string { return "" }},
	}}
	st := &core.State{Schema: "parter", Counters: counters}

	out := formatPrometheus(cfg, st, snap)

	// HELP and TYPE lines for every metric.
	mustContain := []string{
		"# HELP parter_created_total partitions successfully created since process start",
		"# TYPE parter_created_total counter",
		"# HELP parter_dropped_total partitions successfully dropped",
		"# TYPE parter_dropped_total counter",
		"# HELP parter_create_errors_total create attempts that failed including attach/boundary-verification failures",
		"# TYPE parter_create_errors_total counter",
		"# HELP parter_detach_errors_total DETACH attempts that failed in Step 1",
		"# TYPE parter_detach_errors_total counter",
		"# HELP parter_drop_errors_total DROP TABLE attempts that failed in Step 2",
		"# TYPE parter_drop_errors_total counter",
		"# HELP parter_observer_errors_total observability goroutine query failures",
		"# TYPE parter_observer_errors_total counter",
		"# HELP parter_pending current count of pending rows",
		"# TYPE parter_pending gauge",
		"# HELP parter_overdue pending rows where range_start <= now()",
		"# TYPE parter_overdue gauge",
		"# HELP parter_headroom_seconds max(range_end) over created rows minus now() in seconds, clamped at 0; 0 means writes are already hitting DEFAULT",
		"# TYPE parter_headroom_seconds gauge",
		"# HELP parter_created current count of created rows",
		"# TYPE parter_created gauge",
		"# HELP parter_retention_overdue created rows past their retention window, 0 for tables with Retention = 0",
		"# TYPE parter_retention_overdue gauge",
		"# HELP parter_failed current count of failed rows requiring manual investigation",
		"# TYPE parter_failed gauge",
	}
	for _, line := range mustContain {
		if !strings.Contains(out, line) {
			t.Errorf("formatPrometheus output missing line:\n%s\n---full output---\n%s", line, out)
		}
	}

	// Labeled counter sample lines with the bumped values.
	labeledSamples := []string{
		`parter_created_total{table="public.aaa_events"} 5`,
		`parter_created_total{table="contacts.zzz_events"} 0`,
		`parter_dropped_total{table="contacts.zzz_events"} 7`,
		`parter_create_errors_total{table="public.aaa_events"} 2`,
		`parter_detach_errors_total{table="contacts.zzz_events"} 3`,
		`parter_drop_errors_total{table="contacts.zzz_events"} 1`,
		// Gauge sample lines.
		`parter_pending{table="public.aaa_events"} 1`,
		`parter_pending{table="contacts.zzz_events"} 2`,
		`parter_overdue{table="contacts.zzz_events"} 1`,
		`parter_created{table="public.aaa_events"} 3`,
		`parter_retention_overdue{table="contacts.zzz_events"} 2`,
		`parter_failed{table="contacts.zzz_events"} 1`,
	}
	for _, line := range labeledSamples {
		if !strings.Contains(out, line) {
			t.Errorf("formatPrometheus output missing sample line:\n%s\n---full output---\n%s", line, out)
		}
	}

	// observer_errors_total is UNLABELED with a single line.
	if !strings.Contains(out, "parter_observer_errors_total 4\n") {
		t.Errorf("formatPrometheus output missing unlabeled observer line `parter_observer_errors_total 4`:\n%s", out)
	}
	if strings.Contains(out, `parter_observer_errors_total{`) {
		t.Errorf("parter_observer_errors_total must NOT carry a {table=...} label:\n%s", out)
	}

	// HeadroomSeconds float formats with strconv 'g' (1.5, and 0 for the zero table).
	if !strings.Contains(out, `parter_headroom_seconds{table="contacts.zzz_events"} 1.5`) {
		t.Errorf("headroom float for tableB should format as 1.5:\n%s", out)
	}
	if !strings.Contains(out, `parter_headroom_seconds{table="public.aaa_events"} 0`) {
		t.Errorf("headroom float for tableA should format as 0:\n%s", out)
	}

	// Within each metric block, table sample lines are sorted ascending
	// (contacts.zzz_events sorts before public.aaa_events).
	idxContacts := strings.Index(out, `parter_pending{table="contacts.zzz_events"}`)
	idxPublic := strings.Index(out, `parter_pending{table="public.aaa_events"}`)
	if idxContacts < 0 || idxPublic < 0 {
		t.Fatalf("expected both parter_pending sample lines present")
	}
	if idxContacts > idxPublic {
		t.Errorf("parter_pending table lines not sorted ascending: contacts at %d, public at %d", idxContacts, idxPublic)
	}
}

// observability.yass.yaml parter.MetricsChan RETURN / SIDE-EFFECT (WHEN ctx is
// cancelled) / CONTRACT §9: with a nil Global, MetricsChan returns an open
// channel that closes when ctx is cancelled, leaving nothing else affected.
func TestMetricsChanNilGlobal(t *testing.T) {
	prev := core.Global.Load()
	core.Global.Store(nil)
	t.Cleanup(func() { core.Global.Store(prev) })

	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{Tables: []Table{{Name: "public.t", Strategy: Daily, Lookahead: 1, Namer: func(time.Time) string { return "" }}}}
	ch := MetricsChan(ctx, cfg)

	// The channel is open: a non-blocking receive sees no value and no close.
	select {
	case _, ok := <-ch:
		t.Fatalf("channel should be open and empty, got ok=%v", ok)
	default:
	}

	cancel()

	// After cancel, the channel closes.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("channel should be closed after ctx cancel, received a value with ok=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("channel did not close within 2s of ctx cancel")
	}
}

// observability.yass.yaml parter.MetricsChan RETURN / SIDE-EFFECT / CONTRACT §9:
// with a non-nil Global, MetricsChan subscribes to the State's broadcast and
// receives a broadcast payload, then closes on ctx cancel.
func TestMetricsChanNonNilGlobal(t *testing.T) {
	prev := core.Global.Load()
	st := &core.State{Schema: "parter", Counters: core.NewCounters(nil)}
	core.Global.Store(st)
	t.Cleanup(func() { core.Global.Store(prev) })

	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{Tables: []Table{{Name: "public.t", Strategy: Daily, Lookahead: 1, Namer: func(time.Time) string { return "" }}}}
	ch := MetricsChan(ctx, cfg)

	got := make(chan string, 1)
	go func() {
		select {
		case v := <-ch:
			got <- v
		case <-time.After(2 * time.Second):
			got <- "<timeout>"
		}
	}()

	// Give the receiver a moment to block on the channel, then broadcast.
	// Broadcast is non-blocking; retry briefly so the subscriber is registered
	// and reading at the instant of the send.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st.Broadcast("payload")
		select {
		case v := <-got:
			if v != "payload" {
				t.Fatalf("received %q, want %q", v, "payload")
			}
			goto received
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("did not receive broadcast payload within 2s")

received:
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("channel should be closed after ctx cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("channel did not close within 2s of ctx cancel")
	}
}
