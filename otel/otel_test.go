package parterotel

import (
	"context"
	"testing"
	"time"

	"github.com/shakefu/go-pg-parter/internal/core"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newState builds a fresh *core.State with one managed table for the unit
// tests. Each test that touches core.Global saves and restores the prior value.
func newState() *core.State {
	return &core.State{
		Schema:   "parter",
		Tables:   []core.TableInfo{{Name: "public.t", HasRetention: true}},
		Counters: core.NewCounters([]string{"public.t"}),
	}
}

// newTracerProvider returns an SDK TracerProvider backed by an in-memory span
// recorder.
func newTracerProvider() (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	return tp, sr
}

// TestConfigureOtel_PanicBeforeStart covers observability.yass.yaml
// parter.ConfigureOtel ERROR: "WHEN ConfigureOtel is called before Start MUST
// panic, because the observability goroutine is not yet running".
func TestConfigureOtel_PanicBeforeStart(t *testing.T) {
	prev := core.Global.Load()
	defer core.Global.Store(prev)

	core.Global.Store(nil)

	tp, _ := newTracerProvider()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("ConfigureOtel did not panic when called before Start")
		}
	}()
	ConfigureOtel(mp, tp)
}

// TestConfigureOtel_PanicOnSecondCall covers observability.yass.yaml
// parter.ConfigureOtel ERROR: "MUST be called at most once" / "WHEN
// ConfigureOtel is called a second time MUST panic".
func TestConfigureOtel_PanicOnSecondCall(t *testing.T) {
	prev := core.Global.Load()
	defer core.Global.Store(prev)

	core.Global.Store(newState())

	tp1, _ := newTracerProvider()
	mp1 := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("first ConfigureOtel panicked: %v", r)
			}
		}()
		ConfigureOtel(mp1, tp1)
	}()

	tp2, _ := newTracerProvider()
	mp2 := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("second ConfigureOtel did not panic")
			}
		}()
		ConfigureOtel(mp2, tp2)
	}()
}

// TestConfigureOtel_TracerSpans covers observability.yass.yaml
// parter.ConfigureOtel SIDE-EFFECT: span names parter.tick/parter.create/
// parter.retention/parter.observe and parter.partition with attributes
// table/partition_name/range_start/range_end/outcome, plus RecordError on the
// error outcome.
func TestConfigureOtel_TracerSpans(t *testing.T) {
	prev := core.Global.Load()
	defer core.Global.Store(prev)

	st := newState()
	core.Global.Store(st)

	tp, sr := newTracerProvider()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	ConfigureOtel(mp, tp)

	tr := st.Tracer()
	ctx := context.Background()

	_, tick := tr.StartTick(ctx)
	tick.End()
	_, create := tr.StartCreate(ctx)
	create.End()
	_, ret := tr.StartRetention(ctx)
	ret.End()
	_, obs := tr.StartObserve(ctx)
	obs.End()

	rs := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	re := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	tr.PartitionSpan(ctx, core.PartitionAttrs{
		Table: "public.t", PartitionName: "public.t_2026_01",
		RangeStart: rs, RangeEnd: re, Outcome: "created",
	})
	tr.PartitionSpan(ctx, core.PartitionAttrs{
		Table: "public.t", PartitionName: "public.t_2026_02",
		RangeStart: rs, RangeEnd: re, Outcome: "error",
	})

	ended := sr.Ended()
	byName := map[string]int{}
	for _, s := range ended {
		byName[s.Name()]++
	}
	for _, want := range []string{"parter.tick", "parter.create", "parter.retention", "parter.observe", "parter.partition"} {
		if byName[want] == 0 {
			t.Errorf("expected an ended span named %q; got names %v", want, byName)
		}
	}
	if byName["parter.partition"] != 2 {
		t.Errorf("expected 2 parter.partition spans, got %d", byName["parter.partition"])
	}

	// Inspect the partition spans: attributes present, and the error-outcome
	// span recorded an error event.
	const (
		kTable      = attribute.Key("table")
		kPartName   = attribute.Key("partition_name")
		kRangeStart = attribute.Key("range_start")
		kRangeEnd   = attribute.Key("range_end")
		kOutcome    = attribute.Key("outcome")
	)
	sawCreatedOutcome := false
	sawErrorWithRecordedError := false
	for _, s := range ended {
		if s.Name() != "parter.partition" {
			continue
		}
		set := attribute.NewSet(s.Attributes()...)
		for _, k := range []attribute.Key{kTable, kPartName, kRangeStart, kRangeEnd, kOutcome} {
			if _, ok := set.Value(k); !ok {
				t.Errorf("partition span missing attribute %q; attrs=%v", k, s.Attributes())
			}
		}
		if v, ok := set.Value(kTable); !ok || v.AsString() != "public.t" {
			t.Errorf("partition span table attribute = %v, want public.t", v.AsString())
		}
		outcome, _ := set.Value(kOutcome)
		switch outcome.AsString() {
		case "created":
			sawCreatedOutcome = true
		case "error":
			hasErrEvent := false
			for _, ev := range s.Events() {
				if ev.Name == "exception" {
					hasErrEvent = true
				}
			}
			if hasErrEvent {
				sawErrorWithRecordedError = true
			}
		}
	}
	if !sawCreatedOutcome {
		t.Error("did not observe a partition span with outcome=created")
	}
	if !sawErrorWithRecordedError {
		t.Error("error-outcome partition span did not record an error event")
	}
}

// TestConfigureOtel_MetricsRegistered covers observability.yass.yaml
// parter.ConfigureOtel RETURN/SIDE-EFFECT and parter.Metrics dual-naming: all
// counters+gauges register under dotted names, counters/gauges carry the table
// label, observer_errors_total carries no label, and observed values match the
// shared state.
func TestConfigureOtel_MetricsRegistered(t *testing.T) {
	prev := core.Global.Load()
	defer core.Global.Store(prev)

	st := newState()
	st.Counters.Table("public.t").CreatedTotal.Add(3)
	st.SetGauges(&core.GaugeSnapshot{PerTable: map[string]core.TableGauges{
		"public.t": {Pending: 2, Overdue: 1, HeadroomSeconds: 42.5, Created: 5, RetentionOverdue: 1, Failed: 0},
	}})
	core.Global.Store(st)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tp, _ := newTracerProvider()
	ConfigureOtel(mp, tp)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}

	type point struct {
		intVal   int64
		floatVal float64
		isFloat  bool
		attrs    attribute.Set
	}
	collected := map[string][]point{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					collected[m.Name] = append(collected[m.Name], point{intVal: dp.Value, attrs: dp.Attributes})
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					collected[m.Name] = append(collected[m.Name], point{intVal: dp.Value, attrs: dp.Attributes})
				}
			case metricdata.Gauge[float64]:
				for _, dp := range d.DataPoints {
					collected[m.Name] = append(collected[m.Name], point{floatVal: dp.Value, isFloat: true, attrs: dp.Attributes})
				}
			}
		}
	}

	// All dotted names must be registered.
	wantNames := []string{
		"parter.created_total", "parter.dropped_total", "parter.create_errors_total",
		"parter.detach_errors_total", "parter.drop_errors_total", "parter.observer_errors_total",
		"parter.pending", "parter.overdue", "parter.headroom_seconds",
		"parter.created", "parter.retention_overdue", "parter.failed",
	}
	for _, n := range wantNames {
		if len(collected[n]) == 0 {
			t.Errorf("metric %q not collected", n)
		}
	}

	tableKey := attribute.Key("table")

	// parter.created_total == 3 with table=public.t.
	if pts := collected["parter.created_total"]; len(pts) != 1 {
		t.Errorf("parter.created_total: want 1 point, got %d", len(pts))
	} else {
		if pts[0].intVal != 3 {
			t.Errorf("parter.created_total value = %d, want 3", pts[0].intVal)
		}
		if v, ok := pts[0].attrs.Value(tableKey); !ok || v.AsString() != "public.t" {
			t.Errorf("parter.created_total table label = %v (ok=%v), want public.t", v.AsString(), ok)
		}
	}

	// parter.observer_errors_total present with NO table attribute.
	if pts := collected["parter.observer_errors_total"]; len(pts) != 1 {
		t.Errorf("parter.observer_errors_total: want 1 point, got %d", len(pts))
	} else if pts[0].attrs.Len() != 0 {
		t.Errorf("parter.observer_errors_total should have no attributes, got %d", pts[0].attrs.Len())
	}

	// parter.pending == 2.
	if pts := collected["parter.pending"]; len(pts) != 1 {
		t.Errorf("parter.pending: want 1 point, got %d", len(pts))
	} else if pts[0].intVal != 2 {
		t.Errorf("parter.pending value = %d, want 2", pts[0].intVal)
	}

	// parter.headroom_seconds == 42.5 (float).
	if pts := collected["parter.headroom_seconds"]; len(pts) != 1 {
		t.Errorf("parter.headroom_seconds: want 1 point, got %d", len(pts))
	} else {
		if !pts[0].isFloat {
			t.Error("parter.headroom_seconds is not a float gauge")
		}
		if pts[0].floatVal != 42.5 {
			t.Errorf("parter.headroom_seconds value = %v, want 42.5", pts[0].floatVal)
		}
	}
}
