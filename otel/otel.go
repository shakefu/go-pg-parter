// Package parterotel is the OpenTelemetry integration surface for
// go-pg-parter. It is the ONLY package permitted to import
// go.opentelemetry.io/*; the root parter package and internal/core stay
// OTel-free so that "ConfigureOtel never called ⇒ no OTel at build time"
// (observability.yass.yaml parter.ConfigureOtel SIDE-EFFECT) holds.
//
// CONTRACT §12.
package parterotel

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/shakefu/go-pg-parter/internal/core"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName is the OTel scope name used for both the meter and the
// tracer. It matches the module import path.
const instrumentationName = "github.com/shakefu/go-pg-parter"

// ConfigureOtel wires the process-wide shared state (core.Global) to the given
// OpenTelemetry MeterProvider and TracerProvider: it installs an OTel-backed
// core.Tracer and registers observable instruments (dotted parter.* names) that
// read the shared counters and gauges on each collection cycle.
//
// It MUST be called after Start. Calling it before Start (core.Global nil) or a
// second time panics — these are the only two panics permitted here
// (CONTRACT §15). Instrument-construction errors are logged and skipped, never
// panicked (they are practically unreachable given distinct names and the
// once-only guard).
//
// observability.yass.yaml parter.ConfigureOtel.
func ConfigureOtel(mp metric.MeterProvider, tp trace.TracerProvider) {
	st := core.Global.Load()
	if st == nil {
		panic("parterotel: ConfigureOtel called before Start: the observability goroutine is not yet running")
	}
	if !st.MarkOtelConfigured() {
		panic("parterotel: ConfigureOtel called more than once: duplicate OTel instrument registration is not allowed")
	}

	st.SetTracer(&otelTracer{tracer: tp.Tracer(instrumentationName)})

	registerInstruments(mp.Meter(instrumentationName), st)
}

// otelTracer adapts an OTel trace.Tracer to the core.Tracer interface so the
// root package can emit spans without importing OTel.
type otelTracer struct {
	tracer trace.Tracer
}

func (t *otelTracer) StartTick(ctx context.Context) (context.Context, core.Span) {
	ctx, span := t.tracer.Start(ctx, "parter.tick")
	return ctx, &otelSpan{span: span}
}

func (t *otelTracer) StartCreate(ctx context.Context) (context.Context, core.Span) {
	ctx, span := t.tracer.Start(ctx, "parter.create")
	return ctx, &otelSpan{span: span}
}

func (t *otelTracer) StartRetention(ctx context.Context) (context.Context, core.Span) {
	ctx, span := t.tracer.Start(ctx, "parter.retention")
	return ctx, &otelSpan{span: span}
}

func (t *otelTracer) StartObserve(ctx context.Context) (context.Context, core.Span) {
	ctx, span := t.tracer.Start(ctx, "parter.observe")
	return ctx, &otelSpan{span: span}
}

// PartitionSpan starts AND ends a child span describing one per-partition
// outcome. The span name "parter.partition" is an implementation choice: the
// spec names the parent spans (parter.tick/create/retention/observe) but is
// silent on the per-partition span name. When the outcome is "error" it records
// a synthesized error (PartitionAttrs carries no error value) and sets the span
// status to Error.
//
// observability.yass.yaml parter.ConfigureOtel SIDE-EFFECT (span attributes
// table/partition_name/range_start/range_end/outcome).
func (t *otelTracer) PartitionSpan(ctx context.Context, a core.PartitionAttrs) {
	_, span := t.tracer.Start(ctx, "parter.partition", trace.WithAttributes(
		attribute.String("table", a.Table),
		attribute.String("partition_name", a.PartitionName),
		attribute.String("range_start", a.RangeStart.UTC().Format(time.RFC3339Nano)),
		attribute.String("range_end", a.RangeEnd.UTC().Format(time.RFC3339Nano)),
		attribute.String("outcome", a.Outcome),
	))
	if a.Outcome == "error" {
		err := errors.New("partition operation failed")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// otelSpan adapts an OTel trace.Span to the core.Span interface.
type otelSpan struct {
	span trace.Span
}

func (s *otelSpan) End() { s.span.End() }

func (s *otelSpan) RecordError(err error) {
	s.span.RecordError(err)
	if err != nil {
		s.span.SetStatus(codes.Error, err.Error())
	}
}

// registerInstruments registers all observable counters and gauges (dotted
// parter.* names) on the meter, reading the shared counters and gauges in each
// instrument's callback. Constructor and registration errors are logged and
// skipped — never panicked (CONTRACT §15).
//
// observability.yass.yaml parter.Metrics RETURN (dotted names, table label,
// observer_errors_total no label).
func registerInstruments(meter metric.Meter, st *core.State) {
	// Per-table observable Int64 counters. Each callback iterates the
	// configured tables and observes that table's counter with a table label.
	type tableCounter struct {
		name string
		desc string
		read func(*core.TableCounters) int64
	}
	tableCounters := []tableCounter{
		{"parter.created_total", "partitions successfully created since process start",
			func(c *core.TableCounters) int64 { return c.CreatedTotal.Load() }},
		{"parter.dropped_total", "partitions successfully dropped",
			func(c *core.TableCounters) int64 { return c.DroppedTotal.Load() }},
		{"parter.create_errors_total", "create attempts that failed including attach/boundary-verification failures",
			func(c *core.TableCounters) int64 { return c.CreateErrorsTotal.Load() }},
		{"parter.detach_errors_total", "DETACH attempts that failed in Step 1",
			func(c *core.TableCounters) int64 { return c.DetachErrorsTotal.Load() }},
		{"parter.drop_errors_total", "DROP TABLE attempts that failed in Step 2",
			func(c *core.TableCounters) int64 { return c.DropErrorsTotal.Load() }},
	}
	for _, tc := range tableCounters {
		read := tc.read
		_, err := meter.Int64ObservableCounter(tc.name,
			metric.WithDescription(tc.desc),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				for _, ti := range st.Tables {
					if c := st.Counters.Table(ti.Name); c != nil {
						o.Observe(read(c), metric.WithAttributes(attribute.String("table", ti.Name)))
					}
				}
				return nil
			}),
		)
		if err != nil {
			log.Printf("parterotel: register %s: %v", tc.name, err)
		}
	}

	// Process-wide observer_errors_total: single observation, NO table label.
	if _, err := meter.Int64ObservableCounter("parter.observer_errors_total",
		metric.WithDescription("observability goroutine query failures"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(st.Counters.ObserverErrorsTotal.Load())
			return nil
		}),
	); err != nil {
		log.Printf("parterotel: register parter.observer_errors_total: %v", err)
	}

	// Per-table Int64 observable gauges, read from the latest GaugeSnapshot.
	type tableGaugeInt struct {
		name string
		desc string
		read func(core.TableGauges) int64
	}
	intGauges := []tableGaugeInt{
		{"parter.pending", "current count of pending rows",
			func(g core.TableGauges) int64 { return g.Pending }},
		{"parter.overdue", "pending rows where range_start <= now()",
			func(g core.TableGauges) int64 { return g.Overdue }},
		{"parter.created", "current count of created rows",
			func(g core.TableGauges) int64 { return g.Created }},
		{"parter.retention_overdue", "created rows past their retention window, 0 for tables with Retention = 0",
			func(g core.TableGauges) int64 { return g.RetentionOverdue }},
		{"parter.failed", "current count of failed rows requiring manual investigation",
			func(g core.TableGauges) int64 { return g.Failed }},
	}
	for _, ig := range intGauges {
		read := ig.read
		_, err := meter.Int64ObservableGauge(ig.name,
			metric.WithDescription(ig.desc),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				snap := st.Gauges()
				if snap == nil {
					return nil
				}
				for _, ti := range st.Tables {
					if g, ok := snap.PerTable[ti.Name]; ok {
						o.Observe(read(g), metric.WithAttributes(attribute.String("table", ti.Name)))
					}
				}
				return nil
			}),
		)
		if err != nil {
			log.Printf("parterotel: register %s: %v", ig.name, err)
		}
	}

	// Float64 observable gauge for headroom seconds.
	if _, err := meter.Float64ObservableGauge("parter.headroom_seconds",
		metric.WithDescription("max(range_end) over created rows minus now() in seconds, clamped at 0; 0 means writes are already hitting DEFAULT"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			snap := st.Gauges()
			if snap == nil {
				return nil
			}
			for _, ti := range st.Tables {
				if g, ok := snap.PerTable[ti.Name]; ok {
					o.Observe(g.HeadroomSeconds, metric.WithAttributes(attribute.String("table", ti.Name)))
				}
			}
			return nil
		}),
	); err != nil {
		log.Printf("parterotel: register parter.headroom_seconds: %v", err)
	}
}
