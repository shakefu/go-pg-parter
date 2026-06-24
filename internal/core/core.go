// Package core holds the process-wide shared state for go-pg-parter:
// atomic counters, the gauge snapshot, the tracer holder, and the metrics
// subscriber registry. It is the integration seam between the root parter
// package and the optional otel sub-package.
//
// CONTRACT §2: this package MUST NOT import OpenTelemetry or the parter
// package. It has no import cycles.
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// TableInfo describes one registered table for the shared state.
type TableInfo struct {
	Name         string // schema-qualified parent (Table.Name)
	HasRetention bool   // Retention > 0
}

// TableCounters holds the per-table monotonic counters. All fields are
// atomic so the maintenance goroutine can bump them without a mutex.
type TableCounters struct {
	CreatedTotal      atomic.Int64
	DroppedTotal      atomic.Int64
	CreateErrorsTotal atomic.Int64
	DetachErrorsTotal atomic.Int64
	DropErrorsTotal   atomic.Int64
}

// Counters holds all process counters. The per-table map is built once at
// Start and is read-only thereafter, so reads/writes of the contained atomics
// need no mutex.
type Counters struct {
	perTable            map[string]*TableCounters
	ObserverErrorsTotal atomic.Int64 // no label
}

// NewCounters pre-builds the per-table counter map so increments need no
// mutex. Tables may be nil/empty.
func NewCounters(tables []string) *Counters {
	m := make(map[string]*TableCounters, len(tables))
	for _, name := range tables {
		m[name] = &TableCounters{}
	}
	return &Counters{perTable: m}
}

// Table returns the pre-created counter struct for a known table. It returns
// nil only for an unknown table name.
func (c *Counters) Table(name string) *TableCounters {
	return c.perTable[name]
}

// TableGauges is the per-table gauge snapshot for one observability tick.
type TableGauges struct {
	Pending          int64
	Overdue          int64
	HeadroomSeconds  float64
	Created          int64
	RetentionOverdue int64
	Failed           int64
}

// GaugeSnapshot is an immutable snapshot of all per-table gauges published by
// one observability tick.
type GaugeSnapshot struct {
	PerTable map[string]TableGauges
}

// PartitionAttrs are the attributes attached to a per-partition span.
type PartitionAttrs struct {
	Table, PartitionName string
	RangeStart, RangeEnd time.Time
	Outcome              string // "created"|"detached"|"dropped"|"error" — never "skipped"
}

// Span is a minimal tracing span abstraction.
type Span interface {
	End()
	RecordError(err error)
}

// Tracer abstracts span creation so the root package never imports OTel.
type Tracer interface {
	StartTick(ctx context.Context) (context.Context, Span)
	StartCreate(ctx context.Context) (context.Context, Span)
	StartRetention(ctx context.Context) (context.Context, Span)
	StartObserve(ctx context.Context) (context.Context, Span)
	PartitionSpan(ctx context.Context, a PartitionAttrs) // creates+ends a child span itself
}

// noopSpan is the no-op Span used when OTel is not configured.
type noopSpan struct{}

func (noopSpan) End()                  {}
func (noopSpan) RecordError(err error) {}

// noopTracer is the no-op Tracer used when OTel is not configured.
type noopTracer struct{}

func (noopTracer) StartTick(ctx context.Context) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopTracer) StartCreate(ctx context.Context) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopTracer) StartRetention(ctx context.Context) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopTracer) StartObserve(ctx context.Context) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noopTracer) PartitionSpan(ctx context.Context, a PartitionAttrs) {}

// subscriber is one MetricsChan subscription.
type subscriber struct {
	ch     chan string
	closed bool
}

// State is the process-wide shared state set by Start and read by MetricsChan
// (root package) and ConfigureOtel (otel package) via Global.
type State struct {
	Schema   string
	Tables   []TableInfo
	Counters *Counters

	gauges         atomic.Pointer[GaugeSnapshot]
	tracer         atomic.Pointer[Tracer]
	started        atomic.Bool
	otelConfigured atomic.Bool
	subMu          sync.Mutex
	subscribers    []*subscriber
}

// SetGauges publishes a new gauge snapshot.
func (s *State) SetGauges(g *GaugeSnapshot) {
	s.gauges.Store(g)
}

// Gauges returns the most recent gauge snapshot, or nil before the first
// observe tick.
func (s *State) Gauges() *GaugeSnapshot {
	return s.gauges.Load()
}

// Tracer returns the installed tracer, or a no-op tracer when none is set.
func (s *State) Tracer() Tracer {
	if p := s.tracer.Load(); p != nil {
		return *p
	}
	return noopTracer{}
}

// SetTracer installs the tracer (used by ConfigureOtel).
func (s *State) SetTracer(t Tracer) {
	s.tracer.Store(&t)
}

// SetStarted marks the state as started.
func (s *State) SetStarted() {
	s.started.Store(true)
}

// Started reports whether Start has marked this state as running.
func (s *State) Started() bool {
	return s.started.Load()
}

// MarkOtelConfigured atomically marks OTel as configured. It returns true the
// first time and false on every subsequent call.
func (s *State) MarkOtelConfigured() bool {
	return s.otelConfigured.CompareAndSwap(false, true)
}

// Subscribe returns an unbuffered channel that receives Broadcast payloads.
// On ctx.Done the channel is removed from the registry and closed under the
// list mutex, so Broadcast never sends on a closed channel.
func (s *State) Subscribe(ctx context.Context) <-chan string {
	sub := &subscriber{ch: make(chan string)}
	s.subMu.Lock()
	s.subscribers = append(s.subscribers, sub)
	s.subMu.Unlock()

	go func() {
		<-ctx.Done()
		s.subMu.Lock()
		defer s.subMu.Unlock()
		if !sub.closed {
			sub.closed = true
			close(sub.ch)
		}
		for i, other := range s.subscribers {
			if other == sub {
				s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
				break
			}
		}
	}()

	return sub.ch
}

// Broadcast performs a non-blocking send of payload to every live subscriber.
// It holds the same mutex as the close in Subscribe's reaper, skips closed
// subscribers, and drops the payload for any subscriber that is not currently
// reading.
func (s *State) Broadcast(payload string) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for _, sub := range s.subscribers {
		if sub.closed {
			continue
		}
		select {
		case sub.ch <- payload:
		default:
		}
	}
}

// Global is the process-wide State pointer, set by Start and read by
// MetricsChan and ConfigureOtel.
var Global atomic.Pointer[State]
