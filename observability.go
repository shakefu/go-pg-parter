package parter

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shakefu/go-pg-parter/internal/core"
)

// retentionCutoff computes, in Go, the calendar instant before which a 'created'
// partition is past its retention window. It is the wall-clock analogue of the
// SQL "now() - interval '<r> <unit>'" used by the retention pass, but evaluated
// here in the observability goroutine so the retention_overdue gauge does not
// depend on the database for the cutoff. Monthly subtracts r calendar months;
// Weekly subtracts 7*r days; Daily subtracts r days.
//
// This helper lives in observability.go (NOT retention.go, which uses the SQL
// interval string form). CONTRACT §9 / observability.yass.yaml
// parter.ObservabilityGoroutine SIDE-EFFECT.
func retentionCutoff(now time.Time, r int, s RangeStrategy) time.Time {
	switch s {
	case Weekly:
		return now.AddDate(0, 0, -7*r)
	case Daily:
		return now.AddDate(0, 0, -r)
	default: // Monthly
		return now.AddDate(0, -r, 0)
	}
}

// observabilityOffset derives the observability goroutine's tick offset from the
// maintenance goroutine's offset, staggering the two so their ticks do not begin
// together: it reuses the maintenance offset minus one second, wrapping
// modularly within the 60-second tick window (e.g. maintenance offset 0.3s
// yields observability offset 59.3s; 1s yields 0).
//
// CONTRACT §9 / observability.yass.yaml parter.ObservabilityGoroutine RETURN.
func observabilityOffset(maintOffset time.Duration) time.Duration {
	return (maintOffset - time.Second + tickInterval) % tickInterval
}

// computeGauges runs the read-only observability snapshot for one tick. It runs
// a single SELECT over managed_partitions — its own implicit transaction — that
// neither takes nor waits on the FOR UPDATE row locks the maintenance pass
// holds. It returns a GaugeSnapshot with an entry for EVERY table in cfg.Tables
// (a table with no rows still gets a zero-valued entry). Rows whose table_name
// is not in cfg.Tables are ignored defensively.
//
// Per table it computes: Pending (status='pending'); Overdue (pending AND
// range_start <= now()); Created (status='created'); Failed (status='failed');
// RetentionOverdue (created rows past the retention cutoff, 0 when Retention=0);
// HeadroomSeconds (max(range_end) over created rows minus now(), clamped at 0,
// or 0 when there are no created rows). Timestamps are compared in UTC.
//
// On any query/scan error it returns the error; the goroutine caller increments
// parter.observer_errors_total. CONTRACT §9 / observability.yass.yaml
// parter.ObservabilityGoroutine SIDE-EFFECT/ERROR.
func computeGauges(ctx context.Context, cfg Config) (*core.GaugeSnapshot, error) {
	schema := cfg.schema()

	rows, err := cfg.Pool.Query(ctx, fmt.Sprintf(
		"SELECT table_name, status, range_start, range_end FROM %s.managed_partitions", schema))
	if err != nil {
		return nil, fmt.Errorf("parter: computeGauges: query managed_partitions: %w", err)
	}
	defer rows.Close()

	now := time.Now().UTC()

	// Index the configured tables so we can read each table's Strategy/Retention
	// and seed a zero-valued snapshot entry for every table up front.
	byName := make(map[string]Table, len(cfg.Tables))
	per := make(map[string]core.TableGauges, len(cfg.Tables))
	// headroomEnd tracks the max(range_end) over created rows per table; absent
	// from the map means no created row has been seen yet.
	headroomEnd := make(map[string]time.Time, len(cfg.Tables))
	for _, t := range cfg.Tables {
		byName[t.Name] = t
		per[t.Name] = core.TableGauges{}
	}

	for rows.Next() {
		var (
			tableName  string
			status     string
			rangeStart time.Time
			rangeEnd   time.Time
		)
		if err := rows.Scan(&tableName, &status, &rangeStart, &rangeEnd); err != nil {
			return nil, fmt.Errorf("parter: computeGauges: scan row: %w", err)
		}

		t, known := byName[tableName]
		if !known {
			// Defensive: ignore rows for tables not in the current Config.
			continue
		}
		rangeStart = rangeStart.UTC()
		rangeEnd = rangeEnd.UTC()

		g := per[tableName]
		switch status {
		case "pending":
			g.Pending++
			if !rangeStart.After(now) { // range_start <= now()
				g.Overdue++
			}
		case "created":
			g.Created++
			if t.Retention > 0 && rangeEnd.Before(retentionCutoff(now, t.Retention, t.Strategy)) {
				g.RetentionOverdue++
			}
			if cur, ok := headroomEnd[tableName]; !ok || rangeEnd.After(cur) {
				headroomEnd[tableName] = rangeEnd
			}
		case "failed":
			g.Failed++
		}
		per[tableName] = g
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("parter: computeGauges: iterate rows: %w", err)
	}

	// Resolve headroom from the per-table max(range_end) over created rows.
	for name, maxEnd := range headroomEnd {
		g := per[name]
		hs := maxEnd.Sub(now).Seconds()
		if hs < 0 {
			hs = 0
		}
		g.HeadroomSeconds = hs
		per[name] = g
	}

	return &core.GaugeSnapshot{PerTable: per}, nil
}

// promMetric describes one metric block in the Prometheus exposition payload.
type promMetric struct {
	name     string
	typ      string // "counter" | "gauge"
	help     string
	labeled  bool                            // true for per-table metrics
	counter  func(*core.TableCounters) int64 // per-table counter accessor (labeled counters)
	noLabel  func(*core.Counters) int64      // process-wide counter accessor (unlabeled)
	gaugeInt func(core.TableGauges) (int64, bool)
	gaugeF   func(core.TableGauges) (float64, bool)
}

// promMetrics is the FIXED metric inventory and emission order for the
// Prometheus exposition payload. CONTRACT §9 / observability.yass.yaml
// parter.Metrics RETURN. The HELP strings are taken verbatim from the spec
// descriptions.
var promMetrics = []promMetric{
	{
		name: "parter_created_total", typ: "counter", labeled: true,
		help:    "partitions successfully created since process start",
		counter: func(c *core.TableCounters) int64 { return c.CreatedTotal.Load() },
	},
	{
		name: "parter_dropped_total", typ: "counter", labeled: true,
		help:    "partitions successfully dropped",
		counter: func(c *core.TableCounters) int64 { return c.DroppedTotal.Load() },
	},
	{
		name: "parter_create_errors_total", typ: "counter", labeled: true,
		help:    "create attempts that failed including attach/boundary-verification failures",
		counter: func(c *core.TableCounters) int64 { return c.CreateErrorsTotal.Load() },
	},
	{
		name: "parter_detach_errors_total", typ: "counter", labeled: true,
		help:    "DETACH attempts that failed in Step 1",
		counter: func(c *core.TableCounters) int64 { return c.DetachErrorsTotal.Load() },
	},
	{
		name: "parter_drop_errors_total", typ: "counter", labeled: true,
		help:    "DROP TABLE attempts that failed in Step 2",
		counter: func(c *core.TableCounters) int64 { return c.DropErrorsTotal.Load() },
	},
	{
		name: "parter_observer_errors_total", typ: "counter", labeled: false,
		help:    "observability goroutine query failures",
		noLabel: func(c *core.Counters) int64 { return c.ObserverErrorsTotal.Load() },
	},
	{
		name: "parter_pending", typ: "gauge", labeled: true,
		help:     "current count of pending rows",
		gaugeInt: func(g core.TableGauges) (int64, bool) { return g.Pending, true },
	},
	{
		name: "parter_overdue", typ: "gauge", labeled: true,
		help:     "pending rows where range_start <= now()",
		gaugeInt: func(g core.TableGauges) (int64, bool) { return g.Overdue, true },
	},
	{
		name: "parter_headroom_seconds", typ: "gauge", labeled: true,
		help:   "max(range_end) over created rows minus now() in seconds, clamped at 0; 0 means writes are already hitting DEFAULT",
		gaugeF: func(g core.TableGauges) (float64, bool) { return g.HeadroomSeconds, true },
	},
	{
		name: "parter_created", typ: "gauge", labeled: true,
		help:     "current count of created rows",
		gaugeInt: func(g core.TableGauges) (int64, bool) { return g.Created, true },
	},
	{
		name: "parter_retention_overdue", typ: "gauge", labeled: true,
		help:     "created rows past their retention window, 0 for tables with Retention = 0",
		gaugeInt: func(g core.TableGauges) (int64, bool) { return g.RetentionOverdue, true },
	},
	{
		name: "parter_failed", typ: "gauge", labeled: true,
		help:     "current count of failed rows requiring manual investigation",
		gaugeInt: func(g core.TableGauges) (int64, bool) { return g.Failed, true },
	},
}

// escapeLabelValue escapes a Prometheus label value per the text exposition
// rules: backslash -> "\\", double-quote -> "\"", newline -> "\n". Table names
// will not contain these in practice, but escaping keeps the payload well-formed
// regardless.
func escapeLabelValue(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

// formatPrometheus renders the complete Prometheus text exposition payload using
// the underscore parter_* names. For each metric it emits a "# HELP" line, a
// "# TYPE" line, then one sample line per table (table names sorted ascending
// for determinism) — except parter_observer_errors_total, which has no label and
// a single line. Counters are read live from st.Counters; gauges from snap.
// Every cfg.Tables entry gets a sample line even when the value is 0.
//
// CONTRACT §9 / observability.yass.yaml parter.Metrics RETURN.
func formatPrometheus(cfg Config, st *core.State, snap *core.GaugeSnapshot) string {
	// Sorted table names for deterministic output.
	names := make([]string, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		names = append(names, t.Name)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, m := range promMetrics {
		fmt.Fprintf(&b, "# HELP %s %s\n", m.name, m.help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", m.name, m.typ)

		if !m.labeled {
			// Unlabeled process-wide counter (observer_errors_total): single line.
			fmt.Fprintf(&b, "%s %s\n", m.name, strconv.FormatInt(m.noLabel(st.Counters), 10))
			continue
		}

		for _, name := range names {
			label := escapeLabelValue(name)
			switch {
			case m.counter != nil:
				var v int64
				if tc := st.Counters.Table(name); tc != nil {
					v = m.counter(tc)
				}
				fmt.Fprintf(&b, "%s{table=\"%s\"} %s\n", m.name, label, strconv.FormatInt(v, 10))
			case m.gaugeInt != nil:
				v, _ := m.gaugeInt(snap.PerTable[name])
				fmt.Fprintf(&b, "%s{table=\"%s\"} %s\n", m.name, label, strconv.FormatInt(v, 10))
			case m.gaugeF != nil:
				v, _ := m.gaugeF(snap.PerTable[name])
				fmt.Fprintf(&b, "%s{table=\"%s\"} %s\n", m.name, label, strconv.FormatFloat(v, 'g', -1, 64))
			}
		}
	}
	return b.String()
}

// observabilityGoroutine runs the read-only observability tick loop until ctx is
// cancelled. It is started by Start unconditionally — even if neither
// MetricsChan nor ConfigureOtel is ever called — and reuses the maintenance
// offset minus one second (wrapped within the 60s window) so the two goroutines'
// ticks do not begin together.
//
// Each tick is wrapped in a panic-proof closure (the same defer/recover-log-and-
// resume pattern the maintenance goroutine uses): a panic logs and returns so
// the next tick resumes rather than killing the goroutine. Within a tick it
// opens the parter.observe span, computes the gauges, and on success publishes
// them via SetGauges then broadcasts the Prometheus payload to subscribers. On a
// query error it increments parter.observer_errors_total, records the error on
// the span, and resumes on the next tick.
//
// CONTRACT §9 / observability.yass.yaml parter.ObservabilityGoroutine.
func observabilityGoroutine(ctx context.Context, cfg Config, processStart time.Time, maintOffset time.Duration, st *core.State) {
	obsOffset := observabilityOffset(maintOffset)

	work := func(tickCtx context.Context) {
		// Panic-proof: a panic in a tick logs and resumes on the next tick
		// rather than killing the goroutine.
		defer func() {
			if r := recover(); r != nil {
				log.Printf("parter: observability tick panic recovered: %v", r)
			}
		}()

		ctx2, span := st.Tracer().StartObserve(tickCtx)
		defer span.End()

		snap, err := computeGauges(ctx2, cfg)
		if err != nil {
			st.Counters.ObserverErrorsTotal.Add(1)
			span.RecordError(err)
			return
		}

		st.SetGauges(snap)
		payload := formatPrometheus(cfg, st, snap)
		st.Broadcast(payload)
	}

	tickLoop(ctx, processStart, obsOffset, tickInterval, work)
}

// MetricsChan returns a channel that emits the full Prometheus text exposition
// payload (underscore parter_* names, with # HELP/# TYPE/sample lines) once per
// observability tick, suitable for concatenation into an existing /metrics
// response. The channel is unbuffered; ticks the caller is not reading at are
// dropped. When ctx is cancelled the channel is closed.
//
// When the shared State has been installed by Start, this subscribes to its
// broadcast registry. Before Start (Global nil) it returns a channel that simply
// closes on ctx done, so the caller can wire /metrics unconditionally. This
// function MUST NOT import net/http and MUST NOT start or own the observability
// goroutine, which Start owns. cfg is accepted per the spec signature even
// though only Global is consulted.
//
// CONTRACT §9 / observability.yass.yaml parter.MetricsChan.
func MetricsChan(ctx context.Context, cfg Config) <-chan string {
	_ = cfg // cfg is part of the spec signature; only Global is consulted.
	st := core.Global.Load()
	if st != nil {
		return st.Subscribe(ctx)
	}
	ch := make(chan string)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}
