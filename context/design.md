# Go Partition Maintenance Library — Design Document

**Working library name:** TBD (the document uses the placeholder `partcycle` for the metadata schema; the actual schema name is a configuration parameter).

## Problem

PostgreSQL RANGE-partitioned tables require child partitions to exist before rows for that range arrive. Unlike HASH partitions (declared once, static forever), RANGE partitions must be created on an ongoing basis as time advances, and expired partitions must be dropped for retention. Without automation, inserts either error (no covering partition) or land in a DEFAULT partition that defeats the retention benefit.

## Solution

A Go micro-library that maintains RANGE partitions via a metadata table and a background goroutine. Multiple service replicas coordinate through `FOR UPDATE SKIP LOCKED` — no leader election, no external scheduler, no PostgreSQL extensions.

## Metadata Table

The metadata table lives in a configurable schema (default: `partcycle`). The library creates the schema and table via `EnsureReady` if they do not exist.

```sql
CREATE SCHEMA IF NOT EXISTS <configured_schema>;

CREATE TABLE IF NOT EXISTS <configured_schema>.managed_partitions (
    table_name      TEXT        NOT NULL,
    partition_name  TEXT        NOT NULL,
    range_start     TIMESTAMPTZ NOT NULL,
    range_end       TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending',  -- 'pending' | 'created'
    created_at      TIMESTAMPTZ,
    PRIMARY KEY (table_name, partition_name)
);
```

Each row represents a partition that either needs to be created (`pending`) or has been created (`created`). Completing a `pending` row inserts the next period's row in the same transaction, forming a self-sustaining chain.

## Tick Offset Scheme

Each service instance computes a deterministic offset into the 60-second tick window based on its goroutine startup time:

```
offset = (startup_seconds_with_fraction * 23) mod 60
```

Where `startup_seconds_with_fraction` is the seconds-and-microseconds component of the wall clock at goroutine launch (e.g. `47.891234` from `14:23:47.891234`).

23 is prime and coprime to 60, so `x * 23 mod 60` cycles through all 60 integer values before repeating. Properties:

- Consecutive startup seconds map to offsets 23 seconds apart — near-optimal spread for the common case where replicas start within a few seconds of each other.
- Sub-second startup differences contribute proportional sub-second offset differences (50ms apart in startup time produces ~1.15s apart in offset space).
- The mapping is deterministic and reproducible for debugging.

An optional instance number (externally provided — e.g. from a StatefulSet ordinal or environment variable) adds a second term: `(startup_seconds * 23 + instance * 23) mod 60`, forcing additional spread in environments that expose it.

After the initial offset, the goroutine ticks on a fixed 60-second interval. Two instances with different offsets never drift into alignment.

Collision handling: if two instances happen to tick simultaneously, `FOR UPDATE SKIP LOCKED` makes the collision a no-op for the losing instance. It finds no pending rows and sleeps until its next tick.

## Background Goroutine

The goroutine is panic-proof: the inner work function runs inside a `defer recover()` wrapper so a panic logs and resumes on the next tick rather than killing the goroutine.

```
compute offset from startup time (and optional instance number)
sleep until offset

loop:
    run work function inside defer/recover:
        drain all actionable pending rows (create pass)
        drain all expired created rows (retention pass)
    
    select:
        case <-ctx.Done(): return
        case <-time.After(60s):
    
    goto loop
```

### Create Pass

```
BEGIN

SELECT * FROM partcycle.managed_partitions
  WHERE status = 'pending'
    AND range_start <= now() + lookahead_interval
  ORDER BY range_start
  FOR UPDATE SKIP LOCKED
  LIMIT 1

if no row: COMMIT, exit pass

CREATE TABLE IF NOT EXISTS <partition_name>
  PARTITION OF <table_name>
  FOR VALUES FROM (range_start) TO (range_end)

UPDATE partcycle.managed_partitions
  SET status = 'created', created_at = now()
  WHERE (table_name, partition_name) = (...)

INSERT INTO partcycle.managed_partitions
  (table_name, partition_name, range_start, range_end, status)
  VALUES (next period's values)
  ON CONFLICT DO NOTHING

COMMIT
```

Loop back to drain additional pending rows before sleeping. The entire create + update + insert-next runs in one transaction. PostgreSQL DDL is transactional, so a failure at any point rolls back cleanly and the row stays `pending` for the next tick.

`ON CONFLICT DO NOTHING` on the next-row insert makes the chain idempotent if two instances race on adjacent rows.

### Retention Pass

Same loop structure, different query:

```
BEGIN

SELECT * FROM partcycle.managed_partitions
  WHERE status = 'created'
    AND range_end < now() - retention_interval
  ORDER BY range_start
  FOR UPDATE SKIP LOCKED
  LIMIT 1

if no row: COMMIT, exit pass

DROP TABLE IF EXISTS <partition_name>

DELETE FROM partcycle.managed_partitions
  WHERE (table_name, partition_name) = (...)

COMMIT
```

`DROP TABLE` is instant and generates no dead tuples — this is the entire motivation for RANGE partitioning over bulk DELETE for retention.

## Go API

```go
// RangeStrategy defines the partition period.
type RangeStrategy int

const (
    Monthly RangeStrategy = iota
    Weekly
    Daily
)

// Table declares a RANGE-partitioned table to be managed.
type Table struct {
    // Fully qualified table name, e.g. "contacts.contact_state_events".
    Name string

    // Partition period.
    Strategy RangeStrategy

    // Pre-create this many periods ahead of now.
    Lookahead int

    // Drop partitions older than this many periods. 0 means never drop.
    Retention int

    // Child partition naming function. Receives the range start time,
    // returns the unqualified partition name. The library never parses
    // partition names back to time ranges — the metadata table is the
    // source of truth for boundaries — so the naming scheme is arbitrary
    // and exists solely for human readability in \dt output and logs.
    // Required.
    Namer func(rangeStart time.Time) string
}

// Config is the argument bundle for Start and EnsureReady.
type Config struct {
    Pool     *pgxpool.Pool
    Tables   []Table

    // Schema name for the managed_partitions metadata table.
    // Default: "partcycle".
    Schema string

    // Optional instance number for offset spread. 0 means not provided;
    // the startup-time offset alone handles coordination.
    Instance int
}

// EnsureReady synchronously creates any overdue partitions and seeds
// pending rows for all registered tables. Call at service startup or
// from a migration tool before the service takes traffic. Idempotent.
func EnsureReady(ctx context.Context, cfg Config) error

// Start launches the background goroutine. Returns immediately.
// The goroutine runs until ctx is cancelled. Safe to call from
// multiple replicas — SKIP LOCKED coordinates.
func Start(ctx context.Context, cfg Config)
```

## EnsureReady Startup Hook

`EnsureReady` is the synchronous counterpart to the background goroutine. It runs at service boot (or from a migration tool) and:

1. Creates the `partcycle.managed_partitions` table if it does not exist.
2. For each registered table, ensures `pending` rows exist covering now through the lookahead horizon.
3. Executes the create pass synchronously — creates any overdue partitions that should already exist.

This handles cold start: if the service was down for an extended period, `EnsureReady` catches up before the service accepts traffic. The background goroutine handles steady-state from there.

## DEFAULT Partition

Each managed table should have a DEFAULT partition as a safety net. If the maintenance goroutine fails (panic loop, pool exhaustion) while the rest of the service continues writing, inserts land in the DEFAULT partition instead of erroring. The DEFAULT partition's contents are not eligible for `DROP TABLE` retention — they require manual investigation to determine why maintenance stopped and where the rows belong.

The library does not create the DEFAULT partition. It is part of the table's initial DDL, declared alongside the table itself.

## Observability

Observability runs as a separate self-healing goroutine on the same offset scheme as the maintenance goroutine (offset minus 1 second, so the two never collide on the same tick). It queries the metadata table once per minute, maintains in-process counters and gauges, and exposes them through two integration surfaces: a Prometheus-compatible pull channel and an OpenTelemetry collector push configuration.

### Observability Goroutine

The observability goroutine follows the same panic-proof pattern as the maintenance goroutine. It can either reuse the maintenance goroutine's computed offset minus 1 second, or compute its own from the same scheme (the 1-second shift guarantees the two goroutines never query the metadata table on the same tick).

Each tick:

1. Query `managed_partitions` for all rows.
2. Compute derived state:
   - Per-table count of `pending` rows where `range_start <= now()` (overdue creates).
   - Per-table count of `created` rows where `range_end < now() - retention_interval` (overdue drops).
   - Per-table time until the nearest uncreated range boundary (headroom).
3. Diff against previous tick's snapshot to detect events (partition created, partition dropped, create failed/skipped, drop failed/skipped).
4. Update in-process counters and gauges.
5. Emit events to the Prometheus channel and/or OpenTelemetry collector.

### In-Process State

The goroutine maintains the following metrics, aligned with OpenTelemetry semantic conventions:

**Counters (monotonic, cumulative):**

| Metric | Labels | Description |
|--------|--------|-------------|
| `partitions_created_total` | `table` | Partitions successfully created since process start |
| `partitions_dropped_total` | `table` | Partitions successfully dropped since process start |
| `partitions_create_errors_total` | `table` | Create attempts that failed |
| `partitions_drop_errors_total` | `table` | Drop attempts that failed |
| `partitions_skipped_total` | `table` | Ticks where SKIP LOCKED found no actionable rows (another instance handled it) |
| `observer_errors_total` | | Observability goroutine query failures |

**Gauges (point-in-time):**

| Metric | Labels | Description |
|--------|--------|-------------|
| `partitions_pending` | `table` | Current count of `pending` rows |
| `partitions_overdue` | `table` | Pending rows where `range_start <= now()` (should have been created already) |
| `partitions_headroom_seconds` | `table` | Seconds until the nearest uncovered range boundary (0 = writes are hitting DEFAULT) |
| `partitions_created` | `table` | Current count of `created` rows |
| `partitions_retention_overdue` | `table` | Created rows past their retention window |

Counter increments come from the maintenance goroutine (which tracks its own create/drop/skip/error outcomes per tick and writes them to shared atomic counters). The observability goroutine reads the counters and the metadata table to produce the full snapshot.

### Prometheus Integration

```go
// MetricsChan returns a channel that emits a Prometheus-compatible text
// exposition payload once per observability tick. The caller reads from
// the channel and appends the payload to their /metrics scrape handler.
// The channel is unbuffered; if the caller does not read, the
// observability goroutine drops the tick's payload (non-blocking send).
func MetricsChan(cfg Config) <-chan []byte
```

The emitted `[]byte` is a complete Prometheus text exposition block — `# HELP`, `# TYPE`, and metric lines — suitable for concatenation into an existing `/metrics` response. The caller owns the HTTP handler; the library does not import `net/http`.

### OpenTelemetry Integration

```go
// ConfigureOtel registers the library's metrics and trace spans with
// the provided OpenTelemetry MeterProvider and TracerProvider. Once
// configured, every maintenance tick emits a span covering the
// create/drop pass, and all counters and gauges are reported through
// the OTel pipeline (push to collector, OTLP export, etc.).
//
// Call after Start. If not called, the library emits no OTel data.
func ConfigureOtel(meter metric.MeterProvider, tracer trace.TracerProvider)
```

When configured:

- Each maintenance tick is wrapped in a span (`partcycle.tick`) with child spans for `partcycle.create` and `partcycle.drop` passes. Span attributes include `table`, `partition_name`, `range_start`, `range_end`, and outcome (`created`, `skipped`, `error`).
- All counters and gauges from the in-process state are registered as OTel instruments and reported on the provider's export interval.
- The observability goroutine's metadata query is wrapped in a span (`partcycle.observe`) for collector-side latency tracking.

OTel is an optional dependency. If the caller never calls `ConfigureOtel`, the library does not import or reference any OTel packages at build time (the integration surface is behind an interface or a separate sub-package).

### Shared State Between Goroutines

The maintenance goroutine increments atomic counters on each create/drop/skip/error outcome. The observability goroutine reads these atomics plus the metadata table to produce the full metrics snapshot. No mutex is needed — the counters are `atomic.Int64` and the metadata table query is its own transaction.

## Coordination Properties

- **Multi-instance safe:** `FOR UPDATE SKIP LOCKED` means one instance grabs a pending row, all others skip it and sleep. No blocking, no retry storms, no wasted work.
- **Idempotent:** `CREATE TABLE IF NOT EXISTS`, `DROP TABLE IF EXISTS`, `ON CONFLICT DO NOTHING` — every operation is safe to repeat.
- **Self-sustaining:** Completing a partition inserts the next pending row. The chain continues indefinitely as long as at least one instance runs periodically.
- **Atomic:** DDL + metadata update in one transaction. Partial failures roll back cleanly.
- **Observable:** The `pending_partitions` table is the source of truth. A `SELECT WHERE status = 'pending' AND range_start < now()` query that returns rows means maintenance is behind — suitable for an alerting probe.
