# Go Partition Maintenance Library — Design Document

**Library name:** `go-pg-parter` (the document uses `parter` as the default metadata schema name; the actual schema name is a configuration parameter).

## Problem

PostgreSQL RANGE-partitioned tables require child partitions to exist before rows for that range arrive. Unlike HASH partitions (declared once, static forever), RANGE partitions must be created on an ongoing basis as time advances, and expired partitions must be dropped for retention. Without automation, inserts either error (no covering partition) or land in a DEFAULT partition that defeats the retention benefit.

## Solution

A Go micro-library that maintains RANGE partitions via a metadata table and a background goroutine. Multiple service replicas coordinate through `FOR UPDATE SKIP LOCKED` — no leader election, no external scheduler, no PostgreSQL extensions.

## Metadata Table

The metadata table lives in a configurable schema (default: `parter`). The library creates the schema and table via `EnsureReady` if they do not exist.

```sql
CREATE SCHEMA IF NOT EXISTS <configured_schema>;

CREATE TABLE IF NOT EXISTS <configured_schema>.managed_partitions (
    table_name      TEXT        NOT NULL,
    partition_name  TEXT        NOT NULL,
    range_start     TIMESTAMPTZ NOT NULL,
    range_end       TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending',
                    -- 'pending' | 'created' | 'detaching' | 'detached' | 'failed'
    created_at      TIMESTAMPTZ,
    PRIMARY KEY (table_name, partition_name)
);
```

Each row represents a partition in one of five states:

| Status | Meaning |
|--------|---------|
| `pending` | Partition needs to be created and attached. |
| `created` | Partition exists and is attached to the parent. |
| `detaching` | A `DETACH PARTITION ... CONCURRENTLY` has been issued but not yet confirmed complete. |
| `detached` | Partition has been detached from the parent and is awaiting `DROP TABLE`. |
| `failed` | Partition creation failed repeatedly and has been parked. Requires manual intervention. |

### Schema Versioning

The metadata table includes no explicit version marker. Schema evolution is handled by the library's startup path (`EnsureReady`): after the initial `CREATE TABLE IF NOT EXISTS`, the library runs idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements to add any columns introduced in newer library versions. This approach avoids a version-tracking table while remaining safe for rolling deploys (old instances ignore new columns; new instances add them).

## Declarative Reconciliation Model

The library uses declarative reconciliation rather than chain-seeding. Every reconciliation pass — whether triggered by `EnsureReady` at startup or by the background goroutine on each tick — computes the desired set of partitions from scratch and diffs it against the metadata table.

**Algorithm:**

1. For each registered table, compute the set of periods that **should** exist: from the start of the current period boundary through `Lookahead` periods into the future.
   - The current period boundary is determined by truncating `now()` to the start of the current month, ISO week, or day, depending on the table's `RangeStrategy`. All period-boundary computations use UTC. The library calls `time.Now().UTC()` and truncates in UTC — never in the local timezone — so that all instances agree on boundaries regardless of their `TZ` environment.
   - Each subsequent period is computed by advancing one strategy-unit (one month, one week, one day) from the previous boundary.
   - `Lookahead` is an integer count of strategy-unit periods. For example, `Lookahead = 3` with `Monthly` means "ensure partitions exist for the current month plus 3 more months ahead" (4 partitions total covering the current period plus 3 future periods).
2. Query `managed_partitions` for all rows belonging to this table.
3. For any period in the desired set that has no corresponding metadata row, insert a `pending` row (`INSERT ... ON CONFLICT DO NOTHING` on the primary key, so concurrent reconciliation passes from multiple instances are safe).
4. Proceed to the create pass (which drains `pending` rows as before).

This model is self-healing for current and future periods: if a metadata row is manually deleted for a period within the desired set (current period through `Lookahead` periods ahead), the next reconciliation tick re-inserts it. Metadata for past periods (before the current period boundary) is **not** re-inserted — the reconciliation's desired set starts at the current period, not at `now() - retention`. A deleted metadata row for a past partition leaves the physical partition untracked: it persists beyond its retention window indefinitely (a storage leak, not data loss) and must be cleaned up manually. `EnsureReady` and the background goroutine share this same reconciliation logic — there is no separate code path for initial seeding vs. steady-state.

**Caveat:** Self-healing does not recover from `Namer` collisions. If the `Namer` function returns the same name for two distinct periods, the `INSERT ... ON CONFLICT DO NOTHING` silently drops the duplicate on every tick. The collision is deterministic and persistent — only fixing the `Namer` resolves it. See the `Namer` documentation in the Go API section.

## Tick Offset Scheme

Each service instance computes a deterministic offset into the 60-second tick window based on its goroutine startup time:

```
offset = (startup_seconds_with_fraction * 23) mod 60
```

Where `startup_seconds_with_fraction` is the seconds-and-microseconds component of the wall clock at goroutine launch (e.g. `47.891234` from `14:23:47.891234`).

23 is prime and coprime to 60, so for integer inputs `x * 23 mod 60` cycles through all 60 integer values before repeating. With fractional inputs (the normal case), the integer-second component still cycles through all 60 positions while the fractional part adds sub-second spread. Properties:

- Consecutive startup seconds map to offsets 23 seconds apart — near-optimal spread for the common case where replicas start within a few seconds of each other.
- Sub-second startup differences contribute proportional sub-second offset differences (50ms apart in startup time produces ~1.15s apart in offset space).
- The mapping is deterministic and reproducible for debugging.
- The implementation should truncate or round the final offset to millisecond precision to avoid floating-point accumulation issues in tick scheduling.

When `Instance` is non-nil (externally provided — e.g. from a StatefulSet ordinal or environment variable), it adds a second term: `(startup_seconds * 23 + *Instance * 23) mod 60`, forcing additional spread in environments that expose it.

After the initial offset, the goroutine ticks on a fixed-epoch schedule: `next_tick = process_start + offset + N * 60s`. This avoids drift caused by variable work duration — the tick interval is measured from the epoch, not from when the previous tick's work finished. Two instances with different offsets maintain constant separation indefinitely.

Collision handling: if two instances happen to tick simultaneously, `FOR UPDATE SKIP LOCKED` makes the collision a no-op for the losing instance. It finds no pending rows and sleeps until its next tick.

## Background Goroutine

The goroutine is panic-proof: the inner work function runs inside a `defer recover()` wrapper so a panic logs and resumes on the next tick rather than killing the goroutine.

```
compute offset from startup time (and optional instance number)
epoch = process_start + offset
sleep until epoch

N = 0
loop:
    run work function inside defer/recover:
        reconcile pending rows (declarative reconciliation)
        drain all actionable pending rows (create pass)
        if retention > 0: drain all expired created rows (retention pass)
    
    N++
    next_tick = epoch + N * 60s
    // If work took longer than 60s, advance N until next_tick is
    // in the future. This skips missed ticks rather than busy-looping.
    while next_tick <= now():
        N++
        next_tick = epoch + N * 60s
    sleep_duration = next_tick - now()

    select:
        case <-ctx.Done(): return
        case <-time.After(sleep_duration):
    
    goto loop
```

Each tick's work function runs with a derived context carrying a deadline (e.g. 55 seconds, slightly less than the 60-second tick interval). If the context expires — due to a stuck DDL statement, connection-pool exhaustion, or any other delay — the in-progress transaction is cancelled and rolled back, and the goroutine proceeds to the next tick. The tight deadline ensures the goroutine never misses more than one tick due to a single hung operation.

### Create Pass

```
BEGIN

SELECT * FROM parter.managed_partitions
  WHERE table_name = <table_name>
    AND status = 'pending'
  ORDER BY range_start
  FOR UPDATE SKIP LOCKED
  LIMIT 1

if no row: COMMIT, exit pass

CREATE TABLE IF NOT EXISTS <partition_name>  -- schema-qualified: parent's schema prepended to Namer output
  PARTITION OF <table_name>
  FOR VALUES FROM (range_start) TO (range_end)

-- Verify the partition is actually attached with correct boundaries.
-- IF NOT EXISTS succeeds even if the table exists as a standalone
-- (previously detached) table. Query pg_class + pg_inherits to confirm
-- the partition is attached to the parent. If not attached, or if the
-- boundaries don't match, log an error, increment create_errors_total,
-- and leave the row as 'pending' (or advance to 'failed' per the
-- circuit breaker).

UPDATE parter.managed_partitions
  SET status = 'created', created_at = now()
  WHERE (table_name, partition_name) = (...)

COMMIT

increment parter.created_total counter
```

Loop back to drain additional pending rows before sleeping. The entire create + update runs in one transaction. PostgreSQL DDL is transactional, so a failure at any point rolls back cleanly and the row stays `pending` for the next tick. On error, increment `parter.create_errors_total` counter.

### Circuit Breaker

If a `pending` row fails to create on 5 consecutive ticks (across any combination of instances), the library sets its status to `failed` and stops retrying. This prevents a poison row — caused by a dropped parent table, an invalid range, or a conflicting manually-created partition — from consuming a SKIP LOCKED slot and logging errors on every tick indefinitely.

Failed rows are surfaced via the `parter.failed` gauge (see Gauges table in the Observability section).

To retry a failed row, manually update its status back to `pending` in the metadata table. The circuit breaker counter resets when the status changes.

Implementation: add a `fail_count INT NOT NULL DEFAULT 0` column to `managed_partitions`. The create pass increments `fail_count` on each error and sets `status = 'failed'` when `fail_count >= 5`.

### Retention Pass

The retention pass is **skipped entirely** when the table's `Retention` is 0 (meaning "never drop"). This guard is critical: a zero retention interval would cause `range_end < now() - interval '0'` to match all historical partitions, dropping everything.

When `Retention > 0`, the library converts the integer to a calendar interval using the table's `RangeStrategy`: `Retention` periods of the strategy unit (e.g. `Retention = 3` with `Monthly` → `interval '3 months'`; with `Weekly` → `interval '3 weeks'`; with `Daily` → `interval '3 days'`). This `retention_interval` is used in the SQL filter below.

The retention pass uses a two-step detach-then-drop process. `DROP TABLE` on an attached partition acquires `ACCESS EXCLUSIVE` on the parent table, blocking all concurrent reads and writes. To avoid this, we first detach the partition concurrently (which only requires `SHARE UPDATE EXCLUSIVE` — compatible with normal reads and writes), then drop the now-standalone table.

**Step 1 — Detach:**

```
BEGIN

SELECT * FROM parter.managed_partitions
  WHERE table_name = <table_name>
    AND status = 'created'
    AND range_end < now() - retention_interval
  ORDER BY range_start
  FOR UPDATE SKIP LOCKED
  LIMIT 1

if no row: COMMIT, exit pass

UPDATE parter.managed_partitions
  SET status = 'detaching'
  WHERE (table_name, partition_name) = (...)

COMMIT

ALTER TABLE <table_name> DETACH PARTITION <partition_name> CONCURRENTLY
```

`DETACH PARTITION ... CONCURRENTLY` (PostgreSQL 14+) runs outside a transaction block — it cannot be wrapped in `BEGIN/COMMIT`. It holds a `SHARE UPDATE EXCLUSIVE` lock on the parent for most of its duration (compatible with concurrent DML), but briefly escalates to `ACCESS EXCLUSIVE` during the final catalog update — a momentary pause, not a sustained block. Once complete, update the metadata:

```
UPDATE parter.managed_partitions
  SET status = 'detached'
  WHERE (table_name, partition_name) = (...)
    AND status = 'detaching'
```

**Step 2 — Drop:**

```
BEGIN

SELECT * FROM parter.managed_partitions
  WHERE table_name = <table_name>
    AND status = 'detached'
  ORDER BY range_start
  FOR UPDATE SKIP LOCKED
  LIMIT 1

if no row: COMMIT, exit pass

DROP TABLE IF EXISTS <partition_name>

DELETE FROM parter.managed_partitions
  WHERE (table_name, partition_name) = (...)

COMMIT

increment parter.dropped_total counter
```

Partition-based retention via `DROP TABLE` avoids scanning and generating dead tuples — this is the entire motivation for RANGE partitioning over bulk `DELETE` for retention.

**Step 3 — Recover `detaching` rows (crash recovery):**

```
BEGIN

SELECT * FROM parter.managed_partitions
  WHERE table_name = <table_name>
    AND status = 'detaching'
  FOR UPDATE SKIP LOCKED
  LIMIT 1

if no row: COMMIT, exit recovery

-- Record the partition details, then release the row lock.
COMMIT

-- Determine the partition's inheritance state.
SELECT inhdetachpending FROM pg_inherits
  WHERE inhrelid = <partition_oid>

if no row:
    -- Partition is fully detached (or was never attached). Advance state.
    UPDATE parter.managed_partitions
      SET status = 'detached'
      WHERE (table_name, partition_name) = (...)
        AND status = 'detaching'

else if inhdetachpending = true:
    -- A previous DETACH CONCURRENTLY was interrupted mid-operation.
    -- FINALIZE completes the pending detach; it is a no-op if the
    -- background worker already finished.
    ALTER TABLE <table_name> DETACH PARTITION <partition_name> FINALIZE

    UPDATE parter.managed_partitions
      SET status = 'detached'
      WHERE (table_name, partition_name) = (...)
        AND status = 'detaching'

else:  -- inhdetachpending = false, partition is still fully attached
    -- Detach never started (process crashed before issuing the command).
    -- DETACH CONCURRENTLY cannot run inside a transaction block.
    ALTER TABLE <table_name> DETACH PARTITION <partition_name> CONCURRENTLY

    UPDATE parter.managed_partitions
      SET status = 'detached'
      WHERE (table_name, partition_name) = (...)
        AND status = 'detaching'

-- On any error: log, increment parter.drop_errors_total, leave row
-- as 'detaching', continue to next row.
```

Loop back to drain additional rows for all three steps before sleeping. The `detaching` state acts as a crash-recovery marker: if the process dies between issuing `DETACH CONCURRENTLY` and confirming completion, step 3 picks up the row and checks `pg_inherits` and its `inhdetachpending` column to determine the partition's state — fully detached (no `pg_inherits` row, advance straight to `detached`), pending detach (`inhdetachpending = true`, complete the operation with `DETACH PARTITION ... FINALIZE`), or still fully attached (`inhdetachpending = false`, retry with `DETACH PARTITION ... CONCURRENTLY`). On error, increment `parter.drop_errors_total` counter.

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

    // Drop partitions older than this many periods.
    // 0 means never drop — the retention pass is skipped entirely.
    // This is the only safe way to disable retention; there is no
    // sentinel value that means "drop immediately".
    Retention int

    // Child partition naming function. Receives the range start time,
    // returns the unqualified partition name. The library never parses
    // partition names back to time ranges — the metadata table is the
    // source of truth for boundaries — so the naming scheme is arbitrary
    // and exists solely for human readability in \dt output and logs.
    // Required.
    //
    // The library automatically prepends the parent table's schema
    // (extracted from Table.Name) to the Namer output in all DDL
    // statements. For example, if Table.Name = "contacts.contact_state_events"
    // and Namer returns "cse_2025_01", the library uses
    // "contacts.cse_2025_01" in CREATE TABLE, DETACH PARTITION, and
    // DROP TABLE statements. The partition_name column in the metadata
    // table stores the schema-qualified name.
    //
    // IMPORTANT: Namer must produce unique names for every distinct
    // range_start the table will see. If two different periods map to
    // the same name, the second period's INSERT ... ON CONFLICT DO NOTHING
    // silently drops it, and that period will never have a partition
    // created. This failure is persistent — the same collision recurs
    // on every reconciliation tick. Callers should include enough
    // date components to avoid collisions (e.g. for Daily, include
    // year-month-day, not just month-day).
    Namer func(rangeStart time.Time) string
}

// Config is the argument bundle for Start and EnsureReady.
type Config struct {
    Pool     *pgxpool.Pool
    Tables   []Table

    // Schema name for the managed_partitions metadata table.
    // Default: "parter".
    Schema string

    // Optional instance number for additional offset spread.
    // nil means not provided; the startup-time offset alone handles
    // coordination. When set (e.g. from a StatefulSet ordinal),
    // the value is added to the offset formula.
    Instance *int
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

### Config Validation

Both `EnsureReady` and `Start` validate `Config` before proceeding and return an error (or panic for `Start`, which has no error return) on invalid input:

- `Pool` must not be nil.
- `Tables` must contain at least one entry.
- Each `Table.Name` must be non-empty.
- Each `Table.Namer` must not be nil.
- Each `Table.Lookahead` must be ≥ 1. A zero lookahead creates a race at every period boundary: the current period's partition does not exist until the next tick fires after the boundary, leaving up to 60 seconds where inserts hit DEFAULT.
- Each `Table.Retention` must be ≥ 0.
- Each `Table.Strategy` must be a valid `RangeStrategy` value.

## EnsureReady Startup Hook

`EnsureReady` is the synchronous counterpart to the background goroutine. It runs at service boot (or from a migration tool) and:

1. Creates the configured schema if it does not exist (`CREATE SCHEMA IF NOT EXISTS <configured_schema>`).
2. Creates the `parter.managed_partitions` table if it does not exist.
3. For each registered table, runs the declarative reconciliation algorithm: computes the desired set of partitions from the current period boundary through `Lookahead` periods into the future, diffs against existing metadata rows, and inserts `pending` rows for any missing periods.
4. Executes the create pass synchronously — creates any overdue partitions that should already exist.

This handles cold start: if the service was down for an extended period, `EnsureReady` catches up before the service accepts traffic. The background goroutine handles steady-state from there.

**Note on concurrent instances:** Because the create pass uses `FOR UPDATE SKIP LOCKED`, if another instance's background goroutine holds a lock on a pending row when `EnsureReady` runs, that row is skipped. In practice, this is benign: the other instance is actively creating that partition. However, `EnsureReady` does not guarantee that *all* partitions are created when it returns — only that all *uncontested* pending rows have been drained. For a strict guarantee (e.g. in a migration tool), run `EnsureReady` when no background goroutines are active.

## DEFAULT Partition

Each managed table should have a DEFAULT partition as a safety net. If the maintenance goroutine fails (panic loop, pool exhaustion) while the rest of the service continues writing, inserts land in the DEFAULT partition instead of erroring. The DEFAULT partition's contents are not eligible for retention — they require manual investigation to determine why maintenance stopped and where the rows belong.

The library does not create the DEFAULT partition. It is part of the table's initial DDL, declared alongside the table itself.

## Observability

Observability runs as a separate self-healing goroutine on the same offset scheme as the maintenance goroutine (offset minus 1 second, so the two never collide on the same tick). It queries the metadata table once per minute, maintains in-process counters and gauges, and exposes them through two integration surfaces: a Prometheus-compatible pull channel and an OpenTelemetry collector push configuration.

The observability goroutine is started by `Start()` alongside the maintenance goroutine. It runs unconditionally — even if neither `MetricsChan` nor `ConfigureOtel` is called — maintaining the atomic gauges that `MetricsChan` and `ConfigureOtel` consume. `MetricsChan` does not spawn a separate goroutine; it returns a channel that the already-running observability goroutine writes to each tick.

### Observability Goroutine

The observability goroutine follows the same panic-proof pattern as the maintenance goroutine. It can either reuse the maintenance goroutine's computed offset minus 1 second, or compute its own from the same scheme (the 1-second shift guarantees the two goroutines never query the metadata table on the same tick). If the maintenance offset is less than 1 second, the subtraction wraps modularly within the 60-second window (e.g. maintenance offset 0.3s → observability offset 59.3s).

Each tick:

1. Query `managed_partitions` for all rows.
2. Compute derived state:
   - Per-table count of `pending` rows where `range_start <= now()` (overdue creates).
   - Per-table count of `created` rows where `range_end < now() - retention_interval` (overdue drops). Skipped for tables with `Retention = 0` (no retention configured).
   - Per-table time until the nearest uncreated range boundary (headroom).
3. Detect events by combining metadata diffs with atomic counter changes. Status transitions (e.g. `pending` → `created`, `created` → `detaching`) are visible in the metadata diff. Failures (create or drop errors) do **not** change metadata state — a failed `pending` row stays `pending` — so error events are detected by comparing the atomic error counters (`create_errors_total`, `drop_errors_total`) against their previous-tick values.
4. Update in-process counters and gauges.
5. Emit events to the Prometheus channel and/or OpenTelemetry collector.

### In-Process State

The maintenance goroutine updates shared `atomic.Int64` counters directly as it completes each operation (see counter increment steps in the create pass and retention pass pseudocode above). The observability goroutine reads these atomics plus the metadata table to produce the full metrics snapshot.

All metric names use the `parter.` prefix for consistency across Prometheus and OpenTelemetry surfaces.

**Counters (monotonic, cumulative):**

| Metric | Labels | Description |
|--------|--------|-------------|
| `parter.created_total` | `table` | Partitions successfully created since process start |
| `parter.dropped_total` | `table` | Partitions successfully dropped since process start |
| `parter.create_errors_total` | `table` | Create attempts that failed |
| `parter.drop_errors_total` | `table` | Drop attempts that failed |
| `parter.observer_errors_total` | | Observability goroutine query failures |

**Gauges (point-in-time):**

| Metric | Labels | Description |
|--------|--------|-------------|
| `parter.pending` | `table` | Current count of `pending` rows |
| `parter.overdue` | `table` | Pending rows where `range_start <= now()` (should have been created already) |
| `parter.headroom_seconds` | `table` | Seconds until the nearest uncovered range boundary (0 = writes are hitting DEFAULT) |
| `parter.created` | `table` | Current count of `created` rows |
| `parter.retention_overdue` | `table` | Created rows past their retention window (0 for tables with `Retention = 0`) |
| `parter.failed` | `table` | Current count of `failed` rows (requires manual investigation) |

The `partitions_skipped_total` counter from the original design has been removed. `SKIP LOCKED` cannot distinguish "another instance handled this row" from "there was no actionable work" — both result in zero rows returned. The `parter.pending` and `parter.overdue` gauges already surface the important signal (maintenance is falling behind). If per-instance contention detection is wanted, it requires comparing pending counts across instances, which is out of scope for a single-instance metric.

### Prometheus Integration

```go
// MetricsChan returns a channel that emits a Prometheus-compatible text
// exposition payload once per observability tick. The caller reads from
// the channel and appends the payload to their /metrics scrape handler.
// The channel is unbuffered; if the caller does not read, the
// observability goroutine drops the tick's payload (non-blocking send).
//
// The ctx parameter controls the lifetime of the underlying goroutine;
// when ctx is cancelled, the channel is closed.
//
// This function intentionally returns raw text bytes rather than
// Prometheus collector objects — it avoids importing the Prometheus
// client library as a hard dependency of go-pg-parter.
func MetricsChan(ctx context.Context, cfg Config) <-chan []byte
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
// Must be called after Start. Calling before Start panics (the
// observability goroutine is not yet running). Must be called at most
// once; a second call panics to prevent duplicate OTel instrument
// registration. If never called, the library emits no OTel data and
// no OTel packages are imported at build time.
func ConfigureOtel(meter metric.MeterProvider, tracer trace.TracerProvider)
```

When configured:

- Each maintenance tick is wrapped in a span (`parter.tick`) with child spans for `parter.create` and `parter.drop` passes. Span attributes include `table`, `partition_name`, `range_start`, `range_end`, and outcome (`created`, `skipped`, `error`).
- All counters and gauges from the in-process state are registered as OTel instruments and reported on the provider's export interval.
- The observability goroutine's metadata query is wrapped in a span (`parter.observe`) for collector-side latency tracking.

OTel is an optional dependency. If the caller never calls `ConfigureOtel`, the library does not import or reference any OTel packages at build time (the integration surface is in a separate sub-package (`parter/otel`)).

### Shared State Between Goroutines

The maintenance goroutine increments atomic counters on each create/drop/error outcome. The observability goroutine reads these atomics plus the metadata table to produce the full metrics snapshot. No mutex is needed — the counters are `atomic.Int64` and the metadata table query is its own transaction.

## Coordination Properties

- **Multi-instance safe:** `FOR UPDATE SKIP LOCKED` means one instance grabs a pending row, all others skip it and sleep. No blocking, no retry storms, no wasted work.
- **Idempotent:** `CREATE TABLE IF NOT EXISTS`, `DROP TABLE IF EXISTS`, `ON CONFLICT DO NOTHING` — create and drop operations are safe to repeat. The exception is `DETACH PARTITION ... CONCURRENTLY`, which is not idempotent (repeating it on an already-detached or pending-detach partition errors). The metadata state machine (`detaching` → `detached`) and the `inhdetachpending` check in crash recovery guard against duplicate detach attempts.
- **Self-healing:** The declarative reconciliation model computes the desired state from scratch each tick. Missing metadata rows for current and future periods are re-inserted, and broken chains self-repair — no single-point-of-failure seed chain. Past periods (before the current period boundary) are not re-inserted; see the caveat in the Declarative Reconciliation section.
- **Atomic:** DDL + metadata update in one transaction (create pass). Retention uses a two-step detach-then-drop to avoid parent-table lock escalation, with crash-safe state transitions through `detaching` → `detached`.
- **Observable:** The `managed_partitions` table is the source of truth. A `SELECT WHERE status = 'pending' AND range_start < now()` query that returns rows means maintenance is behind — suitable for an alerting probe.
