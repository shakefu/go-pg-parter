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
    table_name        TEXT        NOT NULL,
    partition_name    TEXT        NOT NULL,
    range_start       TIMESTAMPTZ NOT NULL,
    range_end         TIMESTAMPTZ NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'pending',
                      -- 'pending' | 'created' | 'detaching' | 'detached' | 'failed'
    created_at        TIMESTAMPTZ,
    fail_count        INT         NOT NULL DEFAULT 0,  -- circuit breaker; see Create Pass
    detach_started_at TIMESTAMPTZ,                     -- set when status -> 'detaching'; gates crash recovery
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

1. For each registered table, compute the set of periods that **must** exist: from the start of the current period boundary through `Lookahead` periods into the future.
   - The current period boundary is determined by truncating `now()` to the start of the current month, ISO week, or day, depending on the table's `RangeStrategy`. All period-boundary computations use UTC. The library calls `time.Now().UTC()` and truncates in UTC — never in the local timezone — so that all instances agree on boundaries regardless of their `TZ` environment.
   - Each subsequent period is computed by advancing one strategy-unit (one month, one week, one day) from the previous boundary.
   - `Lookahead` is an integer count of strategy-unit periods. For example, `Lookahead = 3` with `Monthly` means "ensure partitions exist for the current month plus 3 more months ahead" (4 partitions total covering the current period plus 3 future periods).
2. Query `managed_partitions` for all rows belonging to this table.
3. For any period in the desired set that has no corresponding metadata row, insert a `pending` row (`INSERT ... ON CONFLICT DO NOTHING` on the primary key, so concurrent reconciliation passes from multiple instances are safe).
4. Proceed to the create pass (which drains this table's `pending` rows; see Create Pass).

Each pass — reconciliation, create, retention — runs per registered table, iterating tables in the order they appear in `Config.Tables` and draining each table fully before moving to the next. (The original design ran a single create query spanning all tables ordered globally by `range_start`; the per-table model trades that global ordering for simpler per-table reasoning and isolation.)

This model is self-healing for current and future periods: if a metadata row is manually deleted for a period within the desired set (current period through `Lookahead` periods ahead), the next reconciliation tick re-inserts it. Metadata for past periods (before the current period boundary) is intentionally **not** re-inserted: the reconciliation's desired set starts at the current period, not at `now() - retention`, precisely so reconciliation never resurrects rows the retention pass has already dropped — re-inserting them would loop the detach/drop cycle forever. Consequently, manually deleting a past-period metadata row leaves the physical partition untracked: it persists beyond its retention window indefinitely (a storage leak, not data loss) and must be cleaned up manually. Operators MUST NOT delete `managed_partitions` rows out of band — the library owns them. `EnsureReady` and the background goroutine share this same reconciliation logic — there is no separate code path for initial seeding vs. steady-state.

**Caveat:** Self-healing does not recover from `Namer` collisions. If the `Namer` function returns the same name for two distinct periods, the `INSERT ... ON CONFLICT DO NOTHING` silently drops the duplicate on every tick. The collision is deterministic and persistent — only fixing the `Namer` resolves it. See the `Namer` documentation in the Go API section.

## Tick Offset Scheme

Each service instance computes a deterministic offset into the 60-second tick window based on its goroutine startup time:

```
offset = (startup_seconds_with_fraction * 23) mod 60
```

Where `startup_seconds_with_fraction` is the seconds-and-microseconds component of the wall clock at goroutine launch (e.g. `47.891234` from `14:23:47.891234`).

23 is coprime to 60, so for integer inputs `x * 23 mod 60` cycles through all 60 integer values before repeating. With fractional inputs (the normal case), the integer-second component still cycles through all 60 positions, and the fractional component `f` contributes `23·f` seconds (ranging over `[0, 23)`), so a sub-second *difference* in startup time maps to a proportionally larger but still sub-tick difference in offset. Properties:

- Consecutive startup seconds map to offsets 23 seconds apart — near-optimal spread for the common case where replicas start within a few seconds of each other.
- Sub-second startup differences contribute proportional sub-second offset differences (50ms apart in startup time produces ~1.15s apart in offset space).
- The mapping is deterministic and reproducible for debugging.
- The implementation rounds the final offset to the nearest millisecond (ties round up) to prevent floating-point accumulation errors in tick scheduling.

When `Instance` is non-nil (externally provided — e.g. from a StatefulSet ordinal or environment variable), it adds a second term: `(startup_seconds_with_fraction * 23 + *Instance * 23) mod 60`, forcing additional spread in environments that expose it. (`*Instance` only spreads usefully modulo 60 — instance ordinals that differ by a multiple of 60 contribute no additional separation; the sub-second startup component still separates them.)

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

Each tick's work function runs with a derived context carrying a fixed 55-second deadline (`TickDeadline`, a non-configurable constant), 5 seconds short of the 60-second tick interval; `TickDeadline < tickInterval` is an invariant. If the context expires — due to a stuck DDL statement, connection-pool exhaustion, or any other delay — the in-progress transaction is cancelled and rolled back, and the goroutine proceeds to the next tick. The tight deadline ensures the goroutine never misses more than one tick due to a single hung operation.

### Create Pass

```
attempted = {}   -- in-memory set of (table_name, partition_name) tried this tick

loop:
  BEGIN

  SELECT * FROM parter.managed_partitions
    WHERE table_name = <table_name>
      AND status = 'pending'
      AND (table_name, partition_name) <> ALL(attempted)  -- skip rows already tried this tick
    ORDER BY range_start
    FOR UPDATE SKIP LOCKED
    LIMIT 1

  if no row: COMMIT, exit pass   -- every pending row is now created or already attempted this tick

  add (table_name, partition_name) to attempted

  CREATE TABLE IF NOT EXISTS <partition_name>  -- schema-qualified: parent's schema prepended to Namer output
    PARTITION OF <table_name>
    FOR VALUES FROM (range_start) TO (range_end)

  -- Verify the partition is actually attached with correct boundaries.
  -- IF NOT EXISTS succeeds even if a table of that name already exists as a
  -- standalone (previously detached) relation, so a "successful" CREATE is
  -- not proof of attachment. Query pg_class + pg_inherits to confirm the
  -- partition is attached to the parent with matching boundaries. A
  -- not-attached or boundary-mismatched result is a create FAILURE (handled
  -- below): it is a poison row and is routed to the circuit breaker.

  if ctx cancelled or deadline exceeded:
    ROLLBACK, exit pass            -- the tick is ending; this is not the row's fault.
                                   -- Do NOT touch fail_count or the row stays 'pending'.

  if create failed OR verification failed:
    ROLLBACK                       -- DDL is transactional; the row stays 'pending'
    increment parter.create_errors_total counter

    if error is transient (connection loss, lock_not_available, serialization_failure):
      continue loop                -- retry next tick; do NOT advance the breaker

    -- Deterministic failure only (invalid range, missing or non-partitioned
    -- parent, name conflict, attach/boundary mismatch, or a non-empty DEFAULT
    -- partition already holding rows in this range — SQLSTATE 23514): advance
    -- the circuit breaker. Persist in a SEPARATE committed transaction — the
    -- ROLLBACK above discarded the create transaction, so an in-transaction
    -- bump would be lost and the breaker could never trip.
    BEGIN
      UPDATE parter.managed_partitions
        SET fail_count = fail_count + 1,
            status     = CASE WHEN fail_count + 1 >= 5 THEN 'failed' ELSE status END
        WHERE (table_name, partition_name) = (...)
          AND status = 'pending'   -- no-op if another instance already created/parked it
    COMMIT

    continue loop                  -- row is in `attempted`, so it is not retried this tick

  UPDATE parter.managed_partitions
    SET status = 'created', created_at = now()
    WHERE (table_name, partition_name) = (...)

  COMMIT

  increment parter.created_total counter
  continue loop
```

Each pending row is attempted **at most once per tick**: the in-memory `attempted` set stops the loop from re-selecting a row that just failed (it is still `pending`), so a single poison row neither livelocks the pass nor starves newer rows behind it in `range_start` order. The create + verify + status update for a *successful* row run in one transaction; PostgreSQL DDL is transactional, so any failure rolls the row back to `pending` cleanly. The `fail_count` increment, by contrast, runs in its own committed transaction *after* the rollback — otherwise it would be discarded along with the failed create.

### Circuit Breaker

After a `pending` row accumulates **5 failed creation attempts** (`fail_count >= 5`), the library sets its status to `failed` and stops retrying. This prevents a poison row — caused by a dropped parent table, an invalid range, a boundary mismatch with a pre-existing relation, or a conflicting manually-created partition — from consuming a SKIP LOCKED slot and logging errors on every tick indefinitely.

The counter is `fail_count` (a column on `managed_partitions`, see the metadata DDL). Because each pending row is attempted at most once per tick per instance (see Create Pass), a single instance takes at least 5 ticks to park a row; with multiple instances each attempting once per their own tick, it may be parked in fewer wall-clock minutes. The count is a running total of failures, not a "consecutive" counter — but a `pending` row that succeeds leaves the `pending` state entirely (it becomes `created`), so for any given row all counted failures are inherently for the same unresolved partition.

`fail_count` is incremented in its own committed transaction *after* the failed create transaction rolls back (see Create Pass pseudocode). An increment inside the rolled-back create transaction would be discarded, and the breaker would never trip.

Only **deterministic** failures advance `fail_count`: an invalid range, a missing or non-partitioned parent, a name conflict, an attach/boundary mismatch, or a non-empty DEFAULT partition that already holds rows belonging to the new partition's range (SQLSTATE 23514, `check_violation`) — failures that recur identically on every attempt. Transient failures (connection loss, lock contention, serialization) and context cancellation/deadline are **not** counted: a transient error increments `parter.create_errors_total` and the row is retried unchanged next tick, while a cancellation ends the pass without touching the row. This keeps a slow or briefly-unavailable database from spuriously parking healthy partitions as `failed`. A create that fails because the DEFAULT partition holds rows in the target range is deterministic and counts toward the 5-attempt budget; recovering it requires relocating those rows out of the DEFAULT before resetting the parked row (see the DEFAULT Partition section).

`EnsureReady` runs the same create pass and shares this counter: a deterministic failure during a startup or migration run advances `fail_count` durably, so enough such runs against a genuinely poison row will park it as `failed` even before the background goroutine starts. This is intended — a poison row is poison regardless of which entry point hit it — and deterministic startup or migration failures count toward the same 5-attempt budget as background-goroutine failures.

Failed rows are surfaced via the `parter.failed` gauge (see Gauges table in the Observability section).

To retry a `failed` row, reset it manually — this must clear `fail_count` as well as the status, or the row re-trips after a single attempt:

```sql
UPDATE <schema>.managed_partitions
  SET status = 'pending', fail_count = 0
  WHERE (table_name, partition_name) = (...) AND status = 'failed';
```

The library performs no automatic retry of `failed` rows; resetting them is an operator action taken after fixing the root cause.

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
  SET status = 'detaching', detach_started_at = now()
  WHERE (table_name, partition_name) = (...)

COMMIT

ALTER TABLE <table_name> DETACH PARTITION <partition_name> CONCURRENTLY
```

`DETACH PARTITION ... CONCURRENTLY` (PostgreSQL 14+) runs outside a transaction block — it cannot be wrapped in `BEGIN/COMMIT`, so it must be issued on a dedicated connection in autocommit mode (e.g. an `Exec` *not* inside a `pgx` `Begin`), separate from the transactions used by the SELECT/UPDATE steps. It holds a `SHARE UPDATE EXCLUSIVE` lock on the parent for most of its duration (compatible with concurrent DML), but briefly escalates to `ACCESS EXCLUSIVE` during the final catalog update — a momentary pause, not a sustained block. If the `DETACH` errors, increment `parter.detach_errors_total` and leave the row as `detaching`; Step 3 reclaims it once it ages past `detach_recovery_timeout`. Once complete, update the metadata:

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
-- On error: ROLLBACK, increment parter.drop_errors_total, leave row as
-- 'detached' (the transaction rolls back cleanly); retry on the next tick.
```

Partition-based retention via `DROP TABLE` avoids scanning and generating dead tuples — this is the entire motivation for RANGE partitioning over bulk `DELETE` for retention.

**Step 3 — Recover `detaching` rows (crash recovery):**

A `detaching` row is normally advanced to `detached` by the same instance that set it, moments after `DETACH ... CONCURRENTLY` returns (Step 1). Step 3 exists only to recover rows whose owning instance **died** mid-detach. It must not touch a detach that another *live* instance is still running: between Step 1's `COMMIT` (which releases the row lock) and the post-detach `UPDATE`, the row sits in `detaching` with no lock held, so a naive recovery `SELECT ... FOR UPDATE SKIP LOCKED` on another instance would pick it up and issue a second, conflicting `DETACH`/`FINALIZE` (these are not concurrency-safe against an in-flight detach). The `detach_started_at` timestamp is a cheap pre-filter, not the safety mechanism: recovery only *considers* rows that have been `detaching` longer than `detach_recovery_timeout` — a duration set comfortably above typical `DETACH ... CONCURRENTLY` completion time so that in steady state a row reaches Step 3 only after its owner has genuinely crashed. Default: 10 minutes. Because `DETACH ... CONCURRENTLY` can legitimately run longer than any fixed timeout (it waits out concurrent transactions), the timeout cannot by itself prove the owner is gone; the authoritative guard is the mandatory liveness check described next.

**Residual risk — closed by a mandatory liveness check.** The elapsed-time gate alone does not eliminate the race, because `DETACH ... CONCURRENTLY` can wait **arbitrarily** long: it holds `SHARE UPDATE EXCLUSIVE` on the parent and blocks until every transaction that could still see the partition completes, so a single long-running *active* query on the parent can keep a detach in flight past *any* fixed `detach_recovery_timeout`. A database GUC cannot be the library's safety mechanism: `idle_in_transaction_session_timeout` terminates only idle-in-transaction sessions, never long-running active queries, and `statement_timeout` is an operator-owned setting the library can neither guarantee nor assert at runtime. Therefore, before reclaiming any gated row, Step 3 **MUST** run a liveness check against `pg_locks` joined to `pg_stat_activity` and skip the row if any *other* backend currently holds `SHARE UPDATE EXCLUSIVE` on the **parent** and is actively running a `DETACH`. Matching on the parent's `SHARE UPDATE EXCLUSIVE` lock — held continuously and gap-free for the entire detach, including its wait phase — together with `state = 'active'`, is what makes the check correct under connection pooling: a pooled backend that ran a failed detach and returned to the pool no longer holds that lock and is no longer active on a `DETACH`, so it is correctly *not* seen as a live detacher; a crashed owner's backend is gone and holds no locks at all. These two cases are exhaustive — at the instant of the check the owner is either holding the parent lock (seen → skip) or absent (safe to reclaim). When Step 3 then issues its own `DETACH ... CONCURRENTLY` (or `FINALIZE`) it must re-acquire that same parent `SHARE UPDATE EXCLUSIVE` lock, and PostgreSQL permits only one detach per parent at a time: any straggler — a slow owner that resumes, or a second instance's concurrent recovery — serializes on that lock rather than running alongside, and whichever loses the race finds the work already done and fails harmlessly (logged, counted in `parter.detach_errors_total`, leaving the row in its correct final state). Two destructive detaches therefore can never run concurrently. The gate is retained only as a cheap pre-filter; the liveness check plus the parent lock are the guarantee.

```
attempted = {}   -- in-memory set of rows examined this tick (as in the Create Pass)

loop:
  BEGIN

  SELECT * FROM parter.managed_partitions
    WHERE table_name = <table_name>
      AND status = 'detaching'
      AND detach_started_at < now() - detach_recovery_timeout   -- pre-filter: apparently-crashed owners only
      AND (table_name, partition_name) <> ALL(attempted)        -- skip rows already examined this tick
    ORDER BY range_start
    FOR UPDATE SKIP LOCKED
    LIMIT 1

  if no row: COMMIT, exit recovery   -- every eligible row examined this tick

  add (table_name, partition_name) to attempted

  -- Record the partition details (parent OID, partition OID), then release the
  -- row lock: DETACH/FINALIZE cannot run inside a transaction block.
  COMMIT

  -- MANDATORY liveness check: is any OTHER backend actively detaching this parent?
  -- Match the PARENT's SHARE UPDATE EXCLUSIVE lock (held continuously for the whole
  -- detach, including its wait phase), NOT the partition's lock and NOT bare PID
  -- liveness — so a pooled connection that ran a failed detach and returned to the
  -- pool is correctly not mistaken for a live detacher.
  SELECT 1 FROM pg_locks l
    JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype = 'relation'
      AND l.relation = <parent_oid>
      AND l.mode     = 'ShareUpdateExclusiveLock'
      AND l.granted
      AND a.pid     <> pg_backend_pid()
      AND a.state    = 'active'
      AND a.query ILIKE '%detach partition%'
    LIMIT 1

  if a row is returned:
    continue loop                  -- a live detach owns this partition; leave it untouched.
                                   -- It is in `attempted`, so it is not re-selected this tick;
                                   -- it remains eligible on a later tick.

  -- No live detach in flight. Determine the partition's inheritance state.
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

  continue loop

-- On any error (including a straggler that lost the parent-lock race against a
-- concurrent detach): log, increment parter.detach_errors_total, leave the row
-- as 'detaching'. The row is in `attempted`, so it is not retried this tick; it
-- remains eligible for recovery on a later tick while it still satisfies the
-- pre-filter.
```

Within a tick the retention pass runs the steps in order **3 → 1 → 2**: first reclaim any crash-stranded `detaching` rows (Step 3), then detach newly-expired partitions (Step 1), then drop everything that has reached `detached` (Step 2). Each step processes its eligible rows before the next begins; Steps 1 and 2 drain theirs fully, while Step 3 examines each eligible row at most once per tick (via its skip set) and leaves any row with a live in-flight detach for a later tick. Because a row Step 1 just set to `detaching` has `detach_started_at = now()`, it is *not* eligible for Step 3 this tick (or any tick before `detach_recovery_timeout` elapses), so a freshly-detached row is not double-processed; it reaches `detached` via Step 1's own post-detach `UPDATE` and is dropped by Step 2 on a subsequent tick.

The `detaching` state acts as a crash-recovery marker: if the process dies between issuing `DETACH CONCURRENTLY` and confirming completion, the row eventually ages past `detach_recovery_timeout` and Step 3 picks it up, confirms via the mandatory liveness check that no `DETACH` is in flight on the parent, then checks `pg_inherits` and its `inhdetachpending` column to determine the partition's state — fully detached (no `pg_inherits` row, advance straight to `detached`), pending detach (`inhdetachpending = true`, complete the operation with `DETACH PARTITION ... FINALIZE`), or still fully attached (`inhdetachpending = false`, re-issue `DETACH PARTITION ... CONCURRENTLY`). On error, increment `parter.detach_errors_total` counter.

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
    // on every reconciliation tick. Callers MUST include enough
    // date components to guarantee unique names (e.g. for Daily, include
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
    // the value is added to the offset formula. Must be >= 0 when set.
    Instance *int

    // How long a partition may remain in the `detaching` state before the
    // retention crash-recovery pass (Step 3) treats it as a candidate for
    // reclamation. This is a cheap pre-filter, not the safety mechanism:
    // Step 3 also runs a mandatory pg_locks/pg_stat_activity liveness check
    // and never reclaims a row whose parent still carries an active DETACH,
    // so the timeout cannot cause two instances to race the same detach.
    // Set it comfortably above typical `DETACH PARTITION ... CONCURRENTLY`
    // completion time so steady-state recovery rarely reaches the liveness
    // check; too high merely delays recovery after a genuine crash. Zero
    // selects the default. Default: 10 minutes.
    DetachRecoveryTimeout time.Duration
}

// EnsureReady synchronously creates any overdue partitions and seeds
// pending rows for all registered tables. Call at service startup or
// from a migration tool before the service takes traffic. Idempotent.
func EnsureReady(ctx context.Context, cfg Config) error

// Start validates Config and verifies that every registered table has a
// DEFAULT partition (panicking if any is missing or Config is invalid),
// then launches the background goroutine and returns. The goroutine runs
// until ctx is cancelled. Safe to call from multiple replicas — SKIP LOCKED
// coordinates.
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
- If `Instance` is non-nil, `*Instance` must be ≥ 0 (a negative ordinal would produce an ill-defined offset).
- `DetachRecoveryTimeout` must be ≥ 0 (0 selects the 10-minute default).

In addition to these in-memory checks, both entry points perform one catalog check before proceeding: each registered table MUST have a DEFAULT partition. `EnsureReady` verifies this and returns an error if a DEFAULT is absent; `Start` performs the same verification and panics. The check is `SELECT EXISTS(SELECT 1 FROM pg_partitioned_table WHERE partrelid = <parent>::regclass AND partdefid <> 0)`. See the DEFAULT Partition section.

## EnsureReady Startup Hook

`EnsureReady` is the synchronous counterpart to the background goroutine. It runs at service boot (or from a migration tool) and:

1. Creates the configured schema if it does not exist (`CREATE SCHEMA IF NOT EXISTS <configured_schema>`).
2. Creates the `parter.managed_partitions` table if it does not exist.
3. Applies idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migrations for any columns introduced in newer library versions (e.g. `fail_count`, `detach_started_at`) — see Schema Versioning. A table created fresh in step 2 already has them; this step upgrades tables created by an older library version.
4. For each registered table, verifies a DEFAULT partition exists on the parent (`SELECT EXISTS(SELECT 1 FROM pg_partitioned_table WHERE partrelid = <parent>::regclass AND partdefid <> 0)`); returns an error if it is absent. The library does not create the DEFAULT — its presence is required and is the operator's responsibility (see DEFAULT Partition).
5. For each registered table, runs the declarative reconciliation algorithm: computes the desired set of partitions from the current period boundary through `Lookahead` periods into the future, diffs against existing metadata rows, and inserts `pending` rows for any missing periods.
6. Executes the create pass synchronously — creates any partitions whose period has already begun.

This handles cold start: if the service was down for an extended period, `EnsureReady` drains all overdue partitions it can — subject to the concurrency note below — before the service accepts traffic, then the background goroutine handles steady-state.

**Note on concurrent instances:** Because the create pass uses `FOR UPDATE SKIP LOCKED`, if another instance's background goroutine holds a lock on a pending row when `EnsureReady` runs, that row is skipped. This skip is intentional, not incidental: the contesting instance is actively creating that partition, and `FOR UPDATE SKIP LOCKED` is what lets concurrent instances coordinate without blocking (consistent with the lock-free coordination model). By design, `EnsureReady` guarantees only that all *uncontested* pending rows are drained when it returns — not that *every* partition exists, since a row locked by a peer is left to that peer. For a strict all-or-nothing guarantee (e.g. a migration tool that must finish before traffic), run `EnsureReady` when no background goroutines are active, so no rows are contested.

## DEFAULT Partition

Every managed table MUST have a DEFAULT partition as a safety net. If the maintenance goroutine fails (panic loop, pool exhaustion) while the rest of the service continues writing, inserts land in the DEFAULT partition instead of erroring. The DEFAULT partition's contents are not eligible for retention — they require manual investigation to determine why maintenance stopped and where the rows belong.

The library does not create the DEFAULT partition — it is part of the table's initial DDL, declared alongside the table itself — but it requires one. `EnsureReady` verifies, for each registered table, that a DEFAULT partition is defined (`SELECT EXISTS(SELECT 1 FROM pg_partitioned_table WHERE partrelid = <parent>::regclass AND partdefid <> 0)`) and returns an error if it is absent; `Start`, which has no error return, verifies the same condition synchronously before launching the goroutine and panics if it is unmet — consistent with its other Config-validation failures. This makes the safety net a precondition checked once at startup, when an operator is present, rather than a silent assumption discovered only when maintenance stalls.

**Interaction with the create pass.** Creating a partition with `CREATE TABLE ... PARTITION OF ... FOR VALUES FROM (range_start) TO (range_end)` briefly takes `ACCESS EXCLUSIVE` on the parent (as any partition creation does); when a DEFAULT partition exists, PostgreSQL additionally scans the DEFAULT — locking it `ACCESS EXCLUSIVE` — to prove no existing row belongs in the new partition's range. If the DEFAULT is empty the scan is trivial and both locks are momentary. If the DEFAULT holds any row whose key falls in the new range, the `CREATE` fails deterministically with `ERROR: updated partition constraint for default partition "..." would be violated by some row` (SQLSTATE 23514, `check_violation`). This is a deterministic create failure: it advances `fail_count` and, after 5 attempts, parks the row as `failed` (surfaced on the `parter.failed` gauge). The library never moves or deletes rows out of the DEFAULT — that is an operator data-placement decision. Recovery: investigate why rows for that period landed in the DEFAULT (maintenance fell behind), relocate them into the correct target relation, then reset the parked row with the circuit-breaker reset SQL (`UPDATE ... SET status = 'pending', fail_count = 0`).

## Observability

Observability runs as a separate self-healing goroutine on the same offset scheme as the maintenance goroutine (offset minus 1 second, staggering the two goroutines' tick starts). It queries the metadata table once per minute, maintains in-process counters and gauges, and exposes them through two integration surfaces: a Prometheus-compatible pull channel and an OpenTelemetry collector push configuration.

The observability goroutine is started by `Start()` alongside the maintenance goroutine. It runs unconditionally — even if neither `MetricsChan` nor `ConfigureOtel` is called — maintaining the atomic gauges that `MetricsChan` and `ConfigureOtel` consume. `MetricsChan` does not spawn a separate goroutine; it returns a channel that the already-running observability goroutine writes to each tick.

### Observability Goroutine

The observability goroutine follows the same panic-proof pattern as the maintenance goroutine. It reuses the maintenance goroutine's computed offset minus 1 second. The 1-second shift staggers the two goroutines' tick *starts* so they do not begin together; it does not guarantee their executions never overlap (a maintenance tick can run far longer than 1 second). That is fine: the observer's tick is a single read-only snapshot `SELECT` that neither takes nor waits on the `FOR UPDATE` row locks the maintenance pass holds, so the two never contend on the metadata table even when they overlap. If the maintenance offset is less than 1 second, the subtraction wraps modularly within the 60-second window (e.g. maintenance offset 0.3s → observability offset 59.3s).

Each tick:

1. Query `managed_partitions` for all rows.
2. Compute derived state:
   - Per-table count of `pending` rows where `range_start <= now()` (overdue creates).
   - Per-table count of `created` rows where `range_end < now() - retention_interval` (overdue drops). Skipped for tables with `Retention = 0` (no retention configured).
   - Per-table headroom: `max(range_end)` over `created` rows minus `now()` (the `parter.headroom_seconds` gauge).
3. Detect events by combining metadata diffs with atomic counter changes. Status transitions (e.g. `pending` → `created`, `created` → `detaching`, `pending` → `failed`) are visible in the metadata diff. But most individual failures do **not** change status — a failing `pending` row stays `pending` (only its `fail_count` advances) until the fifth failure flips it to `failed` — so error events are detected by comparing the atomic error counters (`create_errors_total`, `detach_errors_total`, `drop_errors_total`) against their previous-tick values; the `pending` → `failed` transition is additionally visible in the diff.
4. Update in-process counters and gauges.
5. Emit events to the Prometheus channel and/or OpenTelemetry collector.

### In-Process State

The maintenance goroutine updates shared `atomic.Int64` counters directly as it completes each operation (see counter increment steps in the create pass and retention pass pseudocode above). The observability goroutine reads these atomics plus the metadata table to produce the full metrics snapshot.

Each metric has one canonical identity but is spelled differently on each surface, because the two surfaces have incompatible name character sets. The tables below list the **OpenTelemetry instrument name** (dotted, e.g. `parter.created_total`). Prometheus metric names must match `[a-zA-Z_:][a-zA-Z0-9_:]*` and **cannot contain dots**, so the Prometheus text exposition emits the same metric with each dot replaced by an underscore (e.g. `parter_created_total`), following the standard OpenTelemetry→Prometheus name translation. Thus `ConfigureOtel` reports `parter.*` names and `MetricsChan` emits `parter_*` names; the pseudocode above names the canonical (dotted) instruments.

**Counters (monotonic, cumulative):**

| OTel instrument (Prometheus form) | Labels | Description |
|--------|--------|-------------|
| `parter.created_total` (`parter_created_total`) | `table` | Partitions successfully created since process start |
| `parter.dropped_total` (`parter_dropped_total`) | `table` | Partitions successfully dropped since process start |
| `parter.create_errors_total` (`parter_create_errors_total`) | `table` | Create attempts that failed (includes attach/boundary-verification failures) |
| `parter.detach_errors_total` (`parter_detach_errors_total`) | `table` | `DETACH ... CONCURRENTLY`/`FINALIZE` attempts that failed (Step 1 and Step 3) |
| `parter.drop_errors_total` (`parter_drop_errors_total`) | `table` | `DROP TABLE` attempts that failed (Step 2) |
| `parter.observer_errors_total` (`parter_observer_errors_total`) | | Observability goroutine query failures |

**Gauges (point-in-time):**

| OTel instrument (Prometheus form) | Labels | Description |
|--------|--------|-------------|
| `parter.pending` (`parter_pending`) | `table` | Current count of `pending` rows |
| `parter.overdue` (`parter_overdue`) | `table` | Pending rows where `range_start <= now()` — creation is overdue |
| `parter.headroom_seconds` (`parter_headroom_seconds`) | `table` | `max(range_end)` over `created` rows minus `now()`, in seconds, clamped at 0 — i.e. remaining time before writes would fall past the last created partition and hit DEFAULT. 0 means writes are already hitting DEFAULT. |
| `parter.created` (`parter_created`) | `table` | Current count of `created` rows |
| `parter.retention_overdue` (`parter_retention_overdue`) | `table` | Created rows past their retention window (0 for tables with `Retention = 0`) |
| `parter.failed` (`parter_failed`) | `table` | Current count of `failed` rows (requires manual investigation) |

The `partitions_skipped_total` counter from the original design has been removed. `SKIP LOCKED` cannot distinguish "another instance handled this row" from "there was no actionable work" — both result in zero rows returned. The `parter.pending` and `parter.overdue` gauges already surface the important signal (maintenance is falling behind).

### Prometheus Integration

```go
// MetricsChan registers a subscriber channel that emits a Prometheus-compatible
// text exposition payload once per observability tick. The caller is expected to
// drain the channel into a local variable in a goroutine and serve the most
// recent payload from its /metrics handler (Prometheus scrapes on its own
// schedule, which will not line up with the tick). The channel is unbuffered; if
// the caller is not reading at the instant of a tick, the observability goroutine
// drops that tick's payload (non-blocking send) and emits the next one a tick
// later.
//
// MetricsChan does NOT start or own the observability goroutine — that goroutine
// is owned by Start and lives for Start's context (see Observability). The ctx
// here scopes only THIS subscription: when ctx is cancelled the library stops
// sending to this channel and closes it; the observability goroutine and any
// other subscribers are unaffected. MetricsChan may be called multiple times;
// each call returns an independent channel.
//
// This function intentionally returns raw text bytes rather than
// Prometheus collector objects — it avoids importing the Prometheus
// client library as a hard dependency of go-pg-parter.
func MetricsChan(ctx context.Context, cfg Config) <-chan []byte
```

The emitted `[]byte` is a complete Prometheus text exposition block — `# HELP`, `# TYPE`, and metric lines, with metric names in the Prometheus underscore form (e.g. `parter_created_total`) — suitable for concatenation into an existing `/metrics` response. The caller owns the HTTP handler; the library does not import `net/http`.

### OpenTelemetry Integration

```go
// ConfigureOtel registers the library's metrics and trace spans with
// the provided OpenTelemetry MeterProvider and TracerProvider. Once
// configured, every maintenance tick emits a span covering the
// create and retention passes, and all counters and gauges are reported
// through the OTel pipeline (push to collector, OTLP export, etc.).
//
// Must be called after Start. Calling before Start panics (the
// observability goroutine is not yet running). Must be called at most
// once; a second call panics to prevent duplicate OTel instrument
// registration. If never called, the library emits no OTel data and
// no OTel packages are imported at build time.
func ConfigureOtel(meter metric.MeterProvider, tracer trace.TracerProvider)
```

When configured:

- Each maintenance tick is wrapped in a span (`parter.tick`) with child spans for the `parter.create` pass and the `parter.retention` pass (the latter covering detach, drop, and recovery). Span attributes include `table`, `partition_name`, `range_start`, `range_end`, and outcome (`created`, `detached`, `dropped`, or `error`). There is no `skipped` outcome: `SKIP LOCKED` returning no row is indistinguishable from "no work" and produces no per-partition span (consistent with the removal of the skipped counter).
- All counters and gauges from the in-process state are registered as OTel instruments and reported on the provider's export interval.
- The observability goroutine's metadata query is wrapped in a span (`parter.observe`) for collector-side latency tracking.

OTel is an optional dependency. If the caller never calls `ConfigureOtel`, the library does not import or reference any OTel packages at build time (the integration surface is in a separate sub-package (`parter/otel`)).

### Shared State Between Goroutines

The maintenance goroutine increments atomic counters on each create/drop/error outcome. The observability goroutine reads these atomics plus the metadata table to produce the full metrics snapshot. No mutex is needed — the counters are `atomic.Int64` and the metadata table query is its own transaction.

## Coordination Properties

- **Multi-instance safe:** `FOR UPDATE SKIP LOCKED` means one instance grabs a pending row, all others skip it and sleep. No blocking, no retry storms, no wasted work.
- **Idempotent:** `CREATE TABLE IF NOT EXISTS`, `DROP TABLE IF EXISTS`, `ON CONFLICT DO NOTHING` — create and drop operations are safe to repeat. `DETACH PARTITION ... CONCURRENTLY` is the exception: it is *not* idempotent (re-issuing it against an already-detached or pending-detach partition errors), and Step 1 releases the row lock at `COMMIT` before the detach runs (the detach cannot run inside a transaction), so the lock alone cannot serialize it. Duplicate detaches are prevented by three mechanisms together: (1) the `detach_started_at` + `detach_recovery_timeout` gate keeps steady-state recovery from even considering a live owner's row; (2) before reclaiming, Step 3 runs a mandatory `pg_locks`/`pg_stat_activity` liveness check and skips any row whose parent still carries an active `DETACH`'s `SHARE UPDATE EXCLUSIVE` lock; and (3) when a crashed owner's row is reclaimed, the `inhdetachpending` check selects the correct completion (`FINALIZE` vs re-issue) instead of blindly repeating the command. This is absolute, not best-effort: PostgreSQL permits only one detach per parent at a time, so even a straggler that resumes serializes on the parent lock rather than running concurrently, and the loser fails harmlessly — two destructive detaches never overlap. See "Residual risk — closed by a mandatory liveness check" under Step 3.
- **Self-healing:** The declarative reconciliation model recomputes the desired state from scratch each tick, so a manually-deleted metadata row for a current or future period is re-inserted on the next tick — there is no seed chain to break and thus no single point of failure. Past periods (before the current period boundary) are intentionally not re-inserted — by design, so reconciliation never resurrects rows the retention pass has dropped; out-of-band deletion of past-period rows is unsupported. See the caveat in the Declarative Reconciliation section.
- **Atomic:** DDL + metadata update in one transaction (create pass). Retention uses a two-step detach-then-drop to avoid parent-table lock escalation, with crash-safe state transitions through `detaching` → `detached`.
- **Observable:** The `managed_partitions` table is the source of truth. A `SELECT WHERE status = 'pending' AND range_start < now()` query that returns rows means maintenance is behind — suitable for an alerting probe.
