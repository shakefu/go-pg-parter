# go-pg-parter

>[!WARNING]
> This is not a real project. I mean it is. It probably works. It was an experiment
> in one-shot agentic development. Use at your own risk. Have it for free if you want
> it, it's pubilc domain now.

`go-pg-parter` is a PostgreSQL RANGE partition–maintenance library for Go (module
`github.com/shakefu/go-pg-parter`, root package `parter`).

## Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Install](#install)
- [Prerequisite: a DEFAULT partition](#prerequisite-a-default-partition)
- [Quick start](#quick-start)
- [Configuration reference](#configuration-reference)
- [EnsureReady vs Start](#ensureready-vs-start)
- [How it works](#how-it-works)
- [Observability](#observability)
- [Circuit breaker and manual recovery](#circuit-breaker-and-manual-recovery)

## Overview

`parter` maintains time-ranged partitions on parent tables declaratively. A
background goroutine reconciles a desired set of partitions on each tick: it
creates partitions ahead of time and, optionally, detaches and drops partitions
that have aged past a retention window.

All coordination is lock-free across instances via `FOR UPDATE SKIP LOCKED` on a
metadata table, so multiple replicas may run concurrently against one database
safely. The single source of truth is the `managed_partitions` metadata table,
which lives in the configured `Config.Schema` (default `"parter"`). Each managed
partition is exactly one row in exactly one of four statuses: `pending`,
`created`, `detached`, or `failed`.

## Requirements

- Go 1.26 (from `go.mod`).
- PostgreSQL. The library targets and was validated on PostgreSQL 18. Retention
  uses a plain transactional `ALTER TABLE ... DETACH PARTITION` (not the
  concurrent variant), which is what keeps it compatible with the required
  DEFAULT partition — the concurrent variant is rejected with SQLSTATE 55000
  while a DEFAULT partition exists.
- The caller supplies a `*pgxpool.Pool` from pgx v5
  (`github.com/jackc/pgx/v5 v5.10.0`).

OpenTelemetry support is optional and lives in a separate sub-package; the root
`parter` package imports no OpenTelemetry code unless you import that sub-package
(see [Observability](#observability)).

## Install

```sh
go get github.com/shakefu/go-pg-parter
```

The root package is imported as `parter`:

```go
import "github.com/shakefu/go-pg-parter"
```

OpenTelemetry users also import the sub-package (package name `parterotel`):

```go
import parterotel "github.com/shakefu/go-pg-parter/otel"
```

## Prerequisite: a DEFAULT partition

Every managed parent table MUST be RANGE-partitioned and MUST already have a
DEFAULT partition that the operator creates. The library checks for the DEFAULT
partition's presence and never creates it. Both `EnsureReady` and `Start` run
the same verbatim catalog check:

```sql
SELECT EXISTS(SELECT 1 FROM pg_partitioned_table WHERE partrelid = $1::regclass AND partdefid <> 0)
```

`EnsureReady` returns an error if the DEFAULT is absent; `Start` panics.

Why the DEFAULT is required:

- Writes that fall outside any created partition land in the DEFAULT instead of
  erroring. This keeps the application writeable even if maintenance falls
  behind. (The `parter_headroom_seconds` gauge reaching 0 means writes are
  already hitting the DEFAULT.)
- The plain transactional `DETACH PARTITION` used by the retention pass is
  compatible with a parent that has a DEFAULT partition; the concurrent variant
  is not.

Example operator DDL creating a RANGE-partitioned parent and its DEFAULT
partition:

```sql
CREATE TABLE events (
    id     BIGINT       NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload     JSONB
) PARTITION BY RANGE (occurred_at);

-- The DEFAULT partition the operator must create; parter never creates it.
CREATE TABLE events_default PARTITION OF events DEFAULT;
```

## Quick start

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	parter "github.com/shakefu/go-pg-parter"
)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://localhost:5432/app")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	cfg := parter.Config{
		Pool: pool,
		Tables: []parter.Table{
			{
				Name:      "public.events",
				Strategy:  parter.Monthly,
				Lookahead: 3, // current month + 3 ahead = 4 partitions
				Retention: 6, // drop partitions older than 6 months
				Namer: func(rangeStart time.Time) string {
					return "events_" + rangeStart.Format("2006_01")
				},
			},
		},
	}

	// Synchronous, idempotent startup: creates the metadata schema and table,
	// runs migrations, verifies the DEFAULT partition, reconciles, and drains
	// uncontested pending creates. Returns an error on failure.
	if err := parter.EnsureReady(ctx, cfg); err != nil {
		log.Fatal(err)
	}

	// Launch background maintenance + observability goroutines and return.
	// They run until ctx is cancelled. Start panics on an invalid Config or a
	// missing DEFAULT partition.
	parter.Start(ctx, cfg)

	select {} // keep the process alive
}
```

## Configuration reference

### `Config`

```go
type Config struct {
	Pool     *pgxpool.Pool
	Tables   []Table
	Schema   string
	Instance *int
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `Pool` | `*pgxpool.Pool` | The pgx v5 connection pool. Required, must be non-nil. |
| `Tables` | `[]Table` | The set of managed tables. At least one entry. |
| `Schema` | `string` | Schema for the `managed_partitions` metadata table. Defaults to `"parter"` when empty. |
| `Instance` | `*int` | Optional instance ordinal; `nil` means not provided. When set it must be `>= 0`. Used to stagger the per-instance tick offset across replicas. |

### `Table`

```go
type Table struct {
	Name      string
	Strategy  RangeStrategy
	Lookahead int
	Retention int
	Namer     func(rangeStart time.Time) string
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `Name` | `string` | Fully-qualified parent table name, e.g. `"contacts.contact_state_events"`. Required, non-empty. The name is split on the FIRST dot into `schema.relation`; with no dot the relation is resolved as schema `public`. |
| `Strategy` | `RangeStrategy` | The partition period unit (`Monthly`, `Weekly`, `Daily`). |
| `Lookahead` | `int` | Count of periods to pre-create ahead of now. Must be `>= 1`. The desired set is the current period plus `Lookahead` periods ahead, so `Lookahead = 3` Monthly yields 4 partitions total (current month + 3 ahead). |
| `Retention` | `int` | Count of periods after which partitions are dropped. Must be `>= 0`. `0` means never drop and skips the retention pass entirely. |
| `Namer` | `func(rangeStart time.Time) string` | Maps a period's range start time to the unqualified partition name. Required, non-nil. Must produce a unique name per distinct period (the partition name is half of the metadata primary key). |

`ValidateConfig(cfg Config) error` validates a `Config` in memory before any
database operation and returns a descriptive error naming the offending field,
or `nil` when valid. It does not perform the DEFAULT-partition catalog check.

### `RangeStrategy`

```go
const (
	Monthly RangeStrategy = iota // 0
	Weekly                       // 1
	Daily                        // 2
)
```

Period boundaries are always computed in UTC:

| Strategy | Period span | Period start boundary |
| --- | --- | --- |
| `Monthly` | one calendar month | first day of the calendar month, 00:00:00 UTC |
| `Weekly` | one ISO week (Monday start) | the preceding Monday, 00:00:00 UTC |
| `Daily` | one calendar day | the calendar day start, 00:00:00 UTC |

Each period is the half-open range `[Start, End)`.

## EnsureReady vs Start

```go
func EnsureReady(ctx context.Context, cfg Config) error
func Start(ctx context.Context, cfg Config)
func MaintenanceLoop(ctx context.Context, cfg Config)
```

### `EnsureReady`

Synchronous, idempotent startup hook. It executes in this order:

1. `ValidateConfig(cfg)` — returns the error if non-nil, before any DB work.
2. `CREATE SCHEMA IF NOT EXISTS <schema>`.
3. `CREATE TABLE IF NOT EXISTS <schema>.managed_partitions (...)`.
4. Apply idempotent additive column migrations.
5. For each table: verify a DEFAULT partition exists; return an error if absent
   (it does not create the DEFAULT).
6. For each table: reconcile (seed `pending` rows for the desired periods).
7. For each table: run the create pass synchronously.

It is safe to call repeatedly and returns an error on failure. The create pass
uses `FOR UPDATE SKIP LOCKED`, so on return `EnsureReady` guarantees only that
all UNCONTESTED pending rows are drained — a row currently locked by a concurrent
instance is intentionally skipped, not an error.

### `Start`

Validates the `Config`, then verifies that every registered parent table has a
DEFAULT partition, then launches the background maintenance and observability
goroutines and returns. It has no error return: an invalid `Config` or a
registered table missing its DEFAULT partition are reported by panicking. These
are the only panics the root package emits. `Start` does not create the metadata
schema/table or the parent DEFAULT — that is `EnsureReady`'s and the operator's
responsibility.

Both goroutines run until `ctx` is cancelled. `Start` is safe to call from
multiple replicas concurrently; the maintenance passes coordinate via
`FOR UPDATE SKIP LOCKED`, so no extra coordination is needed.

### `MaintenanceLoop`

The lower-level, independently-callable maintenance loop. It accepts an
already-validated `Config`, does not re-validate it, and runs the background
maintenance loop until `ctx` is cancelled. When launched under `Start` it reuses
the shared state; called on its own it builds a fresh state so counters and the
no-op tracer are available.

## How it works

### Metadata table

`managed_partitions` lives in `Config.Schema` (default `parter`). Its definition,
verbatim:

```sql
CREATE TABLE IF NOT EXISTS <schema>.managed_partitions (
    table_name TEXT NOT NULL,
    partition_name TEXT NOT NULL,
    range_start TIMESTAMPTZ NOT NULL,
    range_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ,
    fail_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (table_name, partition_name)
)
```

The `status` column takes one of four `PartitionStatus` values:

| Constant | Value | Meaning |
| --- | --- | --- |
| `StatusPending` | `"pending"` | needs to be created and attached |
| `StatusCreated` | `"created"` | exists and is attached to the parent |
| `StatusDetached` | `"detached"` | detached from the parent and awaiting `DROP TABLE` |
| `StatusFailed` | `"failed"` | creation failed repeatedly and the row is parked, requiring manual intervention |

### Lifecycle

A row moves `pending → created → detached → (dropped)`, where the final drop
deletes the metadata row entirely. A row that fails to create deterministically
enough times moves `pending → failed`.

### Per-tick passes

Each maintenance tick processes every table in `Config.Tables` order, draining
each table fully before moving to the next, with three passes per table:

1. **reconcile** — compute the desired period set (current period plus
   `Lookahead` ahead) and seed a `pending` metadata row for any desired period
   that has no row yet (insert with `ON CONFLICT ... DO NOTHING`, so concurrent
   instances are safe).
2. **create** — drain `pending` rows: create and verify each partition and flip
   its status to `created` in the same transaction as the DDL.
3. **retention** — only when `Retention > 0`: detach newly-expired `created`
   partitions (flipping them to `detached` atomically), then drop everything
   already `detached`.

### Tick schedule

- Maintenance ticks on a fixed `tickInterval` of 60 seconds, on a fixed-epoch
  schedule; missed ticks are skipped rather than catching up.
- `TickDeadline` is a non-configurable per-tick deadline of 55 seconds. The
  invariant `TickDeadline < tickInterval` guarantees the goroutine never misses
  more than one tick from a single hung operation.
- `TickOffset(startupSecondsWithFraction float64, instance *int) time.Duration`
  computes a per-instance startup offset into the 60-second window so that
  replicas stagger their ticks. The observability goroutine runs on the
  maintenance offset minus one second (wrapped within the 60-second window) so
  the two goroutines do not begin together.

## Observability

There are two independent integrations. Both read the same in-memory counters
and gauges that the maintenance and observability goroutines update.

### Prometheus pull

```go
func MetricsChan(ctx context.Context, cfg Config) <-chan string
```

`MetricsChan` returns an unbuffered channel that emits the full Prometheus text
exposition payload (with `# HELP`/`# TYPE`/sample lines, using the underscore
`parter_*` names) once per observability tick. The payload is suitable for
concatenation into an existing `/metrics` response. The channel is unbuffered:
ticks that the caller is not reading at the moment are dropped (non-blocking).
When `ctx` is cancelled the channel is closed.

`MetricsChan` does not import `net/http` and does not own the observability
goroutine — `Start` owns it. The caller wires the HTTP handler. A minimal
`/metrics` wiring that serves the latest payload:

```go
func metricsHandler(ctx context.Context, cfg parter.Config) http.HandlerFunc {
	ch := parter.MetricsChan(ctx, cfg)
	var (
		mu     sync.RWMutex
		latest string
	)
	go func() {
		for payload := range ch { // closes on ctx cancel
			mu.Lock()
			latest = payload
			mu.Unlock()
		}
	}()
	return func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		body := latest
		mu.RUnlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, body)
	}
}
```

Because the channel drops ticks that nobody is reading, this consumer goroutine
keeps the latest payload available for `/metrics` to return on demand.

The metric inventory (underscore names, verbatim):

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `parter_created_total` | counter | `table` | partitions successfully created since process start |
| `parter_dropped_total` | counter | `table` | partitions successfully dropped |
| `parter_create_errors_total` | counter | `table` | create attempts that failed including attach/boundary-verification failures |
| `parter_detach_errors_total` | counter | `table` | DETACH attempts that failed in Step 1 |
| `parter_drop_errors_total` | counter | `table` | DROP TABLE attempts that failed in Step 2 |
| `parter_observer_errors_total` | counter | (none) | observability goroutine query failures |
| `parter_pending` | gauge | `table` | current count of pending rows |
| `parter_overdue` | gauge | `table` | pending rows where range_start <= now() |
| `parter_headroom_seconds` | gauge | `table` | max(range_end) over created rows minus now() in seconds, clamped at 0; 0 means writes are already hitting DEFAULT |
| `parter_created` | gauge | `table` | current count of created rows |
| `parter_retention_overdue` | gauge | `table` | created rows past their retention window, 0 for tables with Retention = 0 |
| `parter_failed` | gauge | `table` | current count of failed rows requiring manual investigation |

`parter_observer_errors_total` is process-wide and emitted as a single
unlabeled line; every other metric emits one sample line per table (table names
sorted ascending), including a zero line for tables with no rows.

### OpenTelemetry push

```go
import parterotel "github.com/shakefu/go-pg-parter/otel"

func ConfigureOtel(mp metric.MeterProvider, tp trace.TracerProvider)
```

`parterotel.ConfigureOtel` wires the process-wide shared state to the given
OpenTelemetry `MeterProvider` and `TracerProvider`: it installs an OTel-backed
tracer and registers observable instruments under the dotted `parter.*` names.

- It MUST be called AFTER `Start`. Calling it before `Start`, or a second time,
  panics — those are the only two panics that package emits.
- The root `parter` package and `internal/core` stay OTel-free; OTel is only
  pulled in if you import this sub-package.

The dotted instrument names mirror the Prometheus inventory:
`parter.created_total`, `parter.dropped_total`, `parter.create_errors_total`,
`parter.detach_errors_total`, `parter.drop_errors_total`,
`parter.observer_errors_total` (no `table` label), and the gauges
`parter.pending`, `parter.overdue`, `parter.created`, `parter.retention_overdue`,
`parter.failed`, and `parter.headroom_seconds`.

Spans are emitted as a tree: `parter.tick` is the per-tick root, with child
spans `parter.create` and `parter.retention`; the observability tick opens
`parter.observe`. Each per-partition outcome is a child span (`parter.partition`)
carrying `table`, `partition_name`, `range_start`, `range_end`, and `outcome`
attributes.

## Circuit breaker and manual recovery

A partition that fails to create for a **deterministic** reason has its
`fail_count` advanced in a separate committed transaction. Deterministic
failures include:

- an invalid range,
- a missing or non-partitioned parent,
- a name conflict,
- an attach/boundary mismatch (the create appeared to succeed but the relation
  is not actually attached, or it attached with mismatched boundaries),
- a non-empty DEFAULT partition already holding rows in the new partition's
  range (SQLSTATE 23514, `check_violation`).

After five failed attempts the row is parked with status `failed` and is never
retried automatically.

**Transient** failures do not advance the breaker: `lock_not_available`
(SQLSTATE 55P03), `serialization_failure` (40001), and connection loss. The pass
stops and retries on the next tick without touching `fail_count`.

To recover a parked row, an operator resets it manually. The reset must clear
BOTH `status` and `fail_count` — clearing only one re-trips the breaker after a
single attempt. The statement, exposed as the `OperatorResetSQL` constant (it is
documentation only; the library never executes it), is:

```sql
UPDATE <schema>.managed_partitions SET status = 'pending', fail_count = 0 WHERE (table_name, partition_name) = (...) AND status = 'failed'
```

`<schema>` is the configured `Config.Schema` (default `parter`) and the
`(table_name, partition_name)` pair identifies the parked row.

For a row parked because the DEFAULT partition holds rows in the target range,
relocate those rows out of the DEFAULT before resetting the parked row;
otherwise the next create attempt fails the same way.
