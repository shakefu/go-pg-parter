# go-pg-parter — Implementation Contract (shared ground truth for all agents)

This file pins the architecture decisions every agent MUST follow so independently
written files integrate cleanly. The authoritative *requirements* are the `spec/*.yass.yaml`
files; this file pins *how* we realize them in Go. If this contract appears to contradict a
spec, the spec wins — stop and report.

Module: `github.com/shakefu/go-pg-parter`  •  Root package: `parter`  •  Go 1.26  •  Target: PostgreSQL 18.

> **RETENTION REDESIGN (resolved conflict):** PostgreSQL forbids `DETACH PARTITION ... CONCURRENTLY`
> whenever the parent has a DEFAULT partition (SQLSTATE 55000), and every managed table is required
> to have a DEFAULT. Retention therefore uses a **plain, transactional `ALTER TABLE ... DETACH PARTITION`**
> (no CONCURRENTLY). This is atomic, so there is **no crash-recovery machinery**: no `detaching` status,
> no `detach_started_at` column, no `Config.DetachRecoveryTimeout`, no `pg_locks` liveness check, no
> `inhdetachpending`/FINALIZE, no `retentionRecover`. Lifecycle is `pending → created → detached → (dropped)`.

## 0. Repo layout / file → symbol map

Root package `parter` (files at repo root):
- `config.go`        — `RangeStrategy` (+ consts), `Table`, `Config`, `ValidateConfig`, schema helpers.
- `metadata.go`      — managed_partitions DDL constants, `PartitionStatus` (+ consts), schema-versioning migrations, `createSchema`/`createTable`/`applyMigrations`.
- `scheduler.go`     — `TickOffset`, `TickDeadline` const, `tickInterval` const, `tickLoop`, `runMaintenanceTick`, `MaintenanceLoop`.
- `reconcile.go`     — period math (`truncatePeriod`, `advancePeriod`, `desiredPeriods`), `reconcile`.
- `create.go`        — `createPass`, circuit-breaker (`advanceBreaker`), error classification.
- `retention.go`     — `retentionPass`, `retentionDetach`(Step1), `retentionDrop`(Step2), `retentionInterval`.
- `observability.go` — `observabilityGoroutine`, gauge computation, Prometheus text formatting, `MetricsChan`, metric name tables.
- `ensure_ready.go`  — `EnsureReady`, DEFAULT-partition check helper.
- `start.go`         — `Start`.
- `doc.go`           — package doc + operator-facing reset SQL documented.

Internal shared state: `internal/core/` package `core`:
- `core.go` — `Counters`, `TableCounters`, `GaugeSnapshot`, `TableGauges`, `State`, `TableInfo`, `Tracer`/`Span` interfaces, noop tracer, `Global atomic.Pointer[State]`, subscriber registry.
- `internal/core` MUST NOT import OpenTelemetry or `parter`. No import cycles.

OTel sub-package: `otel/` package `parterotel` → import `github.com/shakefu/go-pg-parter/otel`:
- `otel.go` — `ConfigureOtel(mp metric.MeterProvider, tp trace.TracerProvider)`, OTel `Tracer` impl, instrument registration. ONLY this package imports the OTel SDK/API. The root `parter` package MUST NOT import any `go.opentelemetry.io/*` package (satisfies "ConfigureOtel never called ⇒ no OTel at build time").

## 1. Two distinct schemas — do not confuse them
- **Config.Schema** (default `"parter"`): schema holding the `managed_partitions` metadata table. Helper `cfg.schema()` returns `Config.Schema` or `"parter"` if empty. The metadata table is `cfg.schema()+".managed_partitions"`.
- **Parent schema**: extracted from `Table.Name` (e.g. `"contacts.contact_state_events"` → `"contacts"`; split on the first dot, schema=before, rel=after; no dot ⇒ schema `"public"`). Partition DDL and the `partition_name` column live in the **parent** schema.
- `partition_name` column value = `parentSchema + "." + Namer(range_start)` (schema-qualified). Used verbatim in CREATE/DETACH/DROP.

**SQL schema literal:** specs quote `parter.managed_partitions` verbatim. That is the *default-schema rendering*. Always render the metadata table as `cfg.schema()+".managed_partitions"`; with the default Schema this reproduces the quoted literal exactly.

## 2. internal/core API (authoritative signatures)

```go
package core

type TableInfo struct {
    Name         string // schema-qualified parent (Table.Name)
    HasRetention bool   // Retention > 0
}

type TableCounters struct {
    CreatedTotal      atomic.Int64
    DroppedTotal      atomic.Int64
    CreateErrorsTotal atomic.Int64
    DetachErrorsTotal atomic.Int64
    DropErrorsTotal   atomic.Int64
}

type Counters struct {
    perTable            map[string]*TableCounters // built once at Start; read-only after → no mutex
    ObserverErrorsTotal atomic.Int64              // no label
}
func NewCounters(tables []string) *Counters
func (c *Counters) Table(name string) *TableCounters

type TableGauges struct {
    Pending          int64
    Overdue          int64
    HeadroomSeconds  float64
    Created          int64
    RetentionOverdue int64
    Failed           int64
}
type GaugeSnapshot struct{ PerTable map[string]TableGauges }

type PartitionAttrs struct {
    Table, PartitionName string
    RangeStart, RangeEnd time.Time
    Outcome              string // "created"|"detached"|"dropped"|"error" — never "skipped"
}
type Span interface { End(); RecordError(err error) }
type Tracer interface {
    StartTick(ctx context.Context) (context.Context, Span)
    StartCreate(ctx context.Context) (context.Context, Span)
    StartRetention(ctx context.Context) (context.Context, Span)
    StartObserve(ctx context.Context) (context.Context, Span)
    PartitionSpan(ctx context.Context, a PartitionAttrs) // creates+ends a child span itself
}

type State struct {
    Schema   string
    Tables   []TableInfo
    Counters *Counters
    // unexported: gauges atomic.Pointer[GaugeSnapshot]; tracer holder; subscriber list+mutex; started/otelConfigured atomic.Bool
}
func (s *State) SetGauges(g *GaugeSnapshot)
func (s *State) Gauges() *GaugeSnapshot
func (s *State) Tracer() Tracer
func (s *State) SetTracer(t Tracer)
func (s *State) MarkOtelConfigured() bool
func (s *State) Subscribe(ctx context.Context) <-chan string
func (s *State) Broadcast(payload string)

var Global atomic.Pointer[State]
```
(Implemented in Wave A; do not change its surface without updating this contract.)

## 3. config.go

```go
type RangeStrategy int
const ( Monthly RangeStrategy = iota; Weekly; Daily ) // Monthly=0 Weekly=1 Daily=2
func (s RangeStrategy) valid() bool

type Table struct {
    Name      string
    Strategy  RangeStrategy
    Lookahead int
    Retention int
    Namer     func(rangeStart time.Time) string
}
type Config struct {
    Pool     *pgxpool.Pool
    Tables   []Table
    Schema   string
    Instance *int
}
func (c Config) schema() string // Schema or "parter"
func ValidateConfig(cfg Config) error
```
`ValidateConfig` rejects: Pool nil; Tables empty; any Name empty; any Namer nil; any Lookahead<1; any Retention<0; any Strategy invalid; Instance non-nil && *Instance<0. It MUST NOT do the DEFAULT-partition catalog check. Return a non-nil descriptive `error` naming the offending field; nil when valid. (No `DetachRecoveryTimeout` field exists.)

## 4. metadata.go — verbatim DDL

`PartitionStatus` — FOUR states (no `detaching`):
```go
type PartitionStatus string
const ( StatusPending PartitionStatus="pending"; StatusCreated="created"; StatusDetached="detached"; StatusFailed="failed" )
```
Lifecycle: `pending → created → detached → (row deleted on drop)`; `pending → failed` (circuit breaker).
DDL (render `<schema>` = `cfg.schema()`):
```
CREATE SCHEMA IF NOT EXISTS <schema>
```
```
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
Idempotent additive migrations (SchemaVersioning — for tables created by older versions; no version marker):
```
ALTER TABLE <schema>.managed_partitions ADD COLUMN IF NOT EXISTS fail_count INT NOT NULL DEFAULT 0
```
(No `detach_started_at` column.)

## 5. scheduler.go

```go
const tickInterval = 60 * time.Second
const TickDeadline = 55 * time.Second // exported, non-configurable; invariant TickDeadline < tickInterval
func TickOffset(startupSecondsWithFraction float64, instance *int) time.Duration
```
TickOffset: `base = startupSecondsWithFraction*23 (+ float64(*instance)*23 when instance!=nil)`; `off = mod(base,60)`; round nearest ms ties-up: `ms = math.Floor(off*1000+0.5)`; wrap `ms %= 60000`; return `time.Duration(ms)*time.Millisecond`.
startupSecondsWithFraction = `float64(t.Second()) + float64(t.Nanosecond())/1e9`.

`tickLoop(ctx, start, offset, interval, work)` — epoch=start+offset; sleep to epoch; then loop: `work`; `next, n = skipMissed(epoch, n, now, interval)`; sleep to next; return on ctx done. (interval param for fast tests; production passes `tickInterval`.)

`runMaintenanceTick(ctx, cfg, deadline, st)`: `context.WithTimeout(ctx, deadline)` (prod `TickDeadline`); `defer cancel`; `defer recover()` (log + return); open `parter.tick` span; per table in Config.Tables order: reconcile → drain create → (if Retention>0) retention; finish span. On ctx expiry the in-flight txn rolls back.

`MaintenanceLoop(ctx, cfg)` — computes processStart/offset and runs the loop (independently callable per spec). In the Start path, Start computes processStart+offset once and shares it with the observability goroutine.

## 6. reconcile.go

`time.Now().UTC()`. All boundary math in UTC.
```go
func truncatePeriod(now time.Time, s RangeStrategy) time.Time
func advancePeriod(t time.Time, s RangeStrategy) time.Time
func desiredPeriods(now time.Time, s RangeStrategy, lookahead int) []Period // Period{Start,End}; len = lookahead+1
func reconcile(ctx context.Context, cfg Config, t Table) error
```
- Monthly: start=`Date(Y,M,1,0,0,0,0,UTC)`; advance=`AddDate(0,1,0)`.
- Weekly (ISO, Monday start): `wd=(int(now.Weekday())+6)%7`; start=`Date(Y,M,D,0,0,0,0,UTC).AddDate(0,0,-wd)`; advance=`AddDate(0,0,7)`.
- Daily: start=`Date(Y,M,D,0,0,0,0,UTC)`; advance=`AddDate(0,0,1)`.
- desiredPeriods returns Lookahead+1 contiguous periods (current + Lookahead ahead).
`reconcile`: `SELECT partition_name FROM <schema>.managed_partitions WHERE table_name=$1`; for each desired period whose schema-qualified partition_name is absent, `INSERT INTO <schema>.managed_partitions (table_name, partition_name, range_start, range_end, status) VALUES ($1,$2,$3,$4,'pending') ON CONFLICT (table_name, partition_name) DO NOTHING`. Start at current period only; never re-insert past periods. Shared by EnsureReady and the loop.

## 7. create.go

attempted set: in-memory `map[string]bool` of partition_name tried this tick. SQL uses `AND partition_name <> ALL($2::text[])` (table_name constant per pass; equivalent to the spec's composite `(table_name,partition_name) <> ALL(attempted)`).

Loop per table:
```
BEGIN
SELECT * FROM <schema>.managed_partitions
  WHERE table_name=$1 AND status='pending' AND partition_name <> ALL($2::text[])
  ORDER BY range_start FOR UPDATE SKIP LOCKED LIMIT 1
-- no row → COMMIT, exit pass
-- add partition_name to attempted BEFORE acting
SET LOCAL TIME ZONE 'UTC'
CREATE TABLE IF NOT EXISTS <partition_name> PARTITION OF <table_name> FOR VALUES FROM ('<range_start UTC literal>') TO ('<range_end UTC literal>')
-- verify attachment+bounds via pg_class JOIN pg_inherits (no row ⇒ not attached ⇒ FAILURE; bound text mismatch ⇒ FAILURE)
-- success: UPDATE ... SET status='created', created_at=now() WHERE (table_name,partition_name)=($1,$2); COMMIT; CreatedTotal++; PartitionSpan created; continue
```
Boundary literal: `t.UTC().Format("2006-01-02 15:04:05-07:00")`.
- ctx cancelled / deadline exceeded ⇒ ROLLBACK, exit pass, leave pending, DO NOT touch fail_count or create_errors_total; return ctx.Err().
- create OR verify failed ⇒ ROLLBACK, leave pending, CreateErrorsTotal++, PartitionSpan error. transient (55P03, 40001, conn loss) ⇒ no breaker, stop pass. deterministic (invalid range, missing/non-partitioned parent, name conflict, attach/boundary mismatch, 23514) ⇒ advance breaker, continue loop.

Circuit breaker — SEPARATE committed txn AFTER rollback (verbatim):
```
BEGIN; UPDATE <schema>.managed_partitions SET fail_count = fail_count + 1, status = CASE WHEN fail_count + 1 >= 5 THEN 'failed' ELSE status END WHERE (table_name, partition_name) = ($1,$2) AND status = 'pending'; COMMIT
```
Running total. Shared with EnsureReady. No auto-retry. Operator reset SQL (doc.go): `UPDATE <schema>.managed_partitions SET status='pending', fail_count=0 WHERE (table_name,partition_name)=(...) AND status='failed'`.

## 8. retention.go

`retentionInterval(r int, s RangeStrategy) string`: unit Monthly→"months", Weekly→"weeks", Daily→"days"; returns e.g. `3 months`; used inline as `now() - interval '3 months'`.

`retentionPass`: if `t.Retention==0` skip ENTIRELY (a zero interval would match all history). Else compute the interval and run **Step1 (detach) then Step2 (drop)**. Drain each step fully, attempting each eligible row AT MOST ONCE per tick (in-memory attempted set of partition_name) so a persistently failing row neither livelocks nor starves the rest.

Retention uses a **PLAIN** `ALTER TABLE ... DETACH PARTITION` (NOT `... CONCURRENTLY`): CONCURRENTLY is rejected by PostgreSQL when the parent has a DEFAULT partition (SQLSTATE 55000), and every managed table is required to have a DEFAULT. Plain DETACH is catalog-only (no data rewrite), takes a brief ACCESS EXCLUSIVE on the parent, runs **inside** a transaction, and is compatible with a DEFAULT partition. There is **NO** crash-recovery machinery — atomicity makes it unnecessary.

Step1 detach (`retentionDetach`): drain fully (attempted set).
```
BEGIN; SELECT * FROM <schema>.managed_partitions
  WHERE table_name=$1 AND status='created' AND range_end < now() - interval '<n> <unit>'
        AND partition_name <> ALL($2::text[])
  ORDER BY range_start FOR UPDATE SKIP LOCKED LIMIT 1
-- none → COMMIT, exit
-- add partition_name to attempted; in the SAME tx:
ALTER TABLE <table_name> DETACH PARTITION <partition_name>          -- plain, no CONCURRENTLY
UPDATE <schema>.managed_partitions SET status='detached' WHERE (table_name,partition_name)=($1,$2)
COMMIT                                                              -- detach + status flip are ATOMIC
-- success: PartitionSpan Outcome="detached"; continue
-- error: ROLLBACK (row stays 'created', partition still attached); DetachErrorsTotal++; PartitionSpan error; continue (retry next tick)
```
Crash-safe by atomicity: an interrupted/failed detach rolls back wholesale → row stays 'created' → retried next tick. No `detaching` limbo.

Step2 drop (`retentionDrop`): drain fully (attempted set), in a SEPARATE transaction from the detach so the slower DROP never holds the parent's lock.
```
BEGIN; SELECT ... WHERE table_name=$1 AND status='detached' AND partition_name <> ALL($2::text[]) ORDER BY range_start FOR UPDATE SKIP LOCKED LIMIT 1
-- none → COMMIT, exit
DROP TABLE IF EXISTS <partition_name>;
DELETE FROM <schema>.managed_partitions WHERE (table_name,partition_name)=($1,$2);
COMMIT; DroppedTotal++; PartitionSpan Outcome="dropped"
-- error: ROLLBACK; DropErrorsTotal++; leave 'detached'; retry next tick
```

NO `retentionRecover`. NO dedicated autocommit connection. NO `pg_locks` liveness check. NO `inhdetachpending`/FINALIZE. NO `Config.DetachRecoveryTimeout`. Per-partition spans via `st.Tracer().PartitionSpan(ctx, ...)`; the parent `parter.retention` span is opened by the maintenance loop.

## 9. observability.go

Metric inventory — OTel dotted name / Prometheus underscore name / type / label:
- parter.created_total / parter_created_total / counter / table
- parter.dropped_total / parter_dropped_total / counter / table
- parter.create_errors_total / parter_create_errors_total / counter / table
- parter.detach_errors_total / parter_detach_errors_total / counter / table (DETACH attempts that failed in Step 1)
- parter.drop_errors_total / parter_drop_errors_total / counter / table
- parter.observer_errors_total / parter_observer_errors_total / counter / (no label)
- parter.pending / parter_pending / gauge / table
- parter.overdue / parter_overdue / gauge / table
- parter.headroom_seconds / parter_headroom_seconds / gauge / table
- parter.created / parter_created / gauge / table
- parter.retention_overdue / parter_retention_overdue / gauge / table
- parter.failed / parter_failed / gauge / table
NO `partitions_skipped_total`.

`observabilityGoroutine(ctx, cfg, processStart time.Time, maintOffset time.Duration, st *core.State)`: obsOffset=`(maintOffset - time.Second + tickInterval) % tickInterval`; run `tickLoop` with obsOffset and a panic-proof work fn. Each tick (read-only, own txn, no FOR UPDATE; `parter.observe` span):
```
SELECT table_name, status, range_start, range_end FROM <schema>.managed_partitions
```
Per table compute: Pending=count(pending); Overdue=count(pending AND range_start<=now()); Created=count(created); Failed=count(failed); RetentionOverdue=(Retention>0 ? count(created AND range_end < now()-retention) : 0) — compute the calendar cutoff in Go via `retentionCutoff(now, t.Retention, t.Strategy)` (Monthly AddDate(0,-R,0); Weekly AddDate(0,0,-7*R); Daily AddDate(0,0,-R)); HeadroomSeconds=max(range_end over created)-now() clamped at 0 (0 if none). `st.SetGauges`; format Prometheus payload; `st.Broadcast`. On query error `ObserverErrorsTotal++`.

`MetricsChan(ctx context.Context, cfg Config) <-chan string`: `st:=core.Global.Load()`; if st!=nil return `st.Subscribe(ctx)` else a channel closed on ctx done. Unbuffered; underscore names; full `# HELP`/`# TYPE`/lines text. MUST NOT import net/http; MUST NOT start/own the observability goroutine.

`retentionCutoff(now, r, s)` is owned by observability.go (NOT retention.go, which uses the SQL interval string).

## 10. ensure_ready.go

`EnsureReady(ctx context.Context, cfg Config) error` — idempotent. Order: (1) ValidateConfig → (2) CREATE SCHEMA → (3) CREATE TABLE → (4) apply migrations → (5) DEFAULT check per table (error if absent) → (6) reconcile per table → (7) create pass synchronously per table. Uses the SAME `reconcile` and `createPass`/breaker as the loop. Guarantees only that uncontested pending rows are drained.
DEFAULT check (verbatim): `SELECT EXISTS(SELECT 1 FROM pg_partitioned_table WHERE partrelid = $1::regclass AND partdefid <> 0)` with $1=Table.Name. MUST NOT create the DEFAULT.

## 11. start.go

`Start(ctx context.Context, cfg Config)` — no error return. ValidateConfig → panic if invalid. DEFAULT check per table → panic if any absent (same EXISTS query). Build `st`; `st.SetStarted()`; `core.Global.Store(st)`. Compute `processStart=time.Now()`, `offset=TickOffset(...)`. Launch maintenance goroutine and observability goroutine (both unconditionally). Return after validation + DEFAULT check. Goroutines live until ctx cancelled. Safe for multiple replicas (FOR UPDATE SKIP LOCKED).

## 12. OTel sub-package (otel/otel.go, package parterotel)

`func ConfigureOtel(mp metric.MeterProvider, tp trace.TracerProvider)`:
- `st:=core.Global.Load()`; nil ⇒ panic (before Start). `!st.MarkOtelConfigured()` ⇒ panic (second call).
- Build a `core.Tracer` from `tp.Tracer(...)`; spans `parter.tick`/`parter.create`/`parter.retention`/`parter.observe`; `PartitionSpan` = child span with attrs table, partition_name, range_start, range_end, outcome (RecordError when outcome=error). `st.SetTracer`.
- Register OTel instruments on `mp.Meter(...)`: observable Int64 counters (dotted names) reading `st.Counters`; observable gauges reading `st.Gauges()`. Labels `attribute.String("table", name)`; observer_errors_total no label.
- Only this package imports `go.opentelemetry.io/*`. Tests use the OTel SDK as test deps.

## 13. Test harness

- Pure unit tests: no build tag.
- Integration: `//go:build integration`. DSN from env `PARTER_TEST_DSN`; skip if empty. Helpers `testPool(t)`, `uniqueSchema(t, pool)`. Each test isolates: unique Config.Schema AND a unique parent schema + parent partitioned table + **DEFAULT partition** (retention tests MUST include the DEFAULT — plain DETACH is compatible, and its presence proves the 55000 conflict is resolved).
- Run unit: `go test ./...`  •  Run all: `PARTER_TEST_DSN=postgres://parter:parter@localhost:55432/parter?sslmode=disable go test -tags integration ./...`
- Every test names the spec + section it covers in a comment.

## 14. Cross-cutting rules for all agents
- Use LSP via ToolSearch (`select:LSP`) for symbol navigation; prefer it over text search.
- Reproduce quoted SQL/constants/SQLSTATE/lock-mode strings verbatim (modulo the schema templating in §1).
- `go vet ./...` clean; `gofmt`ed. No `panic` except where a spec mandates (Start invalid config / missing DEFAULT; ConfigureOtel before Start / twice).
- If a spec obligation cannot be met or two specs conflict, STOP and report — do not invent.
