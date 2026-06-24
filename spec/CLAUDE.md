# spec/

YASS v1 specification tree for `go-pg-parter`. Each file is one intended Go file; each spec document is one public symbol or named internal behavior. Source of truth: `../context/design-v3.md`. Files use the `# yaml-language-server` schema modeline and validate against the yass v1 JSON Schema; cross-file refs use the `./name@Spec::SLOT` relative form.

## Files (implementation order)

- `root.yass.yaml` — `parter`: library entry point; USES/SEE every module spec below, reaching the whole tree.
- `config.yass.yaml` — `RangeStrategy`, `Table`, `Config` public types and `ValidateConfig` in-memory validation.
- `metadata.yass.yaml` — `managed_partitions` DDL, the four `PartitionStatus` states, idempotent `SchemaVersioning`.
- `scheduler.yass.yaml` — `TickOffset` (formula, round-to-ms, fixed epoch) and `MaintenanceLoop` (panic-recover, 55s `TickDeadline`, missed-tick skip).
- `reconcile.yass.yaml` — `Reconcile`: UTC desired-set diff, `INSERT ... ON CONFLICT DO NOTHING`, past-period asymmetry, Namer-collision caveat.
- `create.yass.yaml` — `CreatePass` (attach-verify, single-txn success) and `CircuitBreaker` (separate-txn `fail_count`, 5-failure park, 23514, reset SQL).
- `retention.yass.yaml` — `RetentionPass` (skip when Retention 0, step order detach→drop), `RetentionDetach` (atomic plain `DETACH PARTITION`, compatible with the parent's DEFAULT), `RetentionDrop`.
- `ensure-ready.yass.yaml` — `EnsureReady`: schema/table/migrations, DEFAULT precondition (error), reconcile + synchronous create, uncontested-only guarantee.
- `start.yass.yaml` — `Start`: validate + verify DEFAULT (panic), launch maintenance and observability goroutines, multi-instance safe.
- `observability.yass.yaml` — `ObservabilityGoroutine`, `Metrics` (dual OTel/Prometheus names), `MetricsChan`, `ConfigureOtel`.
- `coordination.yass.yaml` — `Coordination`: the five cross-cutting INVARIANTs other specs CONFORMS/SEE.
