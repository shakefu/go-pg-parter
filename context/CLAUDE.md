# context/

Design documents and project context for go-pg-parter.

## Documents

- `design-v3.md` — Authoritative design document. Revises v2 with fixes: Prometheus/OTel dual metric naming, circuit-breaker specification (committed `fail_count`, reset, per-tick attempt bounding), detach crash-recovery staleness gate (`detach_started_at`/`DetachRecoveryTimeout`), MetricsChan goroutine ownership, split detach/drop error counters, and assorted consistency fixes.
- `design-v2.md` — Prior revision (superseded by design-v3.md). Retained for historical reference.
- `design.md` — Original design document (superseded). Retained for historical reference.
