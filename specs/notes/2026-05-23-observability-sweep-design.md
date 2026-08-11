# Observability Sweep Design

**Status: historical specification, pending implementation at the time.** The design replaced noisy lifecycle logging with structured metrics and spans, standardized an `obs.Observe` helper, classified verifier failures, and instrumented store and segment operations. Its goal was actionable low-cardinality telemetry without changing runtime behavior. Key references were `internal/obs/`, `internal/store/metrics.go`, `segment/metrics.go`, `segment/seal.go`, and ingest/orchestrator call sites.
