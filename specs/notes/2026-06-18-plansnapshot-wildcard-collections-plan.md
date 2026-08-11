# Implementation Plan: `planSnapshot` Wildcard Collection Filters

This historical plan added prefix matching to the manifest planner, exhaustive and equivalence tests, XRPC wildcard validation, integration/fuzz coverage, and lexicon regeneration. It preserved the existing wire field rather than adding a separate wildcard parameter. Key references were `internal/manifest/plan.go`, `internal/manifest/plan_prefix_equiv_test.go`, `internal/xrpcapi/plansnapshot.go`, and `internal/xrpcapi/plansnapshot_fuzz_test.go`.
