# Wire atmos's listRepos Cursor into Jetstream — Implementation Plan

This historical implementation plan covered cursor load/save helpers, atmos `StartCursor` and `OnPageComplete` wiring, and restart-resume integration tests. The note records the intended completed behavior: restarts skip listRepos pages already processed. Relevant paths were `internal/backfill/cursor.go`, `internal/backfill/run.go`, and `internal/backfill/run_test.go`; see also `specs/notes/2026-05-18-listrepos-cursor-wiring-design.md`.
