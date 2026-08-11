# Wire atmos's `StartCursor` + `OnPageComplete` into Jetstream Backfill

This historical design added a Pebble-backed listRepos cursor so bootstrap could resume from the last fully completed page instead of rescanning from the beginning. The durability rule was to persist each next cursor only after page completion, accepting harmless replay around crashes. Key references were `internal/backfill/cursor.go`, `internal/backfill/run.go`, and `internal/backfill/run_test.go`.
