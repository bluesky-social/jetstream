# `inspect-all` CLI + `/status` Enrichment Implementation Plan

This historical plan extracted shared formatting, built `internal/status.InspectAll`, reshaped status snapshots, and added CLI and HTML renderers with golden and aggregation tests. The initial algorithm was intentionally single-threaded, prioritizing correctness and warning on corrupt files while skipping active segments. References included `internal/status/inspect_all.go`, `cmd/jetstream/inspect_all.go`, `internal/web/templates/status.html`, and http://localhost:8080/status.
