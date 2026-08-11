# `inspect-all` CLI + `/status` Enrichment

This historical design unified database-wide segment aggregation for a new `jetstream inspect-all` command and the existing `/status` page. `internal/status.InspectAll` would scan sealed and transient trees, skip unsealed files, report corrupt-file warnings, and aggregate network, tree, collection, and sequence/timestamp totals. References were `internal/status/inspect_all.go`, `cmd/jetstream/inspect_all.go`, `internal/web/templates/status.html`, and the public `segment` inspection API.
