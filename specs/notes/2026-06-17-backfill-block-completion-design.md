# Backfill Block Completion Design

This historical design fixed durability ordering between segment writes and repository completion metadata. Repositories completed within a block would be queued and marked complete in one Pebble batch only after the covering block was fsynced; crashes before that commit caused safe re-download rather than a false completion. The core references were `internal/ingest.Writer.AppendBatch` and the backfill store/handler completion path.
