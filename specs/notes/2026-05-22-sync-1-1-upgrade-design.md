# Sync 1.1 Upgrade Design

This historical design planned the atmos Sync 1.1 upgrade, including Pebble-backed sync state, identity caching, signature verification, resync conversion, and worker shutdown/error handling. It retained archive events regardless of account takedown status and treated verifier and async failures as observable errors. Relevant paths were `internal/ingest/syncstate`, the then-live consumer code, identity-cache code, and `cmd/jetstream/main.go`.
