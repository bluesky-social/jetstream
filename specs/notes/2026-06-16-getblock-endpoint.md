# `getBlock` XRPC Endpoint Implementation Plan

This historical plan implemented `segment.ReadBlockFrame`, the getBlock lexicon and handler, request metrics, runtime wiring, caching/range semantics, and oracle verification. Active blocks, batch retrieval, manifest changes, and file-descriptor pinning remained intentionally out of scope. Key references were `segment/blockframe.go`, `internal/xrpcapi/getblock.go`, `internal/xrpcapi/metrics.go`, and `internal/oracle/`.
