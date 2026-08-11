# `getBlock` XRPC Endpoint Design

**Status: approved for planning.** This design proposed an immutable octet-stream XRPC endpoint for fetching one sealed compressed block frame, with ETag, range, conditional-request, validation, and observability behavior. It deliberately opened segment files fresh as a corruption guard and excluded active blocks, batching, and descriptor pinning. References were `internal/xrpcapi/getblock.go`, `internal/xrpcapi/server.go`, `segment.ReadBlockFrame`, `internal/jetstreamd/runtime.go`, and `internal/oracle/`.
