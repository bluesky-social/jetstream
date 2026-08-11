# `jetstream inspect-segment` Design

This historical design proposed a CLI for inspecting active or sealed segment files without mutating them. It added a public `segment.Inspect` result and plain-text rendering, with explicit handling for sealed metadata, active frame scans, torn tails, and corruption. Key references were `segment/inspect.go`, `segment/seal.go`, `cmd/jetstream/inspect_segment.go`, and their tests.
