# `jetstream inspect-segment` Implementation Plan

This historical plan refactored frame walking, introduced `segment.Inspect` for sealed and active files, and wired a plain-text `inspect-segment` command. It included active/torn-tail coverage and an end-to-end binary smoke test. Relevant paths were `segment/inspect.go`, `segment/seal.go`, `cmd/jetstream/inspect_segment.go`, and their test files.
