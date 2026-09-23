//go:build darwin

package segment

// syncFile skips fsync on darwin to avoid F_FULLFSYNC latency (~4ms per call
// versus ~20µs for Linux fsync). This reduced macOS test time substantially.
//
// This also weakens durability in macOS binaries. Production targets Linux.
// Supporting production on macOS requires restoring durable syncs, possibly
// through a build tag or F_BARRIERFSYNC.
func syncFile(interface{ Sync() error }) error { return nil }
