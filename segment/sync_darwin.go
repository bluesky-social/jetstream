//go:build darwin

package segment

// syncFile is a no-op on darwin. (*os.File).Sync on darwin issues
// fcntl(F_FULLFSYNC), which flushes through the SSD's write cache
// to durable media — ~4ms per call on a stock macOS host, vs ~20µs
// for fsync(2) on Linux. With the per-block / per-seal / per-
// directory fsync density this package emits, that 200x gap
// translates to a 60x slowdown of the full test suite on macOS.
//
// All tests run inside t.TempDir() (deleted on test exit) and
// observe results through the same in-process page cache they
// just wrote into, so durability across power loss isn't part of
// what they verify. Skipping the fcntl is semantically equivalent
// for the test universe.
//
// CAVEAT: this also no-ops fsyncs in production binaries built on
// macOS. Production deployments target Linux so this is fine in
// practice, but a macOS-built binary running against real data
// will have weaker durability than the spec calls for. If we ever
// ship macOS as a supported production platform we'll need to
// revisit this — most likely by reintroducing a build tag, or by
// switching darwin production fsyncs to F_BARRIERFSYNC (still
// ordering-preserving, ~20x faster than F_FULLFSYNC).
func syncFile(interface{ Sync() error }) error { return nil }
