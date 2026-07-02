//go:build darwin

package timestamp

import "os"

// syncFile is a no-op on darwin for the same reason as segment.syncFile:
// (*os.File).Sync issues fcntl(F_FULLFSYNC) (~4ms vs ~20µs for fsync(2) on
// Linux), tests run in t.TempDir() and read through the same page cache, and
// production targets Linux. See segment/sync_darwin.go for the full rationale
// and the caveat about macOS-built production binaries.
func syncFile(*os.File) error { return nil }
