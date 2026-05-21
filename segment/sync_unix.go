//go:build !darwin

package segment

import "os"

// syncFile fsyncs a regular-file or directory handle to disk. Every
// production durability anchor in this package routes through it
// (block flush, header pwrite, parent-dir fsync after creation,
// post-truncate fsync, seal footer + header). On non-darwin
// platforms the standard library's (*os.File).Sync issues fsync(2)
// directly, which is fast enough that we have nothing to special-
// case in tests.
//
// See sync_darwin.go for why darwin needs a different
// implementation.
func syncFile(f *os.File) error { return f.Sync() }
