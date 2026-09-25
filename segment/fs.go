package segment

import (
	"fmt"
	"os"

	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

func segmentFS(fs vfs.FS) vfs.FS {
	if fs == nil {
		return vfs.Default
	}
	return fs
}

func syncSegmentFile(fs vfs.FS, f vfs.File) error {
	if fs == nil {
		return syncFile(f)
	}
	return f.Sync()
}

func openSegmentReadOnly(fs vfs.FS, path string) (vfs.File, error) {
	return segmentFS(fs).Open(path)
}

func openSegmentReadWrite(fs vfs.FS, path string) (vfs.File, error) {
	return segmentFS(fs).OpenReadWrite(path)
}

// createSegmentFileExclusive fails if path exists. Rewrite removes abandoned
// temporary files first; a file reappearing before creation violates its
// single-rewriter assumption and must not be overwritten.
//
// Production uses atomic O_CREATE|O_EXCL. The strict test VFS has no
// exclusive-create operation, so it uses Stat followed by Create. That
// emulation relies on the power-loss tier's single-threaded execution.
func createSegmentFileExclusive(fs vfs.FS, path string) (vfs.File, error) {
	if fs == nil {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return nil, err
		}
		return osFileAdapter{f}, nil
	}
	if _, err := fs.Stat(path); err == nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrExist}
	} else if !oserror.IsNotExist(err) {
		return nil, err
	}
	return fs.Create(path)
}

// osFileAdapter lifts an *os.File to vfs.File. *os.File already
// satisfies the read/write/sync/stat surface segment I/O uses; this
// only fills the vfs-specific methods (Preallocate, SyncTo, SyncData,
// Prefetch, Fd) with faithful minimal behavior so the O_EXCL production
// path can flow through the same vfs.File-typed seam as the test FS.
type osFileAdapter struct{ *os.File }

func (a osFileAdapter) Preallocate(offset, length int64) error { return nil }
func (a osFileAdapter) SyncData() error                        { return a.Sync() }
func (a osFileAdapter) SyncTo(length int64) (fullSync bool, err error) {
	// os.File.Sync fsyncs the whole file, which satisfies (and exceeds)
	// syncing the requested prefix, so report a full durable sync.
	return true, a.Sync()
}
func (a osFileAdapter) Prefetch(offset, length int64) error { return nil }
func (a osFileAdapter) Fd() uintptr                         { return a.File.Fd() }

func removeSegmentFile(fs vfs.FS, path string) error {
	return segmentFS(fs).Remove(path)
}

func isSegmentNotExist(err error) bool {
	return os.IsNotExist(err) || oserror.IsNotExist(err)
}

func renameSegmentFile(fs vfs.FS, oldPath, newPath string) error {
	return segmentFS(fs).Rename(oldPath, newPath)
}

func statSegmentFile(fs vfs.FS, path string) (os.FileInfo, error) {
	return segmentFS(fs).Stat(path)
}

func syncParentDirFS(fs vfs.FS, path string, faults IOFaultInjector) error {
	sfs := segmentFS(fs)
	dir, err := sfs.OpenDir(sfs.PathDir(path))
	if err != nil {
		return fmt.Errorf("segment: open parent dir: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if faults != nil {
		if err := faults.BeforeSegmentIO(path, IOOpSync); err != nil {
			return fmt.Errorf("segment: fsync parent dir: %w", err)
		}
	}
	if err := syncSegmentFile(fs, dir); err != nil {
		return fmt.Errorf("segment: fsync parent dir: %w", err)
	}
	return nil
}

type truncatingFile interface {
	Truncate(size int64) error
}

func truncateSegmentFile(_ vfs.FS, f vfs.File, size int64) error {
	tf, ok := f.(truncatingFile)
	if !ok {
		return fmt.Errorf("segment: truncate unsupported by filesystem file %T", f)
	}
	return tf.Truncate(size)
}
