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

func createSegmentFileExclusive(fs vfs.FS, path string) (vfs.File, error) {
	sfs := segmentFS(fs)
	if _, err := sfs.Stat(path); err == nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrExist}
	} else if !oserror.IsNotExist(err) {
		return nil, err
	}
	return sfs.Create(path)
}

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
