package ingest

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/vfs"
)

func ingestFS(fs vfs.FS) vfs.FS {
	if fs == nil {
		return vfs.Default
	}
	return fs
}

func statFS(fs vfs.FS, path string) (os.FileInfo, error) {
	return ingestFS(fs).Stat(path)
}

func mkdirAllFS(fs vfs.FS, path string, perm os.FileMode) error {
	sfs := ingestFS(fs)
	if err := sfs.MkdirAll(path, perm); err != nil {
		return fmt.Errorf("ingest: mkdir %s: %w", path, err)
	}
	if fs != nil {
		parent := sfs.PathDir(path)
		dir, err := sfs.OpenDir(parent)
		if err != nil {
			return fmt.Errorf("ingest: fsync parent dir %s: %w", parent, err)
		}
		if err := dir.Sync(); err != nil {
			_ = dir.Close()
			return fmt.Errorf("ingest: fsync parent dir %s: %w", parent, err)
		}
		if err := dir.Close(); err != nil {
			return fmt.Errorf("ingest: close parent dir %s: %w", parent, err)
		}
	}
	return nil
}
