package jetstreamd

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/vfs"
)

func runtimeFS(fs vfs.FS) vfs.FS {
	if fs == nil {
		return vfs.Default
	}
	return fs
}

func mkdirAllRuntimeFS(fs vfs.FS, path string, perm os.FileMode) error {
	rfs := runtimeFS(fs)
	if err := rfs.MkdirAll(path, perm); err != nil {
		return fmt.Errorf("mkdir %s: %w", path, err)
	}
	if fs != nil {
		parent := rfs.PathDir(path)
		dir, err := rfs.OpenDir(parent)
		if err != nil {
			return fmt.Errorf("sync parent dir %s: %w", parent, err)
		}
		if err := dir.Sync(); err != nil {
			_ = dir.Close()
			return fmt.Errorf("sync parent dir %s: %w", parent, err)
		}
		if err := dir.Close(); err != nil {
			return fmt.Errorf("close parent dir %s: %w", parent, err)
		}
	}
	return nil
}
