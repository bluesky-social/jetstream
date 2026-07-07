package orchestrator

import (
	"fmt"
	"os"

	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

func storageFS(fs vfs.FS) vfs.FS {
	if fs == nil {
		return vfs.Default
	}
	return fs
}

func statStorageFS(fs vfs.FS, path string) (os.FileInfo, error) {
	return storageFS(fs).Stat(path)
}

func removeAllStorageFS(fs vfs.FS, path string) error {
	if err := storageFS(fs).RemoveAll(path); err != nil {
		return fmt.Errorf("orchestrator: remove %s: %w", path, err)
	}
	return nil
}

// syncStorageDirFS fsyncs a directory so that entry creations or removals in
// it (e.g. a RemoveAll of a child subtree) are durable before a subsequent
// store mutation that assumes them. Without this, a power loss can leave a
// durable store change ordered ahead of a not-yet-durable dirent change.
func syncStorageDirFS(fs vfs.FS, path string) error {
	dir, err := storageFS(fs).OpenDir(path)
	if err != nil {
		return fmt.Errorf("orchestrator: open dir %s for sync: %w", path, err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("orchestrator: fsync dir %s: %w", path, err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("orchestrator: close dir %s after sync: %w", path, err)
	}
	return nil
}

func isStorageNotExist(err error) bool {
	return os.IsNotExist(err) || oserror.IsNotExist(err)
}
