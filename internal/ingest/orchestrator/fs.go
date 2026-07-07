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

func isStorageNotExist(err error) bool {
	return os.IsNotExist(err) || oserror.IsNotExist(err)
}
