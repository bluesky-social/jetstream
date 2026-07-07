package jetstreamd

import (
	"os"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/cockroachdb/pebble/vfs"
)

func runtimeFS(fs vfs.FS) vfs.FS {
	if fs == nil {
		return vfs.Default
	}
	return fs
}

// mkdirAllRuntimeFS creates the process's DataDir/SegmentsDir at boot and
// fsyncs every directory entry it newly creates so the tree survives a crash
// in the boot window. It delegates to ingest.MkdirAllSyncedFS so the runtime
// and the ingest writer share one power-loss-safe implementation rather than
// two copies that can drift.
func mkdirAllRuntimeFS(fs vfs.FS, path string, perm os.FileMode) error {
	return ingest.MkdirAllSyncedFS(runtimeFS(fs), path, perm, "jetstreamd")
}
