package ingest

import (
	"fmt"
	"os"

	"github.com/cockroachdb/errors/oserror"
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
	// Fsync every directory entry MkdirAll newly creates so the whole chain
	// survives power loss. This must run on the production (fs == nil →
	// vfs.Default) path too: nothing else ever fsyncs SegmentsDir's ancestors,
	// so skipping it would leave a fresh dir's entry vulnerable to rollback,
	// orphaning segment files synced into it. A new dirent is durable only
	// after fsyncing the directory that contains it, so MkdirAll creating N
	// levels needs N parent fsyncs — not just the leaf's. MkdirAllSyncedFS
	// does the stat-walk + bottom-up MkdirAll + shallow-first parent fsyncs.
	return MkdirAllSyncedFS(sfs, path, perm, "ingest")
}

// MkdirAllSyncedFS creates path (like MkdirAll) and fsyncs the parent of every
// directory it newly creates, shallowest-first, so a power loss cannot roll
// back an intermediate dirent while a deeper one is durable. Pre-existing
// ancestors are left untouched (their entries are already durable). prefix
// namespaces the wrapped errors to the calling package.
//
// It is exported so the process runtime (internal/jetstreamd) creates its
// DataDir/SegmentsDir through the same power-loss-safe path the ingest writer
// uses, rather than maintaining a second copy of this subtle logic that could
// drift out of sync.
//
// The strict in-memory vfs used by the power-loss oracle and the host OS
// filesystem both flow through here; sfs must already be the resolved (never
// nil) filesystem.
func MkdirAllSyncedFS(sfs vfs.FS, path string, perm os.FileMode, prefix string) error {
	// Collect the missing ancestors, deepest-first, by walking up until we
	// hit one that already exists (or the filesystem root).
	var missing []string
	for p := path; ; {
		if _, err := sfs.Stat(p); err == nil {
			break
		} else if !oserror.IsNotExist(err) {
			return fmt.Errorf("%s: stat %s: %w", prefix, p, err)
		}
		missing = append(missing, p)
		parent := sfs.PathDir(p)
		if parent == p {
			// Reached the root; it necessarily already exists, so a
			// not-exist Stat above means the FS is lying — stop rather than
			// loop forever.
			break
		}
		p = parent
	}

	if err := sfs.MkdirAll(path, perm); err != nil {
		return fmt.Errorf("%s: mkdir %s: %w", prefix, path, err)
	}

	// Fsync each newly-created directory's parent, shallowest-first, so the
	// ordering matches the order the entries became visible.
	for i := len(missing) - 1; i >= 0; i-- {
		parent := sfs.PathDir(missing[i])
		dir, err := sfs.OpenDir(parent)
		if err != nil {
			return fmt.Errorf("%s: fsync parent dir %s: %w", prefix, parent, err)
		}
		if err := dir.Sync(); err != nil {
			_ = dir.Close()
			return fmt.Errorf("%s: fsync parent dir %s: %w", prefix, parent, err)
		}
		if err := dir.Close(); err != nil {
			return fmt.Errorf("%s: close parent dir %s: %w", prefix, parent, err)
		}
	}
	return nil
}
