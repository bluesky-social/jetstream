package segment

import "context"

// CrashInjector simulates a crash at segment rewrite checkpoints. It is
// defined here to keep this public package independent of
// internal/crashpoint. Production passes nil. The constants below are shared
// with the harness to keep checkpoint names consistent.
type CrashInjector interface {
	SimulateCrash(ctx context.Context, point string) error
}

// Segment-rewrite crash seams. These fire inside Rewrite, in order.
const (
	// CrashPointRewriteTempWritten fires after the rewrite has written all
	// bytes to the temporary replacement file but before fsyncing it.
	CrashPointRewriteTempWritten = "after-segment-rewrite-temp-written"

	// CrashPointRewriteTempSynced fires after the temporary replacement file
	// is fsynced but before renaming it over the original.
	CrashPointRewriteTempSynced = "after-segment-rewrite-temp-synced"

	// CrashPointRewriteRenamed fires after the replacement file is renamed
	// over the original but before fsyncing the parent dir.
	CrashPointRewriteRenamed = "after-segment-rewrite-renamed"

	// CrashPointRewriteDirSynced fires after the parent dir is fsynced. The
	// replacement is durable; callers must still tolerate an error here.
	CrashPointRewriteDirSynced = "after-segment-rewrite-dir-synced"
)
