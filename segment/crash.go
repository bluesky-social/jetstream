package segment

import "context"

// CrashInjector simulates a crash at a named segment-rewrite durability seam.
// It is the segment package's only test-seam dependency: defining it here
// (rather than importing internal/crashpoint) keeps the decode/seal core free
// of internal deps so the public client can import segment cheaply.
//
// Production passes a nil CrashInjector and every seam is a no-op. The point
// strings below are the contract with the test harness; internal/crashpoint
// derives its corresponding Point constants from them, so the values cannot
// drift between the firing site and the injection site.
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
