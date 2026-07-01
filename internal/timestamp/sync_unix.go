//go:build !darwin

package timestamp

import "os"

// syncFile fsyncs a regular-file or directory handle. The bucketer routes its
// durability anchors through it (offset files on evict/Close, the job dir
// after Close) so the manager's Bucketed=true checkpoint — which lets a
// resumed run skip re-parsing — can never outlive the offset files it vouches
// for. Mirrors segment.syncFile; see sync_darwin.go for the darwin split.
func syncFile(f *os.File) error { return f.Sync() }
