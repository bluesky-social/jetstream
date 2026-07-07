package oracle

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type durableOrderOpKind string

const (
	durableOrderSegmentDataWrite durableOrderOpKind = "segment-data-write"
	durableOrderSegmentSync      durableOrderOpKind = "segment-sync"
	durableOrderSegmentRename    durableOrderOpKind = "segment-rename"
	durableOrderSegmentDirSync   durableOrderOpKind = "segment-dir-sync"
	durableOrderStoreWrite       durableOrderOpKind = "store-write"
)

type durableOrderOp struct {
	kind durableOrderOpKind
	path string
	keys []string
}

// durableOrderRecorder is the oracle's journal-test-VFS style checker for the
// storage ordering invariants in specs/invariants.md. It observes segment file
// operations through vfs.WithLogging and metadata commit boundaries through the
// store fault seam, but never injects failures.
type durableOrderRecorder struct {
	mu  sync.Mutex
	ops []durableOrderOp
}

func newDurableOrderRecorder() *durableOrderRecorder {
	return &durableOrderRecorder{}
}

func (r *durableOrderRecorder) WrapFS(fs vfs.FS) vfs.FS {
	return vfs.WithLogging(fs, r.observeFSLog)
}

func (r *durableOrderRecorder) observeFSLog(format string, args ...any) {
	switch format {
	case "write-at(%d, %d): %s":
		if len(args) != 3 {
			return
		}
		offset, ok := args[0].(int64)
		if !ok || offset < int64(segment.ReservedHeaderBytes) {
			return
		}
		path, ok := args[2].(string)
		if !ok || !isActiveSegmentPath(path) {
			return
		}
		r.add(durableOrderOp{kind: durableOrderSegmentDataWrite, path: path})
	case "sync: %s":
		if len(args) != 1 {
			return
		}
		path, ok := args[0].(string)
		if !ok {
			return
		}
		switch {
		case isSegmentPath(path):
			r.add(durableOrderOp{kind: durableOrderSegmentSync, path: path})
		case isSegmentDirPath(path):
			r.add(durableOrderOp{kind: durableOrderSegmentDirSync, path: path})
		}
	case "rename: %s -> %s":
		if len(args) != 2 {
			return
		}
		newPath, ok := args[1].(string)
		if ok && isActiveSegmentPath(newPath) {
			r.add(durableOrderOp{kind: durableOrderSegmentRename, path: newPath})
		}
	}
}

func (r *durableOrderRecorder) BeforeWrite(op store.WriteOp, keys [][]byte) error {
	var keyStrings []string
	for _, key := range keys {
		keyStrings = append(keyStrings, string(key))
	}
	r.add(durableOrderOp{kind: durableOrderStoreWrite, keys: keyStrings})
	return nil
}

func (r *durableOrderRecorder) add(op durableOrderOp) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

func (r *durableOrderRecorder) snapshot() []durableOrderOp {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]durableOrderOp, len(r.ops))
	copy(out, r.ops)
	return out
}

func (r *durableOrderRecorder) Assert(t *testing.T) {
	t.Helper()
	require.NoError(t, checkDurableOrder(r.snapshot()))
}

func checkDurableOrder(ops []durableOrderOp) error {
	needsSync := map[string]int{}
	pendingRenames := map[string]int{}
	var sawSeqCommit bool

	for i, op := range ops {
		switch op.kind {
		case durableOrderSegmentDataWrite:
			needsSync[op.path] = i
		case durableOrderSegmentSync:
			delete(needsSync, op.path)
		case durableOrderSegmentRename:
			pendingRenames[op.path] = i
		case durableOrderSegmentDirSync:
			for path := range pendingRenames {
				if filepath.Dir(path) == op.path {
					delete(pendingRenames, path)
				}
			}
		case durableOrderStoreWrite:
			for _, key := range op.keys {
				if key == "seq/next" || key == "live_segments/seq/next" {
					sawSeqCommit = true
					for path, writeAt := range needsSync {
						if seqKeyCoversSegmentPath(key, path) {
							return fmt.Errorf("oracle: durable order violation: %s commit at op %d before segment data write at op %d was fsynced for %s",
								key, i, writeAt, path)
						}
					}
				}
				if key == "compaction/seq" {
					for path, renameAt := range pendingRenames {
						return fmt.Errorf("oracle: durable order violation: compaction/seq commit at op %d before parent directory fsync for segment rename at op %d (%s)",
							i, renameAt, path)
					}
				}
			}
		}
	}

	if !sawSeqCommit {
		return fmt.Errorf("oracle: durable order recorder observed no seq/next commits")
	}
	for path, renameAt := range pendingRenames {
		return fmt.Errorf("oracle: durable order violation: segment rename at op %d was not followed by parent directory fsync for %s", renameAt, path)
	}
	return nil
}

func isSegmentPath(path string) bool {
	base := filepath.Base(path)
	return strings.HasPrefix(base, "seg_") && (strings.HasSuffix(base, ".jss") || strings.HasSuffix(base, ".jss.tmp"))
}

func isActiveSegmentPath(path string) bool {
	base := filepath.Base(path)
	return strings.HasPrefix(base, "seg_") && strings.HasSuffix(base, ".jss")
}

func isSegmentDirPath(path string) bool {
	base := filepath.Base(path)
	return base == "segments" || base == "live_segments"
}

func seqKeyCoversSegmentPath(key, path string) bool {
	clean := filepath.ToSlash(path)
	switch key {
	case "live_segments/seq/next":
		return strings.Contains(clean, "/backfill/live_segments/")
	case "seq/next":
		return strings.Contains(clean, "/segments/") && !strings.Contains(clean, "/backfill/live_segments/")
	default:
		return false
	}
}

func TestDurableOrderRecorderCatchesSeqCommitBeforeSegmentSync(t *testing.T) {
	t.Parallel()

	err := checkDurableOrder([]durableOrderOp{
		{kind: durableOrderSegmentSync, path: "/data/segments/seg_0000000000.jss"}, // initial header fsync is not enough
		{kind: durableOrderSegmentDataWrite, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderStoreWrite, keys: []string{"seq/next"}},
		{kind: durableOrderSegmentSync, path: "/data/segments/seg_0000000000.jss"},
	})
	require.ErrorContains(t, err, "before segment data write")
}

func TestDurableOrderRecorderAcceptsSegmentSyncBeforeSeqCommit(t *testing.T) {
	t.Parallel()

	require.NoError(t, checkDurableOrder([]durableOrderOp{
		{kind: durableOrderSegmentDataWrite, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderSegmentSync, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderStoreWrite, keys: []string{"seq/next"}},
	}))
}

func TestDurableOrderRecorderCatchesRenameWithoutDirSync(t *testing.T) {
	t.Parallel()

	err := checkDurableOrder([]durableOrderOp{
		{kind: durableOrderSegmentDataWrite, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderSegmentSync, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderStoreWrite, keys: []string{"seq/next"}},
		{kind: durableOrderSegmentRename, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderStoreWrite, keys: []string{"compaction/seq"}},
	})
	require.ErrorContains(t, err, "before parent directory fsync")
}

func TestDurableOrderRecorderAcceptsRenameThenDirSync(t *testing.T) {
	t.Parallel()

	require.NoError(t, checkDurableOrder([]durableOrderOp{
		{kind: durableOrderSegmentDataWrite, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderSegmentSync, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderStoreWrite, keys: []string{"seq/next"}},
		{kind: durableOrderSegmentRename, path: "/data/segments/seg_0000000000.jss"},
		{kind: durableOrderSegmentDirSync, path: "/data/segments"},
		{kind: durableOrderStoreWrite, keys: []string{"compaction/seq"}},
	}))
}
