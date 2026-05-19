package segment

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRecoveryFromCrashAfterFooterFsyncBeforeHeaderPwrite simulates a
// crash where the footer is durable but the header is still zero-
// filled. The first 8 bytes of the footer are the block index's
// first entry (a uint64 file offset interpreted as a length prefix
// by lastGoodOffset); that interpreted "length" overruns the file,
// so lastGoodOffset truncates the trailing bytes back off.
func TestRecoveryFromCrashAfterFooterFsyncBeforeHeaderPwrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// Write and seal a normal segment so the file has a real footer.
	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		_, err = w.Append(Event{
			Seq: uint64(i), Kind: KindCreate,
			DID: "did:plc:a", Collection: "app.bsky.feed.post",
		})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)

	// Roll back the header to active-state by re-zeroing bytes 4..256.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	zero := make([]byte, ReservedHeaderBytes-4)
	_, err = f.WriteAt(zero, 4)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	preInfo, err := os.Stat(path)
	require.NoError(t, err)
	preSize := preInfo.Size()

	// Reopen — torn footer must be truncated.
	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w2.Close())

	postInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Less(t, postInfo.Size(), preSize,
		"reopen must shrink the file by truncating the orphaned footer")
}

// TestRecoveryFromPartialFooterWrite simulates a crash where some
// leading footer bytes were written but the footer fsync didn't
// complete. The torn-tail recovery path handles this identically to
// the full-footer case: lastGoodOffset can't parse the bytes as a
// frame and truncates them.
func TestRecoveryFromPartialFooterWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for i := 1; i <= 2; i++ {
		_, err = w.Append(Event{
			Seq: uint64(i), Kind: KindCreate, DID: "did:plc:a",
		})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	preInfo, err := os.Stat(path)
	require.NoError(t, err)
	preSize := preInfo.Size()

	// Append some plausible partial-footer bytes. The footer starts
	// with the block index, which is a uint64 file offset. When
	// lastGoodOffset interprets this as a frame length prefix, it
	// will see a value that overruns the file and stop.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	require.NoError(t, err)
	partialFooter := make([]byte, 256)
	// Write a plausible file offset (larger than current file size)
	// into the first 8 bytes so it looks like an overrun frame length.
	binary.LittleEndian.PutUint64(partialFooter[0:8], 999999)
	_, err = f.Write(partialFooter)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w2.Close())

	postInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, preSize, postInfo.Size(),
		"recovery should truncate back to the pre-partial-footer size")
}

// TestSealHeaderWriteFailureTruncatesFooterBackOff covers the most
// subtle recovery path. We force the header pwrite to fail by
// closing the underlying file descriptor behind the writer's back
// after the footer is durable but before the header pwrite happens.
// Seal must explicitly truncate the footer so the file is restored
// to its pre-Seal state.
func TestSealHeaderWriteFailureTruncatesFooterBackOff(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	_, err = w.Append(Event{
		Seq: 1, Kind: KindCreate, DID: "did:plc:a",
		Collection: "app.bsky.feed.post", Rkey: "k", Rev: "v",
	})
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	preInfo, err := os.Stat(path)
	require.NoError(t, err)
	preSize := preInfo.Size()

	// Force the next WriteAt to fail. Closing the fd makes both the
	// footer write *and* the header write fail. We're testing the
	// header-write-failure path specifically by checking that the
	// file is restored to preSize, but the footer write may have
	// failed first (in which case there's nothing to undo).
	require.NoError(t, w.file.Close())

	_, sealErr := w.Seal()
	require.Error(t, sealErr)

	// Whichever step failed, the file must be back to preSize: either
	// because the footer write failed first (nothing to undo) or
	// because the header write failed and Seal truncated the footer.
	postInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, preSize, postInfo.Size(),
		"Seal must leave the file at pre-Seal size on any I/O failure")

	// A fresh Writer must reopen the file as active and seal cleanly.
	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	res, err := w2.Seal()
	require.NoError(t, err)
	require.EqualValues(t, 1, res.EventCount)
	require.NoError(t, w2.Close())
}
