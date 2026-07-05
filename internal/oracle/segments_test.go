package oracle

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

func TestCheckSegmentStructureRejectsNonIncreasingOffsets(t *testing.T) {
	t.Parallel()

	err := checkSegmentStructure("seg_00000000000000000000.jss", segment.Header{BlockCount: 2}, []segment.BlockInfo{
		{Offset: 200, EventCount: 1, MinSeq: 1, MaxSeq: 1},
		{Offset: 199, EventCount: 1, MinSeq: 2, MaxSeq: 2},
	})
	require.ErrorContains(t, err, "non-increasing block offset")
}

func TestCheckSegmentStructureRejectsSeqRegressionAcrossBlocks(t *testing.T) {
	t.Parallel()

	err := checkSegmentStructure("seg_00000000000000000000.jss", segment.Header{BlockCount: 2}, []segment.BlockInfo{
		{Offset: 200, EventCount: 1, MinSeq: 10, MaxSeq: 20},
		{Offset: 300, EventCount: 1, MinSeq: 20, MaxSeq: 30},
	})
	require.ErrorContains(t, err, "block seq overlap")
}

func TestObserveSegmentsRejectsFooterCollectionCountMismatch(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	path := filepath.Join(segmentsDir, ingest.SegmentFilename(0))

	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	_, err = w.Append(segment.Event{
		Seq: 1, WitnessedAt: 100, Kind: segment.KindCreate,
		DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "v1",
	})
	require.NoError(t, err)
	_, err = w.Append(segment.Event{
		Seq: 2, WitnessedAt: 200, Kind: segment.KindCreate,
		DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "r2", Rev: "v2",
	})
	require.NoError(t, err)
	_, err = w.Seal()
	require.NoError(t, err)

	corruptFirstCollectionCountAndRefreshChecksum(t, path)

	_, err = ObserveSegments(dataDir)
	require.ErrorContains(t, err, "verify sealed segment")
	require.ErrorContains(t, err, "collection \"app.bsky.feed.post\" count mismatch")
}

func corruptFirstCollectionCountAndRefreshChecksum(t *testing.T, path string) {
	t.Helper()
	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	info, err := f.Stat()
	require.NoError(t, err)
	headerBytes := make([]byte, segment.ReservedHeaderBytes)
	_, err = f.ReadAt(headerBytes, 0)
	require.NoError(t, err)

	collectionBytes := make([]byte, uint64(info.Size())-ins.Header.CollectionIndexOffset)
	_, err = f.ReadAt(collectionBytes, int64(ins.Header.CollectionIndexOffset))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(collectionBytes), 16)

	collectionCount := binary.LittleEndian.Uint32(collectionBytes[0:4])
	require.Greater(t, collectionCount, uint32(0))
	uncompressedSize := binary.LittleEndian.Uint32(collectionBytes[12:16])

	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(1<<20))
	require.NoError(t, err)
	body, err := dec.DecodeAll(collectionBytes[16:], nil)
	require.NoError(t, err)
	dec.Close()
	require.Len(t, body, int(uncompressedSize))
	require.GreaterOrEqual(t, len(body), 5)

	nsidLen := int(body[0])
	require.Greater(t, nsidLen, 0)
	require.GreaterOrEqual(t, len(body), 1+4+nsidLen)
	count := binary.LittleEndian.Uint32(body[1:5])
	binary.LittleEndian.PutUint32(body[1:5], count+1)

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true))
	require.NoError(t, err)
	mutatedCollectionBytes := append([]byte(nil), collectionBytes[:16]...)
	mutatedCollectionBytes = enc.EncodeAll(body, mutatedCollectionBytes)
	require.NoError(t, enc.Close())

	footerPrefix := make([]byte, ins.Header.CollectionIndexOffset-ins.Header.FooterOffset)
	_, err = f.ReadAt(footerPrefix, int64(ins.Header.FooterOffset))
	require.NoError(t, err)
	footer := append(footerPrefix, mutatedCollectionBytes...)

	for i := 4; i < 12; i++ {
		headerBytes[i] = 0
	}
	h := xxh3.New()
	_, _ = h.Write(headerBytes[12:])
	_, _ = h.Write(footer)
	binary.LittleEndian.PutUint64(headerBytes[4:12], h.Sum64())

	_, err = f.WriteAt(footer, int64(ins.Header.FooterOffset))
	require.NoError(t, err)
	require.NoError(t, f.Truncate(int64(ins.Header.FooterOffset)+int64(len(footer))))
	_, err = f.WriteAt(headerBytes, 0)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
}
