package segment

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/jcalabro/gloom"
	"github.com/stretchr/testify/require"
)

func verifyFixture(t *testing.T) string {
	t.Helper()
	events := []Event{
		{Seq: 1, WitnessedAt: 100, Kind: KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "v1"},
		{Seq: 2, WitnessedAt: 200, Kind: KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.like", Rkey: "r2", Rev: "v2"},
		{Seq: 3, WitnessedAt: 300, Kind: KindAccount, DID: "did:plc:a"},
		{Seq: 4, WitnessedAt: 400, Kind: KindIdentity, DID: "did:plc:b"},
		{Seq: 5, WitnessedAt: 500, Kind: KindSync, DID: "did:plc:c"},
		{Seq: 6, WitnessedAt: 600, Kind: KindUpdate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "v3"},
	}
	return sealedSegmentForReader(t, t.TempDir(), events, 2)
}

func TestVerifySealedMetadataPasses(t *testing.T) {
	t.Parallel()

	r := openVerifyFixture(t, verifyFixture(t))
	defer func() { require.NoError(t, r.Close()) }()

	require.NoError(t, VerifySealedMetadata(r))
}

func TestVerifySealedMetadataRejectsCollectionCountMismatch(t *testing.T) {
	t.Parallel()

	path := verifyFixture(t)
	rewriteCollectionIndex(t, path, func(idx *collectionIndex) {
		for i, name := range idx.stringTable {
			if name == "app.bsky.feed.post" {
				idx.eventCounts[i]++
				return
			}
		}
		t.Fatal("fixture missing app.bsky.feed.post")
	})

	r := openVerifyFixture(t, path)
	defer func() { require.NoError(t, r.Close()) }()

	err := VerifySealedMetadata(r)
	require.ErrorContains(t, err, "collection \"app.bsky.feed.post\" count mismatch")
}

func TestVerifySealedMetadataRejectsBlockCollectionMismatch(t *testing.T) {
	t.Parallel()

	path := verifyFixture(t)
	rewriteCollectionIndex(t, path, func(idx *collectionIndex) {
		require.NotEmpty(t, idx.blockBitmasks[0])
		idx.blockBitmasks[0] = idx.blockBitmasks[0][1:]
	})

	r := openVerifyFixture(t, path)
	defer func() { require.NoError(t, r.Close()) }()

	err := VerifySealedMetadata(r)
	require.ErrorContains(t, err, "block 0")
	require.ErrorContains(t, err, "collection")
}

func TestVerifySealedMetadataRejectsSegmentBloomFalseNegative(t *testing.T) {
	t.Parallel()

	path := verifyFixture(t)
	parts := readFooterParts(t, path)
	orig, err := gloom.UnmarshalBinary(parts.segmentBloom)
	require.NoError(t, err)
	empty := gloom.NewWithParams(orig.NumBlocks(), orig.K())
	parts.segmentBloom, err = empty.MarshalBinary()
	require.NoError(t, err)
	writeFooterParts(t, path, parts)

	r := openVerifyFixture(t, path)
	defer func() { require.NoError(t, r.Close()) }()

	err = VerifySealedMetadata(r)
	require.ErrorContains(t, err, "segment DID bloom false negative")
}

func TestVerifySealedMetadataRejectsBlockBloomFalseNegative(t *testing.T) {
	t.Parallel()

	path := verifyFixture(t)
	parts := readFooterParts(t, path)
	count, size, err := decodeBlockBloomsRegionHeader(parts.blockBlooms[:blockBloomsRegionHeaderSize])
	require.NoError(t, err)
	require.Greater(t, count, uint32(0))
	orig, err := decodeBlockBloomFromRegion(parts.blockBlooms, size, 0)
	require.NoError(t, err)
	empty := gloom.NewWithParams(orig.NumBlocks(), orig.K())
	replacement, err := empty.MarshalBinary()
	require.NoError(t, err)
	require.Len(t, replacement, int(size))
	copy(parts.blockBlooms[blockBloomsRegionHeaderSize:blockBloomsRegionHeaderSize+int(size)], replacement)
	writeFooterParts(t, path, parts)

	r := openVerifyFixture(t, path)
	defer func() { require.NoError(t, r.Close()) }()

	err = VerifySealedMetadata(r)
	require.ErrorContains(t, err, "block 0 DID bloom false negative")
}

func openVerifyFixture(t *testing.T, path string) *Reader {
	t.Helper()
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	return r
}

type footerParts struct {
	header          Header
	blockIndex      []byte
	segmentBloom    []byte
	blockBlooms     []byte
	collectionIndex []byte
}

func readFooterParts(t *testing.T, path string) footerParts {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	headerBytes := make([]byte, ReservedHeaderBytes)
	_, err = f.ReadAt(headerBytes, 0)
	require.NoError(t, err)
	h, err := decodeHeader(headerBytes)
	require.NoError(t, err)
	info, err := f.Stat()
	require.NoError(t, err)

	readRange := func(start, end uint64) []byte {
		t.Helper()
		require.GreaterOrEqual(t, end, start)
		out := make([]byte, end-start)
		if len(out) > 0 {
			_, err := f.ReadAt(out, int64(start))
			require.NoError(t, err)
		}
		return out
	}

	return footerParts{
		header:          h,
		blockIndex:      readRange(h.BlockIndexOffset, h.DIDBloomOffset),
		segmentBloom:    readRange(h.DIDBloomOffset, h.BlockDIDBloomOffset),
		blockBlooms:     readRange(h.BlockDIDBloomOffset, h.CollectionIndexOffset),
		collectionIndex: readRange(h.CollectionIndexOffset, uint64(info.Size())),
	}
}

func writeFooterParts(t *testing.T, path string, parts footerParts) {
	t.Helper()
	footer := make([]byte, 0, len(parts.blockIndex)+len(parts.segmentBloom)+len(parts.blockBlooms)+len(parts.collectionIndex))
	footer = append(footer, parts.blockIndex...)
	footer = append(footer, parts.segmentBloom...)
	footer = append(footer, parts.blockBlooms...)
	footer = append(footer, parts.collectionIndex...)

	h := parts.header
	h.Checksum = 0
	headerBytes := encodeHeader(h)
	checksum := xxh3HeaderFooter(headerBytes, footer)
	h.Checksum = checksum
	binary.LittleEndian.PutUint64(headerBytes[4:12], checksum)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	_, err = f.WriteAt(footer, int64(h.FooterOffset))
	require.NoError(t, err)
	require.NoError(t, f.Truncate(int64(h.FooterOffset)+int64(len(footer))))
	_, err = f.WriteAt(headerBytes, 0)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
}

func rewriteCollectionIndex(t *testing.T, path string, mutate func(*collectionIndex)) {
	t.Helper()
	parts := readFooterParts(t, path)
	idx, err := decodeCollectionIndex(parts.collectionIndex)
	require.NoError(t, err)
	mutate(&idx)
	parts.collectionIndex, err = encodeCollectionIndex(idx)
	require.NoError(t, err)
	writeFooterParts(t, path, parts)
}
