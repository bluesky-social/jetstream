package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// FuzzOpenReaderParts feeds OpenReaderParts a header and footer as they
// come from storage (design §20): a generation row's header column and its
// footer object, with no block region. Every query on a Reader it opens must
// error rather than panic, including DecodeBlock on fetched frames.
//
// fixChecksum rewrites a 256-byte header's checksum to match, so mutations
// reach the block index, bloom, and collection index parsers instead of
// stopping at the checksum.
func FuzzOpenReaderParts(f *testing.F) {
	var frames [][]byte
	b, err := NewBlockBuilder(2)
	if err != nil {
		f.Fatal(err)
	}
	for seq := uint64(1); seq <= 5; seq++ {
		full, err := b.Append(Event{
			Seq: seq, WitnessedAt: int64(seq), Kind: KindCreate,
			DID: fmt.Sprintf("did:plc:%d", seq%2), Collection: "app.bsky.feed.post", Rkey: "k",
			Payload: []byte{0xa0},
		})
		if err != nil {
			f.Fatal(err)
		}
		if full {
			fr, _ := b.Encode()
			frames = append(frames, fr)
		}
	}
	fr, _ := b.Encode()
	frames = append(frames, fr)
	header, footer, _, err := BuildSealed(SliceFrameSource(frames))
	if err != nil {
		f.Fatal(err)
	}
	// The valid seed must open, or the target only ever fuzzes rejections.
	if _, err := OpenReaderParts(header, footer, frameFetcher(frames), ReaderOptions{}); err != nil {
		f.Fatal(err)
	}
	empty, emptyFooter, _, err := BuildSealed(SliceFrameSource(nil))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(header, footer, false)
	f.Add(header, footer, true)
	f.Add(empty, emptyFooter, false)
	f.Add(header, footer[:len(footer)/2], true)
	f.Add(header, append(bytes.Clone(footer), 0), true)
	f.Add(header, []byte{}, true)
	f.Add(header[:ReservedHeaderBytes-1], footer, true)
	f.Add([]byte{}, []byte{}, false)
	f.Add(make([]byte, ReservedHeaderBytes), footer, true)

	f.Fuzz(func(t *testing.T, header, footer []byte, fixChecksum bool) {
		if fixChecksum && len(header) == ReservedHeaderBytes {
			header = bytes.Clone(header)
			clear(header[4:12])
			binary.LittleEndian.PutUint64(header[4:12], xxh3HeaderFooter(header, footer))
		}
		_, _ = ReadSealedHeader(bytes.NewReader(header))
		var r *Reader
		r, err := OpenReaderParts(header, footer, func(i int) ([]byte, error) {
			// A frame of the indexed length reaches the decoder; a real
			// fetch is hash-verified, so its content is not the point.
			if n := r.Blocks()[i].CompressedSize; n <= 1<<16 {
				return make([]byte, n), nil
			}
			return nil, fmt.Errorf("block %d is too big to fake", i)
		}, ReaderOptions{})
		if err != nil {
			return
		}
		_ = r.Header()
		_ = r.SegmentBloom()
		_ = r.Collections()
		_ = r.CollectionEventCounts()
		_, _ = r.LoadAllBlockBlooms()
		exerciseReader(r, []string{"did:plc:0", "did:plc:1", ""})
	})
}
