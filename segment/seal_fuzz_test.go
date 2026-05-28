package segment

import "testing"

// FuzzReadHeader ensures decodeHeader never panics or reads past the
// input regardless of how malformed the input is. It returns either
// a valid Header or a sentinel-wrapped error.
func FuzzReadHeader(f *testing.F) {
	// Seed with a few well-formed and obviously malformed inputs.
	good := encodeHeader(Header{
		Version:  1,
		Checksum: 0xDEADBEEF,
	})
	f.Add(good)
	f.Add(make([]byte, ReservedHeaderBytes))
	f.Add([]byte{}) // truncated
	f.Add([]byte("jss0"))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeHeader(data)
	})
}

// FuzzReadBlockIndex covers decodeBlockIndex over arbitrary byte
// inputs paired with arbitrary count values. Output: valid []BlockInfo
// or an error.
func FuzzReadBlockIndex(f *testing.F) {
	f.Add([]byte{}, uint32(0))
	good := encodeBlockIndex([]BlockInfo{{Offset: 256, CompressedSize: 1, EventCount: 1}})
	f.Add(good, uint32(1))

	f.Fuzz(func(t *testing.T, data []byte, count uint32) {
		// Cap count so a hostile value can't drive an enormous
		// allocation in the fuzz target itself.
		if count > 1<<16 {
			t.Skip()
		}
		_, _ = decodeBlockIndex(data, count)
	})
}

// FuzzReadCollectionIndex covers decodeCollectionIndex.
func FuzzReadCollectionIndex(f *testing.F) {
	good, err := encodeCollectionIndex(collectionIndex{
		stringTable:   []string{"app.bsky.feed.post"},
		eventCounts:   []uint32{1},
		blockBitmasks: [][]uint32{{0}},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte{})
	f.Add(make([]byte, collectionIndexHeaderSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeCollectionIndex(data)
	})
}
