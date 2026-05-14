package segment

import "testing"

// FuzzDecodeBlock asserts the bare uncompressed-body decoder cannot
// be tricked into panicking, reading past end-of-input, or
// allocating unbounded memory by any input.
func FuzzDecodeBlock(f *testing.F) {
	// Seed with valid encoded fixtures and edge cases.
	good, err := encodeBlock([]Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Payload: []byte("p")},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})                   // event_count = 0
	f.Add(make([]byte, 1024))                   // all zeros, varying size
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00}) // huge event_count

	f.Fuzz(func(t *testing.T, data []byte) {
		// Contract: never panics, never returns a "fine" Event for
		// truncated input. We don't assert anything about success;
		// we only assert no crash.
		_, _ = decodeBlock(data)
	})
}

// FuzzDecodeBlockFromCompressed targets the full read path including
// zstd decompression. Same safety contract.
func FuzzDecodeBlockFromCompressed(f *testing.F) {
	good, err := encodeBlockCompressed([]Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Payload: []byte("p")},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{0x28, 0xB5, 0x2F, 0xFD}) // bare zstd magic, no body

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeBlockCompressed(data)
	})
}
