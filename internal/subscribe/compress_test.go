package subscribe

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// TestCompressFrame_RoundTripsWithDictionaryReader pins the wire contract:
// a frame produced by compressFrame must decode, using a decoder seeded
// with the SAME embedded dictionary, back to the original bytes. This is
// exactly what a v1 client does (zstd.NewReader(nil, WithDecoderDicts(dict))).
func TestCompressFrame_RoundTripsWithDictionaryReader(t *testing.T) {
	t.Parallel()

	orig := []byte(`{"did":"did:plc:example","kind":"identity","time_us":1700000000000000}`)
	frame := compressFrame(orig)
	require.NotEmpty(t, frame)
	require.NotEqual(t, orig, frame, "frame must actually be compressed/encoded, not pass-through")

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer dec.Close()

	got, err := dec.DecodeAll(frame, nil)
	require.NoError(t, err)
	require.Equal(t, orig, got, "dictionary-decoded frame must equal the original bytes")
}

// TestZstdDictionary_IsEmbedded guards against an empty / missing embed.
func TestZstdDictionary_IsEmbedded(t *testing.T) {
	t.Parallel()
	require.Greater(t, len(zstdDictionary), 100_000,
		"the v1 zstd dictionary is ~113 KB; a tiny value means the embed broke")
}
