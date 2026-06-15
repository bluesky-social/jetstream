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

// TestCompressFrame_RequiresDictionaryToDecode proves the dictionary is
// load-bearing: a frame produced by compressFrame (which uses
// WithEncoderDict) must FAIL to decode with a no-dictionary reader. Without
// this test, a regression that drops WithEncoderDict from the encoder would
// silently pass the round-trip test — the frame would still decode because
// zstd only consults the decoder dictionary when the frame header references
// that dict's ID.
func TestCompressFrame_RequiresDictionaryToDecode(t *testing.T) {
	t.Parallel()

	orig := []byte(`{"did":"did:plc:example","kind":"identity","time_us":1700000000000000}`)
	frame := compressFrame(orig)
	require.NotEmpty(t, frame)

	// A no-dictionary reader must NOT be able to decode a dict-compressed frame.
	dec, err := zstd.NewReader(nil) // no WithDecoderDicts
	require.NoError(t, err)
	defer dec.Close()

	_, err = dec.DecodeAll(frame, nil)
	require.Error(t, err,
		"a frame compressed with the custom dictionary must NOT decode without it — proves the dictionary is load-bearing")

	// Control: a frame built WITHOUT the dictionary decodes fine with a
	// no-dict reader — so the failure above is specifically due to the
	// dictionary, not some unrelated decode error.
	plainEnc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	plainFrame := plainEnc.EncodeAll([]byte(`{"x":1}`), nil)
	require.NoError(t, plainEnc.Close())

	plainDec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer plainDec.Close()

	_, err = plainDec.DecodeAll(plainFrame, nil)
	require.NoError(t, err, "control: a no-dict frame must decode with a no-dict reader")
}

// TestZstdDictionary_IsEmbedded guards against an empty / missing embed.
func TestZstdDictionary_IsEmbedded(t *testing.T) {
	t.Parallel()
	require.Greater(t, len(zstdDictionary), 100_000,
		"the v1 zstd dictionary is ~113 KB; a tiny value means the embed broke")
}
