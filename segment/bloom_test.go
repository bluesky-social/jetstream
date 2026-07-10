package segment

import (
	"errors"
	"testing"

	"github.com/jcalabro/gloom"
	"github.com/stretchr/testify/require"
)

// fixedSizeBlooms returns n filters all built with the same
// parameters; their MarshalBinary outputs must be identical-length.
func fixedSizeBlooms(t *testing.T, n int) []*gloom.Filter {
	t.Helper()
	out := make([]*gloom.Filter, n)
	for i := range out {
		out[i] = gloom.New(64, perBlockBloomFPRate)
	}
	return out
}

func TestEncodeBlockBloomsRegionRoundtrip(t *testing.T) {
	t.Parallel()

	filters := fixedSizeBlooms(t, 4)
	for i, f := range filters {
		// Distinct content so we can verify per-index decode.
		f.AddString("did:plc:" + string(rune('a'+i)))
	}

	region, sizeBytes, err := encodeBlockBloomsRegion(filters)
	require.NoError(t, err)
	require.Greater(t, sizeBytes, uint32(0))

	for i, want := range filters {
		got, err := decodeBlockBloomFromRegion(region, sizeBytes, i)
		require.NoError(t, err)
		require.Equal(t, want.Test([]byte("did:plc:"+string(rune('a'+i)))),
			got.Test([]byte("did:plc:"+string(rune('a'+i)))))
	}
}

func TestEncodeBlockBloomsRegionEmpty(t *testing.T) {
	t.Parallel()

	region, sizeBytes, err := encodeBlockBloomsRegion(nil)
	require.NoError(t, err)
	require.Zero(t, sizeBytes)
	// 8-byte header only: block_count=0, bloom_size_bytes=0.
	require.Len(t, region, 8)
}

func TestEncodeBlockBloomsRegionRejectsMixedSizes(t *testing.T) {
	t.Parallel()

	mixed := []*gloom.Filter{
		gloom.New(64, 0.01),
		gloom.New(64_000, 0.01), // far larger; different MarshalBinary length
	}
	_, _, err := encodeBlockBloomsRegion(mixed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "size mismatch")
}

func TestDecodeBlockBloomFromRegionRejectsOutOfRange(t *testing.T) {
	t.Parallel()

	filters := fixedSizeBlooms(t, 2)
	region, sizeBytes, err := encodeBlockBloomsRegion(filters)
	require.NoError(t, err)

	_, err = decodeBlockBloomFromRegion(region, sizeBytes, 2)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
}

func TestDecodeBlockBloomsRegionHeaderRejectsShort(t *testing.T) {
	t.Parallel()

	_, _, err := decodeBlockBloomsRegionHeader([]byte{0x01, 0x02})
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

func TestDecodeBlockBloomsRegionHeaderRoundtrip(t *testing.T) {
	t.Parallel()

	filters := fixedSizeBlooms(t, 3)
	region, sizeBytes, err := encodeBlockBloomsRegion(filters)
	require.NoError(t, err)

	count, sz, err := decodeBlockBloomsRegionHeader(region[:blockBloomsRegionHeaderSize])
	require.NoError(t, err)
	require.EqualValues(t, len(filters), count)
	require.Equal(t, sizeBytes, sz)
}
