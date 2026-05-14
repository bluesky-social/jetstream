package segment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKindConstants(t *testing.T) {
	t.Parallel()

	// Pinning DESIGN.md §3.2 wire values. Changing any of these
	// silently corrupts every existing segment file.
	require.Equal(t, Kind(1), KindCreate)
	require.Equal(t, Kind(2), KindUpdate)
	require.Equal(t, Kind(3), KindDelete)
	require.Equal(t, Kind(4), KindIdentity)
	require.Equal(t, Kind(5), KindAccount)
	require.Equal(t, Kind(6), KindSync)
}

func TestEventZeroValueIsValid(t *testing.T) {
	t.Parallel()

	// A zero-value Event isn't *meaningful*, but constructing one
	// should never panic. This is a smoke test that the struct
	// definition didn't accidentally embed something with a
	// non-zero zero value.
	var ev Event
	require.Equal(t, Kind(0), ev.Kind)
	require.Empty(t, ev.DID)
	require.Nil(t, ev.Payload)
}

// TestDefaultMaxEventsPerBlockWireFormatAnchor pins the documented
// DESIGN.md §3.2 default (4096) as a literal. Changing this value
// is a wire-format-relevant change because the per-block DID
// bloom filters described in §3.1.3 are sized by it.
func TestDefaultMaxEventsPerBlockWireFormatAnchor(t *testing.T) {
	t.Parallel()
	require.Equal(t, 4096, DefaultMaxEventsPerBlock)
}

// TestReservedHeaderBytesAnchor pins the 256-byte fixed header
// reservation from DESIGN.md §3.1.2. Changing this would shift
// every block offset in every existing file on disk.
func TestReservedHeaderBytesAnchor(t *testing.T) {
	t.Parallel()
	require.Equal(t, 256, reservedHeaderBytes)
}

// TestSealedMagicAnchor pins the 4-byte sealed-segment magic
// "jss0" used at file offset 0. Changing this is a hard-fork of
// every reader that exists.
func TestSealedMagicAnchor(t *testing.T) {
	t.Parallel()
	require.Equal(t, []byte("jss0"), sealedMagic)
}
