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
