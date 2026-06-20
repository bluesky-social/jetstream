package crashpoint

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRejectsUnknownPoint(t *testing.T) {
	t.Parallel()

	_, err := Parse("not-a-real-point")
	require.Error(t, err)
	require.ErrorContains(t, err, "unknown crashpoint")
}

func TestParseRejectsEmptyPoint(t *testing.T) {
	t.Parallel()

	_, err := Parse("")
	require.Error(t, err)
	require.ErrorContains(t, err, "empty crashpoint")
}

// TestAllPointsRoundTripAndAreKnown guards the AllPoints/knownPoints
// single source of truth: every declared point must be non-empty,
// unique, Known, and round-trip through Parse. A new Point constant
// that is not appended to AllPoints will not be Known — adding it here
// forces the author to keep the registry complete.
func TestAllPointsRoundTripAndAreKnown(t *testing.T) {
	t.Parallel()

	// Pin the count so adding a constant without updating AllPoints
	// (and this test) is a conscious, reviewed change.
	require.Len(t, AllPoints, 12)

	seen := make(map[Point]struct{}, len(AllPoints))
	for _, p := range AllPoints {
		require.NotEmpty(t, p.String(), "crashpoint has empty stable name")
		_, dup := seen[p]
		require.False(t, dup, "duplicate crashpoint %q in AllPoints", p)
		seen[p] = struct{}{}

		require.True(t, Known(p), "AllPoints member %q is not Known", p)
		got, err := Parse(p.String())
		require.NoError(t, err, "AllPoints member %q does not Parse", p)
		require.Equal(t, p, got)
	}
}
