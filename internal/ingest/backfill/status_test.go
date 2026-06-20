package backfill

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRepoStatus_RoundTrip confirms the JSON encoder accepts and
// re-emits every field we populate, plus that Active is always
// present (even when false) so downstream code can rely on it.
func TestRepoStatus_RoundTrip(t *testing.T) {
	t.Parallel()
	in := &RepoStatus{
		Backfill: RepoBackfillStatus{
			Status: StatusComplete,
			Rev:    "rev-xyz",
		},
		Active: false,
	}
	b, err := encodeRepoStatus(in)
	require.NoError(t, err)
	require.Contains(t, string(b), `"active":false`)
	require.Contains(t, string(b), `"status":"complete"`)

	out, err := decodeRepoStatus(b)
	require.NoError(t, err)
	require.Equal(t, in.Backfill.Status, out.Backfill.Status)
	require.Equal(t, in.Backfill.Rev, out.Backfill.Rev)
	require.Equal(t, in.Active, out.Active)
}

// TestDecodeRepoStatus_Garbage verifies decode failures are surfaced
// (not silently zeroed) so the engine aborts a Run rather than
// resurrecting a corrupted row as a fresh discovery.
func TestDecodeRepoStatus_Garbage(t *testing.T) {
	t.Parallel()
	_, err := decodeRepoStatus([]byte("not json"))
	require.ErrorContains(t, err, "decode RepoStatus")
}
