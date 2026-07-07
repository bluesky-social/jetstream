package segment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEvent_DisplayTimeUS pins the sentinel-0 display resolver (docs/README.md
// §3.2): the wire time_us is the imported IndexedAt when one was set
// (non-zero), otherwise it falls back to WitnessedAt. Absent any import
// every IndexedAt is 0, so display == witnessed for every event.
func TestEvent_DisplayTimeUS(t *testing.T) {
	t.Parallel()

	const witnessed = int64(1_700_000_000_000_000)
	const imported = int64(1_600_000_000_000_000)

	tests := []struct {
		name      string
		witnessed int64
		indexed   int64
		want      int64
	}{
		{
			name:      "unimported falls back to witnessed",
			witnessed: witnessed,
			indexed:   0,
			want:      witnessed,
		},
		{
			name:      "imported display value wins",
			witnessed: witnessed,
			indexed:   imported,
			want:      imported,
		},
		{
			// A future-dated import still wins; the resolver is a pure
			// sentinel check, not a min/max — the operator is trusted.
			name:      "imported wins even when newer than witnessed",
			witnessed: witnessed,
			indexed:   witnessed + 1,
			want:      witnessed + 1,
		},
		{
			// Both zero is the degenerate never-witnessed case; it
			// resolves to 0 rather than doing anything surprising.
			name:      "both zero resolves to zero",
			witnessed: 0,
			indexed:   0,
			want:      0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := &Event{WitnessedAt: tc.witnessed, IndexedAt: tc.indexed}
			require.Equal(t, tc.want, e.DisplayTimeUS())
		})
	}
}
