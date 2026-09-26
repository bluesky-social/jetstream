package main

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestParsePhase(t *testing.T) {
	t.Parallel()
	p, err := parsePhase("bulk:3000:0:4000000:30000")
	require.NoError(t, err)
	require.Equal(t, phaseSpec{name: "bulk", rate: 3000, bulk: 4_000_000, bulkRate: 30_000}, p)

	p, err = parsePhase("load:3000:5m")
	require.NoError(t, err)
	require.Equal(t, phaseSpec{name: "load", rate: 3000, dur: 5 * time.Minute}, p)

	for _, bad := range []string{"x", "x:1", "x:1:0", "x:-1:1m", "x:1:1m:-2", "x:1:1m:1:-3", "x:1:1m:1:1:1"} {
		_, err := parsePhase(bad)
		require.Error(t, err, bad)
	}
}

// End-to-end latency counts live events only, and the archive does not keep
// the upstream cursor that tells them apart, so the generator's DIDs must.
func TestGeneratorLiveAndBulkAreDistinguishable(t *testing.T) {
	t.Parallel()
	gen := newGenerator(1, 40_000_000)
	now := time.Now()
	for i := range 100 {
		ev := gen.live(now, int64(i+1))
		require.NoError(t, segment.ValidateEvent(ev))
		require.True(t, isLive(&ev), ev.DID)
	}
	for _, ev := range gen.repo(now, 100) {
		require.NoError(t, segment.ValidateEvent(ev))
		require.False(t, isLive(&ev), ev.DID)
	}
}
