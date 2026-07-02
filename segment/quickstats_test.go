package segment_test

import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestQuickStats_Sealed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "sealed.jss")

	w, err := segment.New(segment.Config{
		Path:              path,
		MaxEventsPerBlock: 2,
	})
	require.NoError(t, err)
	for i := range 4 {
		full, err := w.Append(segment.Event{
			Seq:         uint64(i + 1),
			Kind:        segment.KindCreate,
			DID:         "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			WitnessedAt: 1700000000_000_000,
			Collection:  "app.bsky.feed.post",
			Payload:     []byte("hello"),
		})
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)

	got, err := segment.QuickStats(path)
	require.NoError(t, err)
	require.True(t, got.Sealed)
	require.Equal(t, path, got.Path)
	require.Greater(t, got.FileSize, int64(0))
	require.Greater(t, got.CompressedBytes, int64(0))
	require.GreaterOrEqual(t, got.UncompressedBytes, got.CompressedBytes)
}

func TestQuickStats_Active(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "active.jss")

	w, err := segment.New(segment.Config{
		Path:              path,
		MaxEventsPerBlock: 2,
	})
	require.NoError(t, err)
	for i := range 2 {
		_, err := w.Append(segment.Event{
			Seq:         uint64(i + 1),
			Kind:        segment.KindCreate,
			DID:         "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			WitnessedAt: 1700000000_000_000,
			Collection:  "app.bsky.feed.post",
			Payload:     []byte("hi"),
		})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	got, err := segment.QuickStats(path)
	require.NoError(t, err)
	require.False(t, got.Sealed)
	require.Equal(t, path, got.Path)
	require.Greater(t, got.FileSize, int64(0))
	require.Greater(t, got.CompressedBytes, int64(0))
	require.GreaterOrEqual(t, got.UncompressedBytes, got.CompressedBytes)
}
