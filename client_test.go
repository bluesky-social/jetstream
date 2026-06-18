package jetstream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeHost(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "bare host", in: "jetstream.us-west.bsky.network", want: "https://jetstream.us-west.bsky.network"},
		{name: "host:port", in: "localhost:8080", want: "https://localhost:8080"},
		{name: "http url", in: "http://localhost:8080", want: "http://localhost:8080"},
		{name: "https url", in: "https://host", want: "https://host"},
		{name: "ws to http", in: "ws://localhost:8080", want: "http://localhost:8080"},
		{name: "wss to https", in: "wss://host", want: "https://host"},
		{name: "strips path", in: "https://host/subscribe", want: "https://host"},
		{name: "trims space", in: "  host  ", want: "https://host"},
		{name: "empty", in: "", wantErr: true},
		{name: "blank", in: "   ", wantErr: true},
		{name: "bad scheme", in: "ftp://host", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeHost(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestOptionsApply(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	require.Equal(t, defaultBatchSize, cfg.batchSize)
	require.Equal(t, defaultDownloadConc, cfg.downloadConc)
	require.False(t, cfg.backfillRequested())

	for _, opt := range []Option{
		WithCollections([]string{"app.bsky.feed.post", "app.bsky.feed.*"}),
		WithDIDs([]string{"did:plc:abc"}),
		WithAfterSeq(10),
		WithBeforeSeq(100),
		WithBatchSize(256),
		WithDownloadConcurrency(4),
	} {
		opt(&cfg)
	}

	require.Equal(t, []string{"app.bsky.feed.post", "app.bsky.feed.*"}, cfg.collections)
	require.Equal(t, []string{"did:plc:abc"}, cfg.dids)
	require.True(t, cfg.hasAfterSeq)
	require.EqualValues(t, 10, cfg.afterSeq)
	require.True(t, cfg.hasBeforeSeq)
	require.EqualValues(t, 100, cfg.beforeSeq)
	require.Equal(t, 256, cfg.batchSize)
	require.Equal(t, 4, cfg.downloadConc)
	require.True(t, cfg.backfillRequested())
}

func TestOptionsRejectNonPositive(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	WithBatchSize(0)(&cfg)
	WithBatchSize(-5)(&cfg)
	WithDownloadConcurrency(0)(&cfg)
	require.Equal(t, defaultBatchSize, cfg.batchSize, "non-positive batch size must be ignored")
	require.Equal(t, defaultDownloadConc, cfg.downloadConc, "non-positive concurrency must be ignored")
}

func TestOptionsCopySlices(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	src := []string{"a", "b"}
	WithCollections(src)(&cfg)
	src[0] = "mutated"
	require.Equal(t, []string{"a", "b"}, cfg.collections, "options must defensively copy slices")
}

func TestSubscribeValidation(t *testing.T) {
	t.Parallel()

	_, err := Subscribe("")
	require.Error(t, err, "empty host must error")

	_, err = Subscribe("host", WithAfterSeq(100), WithBeforeSeq(100))
	require.Error(t, err, "beforeSeq must be strictly greater than afterSeq")

	_, err = Subscribe("host", WithAfterSeq(100), WithBeforeSeq(50))
	require.Error(t, err)

	c, err := Subscribe("host", WithAfterSeq(10), WithBeforeSeq(100))
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

func TestBatchLastCursor(t *testing.T) {
	t.Parallel()
	var empty Batch
	require.EqualValues(t, 0, empty.LastCursor())

	b := Batch{events: []Event{{Seq: 3}, {Seq: 7}, {Seq: 5}}}
	require.EqualValues(t, 7, b.LastCursor())
	require.Len(t, b.Events(), 3)
}

func TestClosedClientEventsErrors(t *testing.T) {
	t.Parallel()
	c, err := Subscribe("host")
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close(), "Close is idempotent")

	var gotErr error
	for _, err := range c.Events(context.Background()) {
		gotErr = err
		break
	}
	require.Error(t, gotErr, "Events on a closed client must yield an error")
}

// TestPlaceholderEngineReports asserts the skeleton wiring reaches the engine
// and surfaces its (currently not-implemented) status without panicking. This
// test is updated when the real engine lands.
func TestPlaceholderEngineReports(t *testing.T) {
	t.Parallel()
	c, err := Subscribe("host")
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	var gotErr error
	for _, err := range c.Events(context.Background()) {
		gotErr = err
		break
	}
	require.ErrorContains(t, gotErr, "not yet implemented")
}
