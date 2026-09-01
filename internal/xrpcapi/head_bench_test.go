package xrpcapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
)

func BenchmarkHeadGetSegment(b *testing.B) {
	benchHeadArchive(b, func(base string) string {
		return getSegURL(base, ingest.SegmentFilename(0))
	})
}

func BenchmarkHeadGetBlock(b *testing.B) {
	benchHeadArchive(b, func(base string) string {
		return blockURL(base, ingest.SegmentFilename(0), 0)
	})
}

func benchHeadArchive(b *testing.B, makeURL func(string) string) {
	b.Helper()
	dir := b.TempDir()
	writeSealedSegmentBlocks(b, dir, 0, 1, 4096, 1)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	if err != nil {
		b.Fatal(err)
	}
	s := New(Config{Src: m, Logger: slog.Default()})
	ts := httptest.NewServer(archiveMux(s))
	b.Cleanup(ts.Close)
	url := makeURL(ts.URL)
	client := ts.Client()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodHead, url, nil)
		if err != nil {
			b.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			b.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
