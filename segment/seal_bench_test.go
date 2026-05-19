package segment

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func benchEvents(n int) []Event {
	out := make([]Event, n)
	for i := range out {
		out[i] = Event{
			Seq:        uint64(i + 1),
			IndexedAt:  int64(i),
			Kind:       KindCreate,
			DID:        "did:plc:" + string(rune('a'+(i%26))),
			Collection: "app.bsky.feed.post",
			Rkey:       "k",
			Rev:        "rev",
			Payload:    make([]byte, 512),
		}
	}
	return out
}

func BenchmarkSeal(b *testing.B) {
	events := benchEvents(4096)

	for b.Loop() {
		dir := b.TempDir()
		path := filepath.Join(dir, "seg.jss")
		w, err := New(Config{Path: path, MaxEventsPerBlock: 4096})
		require.NoError(b, err)
		for _, ev := range events {
			full, err := w.Append(ev)
			if err != nil {
				b.Fatal(err)
			}
			if full {
				if err := w.Flush(); err != nil {
					b.Fatal(err)
				}
			}
		}
		if _, err := w.Seal(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderOpen(b *testing.B) {
	events := benchEvents(1024)
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 256})
	require.NoError(b, err)
	for _, ev := range events {
		full, err := w.Append(ev)
		if err != nil {
			b.Fatal(err)
		}
		if full {
			if err := w.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		r, err := Open(ReaderConfig{Path: path})
		if err != nil {
			b.Fatal(err)
		}
		_ = r.Close()
	}
}

func BenchmarkReaderOpenNoVerify(b *testing.B) {
	events := benchEvents(1024)
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 256})
	require.NoError(b, err)
	for _, ev := range events {
		full, err := w.Append(ev)
		if err != nil {
			b.Fatal(err)
		}
		if full {
			if err := w.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		r, err := Open(ReaderConfig{Path: path, SkipChecksum: true})
		if err != nil {
			b.Fatal(err)
		}
		_ = r.Close()
	}
}

func BenchmarkBlockBloom(b *testing.B) {
	events := benchEvents(1024)
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 64})
	require.NoError(b, err)
	for _, ev := range events {
		full, err := w.Append(ev)
		if err != nil {
			b.Fatal(err)
		}
		if full {
			if err := w.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(b, err)
	b.Cleanup(func() { _ = r.Close() })

	for i := 0; b.Loop(); i++ {
		_, err := r.BlockBloom(i % len(r.Blocks()))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBlockSealed(b *testing.B) {
	events := benchEvents(4096)
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 4096})
	require.NoError(b, err)
	for _, ev := range events {
		full, err := w.Append(ev)
		if err != nil {
			b.Fatal(err)
		}
		if full {
			if err := w.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(b, err)
	b.Cleanup(func() { _ = r.Close() })

	for b.Loop() {
		_, err := r.DecodeBlock(0)
		if err != nil {
			b.Fatal(err)
		}
	}
}
