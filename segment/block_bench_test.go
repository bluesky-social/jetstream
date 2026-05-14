package segment

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"testing"
)

// makeBenchEvents builds a deterministic block of events sized for
// the requested benchmark scenario. Using a fixed seed gives stable
// numbers across runs.
func makeBenchEvents(b *testing.B, n int, opts benchOpts) []Event {
	b.Helper()
	r := rand.New(rand.NewSource(42))
	events := make([]Event, n)
	for i := range events {
		ev := Event{
			Seq: uint64(i), IndexedAt: int64(i), Kind: KindCreate,
			DID: "did:plc:abcdefghijklmnopqrstuvwx",
		}
		switch {
		case opts.zeroPayload:
			ev.Payload = nil
		case opts.identicalPayload:
			ev.Payload = bytes.Repeat([]byte{0xAB}, 512)
		default:
			ev.Payload = randBytes(r, 512)
		}
		ev.Collection = "app.bsky.feed.post"
		ev.Rkey = "3l3qo2vuowo2b"
		ev.Rev = "3l3qo2vutsw2b"
		events[i] = ev
	}
	return events
}

type benchOpts struct {
	zeroPayload      bool
	identicalPayload bool
}

func BenchmarkEncodeBlock(b *testing.B) {
	cases := []struct {
		name string
		n    int
		opts benchOpts
	}{
		{"256_events_random", 256, benchOpts{}},
		{"4096_events_random", 4096, benchOpts{}},
		{"4096_events_zero_payload", 4096, benchOpts{zeroPayload: true}},
		{"4096_events_identical", 4096, benchOpts{identicalPayload: true}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			events := makeBenchEvents(b, tc.n, tc.opts)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				out, err := encodeBlockCompressed(events)
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(len(out)))
			}
		})
	}
}

func BenchmarkDecodeBlock(b *testing.B) {
	cases := []struct {
		name string
		n    int
		opts benchOpts
	}{
		{"256_events_random", 256, benchOpts{}},
		{"4096_events_random", 4096, benchOpts{}},
		{"4096_events_identical", 4096, benchOpts{identicalPayload: true}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			events := makeBenchEvents(b, tc.n, tc.opts)
			frame, err := encodeBlockCompressed(events)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(frame)))
			b.ResetTimer()
			for b.Loop() {
				if _, err := decodeBlockCompressed(frame); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAppend measures the per-event amortized cost of Append
// with a writer configured large enough that Flush never fires.
func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: b.N + 1})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	template := Event{
		Seq: 0, Kind: KindCreate,
		DID:        "did:plc:abcdefghijklmnopqrstuvwx",
		Collection: "app.bsky.feed.post",
		Rkey:       "3l3qo2vuowo2b",
		Rev:        "3l3qo2vutsw2b",
		Payload:    bytes.Repeat([]byte{0xAB}, 512),
	}

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		template.Seq = uint64(i)
		if _, err := w.Append(template); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFlushToTmpfs measures one full Append-batch + Flush
// cycle. On Linux CI runners t.TempDir() is tmpfs, so this measures
// CPU + zstd time, not real-disk fsync latency. Production fsync
// latency dominates and isn't what this benchmark is for.
func BenchmarkFlushToTmpfs(b *testing.B) {
	events := makeBenchEvents(b, 4096, benchOpts{})

	b.ReportAllocs()

	for b.Loop() {
		dir := b.TempDir()
		path := filepath.Join(dir, "seg.jss")
		w, err := New(Config{Path: path})
		if err != nil {
			b.Fatal(err)
		}

		for j := range events {
			if _, err := w.Append(events[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
