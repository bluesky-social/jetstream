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

// BenchmarkEncodeColumns isolates the columnar encoder from zstd
// so we can see the per-byte rate of just the layout step. The
// compressed benchmarks below mix in zstd time, which on random
// payloads dominates wall-clock — but in production the writer
// reuses a scratch buffer and the columnar step is what we have
// the most control over.
func BenchmarkEncodeColumns(b *testing.B) {
	cases := []struct {
		name string
		n    int
		opts benchOpts
	}{
		{"256_events_random", 256, benchOpts{}},
		{"4096_events_random", 4096, benchOpts{}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			events := makeBenchEvents(b, tc.n, tc.opts)
			scratch := make([]byte, 0, 8<<20)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				scratch = encodeBlockInto(scratch[:0], eventColumns(events))
				b.SetBytes(int64(len(scratch)))
			}
		})
	}
}

// BenchmarkEncodeColumnsPending exercises the writer's hot path:
// the pendingBlock columns implementation backed by parallel slices.
// This is what production Append/Flush actually feeds the encoder.
func BenchmarkEncodeColumnsPending(b *testing.B) {
	cases := []struct {
		name string
		n    int
		opts benchOpts
	}{
		{"4096_events_random", 4096, benchOpts{}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			events := makeBenchEvents(b, tc.n, tc.opts)
			var p pendingBlock
			p.preallocate(tc.n)
			for _, ev := range events {
				p.seq = append(p.seq, ev.Seq)
				p.indexedAt = append(p.indexedAt, ev.IndexedAt)
				p.renderedAt = append(p.renderedAt, ev.RenderedAt)
				p.kind = append(p.kind, uint8(ev.Kind))
				p.collLen = append(p.collLen, uint8(len(ev.Collection)))
				p.didLen = append(p.didLen, uint16(len(ev.DID)))
				p.rkeyLen = append(p.rkeyLen, uint8(len(ev.Rkey)))
				p.revLen = append(p.revLen, uint8(len(ev.Rev)))
				p.eventLen = append(p.eventLen, uint32(len(ev.Payload)))
				p.collections = append(p.collections, ev.Collection...)
				p.dids = append(p.dids, ev.DID...)
				p.rkeys = append(p.rkeys, ev.Rkey...)
				p.revs = append(p.revs, ev.Rev...)
				p.payloads = append(p.payloads, ev.Payload...)
			}
			scratch := make([]byte, 0, 8<<20)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				scratch = encodeBlockInto(scratch[:0], &p)
				b.SetBytes(int64(len(scratch)))
			}
		})
	}
}

// BenchmarkDecodeColumns isolates the uncompressed-body decoder from
// zstd so we can see the cost of the parsing step alone.
func BenchmarkDecodeColumns(b *testing.B) {
	cases := []struct {
		name string
		n    int
		opts benchOpts
	}{
		{"256_events_random", 256, benchOpts{}},
		{"4096_events_random", 4096, benchOpts{}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			events := makeBenchEvents(b, tc.n, tc.opts)
			body, err := encodeBlock(events)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			for b.Loop() {
				if _, err := decodeBlock(body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
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

// BenchmarkAppend measures the per-event amortized cost of Append.
// We isolate Append cost from Flush by resetting the pending buffer
// in-place when it reaches cap, rather than calling Flush (which
// would mix in zstd + write + fsync time). This lets the benchmark
// reflect the steady-state Append hot path across arbitrary b.N
// without bumping into the decoder cap on MaxEventsPerBlock.
func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: DefaultMaxEventsPerBlock})
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
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if w.pending.count() >= w.cfg.MaxEventsPerBlock {
			w.pending.reset()
		}
		template.Seq = uint64(i)
		if _, err := w.Append(template); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSteadyFlush measures the production hot path: one
// already-open writer flushing block after block. This is what the
// firehose ingester actually does; BenchmarkFlushToTmpfs below
// includes file-open/teardown and gives a less useful picture of
// per-block cost.
func BenchmarkSteadyFlush(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	events := makeBenchEvents(b, DefaultMaxEventsPerBlock, benchOpts{})
	b.ReportAllocs()

	for b.Loop() {
		for j := range events {
			if _, err := w.Append(events[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
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
