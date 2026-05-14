package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

func TestValidateAcceptsHappyPath(t *testing.T) {
	t.Parallel()

	ev := Event{
		Seq:        42,
		IndexedAt:  1_700_000_000_000_000,
		RenderedAt: 0,
		Kind:       KindCreate,
		DID:        "did:plc:abcdefghijklmnopqrstuvwx",
		Collection: "app.bsky.feed.post",
		Rkey:       "3l3qo2vuowo2b",
		Rev:        "3l3qo2vutsw2b",
		Payload:    []byte("any drisl bytes"),
	}
	require.NoError(t, validate(ev))
}

func TestValidateRejectsInvalidKind(t *testing.T) {
	t.Parallel()

	t.Run("zero", func(t *testing.T) {
		t.Parallel()
		err := validate(Event{Kind: 0})
		require.ErrorIs(t, err, ErrInvalidKind)
	})

	t.Run("seven", func(t *testing.T) {
		t.Parallel()
		err := validate(Event{Kind: 7})
		require.ErrorIs(t, err, ErrInvalidKind)
	})
}

func TestValidateRejectsOversizedFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mut  func(*Event)
	}{
		{
			name: "did over uint16",
			mut:  func(e *Event) { e.DID = strings.Repeat("a", math.MaxUint16+1) },
		},
		{
			name: "collection over uint8",
			mut:  func(e *Event) { e.Collection = strings.Repeat("a", math.MaxUint8+1) },
		},
		{
			name: "rkey over uint8",
			mut:  func(e *Event) { e.Rkey = strings.Repeat("a", math.MaxUint8+1) },
		},
		{
			name: "rev over uint8",
			mut:  func(e *Event) { e.Rev = strings.Repeat("a", math.MaxUint8+1) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ev := Event{Kind: KindCreate}
			tc.mut(&ev)
			err := validate(ev)
			require.True(t, errors.Is(err, ErrFieldTooLong),
				"expected ErrFieldTooLong, got %v", err)
		})
	}
}

func TestEncodeBlockUncompressedHandcrafted(t *testing.T) {
	t.Parallel()

	events := []Event{
		{
			Seq: 1, IndexedAt: 100, RenderedAt: 0, Kind: KindCreate,
			DID: "d1", Collection: "c1", Rkey: "r1", Rev: "v1",
			Payload: []byte{0xAA, 0xBB},
		},
		{
			Seq: 2, IndexedAt: 200, RenderedAt: 250, Kind: KindIdentity,
			DID: "d22", Collection: "", Rkey: "", Rev: "",
			Payload: nil,
		},
	}

	got, err := encodeBlock(events)
	require.NoError(t, err)

	// Build the expected bytes by hand to pin the layout.
	var want bytes.Buffer
	w := func(v any) {
		require.NoError(t, binary.Write(&want, binary.LittleEndian, v))
	}

	w(uint32(2)) // event_count

	// Fixed-size columns, in spec order:
	w(uint64(1))
	w(uint64(2)) // seq[]
	w(int64(100))
	w(int64(200)) // indexed_at[]
	w(int64(0))
	w(int64(250)) // rendered_at[]
	w(uint8(KindCreate))
	w(uint8(KindIdentity)) // kind[]
	w(uint8(2))
	w(uint8(0)) // collection_len[]
	w(uint16(2))
	w(uint16(3)) // did_len[]
	w(uint8(2))
	w(uint8(0)) // rkey_len[]
	w(uint8(2))
	w(uint8(0)) // rev_len[]
	w(uint32(2))
	w(uint32(0)) // event_len[]

	// Variable-length blobs, in spec order:
	want.WriteString("c1")         // collections
	want.WriteString("d1d22")      // dids
	want.WriteString("r1")         // rkeys
	want.WriteString("v1")         // revs
	want.Write([]byte{0xAA, 0xBB}) // payloads

	require.Equal(t, want.Bytes(), got)
}

func TestEncodeBlockEmptyReturnsError(t *testing.T) {
	t.Parallel()

	// Zero events is not a meaningful block; the writer's Flush is
	// the no-op layer. encodeBlock itself rejects empty input so a
	// caller can never accidentally write a zero-event block.
	_, err := encodeBlock(nil)
	require.Error(t, err)
}

func TestDecodeBlockRoundtripHandcrafted(t *testing.T) {
	t.Parallel()

	events := []Event{
		{
			Seq: 1, IndexedAt: 100, RenderedAt: 0, Kind: KindCreate,
			DID: "d1", Collection: "c1", Rkey: "r1", Rev: "v1",
			Payload: []byte{0xAA, 0xBB},
		},
		{
			Seq: 2, IndexedAt: 200, RenderedAt: 250, Kind: KindIdentity,
			DID: "d22", Collection: "", Rkey: "", Rev: "",
			Payload: nil,
		},
	}

	encoded, err := encodeBlock(events)
	require.NoError(t, err)

	decoded, err := decodeBlock(encoded)
	require.NoError(t, err)

	// The roundtrip must be deep-equal, including Payload == nil
	// (not []byte{}) for the zero-length case.
	require.Equal(t, events, decoded)
}

func TestDecodeBlockTruncatedReturnsError(t *testing.T) {
	t.Parallel()

	events := []Event{{Seq: 1, Kind: KindCreate, DID: "d", Payload: []byte("x")}}
	encoded, err := encodeBlock(events)
	require.NoError(t, err)

	for cut := 0; cut < len(encoded); cut++ {
		// Every prefix shorter than the full block must produce an
		// error, never a panic and never a wrong-but-non-erroring decode.
		_, err := decodeBlock(encoded[:cut])
		require.Error(t, err, "expected error at cut=%d", cut)
	}
}

func TestDecodeBlockBoundedAllocation(t *testing.T) {
	t.Parallel()

	// A header claiming 1 billion events must not provoke a giant
	// allocation; the decoder must validate against input length.
	hostile := make([]byte, 4)
	binary.LittleEndian.PutUint32(hostile, 1_000_000_000)
	_, err := decodeBlock(hostile)
	require.Error(t, err)
}

// TestDecodeBlockRejectsTrailingBytes pins the trailing-bytes
// check in decodeBlock. Without it, encoded-then-padded buffers
// would round-trip as if they were valid, which would mask
// upstream bugs and risk ambiguous-trailer attacks against
// future Reader code.
func TestDecodeBlockRejectsTrailingBytes(t *testing.T) {
	t.Parallel()

	encoded, err := encodeBlock([]Event{
		{Seq: 1, Kind: KindCreate, DID: "d", Payload: []byte("p")},
	})
	require.NoError(t, err)

	padded := append([]byte{}, encoded...)
	padded = append(padded, 0x00)
	_, err = decodeBlock(padded)
	require.ErrorIs(t, err, errTruncatedBlock)
}

// TestDecodeBlockRejectsOutOfRangeKind verifies the per-row Kind
// guard in the decoder (block.go:243-ish) rejects kinds that the
// validator catches on the encode side. A round-trip cannot
// produce one, but a hostile or corrupt buffer can; the decoder
// must not happily emit Kind(0) or Kind(7).
func TestDecodeBlockRejectsOutOfRangeKind(t *testing.T) {
	t.Parallel()

	encoded, err := encodeBlock([]Event{
		{Seq: 1, Kind: KindCreate, DID: "d", Payload: []byte("p")},
	})
	require.NoError(t, err)

	// Locate and corrupt the kind[] byte. encodeBlock writes:
	//   event_count u32 (4) + seq u64 (8) + indexed_at i64 (8) +
	//   rendered_at i64 (8) + kind u8 (1) ...
	// So kind[0] is at offset 4 + 8 + 8 + 8 = 28.
	const kindOffset = 4 + 8 + 8 + 8
	corrupted := append([]byte{}, encoded...)
	corrupted[kindOffset] = 0
	_, err = decodeBlock(corrupted)
	require.ErrorIs(t, err, errTruncatedBlock)

	corrupted[kindOffset] = 7
	_, err = decodeBlock(corrupted)
	require.ErrorIs(t, err, errTruncatedBlock)
}

// TestDecodeBlockRejectsAbsurdEventCount targets the
// maxBlockEventsLimit guard. A header claiming more events than
// the cap (and which would also overflow int on 32-bit builds)
// must be rejected before we attempt allocation.
func TestDecodeBlockRejectsAbsurdEventCount(t *testing.T) {
	t.Parallel()

	hostile := make([]byte, 4)
	binary.LittleEndian.PutUint32(hostile, uint32(maxBlockEventsLimit+1))
	_, err := decodeBlock(hostile)
	require.ErrorIs(t, err, errTruncatedBlock)
}

func TestEncodeBlockCompressedRoundtrip(t *testing.T) {
	t.Parallel()

	events := []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b", Payload: []byte("x")},
	}

	frame, err := encodeBlockCompressed(events)
	require.NoError(t, err)
	require.NotEmpty(t, frame)

	// The frame is a real zstd frame, not the raw body.
	require.NotEqual(t, mustEncode(t, events), frame)

	got, err := decodeBlockCompressed(frame)
	require.NoError(t, err)
	require.Equal(t, events, got)
}

func TestDecodeBlockCompressedRejectsGarbage(t *testing.T) {
	t.Parallel()

	_, err := decodeBlockCompressed([]byte("not a zstd frame"))
	require.Error(t, err)
}

// mustEncode is a test helper; it lives here because it has no
// non-test consumers.
func mustEncode(t *testing.T, events []Event) []byte {
	t.Helper()
	out, err := encodeBlock(events)
	require.NoError(t, err)
	return out
}

// genEvent produces one Event with realistic length distributions.
// Uses *rand.Rand so the generator is deterministic given a seed.
func genEvent(r *rand.Rand) Event {
	// Length distributions chosen to mostly match production atproto
	// shapes while still occasionally exercising the upper bounds.
	didLen := pickLen(r, 32, math.MaxUint16, 0.001)
	collLen := pickLen(r, 24, math.MaxUint8, 0.005)
	rkeyLen := pickLen(r, 13, math.MaxUint8, 0.005)
	revLen := pickLen(r, 13, math.MaxUint8, 0.005)
	payloadLen := pickPayloadLen(r)

	return Event{
		Seq:        r.Uint64(),
		IndexedAt:  int64(r.Uint64()),
		RenderedAt: int64(r.Uint64()),
		Kind:       Kind(1 + r.Intn(6)),
		DID:        randString(r, didLen),
		Collection: randString(r, collLen),
		Rkey:       randString(r, rkeyLen),
		Rev:        randString(r, revLen),
		Payload:    randBytes(r, payloadLen),
	}
}

// pickLen picks a length centered at typical with rare excursions to max.
func pickLen(r *rand.Rand, typical, max int, rareProb float64) int {
	if r.Float64() < rareProb {
		return max
	}
	// Long-tailed but bounded around typical.
	n := typical + r.Intn(typical/2+1) - typical/4
	if n < 0 {
		n = 0
	}
	if n > max {
		n = max
	}
	return n
}

// pickPayloadLen yields nil ~10% of the time (non-commit kinds),
// most-common around ~500 B, with a long tail up to ~16 KB.
func pickPayloadLen(r *rand.Rand) int {
	if r.Float64() < 0.10 {
		return 0
	}
	// Geometric-ish: most around 500, occasional much larger.
	n := int(r.NormFloat64()*250 + 500)
	if n < 0 {
		n = 0
	}
	if n > 16*1024 {
		n = 16 * 1024
	}
	return n
}

func randString(r *rand.Rand, n int) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		// Printable-ish ASCII; the encoder doesn't care about content,
		// but readable bytes make test failures easier to debug.
		b[i] = byte(0x20 + r.Intn(0x5F))
	}
	return string(b)
}

func randBytes(r *rand.Rand, n int) []byte {
	if n == 0 {
		return nil
	}
	b := make([]byte, n)
	r.Read(b)
	return b
}

func TestEncodeBlockRoundtripProperty(t *testing.T) {
	t.Parallel()

	cfg := &quick.Config{MaxCount: 200}

	prop := func(seed int64, n uint16) bool {
		// Map n into [1, 256] for fast tests. The swarm test covers
		// up to 4096; this property is about correctness, not size.
		size := 1 + int(n%256)
		r := rand.New(rand.NewSource(seed))
		events := make([]Event, size)
		for i := range events {
			events[i] = genEvent(r)
		}

		encoded, err := encodeBlockCompressed(events)
		if err != nil {
			t.Logf("encode failed: %v", err)
			return false
		}
		decoded, err := decodeBlockCompressed(encoded)
		if err != nil {
			t.Logf("decode failed: %v", err)
			return false
		}
		if len(decoded) != len(events) {
			return false
		}
		for i := range events {
			if !eventsEqual(events[i], decoded[i]) {
				t.Logf("mismatch at %d: got %+v want %+v", i, decoded[i], events[i])
				return false
			}
		}
		return true
	}

	if err := quick.Check(prop, cfg); err != nil {
		t.Fatal(err)
	}
}

// eventsEqual is reflect.DeepEqual with the one wrinkle that
// nil-vs-empty Payload should be treated as equal during property
// testing — though our decoder produces nil for zero-length, we
// guard against test-helper drift.
func eventsEqual(a, b Event) bool {
	if a.Seq != b.Seq || a.IndexedAt != b.IndexedAt ||
		a.RenderedAt != b.RenderedAt || a.Kind != b.Kind ||
		a.DID != b.DID || a.Collection != b.Collection ||
		a.Rkey != b.Rkey || a.Rev != b.Rev {
		return false
	}
	if len(a.Payload) != len(b.Payload) {
		return false
	}
	for i := range a.Payload {
		if a.Payload[i] != b.Payload[i] {
			return false
		}
	}
	return true
}
