package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"

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
