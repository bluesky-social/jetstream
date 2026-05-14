package segment

import (
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
