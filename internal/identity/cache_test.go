package identity

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sampleIdentity() *identity.Identity {
	return &identity.Identity{
		DID:    atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"),
		Handle: atmos.Handle("alice.test"),
		Keys: map[string]identity.Key{
			"atproto": {
				Type:      "Multikey",
				Multibase: "zQ3shQo7n7VdGV9XEvjyXEFy3sCvi5R8VC2sXkqMfV3oRUDoY",
			},
		},
		Services: map[string]identity.ServiceEndpoint{
			"atproto_pds": {Type: "AtprotoPersonalDataServer", URL: "https://pds.example.org"},
		},
	}
}

func TestCache_GetAbsentReturnsFalse(t *testing.T) {
	t.Parallel()
	c := New(newTestStore(t), DefaultTTL)

	got, ok := c.Get(t.Context(), "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz")
	require.False(t, ok)
	require.Nil(t, got)
}

func TestCache_RoundTrip(t *testing.T) {
	t.Parallel()
	c := New(newTestStore(t), DefaultTTL)
	in := sampleIdentity()

	c.Set(t.Context(), string(in.DID), in)
	got, ok := c.Get(t.Context(), string(in.DID))
	require.True(t, ok)
	require.Equal(t, in.DID, got.DID)
	require.Equal(t, in.Handle, got.Handle)
	require.Equal(t, in.Keys, got.Keys)
	require.Equal(t, in.Services, got.Services)
}

func TestCache_Delete(t *testing.T) {
	t.Parallel()
	c := New(newTestStore(t), DefaultTTL)
	in := sampleIdentity()

	c.Set(t.Context(), string(in.DID), in)
	c.Delete(t.Context(), string(in.DID))

	_, ok := c.Get(t.Context(), string(in.DID))
	require.False(t, ok)
}

// TestCache_ExpiryTreatedAsMiss pins that an entry past its TTL is
// invisible to Get. The implementation uses a now() func we override
// directly via the field.
func TestCache_ExpiryTreatedAsMiss(t *testing.T) {
	t.Parallel()
	c := New(newTestStore(t), 1*time.Hour)

	nowAt := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return nowAt }

	c.Set(t.Context(), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", sampleIdentity())

	// Move clock forward past TTL.
	c.now = func() time.Time { return nowAt.Add(2 * time.Hour) }

	_, ok := c.Get(t.Context(), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	require.False(t, ok, "expired entry must be treated as miss")
}

// TestCache_TruncatedEntryTreatedAsMiss pins that a stored value
// shorter than the 8-byte expiry header is treated as a miss
// rather than panicking on the slice indexing.
func TestCache_TruncatedEntryTreatedAsMiss(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	c := New(s, DefaultTTL)

	require.NoError(t, s.Set([]byte(keyPrefix+"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"), []byte{0xFE, 0xED}, store.SyncWrites))

	_, ok := c.Get(t.Context(), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	require.False(t, ok)
}

// TestCache_UndecodableJSONTreatedAsMiss pins the recovery posture
// for a corrupted cache entry whose expiry header is intact but
// whose body is not valid JSON. The verifier must NOT see a decode
// error — Get returns (nil, false) and the next Set overwrites.
func TestCache_UndecodableJSONTreatedAsMiss(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	c := New(s, DefaultTTL)

	// Build [8B future expiry][non-JSON garbage] under the cache key
	// directly. The expiry must be in the future so the TTL check
	// doesn't preempt the decode branch.
	var hdr [8]byte
	expiry := time.Now().Add(1 * time.Hour).UnixNano()
	binary.BigEndian.PutUint64(hdr[:], uint64(expiry))

	body := []byte("not json at all, definitely not}{")
	val := append(hdr[:], body...)

	require.NoError(t, s.Set([]byte(keyPrefix+"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"), val, store.SyncWrites))

	got, ok := c.Get(t.Context(), "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
	require.False(t, ok, "undecodable JSON must surface as a cache miss")
	require.Nil(t, got)
}

// TestCache_ImplementsInterface is a compile-time check.
func TestCache_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ identity.Cache = (*PebbleCache)(nil)
}
