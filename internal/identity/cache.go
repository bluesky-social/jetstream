package identity

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos/identity"
)

// keyPrefix is the pebble key namespace this package owns.
const keyPrefix = "sync/identity/"

const DefaultTTL = 24 * time.Hour

// expiryHeaderLen is the inline 8-byte big-endian unix-nano expiry
// stamp prepended to every cached value. We embed expiry in the value
// rather than maintain a parallel index to keep the pebble keyspace
// flat — TTL filtering happens on Get, and expired rows are
// overwritten by the next Set rather than swept by a background job.
//
// Expiry is stored as a uint64 unix-nano timestamp, decoded as int64
// for time.Unix arithmetic. This silently wraps around year 2262;
// in practice ttl is bounded (DefaultTTL = 6h) so the wrap is
// unreachable, but a hypothetical "pre-set far-future entry" caller
// would need to either bound the input or switch to a wider format.
const expiryHeaderLen = 8

// PebbleCache implements identity.Cache against a *store.Store. The
// stored value is [8B unix-nano expiry][JSON identity bytes]. Construction
// is cheap; a single instance per process is the expected pattern.
type PebbleCache struct {
	s   *store.Store
	ttl time.Duration

	// now is overridable for tests. The field is exported indirectly:
	// tests in the same package set it directly.
	now func() time.Time
}

// New constructs a PebbleCache backed by s with the given TTL.
// Use DefaultTTL unless you know you want something else.
func New(s *store.Store, ttl time.Duration) *PebbleCache {
	return &PebbleCache{
		s:   s,
		ttl: ttl,
		now: time.Now,
	}
}

func cacheKey(did string) []byte {
	return []byte(keyPrefix + did)
}

// Get returns the cached identity. Returns (nil, false) for absent,
// expired, or undecodable entries. Decode failure is silently swept
// — the verifier will re-resolve and Set will overwrite the bad
// row.
func (c *PebbleCache) Get(_ context.Context, did string) (*identity.Identity, bool) {
	val, closer, err := c.s.Get(cacheKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false
	}
	if err != nil {
		// Pebble I/O failure is treated as miss; the verifier will
		// re-resolve, and the next Set will overwrite or refresh.
		return nil, false
	}
	defer func() { _ = closer.Close() }()

	if len(val) < expiryHeaderLen {
		return nil, false
	}
	expiryNano := int64(binary.BigEndian.Uint64(val[:expiryHeaderLen]))
	expiry := time.Unix(0, expiryNano)
	if !c.now().Before(expiry) {
		return nil, false
	}

	var ident identity.Identity
	if err := json.Unmarshal(val[expiryHeaderLen:], &ident); err != nil {
		return nil, false
	}
	return &ident, true
}

// Set writes the identity with TTL applied from now(). No fsync —
// cache writes are not on the verifier's durability critical path.
// A crash that loses one Set just costs a re-resolve on next boot,
// and the identity.Cache contract has no ordering guarantee.
func (c *PebbleCache) Set(_ context.Context, did string, ident *identity.Identity) {
	body, err := json.Marshal(ident)
	if err != nil {
		// identity.Identity has no fields that can fail JSON
		// marshalling; a non-nil error here would surface a bug
		// in atmos's type shape, not a runtime condition. Drop
		// silently — the verifier will re-resolve next time.
		return
	}
	expiry := c.now().Add(c.ttl).UnixNano()

	buf := make([]byte, 0, expiryHeaderLen+len(body))
	var hdr [expiryHeaderLen]byte
	binary.BigEndian.PutUint64(hdr[:], uint64(expiry))
	buf = append(buf, hdr[:]...)
	buf = append(buf, body...)

	// Best-effort: ignore pebble errors. Same recovery posture as
	// JSON marshal failure — the next resolve overwrites.
	_ = c.s.Set(cacheKey(did), buf, pebble.NoSync)
}

// Delete removes the cache entry. The identity package calls this
// when a DID resolution becomes invalid; expired-by-TTL covers the
// common case.
func (c *PebbleCache) Delete(_ context.Context, did string) {
	// Best-effort: ignore pebble errors.
	_ = c.s.Delete(cacheKey(did), pebble.NoSync)
}
