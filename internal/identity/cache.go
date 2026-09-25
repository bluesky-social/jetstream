package identity

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/jcalabro/atmos/identity"
)

// keyPrefix is the key namespace this package owns.
const keyPrefix = "sync/identity/"

const DefaultTTL = 24 * time.Hour

// expiryHeaderLen is the inline 8-byte big-endian unix-nano expiry
// stamp prepended to every cached value. We embed expiry in the value
// rather than maintain a parallel index to keep the keyspace
// flat — TTL filtering happens on Get, and expired rows are
// overwritten by the next Set rather than swept by a background job.
//
// Expiry is stored as a uint64 unix-nano timestamp, decoded as int64
// for time.Unix arithmetic. This silently wraps around year 2262;
// in practice ttl is bounded (DefaultTTL = 24h) so the wrap is
// unreachable, but a hypothetical "pre-set far-future entry" caller
// would need to either bound the input or switch to a wider format.
const expiryHeaderLen = 8

// KV is the byte store under Cache. Every method is best effort: a lost write
// or a failed read costs one re-resolve, never correctness, so KV has no
// errors to report and implementations need not be durable.
type KV interface {
	// Get returns a value the caller may retain, or false when the key is
	// absent or unreadable.
	Get(ctx context.Context, key []byte) ([]byte, bool)
	Set(ctx context.Context, key, value []byte)
	Delete(ctx context.Context, key []byte)
}

// NewPebbleKV returns the local-mode KV: meta.pebble, written without WAL
// syncs so cache churn stays off the fsync path.
func NewPebbleKV(s *pebblestore.Store) KV {
	return pebbleKV{s: s}
}

type pebbleKV struct {
	s *pebblestore.Store
}

func (kv pebbleKV) Get(ctx context.Context, key []byte) ([]byte, bool) {
	// Any error, including an I/O failure, is a miss: the verifier
	// re-resolves and the next Set overwrites or refreshes.
	val, err := kv.s.Get(ctx, key)
	return val, err == nil
}

func (kv pebbleKV) Set(_ context.Context, key, value []byte) {
	_ = kv.s.SetNoSync(key, value)
}

func (kv pebbleKV) Delete(_ context.Context, key []byte) {
	_ = kv.s.DeleteNoSync(key)
}

// Cache implements identity.Cache over a KV. The stored value is
// [8B unix-nano expiry][JSON identity bytes]. Construction is cheap; a single
// instance per process is the expected pattern.
type Cache struct {
	kv  KV
	ttl time.Duration

	// now is overridable for tests. The field is exported indirectly:
	// tests in the same package set it directly.
	now func() time.Time
}

var _ identity.Cache = (*Cache)(nil)

// New constructs a Cache backed by kv with the given TTL.
// Use DefaultTTL unless you know you want something else.
func New(kv KV, ttl time.Duration) *Cache {
	return &Cache{
		kv:  kv,
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
func (c *Cache) Get(ctx context.Context, did string) (*identity.Identity, bool) {
	val, ok := c.kv.Get(ctx, cacheKey(did))
	if !ok || len(val) < expiryHeaderLen {
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

// Set writes the identity with TTL applied from now(). Best effort —
// cache writes are not on the verifier's durability critical path.
// A crash that loses one Set just costs a re-resolve on next boot,
// and the identity.Cache contract has no ordering guarantee.
func (c *Cache) Set(ctx context.Context, did string, ident *identity.Identity) {
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

	c.kv.Set(ctx, cacheKey(did), buf)
}

// Delete removes the cache entry. The identity package calls this
// when a DID resolution becomes invalid; expired-by-TTL covers the
// common case.
func (c *Cache) Delete(ctx context.Context, did string) {
	c.kv.Delete(ctx, cacheKey(did))
}
