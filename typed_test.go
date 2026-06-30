package jetstream

import (
	"context"
	"testing"

	"github.com/jcalabro/atmos/api/bsky"
	comatproto "github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// scriptEngine is a fake engine that yields a fixed sequence of batches (then
// ends), for exercising the public typed/iterator layer without a real server.
type scriptEngine struct {
	batches []*Batch
}

func (e *scriptEngine) run(_ context.Context, yield func(*Batch, error) bool) {
	for _, b := range e.batches {
		if !yield(b, nil) {
			return
		}
	}
}
func (e *scriptEngine) close() error { return nil }
func (e *scriptEngine) stats() Stats { return Stats{} }

func clientWithBatches(batches ...*Batch) *Client {
	return &Client{engine: &scriptEngine{batches: batches}}
}

// likeCBOR builds the raw DAG-CBOR for an app.bsky.feed.like record, as the
// engine would deliver in RecordCBOR under WithRawRecords.
func likeCBOR(t *testing.T, uri, cid string) []byte {
	t.Helper()
	rec := &bsky.FeedLike{
		CreatedAt: "2024-11-20T15:27:04.328Z",
		Subject:   comatproto.RepoStrongRef{URI: uri, CID: cid},
	}
	raw, err := rec.MarshalCBOR()
	require.NoError(t, err)
	return raw
}

func likeCommit(t *testing.T, seq uint64, rkey, uri, cid string) Event {
	t.Helper()
	return Event{
		DID: "did:plc:a", Seq: seq, Kind: KindCommit,
		Commit: &Commit{Operation: OpCreate, Collection: "app.bsky.feed.like", Rkey: rkey, RecordCBOR: likeCBOR(t, uri, cid)},
	}
}

// TestTypedEventsDecodesLikes is the happy path: create commits of the requested
// collection decode into *bsky.FeedLike with correct fields, in order.
func TestTypedEventsDecodesLikes(t *testing.T) {
	t.Parallel()
	b := &Batch{events: []Event{
		likeCommit(t, 1, "r1", "at://did:plc:x/app.bsky.feed.post/p1", "bafy1"),
		likeCommit(t, 2, "r2", "at://did:plc:y/app.bsky.feed.post/p2", "bafy2"),
	}}
	c := clientWithBatches(b)

	var got []*bsky.FeedLike
	for tb, err := range TypedEvents[bsky.FeedLike](context.Background(), c, "app.bsky.feed.like") {
		require.NoError(t, err)
		for _, te := range tb.Events() {
			require.NoError(t, te.DecodeErr)
			require.NotNil(t, te.Record)
			got = append(got, te.Record)
		}
	}
	require.Len(t, got, 2)
	require.Equal(t, "at://did:plc:x/app.bsky.feed.post/p1", got[0].Subject.URI)
	require.Equal(t, "at://did:plc:y/app.bsky.feed.post/p2", got[1].Subject.URI)
	// Distinct slab slots (no aliasing of one record across events).
	require.NotSame(t, got[0], got[1])
}

// TestTypedEventsPassesThroughNonDecodable verifies deletes, other collections,
// and non-commit events are delivered with a nil Record and nil DecodeErr —
// never dropped, never mis-decoded.
func TestTypedEventsPassesThroughNonDecodable(t *testing.T) {
	t.Parallel()
	b := &Batch{events: []Event{
		likeCommit(t, 1, "r1", "at://x/y/z", "bafy"),
		{DID: "did:plc:a", Seq: 2, Kind: KindCommit, Commit: &Commit{Operation: OpDelete, Collection: "app.bsky.feed.like", Rkey: "r1"}},                           // delete: no record
		{DID: "did:plc:a", Seq: 3, Kind: KindCommit, Commit: &Commit{Operation: OpCreate, Collection: "app.bsky.feed.post", Rkey: "p1", RecordCBOR: []byte{0xa0}}}, // other collection
		{DID: "did:plc:a", Seq: 4, Kind: KindIdentity, Identity: &Identity{DID: "did:plc:a", Handle: "h.test"}},                                                    // non-commit
	}}
	c := clientWithBatches(b)

	var decoded, passthrough int
	for tb, err := range TypedEvents[bsky.FeedLike](context.Background(), c, "app.bsky.feed.like") {
		require.NoError(t, err)
		for _, te := range tb.Events() {
			require.NoError(t, te.DecodeErr, "no passthrough event should produce a decode error")
			if te.Record != nil {
				decoded++
			} else {
				passthrough++
			}
		}
	}
	require.Equal(t, 1, decoded, "only the like create decodes")
	require.Equal(t, 3, passthrough, "delete, other-collection, and identity pass through with nil Record")
}

// TestTypedEventsSurfacesDecodeError verifies a corrupt record of the requested
// collection yields DecodeErr (never a zero-value record presented as success),
// while the surrounding good records still decode.
func TestTypedEventsSurfacesDecodeError(t *testing.T) {
	t.Parallel()
	bad := Event{DID: "did:plc:a", Seq: 2, Kind: KindCommit,
		Commit: &Commit{Operation: OpCreate, Collection: "app.bsky.feed.like", Rkey: "bad", RecordCBOR: []byte{0xff, 0x00, 0x13}}} // not valid CBOR map
	b := &Batch{events: []Event{
		likeCommit(t, 1, "r1", "at://x/y/z", "bafy"),
		bad,
		likeCommit(t, 3, "r3", "at://a/b/c", "bafy3"),
	}}
	c := clientWithBatches(b)

	var ok, errs int
	for tb, err := range TypedEvents[bsky.FeedLike](context.Background(), c, "app.bsky.feed.like") {
		require.NoError(t, err)
		for _, te := range tb.Events() {
			if te.DecodeErr != nil {
				errs++
				require.Nil(t, te.Record, "a decode error must not also present a record")
				require.Equal(t, uint64(2), te.Event.Seq)
			} else if te.Record != nil {
				ok++
			}
		}
	}
	require.Equal(t, 2, ok, "the two good likes decode")
	require.Equal(t, 1, errs, "the corrupt like surfaces exactly one decode error")
}

// TestTypedEventsRejectsEmptyCollection guards the mis-decode safety measure: an
// empty collection is rejected up front rather than decoding everything.
func TestTypedEventsRejectsEmptyCollection(t *testing.T) {
	t.Parallel()
	c := clientWithBatches(&Batch{events: []Event{likeCommit(t, 1, "r1", "at://x/y/z", "bafy")}})
	var sawErr error
	var batches int
	for tb, err := range TypedEvents[bsky.FeedLike](context.Background(), c, "") {
		if err != nil {
			sawErr = err
			continue
		}
		_ = tb
		batches++
	}
	require.Error(t, sawErr, "empty collection must be rejected")
	require.Zero(t, batches, "no batches delivered on the rejection path")
}

// TestTypedEventsForwardsStreamError verifies an underlying recoverable error is
// forwarded (nil batch) and iteration continues, preserving the Events contract.
func TestTypedEventsForwardsStreamError(t *testing.T) {
	t.Parallel()
	// scriptEngine yields only batches; use a dedicated engine that yields an error.
	c := &Client{engine: &errThenBatchEngine{
		err:   context.Canceled,
		batch: &Batch{events: []Event{likeCommit(t, 5, "r5", "at://x/y/z", "bafy")}},
	}}
	var sawErr error
	var decoded int
	for tb, err := range TypedEvents[bsky.FeedLike](context.Background(), c, "app.bsky.feed.like") {
		if err != nil {
			sawErr = err
			continue
		}
		for _, te := range tb.Events() {
			if te.Record != nil {
				decoded++
			}
		}
	}
	require.ErrorIs(t, sawErr, context.Canceled)
	require.Equal(t, 1, decoded, "iteration continues past a recoverable error")
}

type errThenBatchEngine struct {
	err   error
	batch *Batch
}

func (e *errThenBatchEngine) run(_ context.Context, yield func(*Batch, error) bool) {
	if !yield(nil, e.err) {
		return
	}
	yield(e.batch, nil)
}
func (e *errThenBatchEngine) close() error { return nil }
func (e *errThenBatchEngine) stats() Stats { return Stats{} }

// TestTypedBatchLastCursor mirrors Batch.LastCursor for the typed batch.
func TestTypedBatchLastCursor(t *testing.T) {
	t.Parallel()
	var empty TypedBatch[bsky.FeedLike]
	require.Zero(t, empty.LastCursor())
	tb := TypedBatch[bsky.FeedLike]{events: []TypedEvent[bsky.FeedLike]{
		{Event: Event{Seq: 3}}, {Event: Event{Seq: 9}}, {Event: Event{Seq: 5}},
	}}
	require.EqualValues(t, 9, tb.LastCursor())
}

// sanity: the constraint accepts the atmos generated type and round-trips.
func TestFeedLikeUnmarshalSanity(t *testing.T) {
	t.Parallel()
	raw := likeCBOR(t, "at://x/y/z", "bafy")
	var fl bsky.FeedLike
	require.NoError(t, fl.UnmarshalCBOR(raw))
	require.Equal(t, "at://x/y/z", fl.Subject.URI)
	// confirm the bytes are what cbor expects (no panic, valid map)
	_, err := cbor.UnmarshalNoCopy(raw)
	require.NoError(t, err)
}
