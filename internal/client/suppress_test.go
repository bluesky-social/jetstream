package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/jetstream/internal/overlay"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/stretchr/testify/require"
)

// overlayServer serves a fixed getTombstones blob.
func overlayServer(t *testing.T, blob []byte) *xrpc.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/network.bsky.jetstream.getTombstones", r.URL.Path)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(blob)
	}))
	t.Cleanup(srv.Close)
	return &xrpc.Client{Host: srv.URL}
}

func recordTombstoneSnapshot(did, collection, rkey string, seq uint64) tombstone.Snapshot {
	snap := emptySnapshot()
	snap.Records[tombstone.RecordKey{DID: did, Collection: collection, Rkey: rkey}] = seq
	return snap
}

func TestSuppressorSeedFromOverlayRecordTombstone(t *testing.T) {
	t.Parallel()
	// Overlay covers (W=10, M=100]; a record deleted at seq 50.
	snap := recordTombstoneSnapshot("did:plc:a", "app.bsky.feed.post", "r1", 50)
	blob := overlay.Encode(snap, 10, 100)
	xc := overlayServer(t, blob)

	s := NewSuppressor()
	w, m, err := s.SeedFromOverlay(context.Background(), xc)
	require.NoError(t, err)
	require.EqualValues(t, 10, w)
	require.EqualValues(t, 100, m)

	// A create at seq 30 (< 50 tombstone) is suppressed.
	older := segment.Event{Seq: 30, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}
	drop, reason := s.ShouldDrop(&older)
	require.True(t, drop)
	require.Equal(t, "record", reason)

	// A create at the tombstone seq is NOT suppressed (strictly-less rule).
	atSeq := segment.Event{Seq: 50, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}
	drop, _ = s.ShouldDrop(&atSeq)
	require.False(t, drop)

	// A different record is untouched.
	other := segment.Event{Seq: 30, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r2"}
	drop, _ = s.ShouldDrop(&other)
	require.False(t, drop)
}

func TestSuppressorDIDTombstoneWindow(t *testing.T) {
	t.Parallel()
	// Account deleted at seq 100: suppresses that DID's materializations
	// below 100, but a repost at 200 (reactivation) is NOT masked.
	snap := emptySnapshot()
	snap.DIDs["did:plc:gone"] = tombstone.DIDTombstone{Seq: 100, Reason: "account"}
	blob := overlay.Encode(snap, 0, 250)
	xc := overlayServer(t, blob)

	s := NewSuppressor()
	_, _, err := s.SeedFromOverlay(context.Background(), xc)
	require.NoError(t, err)

	below := segment.Event{Seq: 40, Kind: segment.KindCreate, DID: "did:plc:gone", Collection: "c", Rkey: "r"}
	drop, reason := s.ShouldDrop(&below)
	require.True(t, drop)
	require.Equal(t, "account", reason)

	above := segment.Event{Seq: 200, Kind: segment.KindCreate, DID: "did:plc:gone", Collection: "c", Rkey: "r"}
	drop, _ = s.ShouldDrop(&above)
	require.False(t, drop, "no permanent-tombstone: seq >= tombstone is live")
}

func TestSuppressorNeverDropsNonMaterialization(t *testing.T) {
	t.Parallel()
	snap := recordTombstoneSnapshot("did:plc:a", "c", "r", 1000)
	blob := overlay.Encode(snap, 0, 1000)
	xc := overlayServer(t, blob)
	s := NewSuppressor()
	_, _, err := s.SeedFromOverlay(context.Background(), xc)
	require.NoError(t, err)

	// Delete / identity / account / sync rows are never suppressed even at
	// low seq — they are not materializations.
	del := segment.Event{Seq: 5, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"}
	drop, _ := s.ShouldDrop(&del)
	require.False(t, drop)
}

func TestSuppressorObserveLiveTombstone(t *testing.T) {
	t.Parallel()
	// Empty overlay; a delete arrives "live" and must then suppress an
	// earlier create for the same key (eager combined-set suppression).
	blob := overlay.Encode(emptySnapshot(), 0, 0)
	xc := overlayServer(t, blob)
	s := NewSuppressor()
	_, _, err := s.SeedFromOverlay(context.Background(), xc)
	require.NoError(t, err)

	create := segment.Event{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"}
	drop, _ := s.ShouldDrop(&create)
	require.False(t, drop, "no tombstone yet")

	liveDelete := segment.Event{Seq: 90, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"}
	require.NoError(t, s.ObserveLive(&liveDelete))

	drop, reason := s.ShouldDrop(&create)
	require.True(t, drop, "create below the live delete must now be suppressed")
	require.Equal(t, "record", reason)
}

func TestSuppressorMalformedBlob(t *testing.T) {
	t.Parallel()
	xc := overlayServer(t, []byte("not a jsto blob"))
	s := NewSuppressor()
	_, _, err := s.SeedFromOverlay(context.Background(), xc)
	require.Error(t, err)
}
