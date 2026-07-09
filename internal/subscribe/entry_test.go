package subscribe

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestEntry_EncodedMemoizesOnce(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	ev := &segment.Event{
		Seq: 7, WitnessedAt: 1000, Kind: segment.KindIdentity,
		DID: "did:plc:x", Payload: nil,
	}
	e := newEntry(ev)
	e.encodeFn = func(ev *segment.Event) ([]byte, error) {
		calls.Add(1)
		return []byte(`{"ok":true}`), nil
	}

	const N = 50
	var wg sync.WaitGroup
	results := make([][]byte, N)
	wg.Add(N)
	for i := range N {
		go func(i int) {
			defer wg.Done()
			body, err := e.Encoded()
			require.NoError(t, err)
			results[i] = body
		}(i)
	}
	wg.Wait()

	require.Equal(t, int64(1), calls.Load(), "encode must run exactly once")
	for i := range N {
		require.Equal(t, []byte(`{"ok":true}`), results[i])
	}
}

func TestEntry_MemoizesSkipSentinel(t *testing.T) {
	t.Parallel()
	ev := &segment.Event{Seq: 9, Kind: segment.KindSync, DID: "did:plc:s"}
	e := newEntry(ev)
	body, err := e.Encoded()
	require.ErrorIs(t, err, errSkipEvent)
	require.Nil(t, body)
	// Second call returns the same memoized sentinel.
	body2, err2 := e.Encoded()
	require.ErrorIs(t, err2, errSkipEvent)
	require.Nil(t, body2)
}

func TestEntry_MemoizesV1AndV2Independently(t *testing.T) {
	t.Parallel()
	var v1Calls atomic.Int64
	var v2Calls atomic.Int64
	e := newEntry(&segment.Event{Seq: 1, Kind: segment.KindDelete, DID: "did:plc:s"})
	e.encodeFn = func(*segment.Event) ([]byte, error) {
		v1Calls.Add(1)
		return []byte(`{"mode":"v1"}`), nil
	}
	e.encodeV2Fn = func(*segment.Event) ([]byte, error) {
		v2Calls.Add(1)
		return []byte(`{"mode":"v2"}`), nil
	}

	for range 3 {
		body, err := e.Encoded()
		require.NoError(t, err)
		require.Equal(t, []byte(`{"mode":"v1"}`), body)

		body, err = e.EncodedV2()
		require.NoError(t, err)
		require.Equal(t, []byte(`{"mode":"v2"}`), body)
	}

	require.Equal(t, int64(1), v1Calls.Load())
	require.Equal(t, int64(1), v2Calls.Load())
}

func TestEntry_CompressedMemoizesOnceAndDecodes(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	e := newEntry(&segment.Event{Seq: 3, Kind: segment.KindIdentity, DID: "did:plc:c"})
	e.encodeFn = func(*segment.Event) ([]byte, error) {
		calls.Add(1)
		return []byte(`{"did":"did:plc:c","kind":"identity"}`), nil
	}

	const N = 50
	var wg sync.WaitGroup
	results := make([][]byte, N)
	wg.Add(N)
	for i := range N {
		go func(i int) {
			defer wg.Done()
			body, err := e.Compressed()
			require.NoError(t, err)
			results[i] = body
		}(i)
	}
	wg.Wait()

	require.Equal(t, int64(1), calls.Load(), "underlying JSON encode must run exactly once")
	for i := range N {
		require.Equal(t, results[0], results[i], "all callers see the same memoized frame")
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer dec.Close()
	got, err := dec.DecodeAll(results[0], nil)
	require.NoError(t, err)
	require.Equal(t, []byte(`{"did":"did:plc:c","kind":"identity"}`), got)
}

// TestEntry_CompressedPropagatesSkipSentinel ensures the compressed path
// surfaces errSkipEvent unchanged (so the loop advances without sending),
// exactly like the JSON path.
func TestEntry_CompressedPropagatesSkipSentinel(t *testing.T) {
	t.Parallel()
	e := newEntry(&segment.Event{Seq: 9, Kind: segment.KindSync, DID: "did:plc:s"})
	body, err := e.Compressed()
	require.ErrorIs(t, err, errSkipEvent)
	require.Nil(t, body)
}

// TestEntry_CompressedV2_SyncEmitsDecodableFrame pins the divergence
// between the v1 and v2 wire shapes for KindSync events: the v1 path
// returns errSkipEvent (no frame emitted) while the v2 path emits a
// real frame. This catches any future mis-wiring of CompressedV2 to
// the v1 source.
func TestEntry_CompressedV2_SyncEmitsDecodableFrame(t *testing.T) {
	t.Parallel()

	sync := &comatproto.SyncSubscribeRepos_Sync{
		DID:    "did:plc:testsync",
		Rev:    "rev-sync-test",
		Seq:    555,
		Time:   "2026-05-25T00:00:00Z",
		Blocks: []byte{0x01, 0x02},
	}
	payload, err := sync.MarshalCBOR()
	require.NoError(t, err)

	e := newEntry(&segment.Event{
		Seq:         77,
		WitnessedAt: 1_700_000_000_000_000,
		Kind:        segment.KindSync,
		DID:         "did:plc:testsync",
		Rev:         "rev-sync-test",
		Payload:     payload,
	})

	// v1 path must skip #sync events.
	compressedBody, compressedErr := e.Compressed()
	require.ErrorIs(t, compressedErr, errSkipEvent, "v1 Compressed must return errSkipEvent for KindSync")
	require.Nil(t, compressedBody)

	// v2 path must emit a frame decodable with the V2 dictionary (and,
	// deliberately, NOT with the legacy v1 dictionary — the v2 endpoint's
	// compression contract is independent of v1's frozen scheme).
	v2Body, v2Err := e.CompressedV2()
	require.NoError(t, v2Err, "CompressedV2 must not return an error for KindSync")
	require.NotNil(t, v2Body, "CompressedV2 must return a non-nil frame for KindSync")

	v1Dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer v1Dec.Close()
	_, err = v1Dec.DecodeAll(v2Body, nil)
	require.Error(t, err, "v2 frames must NOT decode with the legacy v1 dictionary")

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionaryV2))
	require.NoError(t, err)
	defer dec.Close()
	decoded, err := dec.DecodeAll(v2Body, nil)
	require.NoError(t, err)

	require.Contains(t, string(decoded), `"kind":"sync"`, "decoded frame must contain kind:sync")
	require.Contains(t, string(decoded), "did:plc:testsync", "decoded frame must contain the DID")
}
